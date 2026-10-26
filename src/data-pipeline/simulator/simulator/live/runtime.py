import asyncio
import heapq
import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4

import httpx
from confluent_kafka import Producer

from simulator.contracts.payments.v1.labels_pb2 import (
    LABEL_SOURCE_CHARGEBACK,
    LABEL_SOURCE_SIMULATION_TRUTH,
    FraudLabel,
)
from simulator.identity import entity_id
from simulator.live.faults import run_faults
from simulator.live.model import card_number
from simulator.live.source import load_model
from simulator.profiles import SCENARIO_NAMES


def report(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


class Gateway:
    def __init__(self, config, client, api_key):
        self.config, self.client, self.api_key = config, client, api_key

    async def request(self, method, path, key, body=None, bearer=None):
        headers = {"Authorization": f"Bearer {bearer or self.api_key}", "Idempotency-Key": key}
        for attempt in range(self.config.request_retries + 1):
            try:
                response = await self.client.request(method, path, headers=headers, json=body)
                if response.status_code not in (429, 502, 503, 504):
                    if not response.is_success:
                        raise RuntimeError(f"gateway request failed with status {response.status_code}")
                    return response.json()
            except httpx.TransportError:
                pass
            if attempt < self.config.request_retries:
                await asyncio.sleep(min(0.25 * 2**attempt, 5))
        raise RuntimeError("gateway retry budget exhausted")


class Labels:
    def __init__(self, config, gateway, integration_id, producer):
        self.config, self.gateway, self.integration_id, self.producer = config, gateway, integration_id, producer
        self.publish_lock = asyncio.Lock()
        self.heap = []
        self.sequence = 0
        self.truth_published = self.chargebacks_published = self.unfinished_chargebacks = 0

    async def publish(self, payment_id, attempt_id, occurred_at, fraud, source, scenario=None):
        async with self.publish_lock:
            label = FraudLabel(
                integration_id=self.integration_id,
                payment_id=payment_id,
                attempt_id=attempt_id,
                is_fraud=fraud,
                source=source,
            )
            label.occurred_at.FromDatetime(occurred_at)
            if scenario is not None:
                label.scenario = scenario

            def send():
                label.label_available_at.FromDatetime(datetime.now(timezone.utc))
                body = label.SerializeToString()
                failures = []
                self.producer.produce(
                    self.config.label_topic,
                    key=payment_id.encode(),
                    value=body,
                    on_delivery=lambda err, msg: failures.append(err) if err else None,
                )
                if self.producer.flush(30) or failures:
                    raise RuntimeError("label publication failed")

            await asyncio.to_thread(send)
            if source == LABEL_SOURCE_SIMULATION_TRUTH:
                self.truth_published += 1
            else:
                self.chargebacks_published += 1

    async def schedule(self, payment_id, attempt_id, occurred_at, fraud, friendly, delay):
        if not (fraud or friendly):
            return
        if len(self.heap) >= self.config.max_pending_labels:
            raise RuntimeError("chargeback schedule capacity exhausted")
        self.sequence += 1
        heapq.heappush(
            self.heap,
            (
                time.monotonic() + delay / self.config.label_time_scale,
                self.sequence,
                payment_id,
                attempt_id,
                occurred_at,
            ),
        )

    async def run(self, stop):
        while not stop.is_set():
            while self.heap and self.heap[0][0] <= time.monotonic():
                _, _, payment_id, attempt_id, occurred_at = self.heap[0]
                payment = await self.gateway.request("GET", f"/v1/payments/{payment_id}", "label-status")
                status = next((a["status"] for a in payment["attempts"] if a["attempt_id"] == attempt_id), None)
                if status == "SUCCEEDED":
                    await self.publish(payment_id, attempt_id, occurred_at, True, LABEL_SOURCE_CHARGEBACK)
                else:
                    self.unfinished_chargebacks += int(status in (None, "PENDING"))
                    report("chargeback_not_succeeded", count=1)
                heapq.heappop(self.heap)
            await asyncio.sleep(0.1)


async def traffic(config, model, gateway, labels, stop):
    run_id = uuid4().hex
    slots = asyncio.Semaphore(config.concurrency)
    pending = set()
    failures = []
    started = time.monotonic()
    ordinal = completed = 0
    max_lag = 0.0

    async def purchase(number, due):
        nonlocal completed, max_lag
        try:
            now = datetime.now(timezone.utc)
            max_lag = max(max_lag, time.monotonic() - due)
            order, signals, index, behavior, scenario, fraud, friendly, delay = model.purchase(
                config.seed, number, now
            )
            key = f"live-{run_id}-{number}"
            token = await gateway.request(
                "POST", "/v1/payment-methods", f"{key}-token", {"card_number": card_number(index, behavior)}
            )
            if token["fingerprint"] != model.profiles.method_pool[index]:
                raise ValueError("vault fingerprint differs from historical identity")
            order["order_reference"] = key
            order["return_url"] = config.return_url
            payment = await gateway.request("POST", "/v1/payments", f"{key}-payment", order)
            expected_merchant = entity_id(model.config.integration_id, "merchant", order["seller"]["external_id"])
            expected_customer = entity_id(model.config.integration_id, "customer", order["buyer"]["external_id"])
            if (payment["merchant_id"], payment["customer_id"]) != (expected_merchant, expected_customer):
                raise ValueError("gateway integration differs from historical identities")
            attempt = await gateway.request(
                "POST",
                f"/v1/checkout/sessions/{payment['session_id']}/attempts",
                f"{key}-attempt",
                {"payment_method_token": token["payment_method_token"], **signals},
                payment["checkout_token"],
            )
            await labels.publish(
                payment["payment_id"],
                attempt["attempt_id"],
                now,
                fraud,
                LABEL_SOURCE_SIMULATION_TRUTH,
                SCENARIO_NAMES[scenario],
            )
            await labels.schedule(payment["payment_id"], attempt["attempt_id"], now, fraud, friendly, delay)
            completed += 1
        except Exception as error:
            failures.append(error)
        finally:
            slots.release()

    try:
        while not stop.is_set() and time.monotonic() - started < config.duration_seconds:
            if failures:
                raise failures[0]
            due = started + ordinal / config.payments_per_second
            if due >= started + config.duration_seconds:
                break
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(0.001, due - time.monotonic()))
                break
            except TimeoutError:
                pass
            await slots.acquire()
            if stop.is_set():
                slots.release()
                break
            task = asyncio.create_task(purchase(ordinal, due))
            pending.add(task)
            task.add_done_callback(pending.discard)
            ordinal += 1
        await asyncio.gather(*pending)
        if failures:
            raise failures[0]
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        report(
            "traffic_stop",
            admitted=ordinal,
            completed=completed,
            unfinished=ordinal - completed,
            max_arrival_lag_seconds=max_lag,
        )
        stop.set()


async def run_live(config):
    api_key = os.environ.get("GATEWAY_API_KEY")
    if not api_key:
        raise ValueError("GATEWAY_API_KEY is required")
    model = await asyncio.to_thread(load_model, config)
    stop, thread_stop = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    producer = Producer({"bootstrap.servers": config.kafka_brokers, "enable.idempotence": True})
    async with httpx.AsyncClient(base_url=config.gateway_url, timeout=config.request_timeout_seconds) as client:
        gateway = Gateway(config, client, api_key)
        labels = Labels(config, gateway, str(model.config.integration_id), producer)
        tasks = [
            asyncio.create_task(traffic(config, model, gateway, labels, stop)),
            asyncio.create_task(labels.run(stop)),
        ]
        if config.faults.enabled:
            tasks.append(asyncio.create_task(asyncio.to_thread(run_faults, config, thread_stop)))
        stopper = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stopper:
                    task.result()
        finally:
            stop.set()
            thread_stop.set()
            stopper.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(*tasks[:2], return_exceptions=True), timeout=35)
            except TimeoutError:
                pass
            results = await asyncio.gather(*tasks, return_exceptions=True)
            report(
                "live_stop",
                pending_chargebacks=len(labels.heap) + labels.unfinished_chargebacks,
                truth_published=labels.truth_published,
                chargebacks_published=labels.chargebacks_published,
                label_schedule="in-memory; lost on process failure",
                failed_loops=sum(isinstance(r, Exception) for r in results),
            )
        for result in results:
            if isinstance(result, Exception):
                raise RuntimeError("live loop failed") from result
