import asyncio
import heapq
import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx
from confluent_kafka import Producer

from simulator.contracts.payments.v1.labels_pb2 import (
    LABEL_SOURCE_CHARGEBACK,
    LABEL_SOURCE_SIMULATION_TRUTH,
    FraudLabel,
)
from simulator.live.model import card_number
from simulator.live.redelivery import run_redelivery
from simulator.live.source import load_model
from simulator.world import entity_id


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


def hold_seconds(config, run_id, ordinal):
    digest = sha256(f"{config.seed}:late:{run_id}:{ordinal}".encode()).digest()
    selection, position = (int.from_bytes(digest[i : i + 8], "big") / 2**64 for i in (0, 8))
    if selection >= config.late_rate:
        return 0
    return config.late_min_seconds + position * (config.late_max_seconds - config.late_min_seconds)


async def traffic(config, model, gateway, labels, stop):
    run_id = uuid4().hex
    report(
        "traffic_start",
        run_id=run_id,
        manifest=config.manifest,
        manifest_sha256=config.manifest_sha256,
        generator_digest=model.manifest["generator_digest"],
        world_digest=model.world.digest,
        clock="UTC-wall-clock",
        late_rate=config.late_rate,
        late_seconds=[config.late_min_seconds, config.late_max_seconds],
    )
    slots = asyncio.Semaphore(config.concurrency)
    pending = set()
    failures = []
    started = time.monotonic()
    deadline = started + config.duration_seconds if config.duration_seconds else None
    ordinal = completed = held = late_sent = late_unsent = 0
    max_lag = 0.0
    late_delays = []

    async def purchase(number, due, clicked=None):
        nonlocal completed, max_lag, late_sent
        try:
            now = clicked or datetime.now(timezone.utc)
            if clicked is None:
                max_lag = max(max_lag, time.monotonic() - due)
            order, signals, index, behavior, scenario, fraud, friendly, delay = model.purchase(
                config.seed, number, now
            )
            if clicked is not None:
                signals = {
                    **signals,
                    "simulated_client": {**signals["simulated_client"], "attempted_at": now.isoformat()},
                }
            key = f"live-{run_id}-{number}"
            token = await gateway.request(
                "POST", "/v1/payment-methods", f"{key}-token", {"card_number": card_number(index, behavior)}
            )
            if token["fingerprint"] != model.world.payment_method_fingerprint(index):
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
                scenario,
            )
            await labels.schedule(payment["payment_id"], attempt["attempt_id"], now, fraud, friendly, delay)
            completed += 1
            if clicked is not None:
                late_sent += 1
                late_delays.append((datetime.now(timezone.utc) - clicked).total_seconds())
        except Exception as error:
            failures.append(error)
        finally:
            slots.release()

    async def hold(number, due, seconds):
        nonlocal held, late_unsent
        clicked = datetime.now(timezone.utc)
        held += 1
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except TimeoutError:
            pass
        except asyncio.CancelledError:
            late_unsent += 1
            raise
        else:
            late_unsent += 1
            return
        finally:
            held -= 1
        await slots.acquire()
        await purchase(number, due, clicked)

    try:
        while not stop.is_set() and (deadline is None or time.monotonic() < deadline):
            if failures:
                raise failures[0]
            due = started + ordinal / config.payments_per_second
            if deadline is not None and due >= deadline:
                break
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(0.001, due - time.monotonic()))
                break
            except TimeoutError:
                pass
            seconds = hold_seconds(config, run_id, ordinal)
            if seconds:
                if held >= config.max_held_attempts:
                    raise RuntimeError("store-and-forward capacity exhausted")
                task = asyncio.create_task(hold(ordinal, due, seconds))
            else:
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
            late_sent=late_sent,
            late_unsent=late_unsent,
            late_delay_seconds=[min(late_delays), max(late_delays)] if late_delays else None,
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
        if config.redelivery.enabled:
            tasks.append(asyncio.create_task(asyncio.to_thread(run_redelivery, config, thread_stop)))
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


def main():
    from simulator.live.config import load_live_config

    path = Path(os.environ.get("SIMULATOR_CONFIG", "/etc/simulator/live.yaml"))
    asyncio.run(run_live(load_live_config(path)))


if __name__ == "__main__":
    main()
