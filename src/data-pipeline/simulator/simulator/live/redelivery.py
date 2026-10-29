import json
import time
from hashlib import sha256

from confluent_kafka import Consumer, Producer

from simulator.contracts.payments.v1.events_pb2 import PaymentEvent

MARKER = "x-simulator-redelivery"


def selected(seed, event_id, rate):
    digest = sha256(f"{seed}:redelivery:{event_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64 < rate


def report(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


def run_redelivery(config, stop):
    settings = config.redelivery
    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_brokers,
            "group.id": settings.group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            # Only offsets stored after a record is fully handled are committed.
            "enable.auto.offset.store": False,
        }
    )
    producer = Producer({"bootstrap.servers": config.kafka_brokers, "enable.idempotence": True})
    consumed = chosen = acknowledged = skipped = 0
    last_report = time.monotonic()
    consumer.subscribe([settings.topic])
    report("redelivery_start", settings=settings.model_dump(), recovery="at-least-once")
    try:
        while not stop.is_set():
            if time.monotonic() - last_report >= 5:
                report("redelivery_progress", consumed=consumed, selected=chosen, acknowledged=acknowledged)
                last_report = time.monotonic()
            message = consumer.poll(0.25)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("redelivery consumer failed")
            headers = message.headers() or []
            if any(name == MARKER for name, _ in headers):
                skipped += 1
                consumer.store_offsets(message=message)
                continue
            event = PaymentEvent.FromString(message.value())
            if not event.event_id:
                raise ValueError("malformed committed event")
            consumed += 1
            if selected(config.seed, event.event_id, settings.rate):
                chosen += 1
                failures = []
                producer.produce(
                    settings.topic,
                    partition=message.partition(),
                    key=message.key(),
                    value=message.value(),
                    headers=[*headers, (MARKER, b"1")],
                    on_delivery=lambda error, _: failures.append(error) if error else None,
                )
                if producer.flush(30) or failures:
                    raise RuntimeError("redelivery publication failed")
                acknowledged += 1
            consumer.store_offsets(message=message)
    finally:
        producer.flush(10)
        consumer.close()
        report(
            "redelivery_stop",
            consumed=consumed,
            selected=chosen,
            acknowledged=acknowledged,
            skipped_own_copies=skipped,
            observed_rate=chosen / consumed if consumed else 0,
            recovery="uncommitted records replay; additional copies possible",
        )
