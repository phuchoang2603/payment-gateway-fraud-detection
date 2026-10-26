import json
import os
import threading
import time
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from simulator.contracts.payments.v1.events_pb2 import PaymentEvent
from simulator.live import faults
from simulator.live.config import Faults, LiveConfig


@pytest.mark.skipif(not os.environ.get("KAFKA_TEST_BROKERS"), reason="Kafka recovery contract runs in CI")
def test_held_record_replays_and_forwards_original_bytes_with_duplicates(monkeypatch):
    brokers = os.environ["KAFKA_TEST_BROKERS"]
    suffix = uuid4().hex
    source, destination = f"source-{suffix}", f"faulty-{suffix}"
    admin = AdminClient({"bootstrap.servers": brokers})
    for result in admin.create_topics([NewTopic(source, 1, 1), NewTopic(destination, 1, 1)]).values():
        result.result(30)
    event = PaymentEvent(event_id="e", payment_id="p")
    event.payment_expired.present = True
    body = event.SerializeToString()
    headers = [("x", b"first"), ("x", b"second")]
    producer = Producer({"bootstrap.servers": brokers})
    producer.produce(source, key=b"payment", value=body, headers=headers)
    assert producer.flush(30) == 0
    held = threading.Event()
    stop = threading.Event()
    errors = []

    def observe(line, **kwargs):
        progress = json.loads(line)
        if progress["event"] == "fault_progress" and progress["pending_records"] == 1:
            held.set()

    monkeypatch.setattr(faults, "print", observe, raising=False)
    settings = Faults(
        source=source,
        destination=destination,
        group=f"group-{suffix}",
        late_rate=1,
        late_min_seconds=120,
        late_max_seconds=120,
        max_records=1,
    )
    config = LiveConfig(
        manifest="s3://synthetic-source/historical/test/manifest.json",
        manifest_sha256="0" * 64,
        kafka_brokers=brokers,
        faults=settings,
    )

    def run(cfg):
        try:
            faults.run_faults(cfg, stop)
        except Exception as error:
            errors.append(error)

    worker = threading.Thread(target=run, args=(config,), daemon=True)
    worker.start()
    try:
        assert held.wait(40), "held backlog was never admitted"
    finally:
        stop.set()
        worker.join(15)
    assert not worker.is_alive() and not errors
    stop.clear()
    replay = config.model_copy(
        update={
            "faults": settings.model_copy(update={"late_rate": 0, "duplicate_rate": 1, "duplicate_delay_seconds": 0})
        }
    )
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"observer-{suffix}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([destination])
    worker = threading.Thread(target=run, args=(replay,), daemon=True)
    worker.start()
    deliveries = []
    try:
        deadline = time.monotonic() + 40
        while len(deliveries) < 2 and time.monotonic() < deadline:
            record = consumer.poll(0.2)
            if record is not None:
                assert record.error() is None
                deliveries.append((record.key(), record.value(), record.headers()))
        assert deliveries == [(b"payment", body, headers)] * 2
    finally:
        stop.set()
        worker.join(15)
        consumer.close()
    assert not worker.is_alive() and not errors
