import threading
import time
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.kafka import KafkaContainer

from simulator.contracts.payments.v1.events_pb2 import PaymentEvent
from simulator.live import redelivery
from simulator.live.config import LiveConfig, Redelivery


@pytest.fixture(scope="module")
def brokers():
    with KafkaContainer("confluentinc/cp-kafka:7.8.0").with_kraft() as kafka:
        yield kafka.get_bootstrap_server()


def test_redelivered_copy_is_identical_on_same_partition_and_not_copied_again(brokers):
    topic = f"payments-{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": brokers})
    for result in admin.create_topics([NewTopic(topic, 3, 1)]).values():
        result.result(30)
    config = LiveConfig(
        manifest="s3://platform-dev/datasets/payment-gateway/historical/test/manifest.json",
        manifest_sha256="0" * 64,
        kafka_brokers=brokers,
        redelivery=Redelivery(enabled=True, topic=topic, group=f"group-{uuid4().hex}", rate=1),
    )
    stop = threading.Event()
    errors = []

    def run():
        try:
            redelivery.run_redelivery(config, stop)
        except Exception as error:
            errors.append(error)

    observer = Consumer(
        {"bootstrap.servers": brokers, "group.id": f"observer-{uuid4().hex}", "auto.offset.reset": "earliest"}
    )
    observer.subscribe([topic])
    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 40
        while not observer.assignment() and time.monotonic() < deadline:
            observer.poll(0.2)
        time.sleep(8)
        event = PaymentEvent(event_id="e", payment_id="p")
        event.payment_expired.present = True
        body = event.SerializeToString()
        producer = Producer({"bootstrap.servers": brokers})
        producer.produce(topic, key=b"payment", value=body, partition=2, headers=[("x", b"first")])
        assert producer.flush(30) == 0
        records = []
        while len(records) < 3 and time.monotonic() < deadline:
            record = observer.poll(0.2)
            if record is not None:
                assert record.error() is None
                records.append((record.partition(), record.key(), record.value(), record.headers()))
        assert records == [
            (2, b"payment", body, [("x", b"first")]),
            (2, b"payment", body, [("x", b"first"), (redelivery.MARKER, b"1")]),
        ]
    finally:
        stop.set()
        worker.join(15)
        observer.close()
    assert not worker.is_alive() and not errors
