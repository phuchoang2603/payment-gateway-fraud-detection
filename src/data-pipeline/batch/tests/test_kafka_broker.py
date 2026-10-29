import os
from uuid import uuid4

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import functions as F

from batch import bronze

BROKERS = os.environ.get("KAFKA_TEST_BROKERS")
pytestmark = pytest.mark.skipif(not BROKERS, reason="needs KAFKA_TEST_BROKERS; no broker runs in CI")


def varint(number):
    out = bytearray()
    while True:
        byte = number & 0x7F
        number >>= 7
        if number:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def field_bytes(number, data):
    return varint((number << 3) | 2) + varint(len(data)) + data


def field_varint(number, value):
    return varint(number << 3) + varint(value)


def payment_event(index):
    timestamp = field_varint(1, 1785498000)
    return (
        field_bytes(1, f"evt-{index}".encode())
        + field_bytes(2, f"pay-{index}".encode())
        + field_varint(3, 1)
        + field_bytes(4, b"int-1")
        + field_bytes(5, timestamp)
        + field_bytes(6, timestamp)
        + field_varint(7, 3)
        + field_bytes(8, b"corr-1")
    )


def create_topic(name):
    admin = AdminClient({"bootstrap.servers": BROKERS})
    for future in admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)]).values():
        future.result()


def produce(topic, values):
    producer = Producer({"bootstrap.servers": BROKERS})
    for value in values:
        producer.produce(topic, value=value, partition=0)
    producer.flush(30)


@pytest.fixture
def topics(monkeypatch):
    events, labels = f"payments.events.{uuid4().hex}", f"fraud.labels.{uuid4().hex}"
    create_topic(events)
    create_topic(labels)
    monkeypatch.setenv("PAYMENT_EVENTS_TOPIC", events)
    monkeypatch.setenv("FRAUD_LABELS_TOPIC", labels)
    monkeypatch.setenv("KAFKA_STARTING_OFFSETS", "earliest")
    return events


def test_each_run_continues_from_the_rows_it_wrote(spark, topics):
    produce(topics, [payment_event(i) for i in range(5)] + [b"\xff\xff\xff"])
    first = bronze.run_kafka(spark, BROKERS, "run-1")["topics"][topics]
    assert (first["accepted"], first["rejected"]) == (5, 1)

    produce(topics, [payment_event(i) for i in range(5, 8)])
    second = bronze.run_kafka(spark, BROKERS, "run-2")["topics"][topics]
    assert (second["accepted"], second["rejected"]) == (3, 0)
    assert second["accepted_starts"] == {"0": 5}
    assert second["rejected_starts"] == {"0": 6}

    third = bronze.run_kafka(spark, BROKERS, "run-3")["topics"][topics]
    assert third == {"table": bronze.EVENTS, "accepted": 0, "rejected": 0}

    kafka = spark.table(bronze.EVENTS).where(F.col("_kafka_topic") == topics)
    assert kafka.count() == kafka.select("event_id").distinct().count() == 8
    assert spark.table(bronze.REJECTED).where(F.col("_kafka_topic") == topics).count() == 1
