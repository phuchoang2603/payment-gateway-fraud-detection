from uuid import uuid4

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import functions as F
from testcontainers.community.kafka import KafkaContainer

from batch import bronze

SECONDS = 1785498000


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


def timestamp():
    return field_bytes(5, field_varint(1, SECONDS))


def payment_event(index, event_id=None):
    payment_succeeded = field_bytes(15, field_bytes(1, b"att-1") + field_bytes(2, b"ref-1"))
    return (
        field_bytes(1, (f"evt-{index}" if event_id is None else event_id).encode())
        + field_bytes(2, f"pay-{index}".encode())
        + field_varint(3, 1)
        + field_bytes(4, b"int-1")
        + timestamp()
        + field_bytes(6, field_varint(1, SECONDS))
        + field_varint(7, 3)
        + field_bytes(8, b"corr-1")
        + payment_succeeded
    )


def fraud_label(payment_id, attempt_id):
    body = field_bytes(1, b"int-1") + field_bytes(2, payment_id.encode())
    if attempt_id is not None:
        body += field_bytes(3, attempt_id.encode())
    return body + timestamp() + field_bytes(6, field_varint(1, SECONDS)) + field_varint(7, 2)


@pytest.fixture(scope="module")
def brokers():
    with KafkaContainer("confluentinc/cp-kafka:7.8.0").with_kraft() as kafka:
        yield kafka.get_bootstrap_server()


@pytest.fixture
def topics(brokers, monkeypatch):
    events, labels = f"payments.events.{uuid4().hex}", f"fraud.labels.{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": brokers})
    for future in admin.create_topics([NewTopic(events, 1, 1), NewTopic(labels, 1, 1)]).values():
        future.result(timeout=30)
    monkeypatch.setenv("PAYMENT_EVENTS_TOPIC", events)
    monkeypatch.setenv("FRAUD_LABELS_TOPIC", labels)
    monkeypatch.setenv("KAFKA_STARTING_OFFSETS", "earliest")
    return events, labels


def produce(brokers, topic, values):
    producer = Producer({"bootstrap.servers": brokers})
    for value in values:
        producer.produce(topic, value=value, partition=0)
    assert producer.flush(30) == 0


def rejected_errors(spark, topic):
    return sorted(row.error for row in spark.table(bronze.REJECTED).where(F.col("_kafka_topic") == topic).collect())


def test_each_run_resumes_from_its_rows_and_a_rerun_reads_nothing(spark, brokers, topics):
    events, _ = topics
    produce(brokers, events, [payment_event(i) for i in range(5)] + [b"\xff\xff\xff"])
    first = bronze.run_kafka(spark, brokers, "run-1")["topics"][events]
    assert (first["accepted"], first["rejected"]) == (5, 1)

    produce(brokers, events, [payment_event(i) for i in range(5, 8)])
    second = bronze.run_kafka(spark, brokers, "run-2")["topics"][events]
    assert (second["accepted"], second["rejected"]) == (3, 0)
    assert second["accepted_starts"] == {"0": 5}
    assert second["rejected_starts"] == {"0": 6}

    third = bronze.run_kafka(spark, brokers, "run-3")["topics"][events]
    assert (third["accepted"], third["rejected"]) == (0, 0)

    accepted = spark.table(bronze.EVENTS).where(F.col("_kafka_topic") == events)
    assert accepted.count() == accepted.select("event_id").distinct().count() == 8
    assert rejected_errors(spark, events) == ["protobuf decoding failed"]


def test_empty_key_is_rejected_with_its_reason(spark, brokers, topics):
    events, _ = topics
    produce(brokers, events, [payment_event(0, event_id=""), payment_event(1)])

    report = bronze.run_kafka(spark, brokers, "run-empty-key")["topics"][events]

    assert (report["accepted"], report["rejected"]) == (1, 1)
    assert rejected_errors(spark, events) == ["missing event_id"]


def test_label_without_attempt_is_rejected_with_its_reason(spark, brokers, topics):
    _, labels = topics
    produce(brokers, labels, [fraud_label("pay-1", "att-1"), fraud_label("pay-2", None)])

    report = bronze.run_kafka(spark, brokers, "run-no-attempt")["topics"][labels]

    assert (report["accepted"], report["rejected"]) == (1, 1)
    assert rejected_errors(spark, labels) == ["missing attempt_id"]
