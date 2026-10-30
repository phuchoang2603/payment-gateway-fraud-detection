from datetime import datetime, timezone
from uuid import uuid4

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import functions as F

from batch import bronze
from batch.schema import EVENT_COLUMNS, LABEL_COLUMNS, business_schema
from tests.historical import EVENTS_PART, LABELS_PART, write_manifest, write_part

SECONDS = 1785498000
MOMENT = datetime.fromtimestamp(SECONDS, tz=timezone.utc)


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
    return body + timestamp() + field_bytes(6, field_varint(1, SECONDS)) + field_varint(7, 2) + field_bytes(8, b"none")


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


def historical_event(index):
    return {
        "event_id": f"evt-{index}",
        "payment_id": f"pay-{index}",
        "aggregate_version": 1,
        "integration_id": "int-1",
        "occurred_at": MOMENT,
        "recorded_at": MOMENT,
        "schema_version": 3,
        "correlation_id": "corr-1",
        "payment_created": None,
        "attempt_started": None,
        "risk_decision_recorded": None,
        "processor_operation_requested": None,
        "processor_outcome_unknown": None,
        "attempt_declined": None,
        "payment_succeeded": {"attempt_id": "att-1", "processor_reference": "ref-1"},
        "payment_expired": None,
    }


def historical_label():
    return {
        "integration_id": "int-1",
        "payment_id": "pay-1",
        "attempt_id": "att-1",
        "is_fraud": False,
        "occurred_at": MOMENT,
        "label_available_at": MOMENT,
        "source": "LABEL_SOURCE_SIMULATION_TRUTH",
        "scenario": "none",
    }


def test_kafka_decoding_matches_historical_parquet_for_the_same_events(spark, brokers, topics, tmp_path):
    events, labels = topics
    version = f"v-parity-{uuid4().hex}"
    produce(brokers, events, [payment_event(0), payment_event(1)])
    produce(brokers, labels, [fraud_label("pay-1", "att-1")])
    bronze.run_kafka(spark, brokers, "run-parity-kafka")

    files = [
        write_part(
            spark,
            tmp_path,
            EVENTS_PART,
            [historical_event(0), historical_event(1)],
            business_schema(spark, EVENT_COLUMNS),
        ),
        write_part(spark, tmp_path, LABELS_PART, [historical_label()], business_schema(spark, LABEL_COLUMNS)),
    ]
    write_manifest(tmp_path, files, version)
    bronze.run_historical(spark, str(tmp_path), version, "run-parity-historical")

    for table, columns, topic in ((bronze.EVENTS, EVENT_COLUMNS, events), (bronze.LABELS, LABEL_COLUMNS, labels)):
        kafka = (
            spark.table(table).where((F.col("_source") == "kafka") & (F.col("_kafka_topic") == topic)).select(*columns)
        )
        historical = (
            spark.table(table)
            .where((F.col("_source") == "historical") & (F.col("_dataset_version") == version))
            .select(*columns)
        )
        assert kafka.count() == historical.count() > 0
        assert kafka.exceptAll(historical).isEmpty()
        assert historical.exceptAll(kafka).isEmpty()
