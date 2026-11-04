import json
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("pyiceberg")

import pyarrow as pa  # noqa: E402
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition  # noqa: E402
from pyiceberg.catalog import load_catalog  # noqa: E402
from test_history import bronze  # noqa: E402

from fraud_streaming import history  # noqa: E402
from fraud_streaming.contracts import PaymentEvent  # noqa: E402

FIRST = datetime(2025, 9, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def kafka():
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer("confluentinc/cp-kafka:7.9.2") as container:
        yield container.get_bootstrap_server()


@pytest.fixture
def bronze_table(tmp_path, monkeypatch):
    """A local Bronze table holding one attempt start per row, out of order across files and days."""
    catalog = load_catalog(
        "test", type="sql", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}"
    )
    catalog.create_namespace("bronze")
    template = bronze()
    rows = []
    # Two copies at the same instant in different files, a day boundary, the lead-in day, and a row past the end.
    for minutes, payment, source in [
        (60 * 30, "pay-3", "part-1.parquet"),
        (5, "pay-1", "part-1.parquet"),
        (5, "pay-1", "part-0.parquet"),
        (-60, "pay-0", "part-0.parquet"),
        (60 * 24 - 1, "pay-2", "part-0.parquet"),
        (60 * 24 * 2 + 1, "pay-late", "part-0.parquet"),
    ]:
        row = json.loads(json.dumps(template, default=str))
        row["payment_id"] = payment
        row["event_id"] = f"evt-{payment}"
        row["recorded_at"] = FIRST + timedelta(minutes=minutes)
        row["occurred_at"] = row["recorded_at"]
        row["_source_file"] = source
        row["_kafka_offset"] = None
        rows.append(row)
    table = pa.Table.from_pylist(rows)
    table = table.set_column(
        table.schema.get_field_index("_kafka_offset"),
        "_kafka_offset",
        pa.array([None] * len(rows), pa.int64()),
    )
    # Iceberg format 2 has no column type for an all-null value; such fields go absent rather than null.
    table = table.select(
        [name for name in table.column_names if not pa.types.is_null(table[name].type)]
    )
    created = catalog.create_table("bronze.payment_events", schema=table.schema)
    created.append(table)
    monkeypatch.setattr(history, "lake", lambda: catalog)
    monkeypatch.setenv("DATASET_VERSION", "v19")
    return rows


def consume(brokers, topic):
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": "seed-test",
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.assign([TopicPartition(topic, 0, 0)])
    records = []
    try:
        while True:
            message = consumer.poll(10)
            assert message is not None, f"no EOF from {topic}"
            if message.error():
                assert message.error().code() == KafkaError._PARTITION_EOF
                return records
            records.append(message)
    finally:
        consumer.close()


def test_seed_writes_the_unit_in_replay_order_between_bootstrap_and_final_clocks(
    kafka, bronze_table, tmp_path, monkeypatch
):
    monkeypatch.setenv("FEATURE_EPOCH", "history-ci")
    monkeypatch.setenv("STREAM_RUN", "history-ci-seed")
    monkeypatch.setenv("KAFKA_BROKERS", kafka)
    monkeypatch.setenv("HISTORY_SEED_REPORT", str(tmp_path / "seed.json"))
    # A failed first attempt left a partial unit behind; the retried seed must replace it, not append to it.
    monkeypatch.setattr("sys.argv", ["history", "topics"])
    history.main()
    history.seed("2025-09-01", "2025-09-02")

    history.seed("2025-09-01", "2025-09-03")

    report = json.loads((tmp_path / "seed.json").read_text())
    events = consume(kafka, "payments.events.history-ci-seed")
    clocks = [json.loads(m.value()) for m in consume(kafka, "fraud.clock.history-ci-seed")]
    # pay-late is past the exclusive end; the lead-in day (pay-0) is included.
    assert [m.key() for m in events] == [b"pay-0", b"pay-1", b"pay-1", b"pay-2", b"pay-3"]
    facts = [PaymentEvent.FromString(m.value()) for m in events]
    recorded = [fact.recorded_at.ToMicroseconds() for fact in facts]
    # The decoded facts are the live job's input: same attempt and keys as the Bronze row they came from.
    assert {
        (
            fact.integration_id,
            fact.attempt_started.context.customer.customer_id,
            fact.attempt_started.signals.payment_method.fingerprint,
        )
        for fact in facts
    } == {("i", "c", "card")}
    assert recorded == sorted(recorded)
    assert [m.offset() for m in events] == list(range(5))
    assert [c["sequence"] for c in clocks] == [0, 1]
    assert [c["input_offsets"] for c in clocks] == [{"0": 0}, {"0": 5}]
    assert clocks[0]["clock_us"] == int((FIRST - timedelta(days=1)).timestamp() * 1_000_000)
    assert clocks[1]["clock_us"] > recorded[-1]
    assert report["done"] and report["input_end"] == 5 and report["clock_end"] == 2


def seed_env(monkeypatch, tmp_path, kafka, run):
    monkeypatch.setenv("FEATURE_EPOCH", "history-ci")
    monkeypatch.setenv("STREAM_RUN", run)
    monkeypatch.setenv("KAFKA_BROKERS", kafka)
    monkeypatch.setenv("HISTORY_SEED_REPORT", str(tmp_path / "seed.json"))


def reading_bronze(monkeypatch):
    reads = []
    real = history.ordered_rows
    monkeypatch.setattr(
        history,
        "ordered_rows",
        lambda *args, **kwargs: reads.append(args[-2:]) or real(*args, **kwargs),
    )
    return reads


def test_a_finished_seed_is_reused_and_only_the_run_outputs_are_reset(
    kafka, bronze_table, tmp_path, monkeypatch
):
    seed_env(monkeypatch, tmp_path, kafka, "history-ci-reuse")
    history.seed("2025-09-01", "2025-09-02")
    before = [m.value() for m in consume(kafka, "payments.events.history-ci-reuse")]
    producer = Producer({"bootstrap.servers": kafka})
    producer.produce(
        "fraud.feature-updates.v2.history-ci-reuse", value=b"left by an earlier Flink run"
    )
    producer.flush(30)
    reads = reading_bronze(monkeypatch)

    history.seed("2025-09-01", "2025-09-02")

    assert reads == []
    assert [m.value() for m in consume(kafka, "payments.events.history-ci-reuse")] == before
    assert consume(kafka, "fraud.feature-updates.v2.history-ci-reuse") == []


@pytest.mark.parametrize("change", ["input", "range"])
def test_a_seed_is_redone_when_its_topics_or_range_changed(
    kafka, bronze_table, tmp_path, monkeypatch, change
):
    seed_env(monkeypatch, tmp_path, kafka, f"history-ci-redo-{change}")
    history.seed("2025-09-01", "2025-09-02")
    if change == "input":
        producer = Producer({"bootstrap.servers": kafka})
        producer.produce(f"payments.events.history-ci-redo-{change}", value=b"extra")
        producer.flush(30)
    reads = reading_bronze(monkeypatch)

    history.seed("2025-09-01", "2025-09-02" if change == "input" else "2025-09-03")

    assert reads
    keys = [m.key() for m in consume(kafka, f"payments.events.history-ci-redo-{change}")]
    assert b"pay-0" in keys and b"extra" not in [
        m.value() for m in consume(kafka, f"payments.events.history-ci-redo-{change}")
    ]
