import json

import pytest
from pyspark.sql import functions as F

from batch import bronze, silver
from batch.schema import EVENT_COLUMNS, LABEL_COLUMNS, business_schema
from tests.events import approved, at, created, declined, label, requested, started, succeeded


def provenance(frame, source):
    return frame.withColumns(
        {
            "_source": F.lit(source),
            "_dataset_version": F.lit("v-test" if source == "historical" else None).cast("string"),
            "_source_file": F.lit("payment_events/part.parquet" if source == "historical" else None).cast("string"),
            "_kafka_topic": F.lit(None if source == "historical" else "payments.events.v1").cast("string"),
            "_kafka_partition": F.lit(None).cast("int"),
            "_kafka_offset": F.lit(None).cast("bigint"),
            "_kafka_timestamp": F.lit(None).cast("timestamp"),
            "_ingested_at": F.current_timestamp(),
            "_run_id": F.lit("bronze-test"),
        }
    )


def load_bronze(spark, events, labels, source="historical"):
    bronze.ensure_tables(spark)
    spark.createDataFrame(events, business_schema(spark, EVENT_COLUMNS)).transform(
        lambda frame: provenance(frame, source)
    ).writeTo(bronze.EVENTS).overwrite(F.lit(True))
    if labels is not None:
        spark.createDataFrame(labels, business_schema(spark, LABEL_COLUMNS)).transform(
            lambda frame: provenance(frame, source)
        ).writeTo(bronze.LABELS).overwrite(F.lit(True))


@pytest.fixture
def report_root(tmp_path, monkeypatch):
    monkeypatch.setenv("RUN_REPORT_ROOT", str(tmp_path / "reports"))
    return tmp_path / "reports"


def test_identical_copies_collapse_to_earliest_and_conflicts_leave_the_accepted_tables(spark, report_root):
    events = [
        created("pay-1", "evt-1", recorded=0),
        created("pay-1", "evt-1", recorded=5),
        created("pay-2", "evt-2", amount=100),
        created("pay-2", "evt-2", amount=999),
    ]
    load_bronze(spark, events, [])

    silver.run(spark, "run-1")

    accepted = spark.table(silver.PAYMENT_EVENTS)
    assert [r.event_id for r in accepted.collect()] == ["evt-1"]
    assert accepted.first()["recorded_at"] == at(0).replace(tzinfo=None)
    rejected = spark.table(silver.REJECTED).where(F.col("rejected_table") == "payment_events")
    assert {r.reason for r in rejected.collect()} == {"evt-2"}
    assert rejected.where(F.col("payment_id") == "pay-2").count() == 2
    assert spark.table(silver.PAYMENT_EVENTS).where(F.col("payment_id") == "pay-2").count() == 0


def test_attempt_lifecycle_and_label_linkage(spark, report_root):
    events = [
        created("pay-open", "e-1", minutes=0),
        started("pay-open", "e-2", 2, 1, "att-1"),
        approved("pay-open", "e-3", 3, 2, "att-1"),
        requested("pay-open", "e-4", 4, 3, "att-1"),
        declined("pay-open", "e-5", 5, 4, "att-1"),
        created("pay-won", "w-1", minutes=0),
        started("pay-won", "w-2", 2, 1, "att-2"),
        approved("pay-won", "w-3", 3, 2, "att-2"),
        requested("pay-won", "w-4", 4, 3, "att-2"),
        succeeded("pay-won", "w-5", 5, 4, "att-2"),
        started("pay-won", "w-6", 6, 5, "att-3"),
        declined("pay-won", "w-7", 7, 6, "att-3", source="DECLINE_SOURCE_RISK"),
    ]
    labels = [
        label("pay-open", "att-1", available=10),
        label("pay-open", "att-9", available=10),
        label("pay-open", "att-1", available=10, source="LABEL_SOURCE_CHARGEBACK", minutes=4),
        label("pay-won", "att-4", available=20),
        label("pay-won", "att-2", available=0),
    ]
    load_bronze(spark, events, labels)

    silver.run(spark, "run-2")

    attempts = {r.attempt_id: r for r in spark.table(silver.ATTEMPTS).collect()}
    assert attempts["att-1"].outcome == "declined"
    assert attempts["att-2"].outcome == "succeeded"
    assert attempts["att-2"].payment_status == "succeeded"
    assert all(a.lifecycle_valid for a in attempts.values())
    linkage = {(r.payment_id, r.attempt_id, r.source): r.linkage for r in spark.table(silver.LABELS).collect()}
    assert linkage[("pay-open", "att-1", "LABEL_SOURCE_SIMULATION_TRUTH")] == "linked"
    assert linkage[("pay-open", "att-9", "LABEL_SOURCE_SIMULATION_TRUTH")] == "violation"
    assert linkage[("pay-open", "att-1", "LABEL_SOURCE_CHARGEBACK")] == "violation"
    assert linkage[("pay-won", "att-4", "LABEL_SOURCE_SIMULATION_TRUTH")] == "violation"
    assert linkage[("pay-won", "att-2", "LABEL_SOURCE_SIMULATION_TRUTH")] == "violation"


def test_labels_without_their_payment_are_orphans_when_historical_and_pending_when_live(spark, report_root):
    load_bronze(spark, [created("pay-1", "evt-1")], [])
    historical = provenance(
        spark.createDataFrame([label("pay-gone", "att-1", available=5)], business_schema(spark, LABEL_COLUMNS)),
        "historical",
    )
    live = provenance(
        spark.createDataFrame([label("pay-later", "att-2", available=5)], business_schema(spark, LABEL_COLUMNS)),
        "kafka",
    )
    historical.writeTo(bronze.LABELS).append()
    live.writeTo(bronze.LABELS).append()

    silver.run(spark, "run-orphans")

    linkage = {r.payment_id: r.linkage for r in spark.table(silver.LABELS).collect()}
    assert linkage == {"pay-gone": "orphan", "pay-later": "pending"}


def test_rerun_on_unchanged_bronze_produces_equal_tables(spark, report_root):
    events = [created("pay-1", "evt-1"), started("pay-1", "evt-2", 2, 1, "att-1")]
    load_bronze(spark, events, [label("pay-1", "att-1", available=5)])

    first_report = silver.run(spark, "run-3")
    first = spark.table(silver.ATTEMPTS)
    second_report = silver.run(spark, "run-4")
    second = spark.table(silver.ATTEMPTS)

    assert first.exceptAll(second).count() == 0
    assert second.exceptAll(first).count() == 0
    assert second_report["lifecycle_invalid"] == 0
    snapshots = json.loads((report_root / "run-4" / "silver.json").read_text())["silver_snapshots"]
    assert set(snapshots) == {silver.PAYMENT_EVENTS, silver.ATTEMPTS, silver.LABELS, silver.REJECTED}
    assert (
        first_report["silver_snapshots"][silver.ATTEMPTS]["snapshot_id"] != snapshots[silver.ATTEMPTS]["snapshot_id"]
    )
