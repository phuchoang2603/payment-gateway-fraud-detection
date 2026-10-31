import json
from datetime import timedelta

from pyspark.sql import functions as F

from batch.bronze import ingest as bronze
from batch.bronze.schema import LABEL_COLUMNS, business_schema
from batch.days import every_day
from batch.silver import curate as silver
from tests.events import BASE, approved, at, created, declined, label, requested, started, succeeded
from tests.lake import load_bronze, provenance

DAY = 24 * 60
FIRST = BASE.date()


def run(spark, run_id, first=FIRST, last=FIRST):
    return silver.run(spark, run_id, first, last)


def test_identical_copies_collapse_to_earliest_and_conflicts_leave_the_accepted_tables(spark, report_root):
    events = [
        created("pay-1", "evt-1", recorded=0),
        created("pay-1", "evt-1", recorded=5),
        created("pay-2", "evt-2", amount=100),
        created("pay-2", "evt-2", amount=999),
        created("pay-2", "evt-3", minutes=1),
    ]
    load_bronze(spark, events, [])

    report = run(spark, "run-1")

    accepted = spark.table(silver.PAYMENT_EVENTS)
    assert [r.event_id for r in accepted.collect()] == ["evt-1"]
    assert accepted.first()["recorded_at"] == at(0).replace(tzinfo=None)
    rejected = spark.table(silver.REJECTED).where(F.col("rejected_table") == "payment_events")
    assert {r.reason for r in rejected.collect()} == {"evt-2"}
    assert rejected.where(F.col("payment_id") == "pay-2").count() == 3
    assert report["events"]["distinct_events"] == 3
    assert report["events"]["conflicting_payments"] == 1
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

    run(spark, "run-2")

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

    run(spark, "run-orphans")

    linkage = {r.payment_id: r.linkage for r in spark.table(silver.LABELS).collect()}
    assert linkage == {"pay-gone": "orphan", "pay-later": "pending"}


def test_rerun_on_unchanged_bronze_produces_equal_tables(spark, report_root):
    events = [created("pay-1", "evt-1"), started("pay-1", "evt-2", 2, 1, "att-1")]
    load_bronze(spark, events, [label("pay-1", "att-1", available=5)])

    first_report = run(spark, "run-3")
    first = spark.table(silver.ATTEMPTS)
    run(spark, "run-4")
    second = spark.table(silver.ATTEMPTS)

    assert first.exceptAll(second).count() == 0
    assert second.exceptAll(first).count() == 0
    assert second.where(~F.col("lifecycle_valid")).count() == 0
    snapshots = json.loads((report_root / "run-4" / "silver.json").read_text())["silver_snapshots"]
    assert set(snapshots) == {silver.PAYMENT_EVENTS, silver.ATTEMPTS, silver.LABELS, silver.REJECTED}
    assert (
        first_report["silver_snapshots"][silver.ATTEMPTS]["snapshot_id"] != snapshots[silver.ATTEMPTS]["snapshot_id"]
    )


def payment(n, minutes, attempt=True):
    events = [created(f"pay-{n}", f"e-{n}-1", minutes=minutes)]
    if attempt:
        events += [
            started(f"pay-{n}", f"e-{n}-2", 2, minutes + 1, f"att-{n}"),
            approved(f"pay-{n}", f"e-{n}-3", 3, minutes + 2, f"att-{n}"),
            succeeded(f"pay-{n}", f"e-{n}-4", 4, minutes + 3, f"att-{n}"),
        ]
    return events


def history():
    # pay-2 begins two minutes before midnight UTC and finishes after it; pay-4 has a conflicting redelivery.
    midnight = DAY - 12 * 60
    events = [
        *payment(1, 0),
        *payment(2, midnight - 2),
        *payment(3, midnight + 30),
        *payment(4, 3 * DAY),
        created("pay-4", "e-4-1", minutes=3 * DAY, amount=999),
    ]
    labels = [
        label("pay-1", "att-1", available=2 * DAY),
        label("pay-3", "att-3", available=2 * DAY + 60, source="LABEL_SOURCE_CHARGEBACK", minutes=midnight + 33),
    ]
    return events, labels


def tables(spark):
    return {table: spark.table(table).drop("_run_id").localCheckpoint() for table in silver.TABLES}


def test_a_payment_belongs_to_the_day_of_its_first_event(spark, report_root):
    events, labels = history()
    load_bronze(spark, events, labels)

    run(spark, "first-day", FIRST + timedelta(days=1), FIRST + timedelta(days=1))

    by_payment = {r.payment_id: r.payment_day for r in spark.table(silver.ATTEMPTS).collect()}
    assert by_payment == {"pay-3": FIRST + timedelta(days=1)}
    run(spark, "day-zero", FIRST, FIRST)
    late = spark.table(silver.PAYMENT_EVENTS).where(F.col("payment_id") == "pay-2")
    assert {r.payment_day for r in late.collect()} == {FIRST}
    assert late.count() == 4


def test_daily_runs_build_the_same_silver_as_one_backfill(spark, report_root):
    events, labels = history()
    load_bronze(spark, events, labels)
    last = FIRST + timedelta(days=4)

    run(spark, "backfill", FIRST, last)
    backfill = tables(spark)
    for table in silver.TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    for day in every_day(FIRST, last):
        run(spark, f"daily-{day}", day, day)
    daily = tables(spark)

    for table in silver.TABLES:
        only_backfill = [r.asDict() for r in backfill[table].exceptAll(daily[table]).collect()]
        only_daily = [r.asDict() for r in daily[table].exceptAll(backfill[table]).collect()]
        assert not only_backfill and not only_daily, f"{table}: {only_backfill} vs {only_daily}"
    linkage = {r.payment_id: r.linkage for r in daily[silver.LABELS].collect()}
    assert linkage == {"pay-1": "linked", "pay-3": "linked"}
    assert {r.payment_id for r in daily[silver.REJECTED].collect()} == {"pay-4"}
