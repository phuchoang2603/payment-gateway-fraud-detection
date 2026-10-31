import copy
from datetime import date, timedelta

import pytest
from pyspark.sql import functions as F

from batch.bronze import ingest as bronze
from batch.gold import features as gold
from batch.silver import curate as silver
from tests.events import BASE, approved, created, label, started, succeeded
from tests.lake import deliver

SILVER_TABLES = (silver.PAYMENT_EVENTS, silver.ATTEMPTS, silver.LABELS, silver.REJECTED)
DAY = 24 * 60
FIRST = BASE.date()


@pytest.fixture
def lake(spark, report_root):
    for table in (*gold.TABLES, *SILVER_TABLES, bronze.EVENTS, bronze.LABELS):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    bronze.ensure_tables(spark)
    return {"offsets": {bronze.EVENTS: 0, bronze.LABELS: 0}}


def start(payment, event, version, minutes, attempt, customer="c-1", card="pm-1", address="f"):
    event_row = copy.deepcopy(started(payment, event, version, minutes, attempt))
    attempt_started = event_row["attempt_started"]
    attempt_started["context"]["customer"]["customer_id"] = customer
    attempt_started["context"]["snapshot"]["shipping"]["address_fingerprint"] = address
    attempt_started["signals"]["payment_method"]["fingerprint"] = card
    return event_row


def payment(n, day, customer="c-1", card="pm-1", address="f"):
    minutes = day * DAY + n
    return [
        created(f"pay-{n}", f"e-{n}-1", minutes=minutes),
        start(f"pay-{n}", f"e-{n}-2", 2, minutes + 1, f"att-{n}", customer, card, address),
        approved(f"pay-{n}", f"e-{n}-3", 3, minutes + 2, f"att-{n}"),
        succeeded(f"pay-{n}", f"e-{n}-4", 4, minutes + 3, f"att-{n}"),
    ]


def silver_history(spark, lake):
    events = [
        *payment(1, 0),
        *payment(2, 2, address="g"),
        *payment(3, 5, customer="c-2"),
        *payment(4, 9, card="pm-2"),
        *payment(5, 12),
    ]
    deliver(spark, lake, bronze.EVENTS, events)
    deliver(spark, lake, bronze.LABELS, [label("pay-1", "att-1", available=DAY + 5, minutes=1)])
    silver.run(spark, "silver-base", "historical")


def tables(spark) -> dict:
    return {table: spark.table(table).drop("created").localCheckpoint() for table in gold.TABLES}


def days(first: date, last: date):
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def assert_same(left: dict, right: dict, step: str):
    for table in gold.TABLES:
        only_left = [row.asDict() for row in left[table].exceptAll(right[table]).collect()]
        only_right = [row.asDict() for row in right[table].exceptAll(left[table]).collect()]
        assert not only_left and not only_right, f"{step} {table}: {only_left} vs {only_right}"


def test_daily_runs_build_the_same_gold_as_one_backfill(spark, lake):
    silver_history(spark, lake)
    last = FIRST + timedelta(days=14)
    report = gold.run(spark, "backfill", FIRST, last)
    assert report["start"] == str(FIRST) and report["end"] == str(last)
    backfill = tables(spark)
    assert backfill[gold.FEAT_CUSTOMER].count() > 0
    assert backfill[gold.FEAT_ATTEMPT_RT].count() == 5
    assert backfill[gold.LABEL_ATTEMPT].count() == 1

    for table in gold.TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    for day in days(FIRST, last):
        gold.run(spark, f"daily-{day}", day, day)

    assert_same(backfill, tables(spark), "daily")


def test_a_rerun_of_a_range_changes_nothing(spark, lake):
    silver_history(spark, lake)
    first = gold.run(spark, "gold-1", FIRST, FIRST + timedelta(days=14))
    before = {table: info["snapshot_id"] for table, info in first["tables"].items()}
    gold.run(spark, "gold-2", FIRST + timedelta(days=3), FIRST + timedelta(days=8))

    for table, snapshot in before.items():
        old = spark.read.option("versionAsOf", snapshot).table(table).drop("created")
        new = spark.table(table).drop("created")
        assert old.exceptAll(new).count() == 0, table
        assert new.exceptAll(old).count() == 0, table


def test_a_late_label_rebuilds_the_label_rows_of_its_attempt_day(spark, lake):
    silver_history(spark, lake)
    gold.run(spark, "gold-1", FIRST, FIRST + timedelta(days=14))
    deliver(
        spark,
        lake,
        bronze.LABELS,
        [label("pay-2", "att-2", available=20 * DAY, source="LABEL_SOURCE_CHARGEBACK", minutes=2 * DAY + 3)],
    )
    silver.run(spark, "silver-labels", "incremental")
    arrival = (BASE + timedelta(minutes=20 * DAY)).date()

    report = gold.run(spark, "gold-2", arrival, arrival)

    assert str(FIRST + timedelta(days=2)) in report["days"]
    labels = spark.table(gold.LABEL_ATTEMPT)
    assert (
        labels.where((F.col("payment_id") == "pay-2") & (F.col("label_source") == "LABEL_SOURCE_CHARGEBACK")).count()
        == 1
    )
    assert labels.count() == 2


def test_the_daily_range_defaults_to_yesterday_and_the_lookback():
    today = date(2026, 10, 31)
    assert gold.day_range({}, today) == (date(2026, 10, 28), date(2026, 10, 30))
    assert gold.day_range({"DP3_LOOKBACK_DAYS": "0"}, today) == (date(2026, 10, 30), date(2026, 10, 30))
    assert gold.day_range({"DP3_START": "2026-09-01", "DP3_END": "2026-09-30"}, today) == (
        date(2026, 9, 1),
        date(2026, 9, 30),
    )
