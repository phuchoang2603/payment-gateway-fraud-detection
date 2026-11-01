import copy
import itertools
from datetime import date, timedelta

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st
from pyspark.sql import functions as F

from batch.bronze import ingest as bronze
from batch.days import day_range
from batch.gold import features as gold
from batch.silver import curate as silver
from tests.events import BASE, approved, created, label, started, succeeded
from tests.lake import chunks, deliver

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
    silver.run(spark, "silver-base", FIRST, FIRST + timedelta(days=14))


def tables(spark) -> dict:
    return {table: spark.table(table).drop("created").localCheckpoint() for table in gold.TABLES}


def assert_same(left: dict, right: dict, step: str):
    for table in gold.TABLES:
        only_left = [row.asDict() for row in left[table].exceptAll(right[table]).collect()]
        only_right = [row.asDict() for row in right[table].exceptAll(left[table]).collect()]
        assert not only_left and not only_right, f"{step} {table}: {only_left} vs {only_right}"


SPAN = 14
OFFSETS = st.integers(0, SPAN)


def test_any_split_of_a_range_and_any_rerun_build_the_same_gold_as_one_backfill(spark, lake):
    silver_history(spark, lake)
    last = FIRST + timedelta(days=SPAN)
    report = gold.run(spark, "backfill", FIRST, last)
    assert report["start"] == str(FIRST) and report["end"] == str(last)
    backfill = tables(spark)
    assert backfill[gold.FEAT_CUSTOMER].count() > 0
    assert backfill[gold.LABEL_ATTEMPT].count() == 1
    runs = itertools.count()

    # Each example is several Spark runs, so few examples. The explicit one is the schedule as it runs: a backfill,
    # then one run per day for the last days.
    @settings(max_examples=2, deadline=None, database=None, derandomize=True)
    @given(cuts=st.sets(st.integers(1, SPAN), max_size=4), rerun=st.tuples(OFFSETS, OFFSETS).map(sorted))
    @example(cuts=set(range(SPAN - 2, SPAN + 1)), rerun=[3, 8])
    def split(cuts, rerun):
        for table in gold.TABLES:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
        for start, end in chunks(FIRST, last, cuts):
            gold.run(spark, f"chunk-{next(runs)}", start, end)
        assert_same(backfill, tables(spark), f"split before {sorted(cuts)}")

        gold.run(spark, f"rerun-{next(runs)}", FIRST + timedelta(days=rerun[0]), FIRST + timedelta(days=rerun[1]))
        assert_same(backfill, tables(spark), f"rerun of {rerun}")

    split()


def test_a_late_label_rebuilds_the_label_rows_of_its_attempt_day(spark, lake):
    silver_history(spark, lake)
    gold.run(spark, "gold-1", FIRST, FIRST + timedelta(days=14))
    deliver(
        spark,
        lake,
        bronze.LABELS,
        [label("pay-2", "att-2", available=20 * DAY, source="LABEL_SOURCE_CHARGEBACK", minutes=2 * DAY + 3)],
    )
    arrival = (BASE + timedelta(minutes=20 * DAY)).date()
    silver.run(spark, "silver-labels", arrival, arrival)

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
    assert day_range({}, today) == (date(2026, 10, 28), date(2026, 10, 30))
    assert day_range({"DP_LOOKBACK_DAYS": "0"}, today) == (date(2026, 10, 30), date(2026, 10, 30))
    assert day_range({"DP_START": "2026-09-01", "DP_END": "2026-09-30"}, today) == (
        date(2026, 9, 1),
        date(2026, 9, 30),
    )
