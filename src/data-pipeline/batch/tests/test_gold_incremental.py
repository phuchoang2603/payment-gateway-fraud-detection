import copy
from datetime import timedelta

import pytest
from pyspark.sql import functions as F

from batch import bronze, gold, incremental, silver
from batch.schema import EVENT_COLUMNS, LABEL_COLUMNS, business_schema
from tests.events import BASE, approved, created, label, started, succeeded

TOPICS = {bronze.EVENTS: "payments.events.v1", bronze.LABELS: "fraud.labels.v1"}
COLUMNS = {bronze.EVENTS: EVENT_COLUMNS, bronze.LABELS: LABEL_COLUMNS}
SILVER_TABLES = (silver.PAYMENT_EVENTS, silver.ATTEMPTS, silver.LABELS, silver.REJECTED)
DAY = 24 * 60


@pytest.fixture
def report_root(tmp_path, monkeypatch):
    monkeypatch.setenv("RUN_REPORT_ROOT", str(tmp_path / "reports"))
    return tmp_path / "reports"


@pytest.fixture
def lake(spark, report_root):
    for table in (*gold.TABLES, *SILVER_TABLES, bronze.EVENTS, bronze.LABELS):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    bronze.ensure_tables(spark)
    return {"offsets": {bronze.EVENTS: 0, bronze.LABELS: 0}}


@pytest.fixture
def clock(monkeypatch):
    """The Silver commit time DP3 reads, so a test can move the anchor cap across days."""
    now = {"at": BASE + timedelta(days=31)}
    monkeypatch.setattr(gold, "commit_time", lambda spark, table, snapshot: now["at"])
    return now


def deliver(spark, lake, table, rows):
    for values in rows:
        offset = lake["offsets"][table]
        lake["offsets"][table] += 1
        spark.createDataFrame([values], business_schema(spark, COLUMNS[table])).withColumns(
            {
                "_source": F.lit("kafka"),
                "_dataset_version": F.lit(None).cast("string"),
                "_source_file": F.lit(None).cast("string"),
                "_kafka_topic": F.lit(TOPICS[table]),
                "_kafka_partition": F.lit(0),
                "_kafka_offset": F.lit(offset).cast("bigint"),
                "_kafka_timestamp": F.current_timestamp(),
                "_ingested_at": F.current_timestamp(),
                "_run_id": F.lit("bronze-kafka-test"),
            }
        ).writeTo(table).append()


def start(payment, event, version, minutes, attempt, customer="c-1", card="pm-1", address="f"):
    event_row = copy.deepcopy(started(payment, event, version, minutes, attempt))
    attempt_started = event_row["attempt_started"]
    attempt_started["context"]["customer"]["customer_id"] = customer
    attempt_started["context"]["snapshot"]["shipping"]["address_fingerprint"] = address
    attempt_started["signals"]["payment_method"]["fingerprint"] = card
    return event_row


def historical_gold(spark, clock) -> dict:
    snapshots, frames = gold.silver_inputs(spark)
    attempts = gold.valid_attempts(frames[silver.ATTEMPTS])
    first = gold.history_start(attempts)
    cap = gold.midnight(clock["at"])
    expected = {view.table: gold.features(view, attempts, first, cap, cap) for view in gold.VIEWS}
    expected[gold.DIM_CUSTOMER] = gold.dim_customer(gold.profiles(frames[silver.PAYMENT_EVENTS], attempts))
    expected[gold.LABEL_ATTEMPT] = gold.label_attempt(frames[silver.LABELS], attempts)
    return expected


def assert_matches_historical(spark, clock, step):
    for table, expected in historical_gold(spark, clock).items():
        actual = spark.table(table).drop("created")
        expected = expected.drop("created").select(*actual.columns)
        only_incremental = [row.asDict() for row in actual.exceptAll(expected).collect()]
        only_historical = [row.asDict() for row in expected.exceptAll(actual).collect()]
        assert not only_incremental, f"{step} {table}: rows only in incremental: {only_incremental}"
        assert not only_historical, f"{step} {table}: rows only in historical: {only_historical}"


def test_incremental_gold_equals_a_historical_run_after_every_dp2_batch(spark, lake, clock):
    deliver(
        spark,
        lake,
        bronze.EVENTS,
        [
            created("pay-1", "e-1"),
            start("pay-1", "e-2", 2, 1, "att-1"),
            approved("pay-1", "e-3", 3, 2, "att-1"),
        ],
    )
    silver.run(spark, "silver-base", "historical")
    gold.run(spark, "gold-base", "historical")

    steps = [
        (
            "new-payments",
            32,
            [
                created("pay-2", "f-1", minutes=32 * DAY),
                start("pay-2", "f-2", 2, 32 * DAY + 1, "att-2", customer="c-2", card="pm-2"),
                created("pay-3", "g-1", minutes=32 * DAY + 5),
                start("pay-3", "g-2", 2, 32 * DAY + 6, "att-3", customer="c-3", card="pm-3"),
            ],
            [],
        ),
        (
            "late-chargeback",
            34,
            [succeeded("pay-1", "e-4", 4, 3, "att-1")],
            [label("pay-1", "att-1", available=20 * DAY, source="LABEL_SOURCE_CHARGEBACK", minutes=3)],
        ),
        ("conflicting-redelivery-removes-attempts", 35, [created("pay-2", "f-1", minutes=32 * DAY, amount=999)], []),
        (
            "profile-change",
            36,
            [
                created("pay-4", "h-1", minutes=35 * DAY),
                start("pay-4", "h-2", 2, 35 * DAY + 1, "att-4", address="g"),
            ],
            [],
        ),
        (
            "conflicting-attempt-started",
            37,
            [start("pay-4", "h-2", 2, 35 * DAY + 1, "att-4", address="h")],
            [],
        ),
        (
            "expiry-with-no-new-data-for-the-entity",
            70,
            [
                created("pay-9", "z-1", minutes=69 * DAY),
                start("pay-9", "z-2", 2, 69 * DAY + 1, "att-9", "c-9", "pm-9"),
            ],
            [],
        ),
    ]
    for name, day, events, labels in steps:
        deliver(spark, lake, bronze.EVENTS, events)
        deliver(spark, lake, bronze.LABELS, labels)
        silver.run(spark, f"silver-{name}", "incremental")
        clock["at"] = BASE + timedelta(days=day, hours=3)
        report = gold.run(spark, f"gold-{name}", "incremental")
        assert report["mode"] == "incremental"
        assert_matches_historical(spark, clock, name)

    assert spark.table(gold.FEAT_CUSTOMER).where(F.col("customer_id") == "c-2").count() == 0
    assert spark.table(gold.FEAT_CUSTOMER).where(F.col("customer_id") == "c-3").count() == 3


def based(spark, lake):
    deliver(spark, lake, bronze.EVENTS, [created("pay-1", "e-1"), start("pay-1", "e-2", 2, 1, "att-1")])
    silver.run(spark, "silver-base", "historical")


def test_incremental_gold_refuses_without_a_previous_run(spark, lake, clock):
    based(spark, lake)

    with pytest.raises(incremental.IncrementalRefused, match="no published DP3 run"):
        gold.run(spark, "gold", "incremental")


def test_incremental_gold_refuses_after_a_historical_dp2_run(spark, lake, clock):
    based(spark, lake)
    gold.run(spark, "gold-base", "historical")
    silver.run(spark, "silver-again", "historical")

    with pytest.raises(incremental.IncrementalRefused, match="historical DP2 run"):
        gold.run(spark, "gold", "incremental")


def test_incremental_gold_refuses_an_expired_start_snapshot(spark, lake, clock):
    based(spark, lake)
    gold.run(spark, "gold-base", "historical")
    for n in range(2):
        payment = f"pay-{n + 5}"
        deliver(
            spark,
            lake,
            bronze.EVENTS,
            [created(payment, f"x-{n}", minutes=n), start(payment, f"y-{n}", 2, n + 1, f"att-{n + 5}")],
        )
        silver.run(spark, f"silver-{n}", "incremental")
    spark.sql(
        f"CALL {silver.ICEBERG_CATALOG}.system.expire_snapshots(table => 'silver.attempts', "
        "older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)"
    ).collect()

    with pytest.raises(incremental.IncrementalRefused, match="expired"):
        gold.run(spark, "gold", "incremental")


def test_incremental_gold_refuses_when_the_history_start_moves(spark, lake, clock):
    based(spark, lake)
    gold.run(spark, "gold-base", "historical")
    deliver(
        spark,
        lake,
        bronze.EVENTS,
        [created("pay-0", "w-1", minutes=-2 * DAY), start("pay-0", "w-2", 2, -2 * DAY + 1, "att-0")],
    )
    silver.run(spark, "silver-earlier", "incremental")

    with pytest.raises(incremental.IncrementalRefused, match="earliest Silver attempt day"):
        gold.run(spark, "gold", "incremental")


def test_incremental_gold_refuses_a_changed_logic_version(spark, lake, clock, monkeypatch):
    based(spark, lake)
    gold.run(spark, "gold-base", "historical")
    monkeypatch.setattr(gold, "LOGIC_VERSION", "next")

    with pytest.raises(incremental.IncrementalRefused, match="logic version"):
        gold.run(spark, "gold", "incremental")
