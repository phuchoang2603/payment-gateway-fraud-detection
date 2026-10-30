import pytest

from batch.bronze import ingest as bronze
from batch.silver import curate as silver
from batch.silver import incremental
from tests.events import created, label, started, succeeded
from tests.lake import deliver, load_bronze

SILVER_TABLES = (silver.PAYMENT_EVENTS, silver.ATTEMPTS, silver.LABELS, silver.REJECTED)
DAY = 24 * 60


@pytest.fixture
def lake(spark):
    for table in (*SILVER_TABLES, bronze.EVENTS, bronze.LABELS):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    bronze.ensure_tables(spark)
    return {"offsets": {bronze.EVENTS: 0, bronze.LABELS: 0}}


def comparable(frame, table):
    return frame.drop("_run_id") if table == silver.REJECTED else frame


def assert_matches_historical(spark, run_id):
    """The incremental run's Silver tables equal a historical run over the same Bronze."""
    pinned = {table: silver.current_snapshot(spark, table) for table in SILVER_TABLES}
    silver.run(spark, f"{run_id}-historical", "historical")
    for table, snapshot in pinned.items():
        incremental_rows = comparable(spark.read.option("versionAsOf", snapshot).table(table), table)
        historical_rows = comparable(spark.table(table), table)
        only_incremental = [row.asDict() for row in incremental_rows.exceptAll(historical_rows).collect()]
        only_historical = [row.asDict() for row in historical_rows.exceptAll(incremental_rows).collect()]
        assert not only_incremental, f"{run_id} {table}: rows only in incremental: {only_incremental}"
        assert not only_historical, f"{run_id} {table}: rows only in historical: {only_historical}"


def test_incremental_runs_equal_a_historical_run_after_every_batch(spark, lake, report_root):
    deliver(spark, lake, bronze.EVENTS, [created("pay-1", "e-1"), started("pay-1", "e-2", 2, 1, "att-1")])
    silver.run(spark, "base", "historical")

    batches = [
        (
            "new-payment-and-late-duplicate",
            [created("pay-2", "f-1", minutes=10), created("pay-1", "e-1", recorded=5)],
            [],
        ),
        ("conflicting-redelivery", [created("pay-2", "f-1", minutes=10, amount=999)], []),
        ("label-before-payment", [], [label("pay-3", "att-3", available=30)]),
        ("payment-after-label", [created("pay-3", "g-1", minutes=20), started("pay-3", "g-2", 2, 21, "att-3")], []),
        (
            "chargeback-20-days-later",
            [succeeded("pay-1", "e-3", 3, 2, "att-1")],
            [label("pay-1", "att-1", available=20 * DAY, source="LABEL_SOURCE_CHARGEBACK", minutes=3)],
        ),
    ]
    for name, events, labels in batches:
        deliver(spark, lake, bronze.EVENTS, events)
        deliver(spark, lake, bronze.LABELS, labels)
        report = silver.run(spark, name, "incremental")
        assert report["affected_payments"] >= 1, name
        assert_matches_historical(spark, name)

    report = silver.run(spark, "nothing-new", "incremental")
    assert report["affected_payments"] == 0


def test_the_attempts_changelog_shows_only_the_changed_attempt(spark, lake, report_root):
    deliver(
        spark,
        lake,
        bronze.EVENTS,
        [
            created("pay-1", "e-1"),
            started("pay-1", "e-2", 2, 1, "att-1"),
            created("pay-2", "f-1"),
            started("pay-2", "f-2", 2, 1, "att-2"),
        ],
    )
    silver.run(spark, "base", "historical")
    before = silver.current_snapshot(spark, silver.ATTEMPTS)
    deliver(spark, lake, bronze.EVENTS, [succeeded("pay-1", "e-3", 3, 2, "att-1")])

    silver.run(spark, "inc-1", "incremental")

    after = silver.current_snapshot(spark, silver.ATTEMPTS)
    view = spark.sql(
        f"CALL {silver.ICEBERG_CATALOG}.system.create_changelog_view("
        "table => 'silver.attempts', "
        f"options => map('start-snapshot-id', '{before}', 'end-snapshot-id', '{after}'), "
        "net_changes => true)"
    ).first()[0]
    changes = {(row.attempt_id, row._change_type, row.outcome) for row in spark.table(view).collect()}
    assert changes == {("att-1", "DELETE", "pending"), ("att-1", "INSERT", "succeeded")}


def test_incremental_refuses_without_a_previous_run(spark, lake, report_root):
    deliver(spark, lake, bronze.EVENTS, [created("pay-1", "e-1")])

    with pytest.raises(incremental.IncrementalRefused, match="no published Silver run"):
        silver.run(spark, "inc", "incremental")


def test_incremental_refuses_after_a_historical_bronze_reload(spark, lake, report_root):
    deliver(spark, lake, bronze.EVENTS, [created("pay-1", "e-1")])
    silver.run(spark, "base", "historical")
    load_bronze(spark, [created("pay-9", "z-1")], [])

    with pytest.raises(incremental.IncrementalRefused, match="Bronze changed historical rows"):
        silver.run(spark, "inc", "incremental")


def test_incremental_refuses_a_changed_logic_version(spark, lake, report_root, monkeypatch):
    deliver(spark, lake, bronze.EVENTS, [created("pay-1", "e-1")])
    silver.run(spark, "base", "historical")
    monkeypatch.setattr(incremental, "LOGIC_VERSION", "next")

    with pytest.raises(incremental.IncrementalRefused, match="logic version"):
        silver.run(spark, "inc", "incremental")


def test_incremental_refuses_payments_with_rows_before_the_lookback(spark, lake, report_root, monkeypatch):
    deliver(spark, lake, bronze.EVENTS, [created("pay-1", "e-1"), started("pay-1", "e-2", 2, 1, "att-1")])
    silver.run(spark, "base", "historical")
    monkeypatch.setenv(incremental.LOOKBACK_ENV, "1")
    deliver(
        spark,
        lake,
        bronze.LABELS,
        [label("pay-1", "att-1", available=20 * DAY, source="LABEL_SOURCE_CHARGEBACK", minutes=3)],
    )

    with pytest.raises(incremental.IncrementalRefused, match="lookback"):
        silver.run(spark, "inc", "incremental")
