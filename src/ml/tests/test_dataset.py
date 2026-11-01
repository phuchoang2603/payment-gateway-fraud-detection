from datetime import UTC, date, datetime, timedelta

import pytest
from fraud_feast.repo import DAILY_FIELDS, DAILY_TABLES, KEYS, STREAM_FIELDS, TABLES

from risk_model import dataset

FIRST = datetime(2026, 7, 29, tzinfo=UTC)
LAST = date(2026, 8, 2)
CUTOFF = "2026-08-02T00:00:00Z"
ATTEMPTS = 300
ENTITY_SCHEMA = (
    "integration_id string, payment_id string, attempt_id string, customer_id string, "
    "payment_method_fingerprint string, label boolean, started_at timestamp, "
    "event_timestamp timestamp, scored_at timestamp, amount_minor long, "
    "customer_account_created_at timestamp, merchant_account_created_at timestamp, "
    "shipping_country string, ip_country string, item_count long, has_device boolean"
)


def seed(spark):
    for scope, key in KEYS.items():
        fields = DAILY_FIELDS[scope]
        spark.createDataFrame(
            [
                (
                    "i",
                    "c0" if scope == "customer" else "m0",
                    FIRST - timedelta(days=1),
                    FIRST,
                    *[0.5 if name.startswith("decline_rate") else 4 for name in fields],
                )
            ],
            f"integration_id string, {key} string, event_timestamp timestamp, created timestamp, "
            + ", ".join(
                f"{name} {'double' if name.startswith('decline_rate') else 'long'}"
                for name in fields
            ),
        ).writeTo(f"lake.{DAILY_TABLES[scope]}").using("iceberg").createOrReplace()
        stream = STREAM_FIELDS[scope]
        spark.createDataFrame(
            [
                (
                    "i",
                    "c0" if scope == "customer" else "m0",
                    at,
                    FIRST,
                    "history-ci",
                    *[2] * len(stream),
                )
                for at in (FIRST + timedelta(seconds=1), FIRST + timedelta(days=3, hours=23))
            ],
            f"integration_id string, {key} string, event_timestamp timestamp, created timestamp, "
            "epoch string, " + ", ".join(f"{name} long" for name in stream),
        ).writeTo(f"lake.{TABLES[scope]}").using("iceberg").createOrReplace()
    for name in dataset.LABEL_TABLES:
        spark.createDataFrame([(1,)], "x int").writeTo(f"lake.{name}").using(
            "iceberg"
        ).createOrReplace()


def entities(spark):
    rows = []
    for number in range(ATTEMPTS):
        started = FIRST + timedelta(minutes=17 * number)
        rows.append(
            (
                "i",
                f"payment-{number}",
                f"attempt-{number}",
                f"c{number % 3}",
                f"m{number % 3}",
                number % 15 == 0,
                started,
                started,
                started,
                1000 + number,
                started - timedelta(days=30),
                started - timedelta(days=90),
                "US",
                "CA" if number % 2 else "US",
                2,
                True,
            )
        )
    return spark.createDataFrame(rows, ENTITY_SCHEMA)


@pytest.fixture
def prepared(spark, feature_store, monkeypatch):
    spark.sql("DROP TABLE IF EXISTS lake.ml.payment_risk_v1_examples")
    seed(spark)
    monkeypatch.setattr(dataset, "entity_rows", lambda session, *_: entities(session))
    return spark, feature_store


def examples(spark):
    return spark.table("lake.ml.payment_risk_v1_examples")


def test_negatives_are_sampled_by_stable_bucket_and_every_positive_is_kept(prepared):
    spark, store = prepared
    narrow = dataset.build(spark, store, FIRST.date(), LAST, CUTOFF, sample_buckets=100)
    kept = {tuple(row) for row in examples(spark).select("attempt_id").collect()}
    positives = {f"attempt-{n}" for n in range(ATTEMPTS) if n % 15 == 0}
    assert narrow.positives == len(positives)
    assert {name for (name,) in kept} >= positives
    assert len(kept) < ATTEMPTS
    dataset.build(spark, store, FIRST.date(), LAST, CUTOFF, sample_buckets=400)
    broader = {tuple(row) for row in examples(spark).select("attempt_id").collect()}
    assert kept < broader


def test_features_come_from_the_service_and_nulls_stay_null(prepared):
    spark, store = prepared
    built = dataset.build(spark, store, FIRST.date(), LAST, CUTOFF, sample_buckets=1000)
    frame = examples(spark).toPandas().set_index("attempt_id")
    assert built.features == dataset.feature_names(store)
    assert frame.loc["attempt-0", "request_features__amount_minor"] == 1000
    assert frame.loc["attempt-1", "request_features__ip_shipping_country_mismatch"]
    assert frame.loc["attempt-0", "customer_daily__attempt_count_7d"] == 4
    assert frame.loc["attempt-0", "request_features__scored_hour_utc"] == 0
    assert frame.loc["attempt-0", "request_features__customer_amount_to_30d_avg"] == 1000.0
    assert frame["customer_stream__customer_snapshot_attempt_count_5m"].isna().any()


def test_build_fails_without_writing_when_a_source_table_moves(prepared, monkeypatch):
    spark, store = prepared

    def moving(session, *_):
        session.createDataFrame([(2,)], "x int").writeTo("lake.gold.label_attempt").append()
        return entities(session)

    monkeypatch.setattr(dataset, "entity_rows", moving)
    with pytest.raises(RuntimeError, match="changed during the build"):
        dataset.build(spark, store, FIRST.date(), LAST, CUTOFF)
    assert not spark.catalog.tableExists("lake.ml.payment_risk_v1_examples")


def test_identical_rebuild_adds_nothing_and_old_version_stays_readable(prepared):
    spark, store = prepared
    first = dataset.build(spark, store, FIRST.date(), LAST, CUTOFF)
    count = examples(spark).count()
    second = dataset.build(spark, store, FIRST.date(), LAST, CUTOFF)
    assert examples(spark).count() == count
    if second.changed:
        summary = dict(
            spark.sql(
                "SELECT summary FROM lake.ml.payment_risk_v1_examples.snapshots "
                f"WHERE snapshot_id = {second.snapshot_id}"
            ).first()[0]
        )
        assert int(summary.get("added-records", 0)) == 0
        assert int(summary.get("added-position-deletes", 0)) == 0
    pinned = spark.read.option("versionAsOf", first.snapshot_id).table(
        "lake.ml.payment_risk_v1_examples"
    )
    assert pinned.count() == count


def test_restaging_a_chunk_replaces_its_rows(prepared):
    spark, store = prepared
    dataset.prepare(spark, store, FIRST.date(), LAST)
    first = dataset.stage(spark, store, FIRST.date(), LAST, CUTOFF)
    second = dataset.stage(spark, store, FIRST.date(), LAST, CUTOFF)
    staged = spark.table(f"lake.{dataset.STAGE}").count()
    spark.sql(f"DROP TABLE lake.{dataset.STAGE}")
    assert first == second == staged > 0
