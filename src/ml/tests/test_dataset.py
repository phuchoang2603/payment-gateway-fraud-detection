"""Dataset build tests on a local Spark warehouse and a Feast test repository."""

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


def build(spark, store, start, end, cutoff, sample_buckets=dataset.SAMPLE_BUCKETS):
    """Runs the pipeline's dataset steps in order: prepare, stage each chunk, then merge."""
    before = dataset.prepare(spark, store, start, end)
    try:
        for first, last in dataset.chunks(start, end):
            dataset.stage(spark, store, first, last, cutoff, sample_buckets)
        return dataset.merge_examples(spark, store, start, end, cutoff, before, sample_buckets)
    finally:
        dataset.drop_stage(spark)


def seed(spark):
    """Writes one daily row and two stream rows per entity, and the label tables, as source data."""
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
    """Returns 300 synthetic attempts, every 15th a fraud, as the entity rows Feast would be given."""
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
    """Seeds the sources, stubs the entity rows and returns the Spark session and the Feast store."""
    spark.sql("DROP TABLE IF EXISTS lake.ml.payment_risk_v1_examples")
    seed(spark)
    monkeypatch.setattr(dataset, "entity_rows", lambda session, *_: entities(session))
    return spark, feature_store


def examples(spark):
    """Returns the examples table."""
    return spark.table("lake.ml.payment_risk_v1_examples")


def test_negatives_are_sampled_by_stable_bucket_and_every_positive_is_kept(prepared):
    """A narrower sample is a subset of a broader one and always keeps every positive."""
    spark, store = prepared
    narrow = build(spark, store, FIRST.date(), LAST, CUTOFF, sample_buckets=100)
    kept = {tuple(row) for row in examples(spark).select("attempt_id").collect()}
    positives = {f"attempt-{n}" for n in range(ATTEMPTS) if n % 15 == 0}
    assert narrow.positives == len(positives)
    assert {name for (name,) in kept} >= positives
    assert len(kept) < ATTEMPTS
    build(spark, store, FIRST.date(), LAST, CUTOFF, sample_buckets=400)
    broader = {tuple(row) for row in examples(spark).select("attempt_id").collect()}
    assert kept < broader


def test_features_come_from_the_service_and_nulls_stay_null(prepared):
    """Feature columns carry the service's values, and a missing value stays null."""
    spark, store = prepared
    built = build(spark, store, FIRST.date(), LAST, CUTOFF, sample_buckets=1000)
    frame = examples(spark).toPandas().set_index("attempt_id")
    assert built.features == dataset.feature_names(store)
    assert frame.loc["attempt-0", "request_features__amount_minor"] == 1000
    assert frame.loc["attempt-1", "request_features__ip_shipping_country_mismatch"]
    assert frame.loc["attempt-0", "customer_daily__attempt_count_7d"] == 4
    assert frame.loc["attempt-0", "request_features__scored_hour_utc"] == 0
    assert frame.loc["attempt-0", "request_features__customer_amount_to_30d_avg"] == 1000.0
    assert frame["customer_stream__customer_snapshot_attempt_count_5m"].isna().any()


def test_build_fails_without_writing_when_a_source_table_moves(prepared, monkeypatch):
    """A source table changing during the build fails it before anything is written."""
    spark, store = prepared

    def moving(session, *_):
        session.createDataFrame([(2,)], "x int").writeTo("lake.gold.label_attempt").append()
        return entities(session)

    monkeypatch.setattr(dataset, "entity_rows", moving)
    with pytest.raises(RuntimeError, match="changed during the build"):
        build(spark, store, FIRST.date(), LAST, CUTOFF)
    assert not spark.catalog.tableExists("lake.ml.payment_risk_v1_examples")


def test_identical_rebuild_adds_nothing_and_old_version_stays_readable(prepared):
    """Rebuilding the same range adds no rows, and an earlier snapshot still reads."""
    spark, store = prepared
    first = build(spark, store, FIRST.date(), LAST, CUTOFF)
    count = examples(spark).count()
    second = build(spark, store, FIRST.date(), LAST, CUTOFF)
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
    """Staging a range twice leaves one copy of its rows."""
    spark, store = prepared
    dataset.prepare(spark, store, FIRST.date(), LAST)
    first = dataset.stage(spark, store, FIRST.date(), LAST, CUTOFF)
    second = dataset.stage(spark, store, FIRST.date(), LAST, CUTOFF)
    staged = spark.table(f"lake.{dataset.STAGE}").count()
    spark.sql(f"DROP TABLE lake.{dataset.STAGE}")
    assert first == second == staged > 0


def test_retry_after_a_partial_stage_replaces_the_whole_chunk(prepared, monkeypatch):
    """An interrupted append followed by a retry leaves every attempt exactly once."""
    spark, store = prepared
    dataset.prepare(spark, store, FIRST.date(), LAST)
    day_rows = dataset.day_rows

    def interrupted(*args):
        if args[-1] == FIRST.date() + timedelta(days=1):
            raise RuntimeError("interrupted staging")
        return day_rows(*args)

    monkeypatch.setattr(dataset, "day_rows", interrupted)
    with pytest.raises(RuntimeError, match="interrupted staging"):
        dataset.stage(spark, store, FIRST.date(), LAST, CUTOFF)
    partial = spark.table(f"lake.{dataset.STAGE}").count()
    assert partial > 0
    monkeypatch.setattr(dataset, "day_rows", day_rows)
    total = dataset.stage(spark, store, FIRST.date(), LAST, CUTOFF)
    staged = spark.table(f"lake.{dataset.STAGE}")
    assert total > partial
    assert staged.count() == staged.select("attempt_id").distinct().count() == total
    dataset.drop_stage(spark)


def test_a_finished_build_drops_its_staging_table(prepared):
    """The staging table is gone once the rows are merged, and dropping it again is harmless."""
    spark, store = prepared
    build(spark, store, FIRST.date(), LAST, CUTOFF)
    assert not spark.catalog.tableExists(f"lake.{dataset.STAGE}")
    dataset.drop_stage(spark)


def test_a_merge_with_nothing_staged_fails(prepared):
    """Merging an empty staging table raises and writes nothing."""
    spark, store = prepared
    before = dataset.prepare(spark, store, FIRST.date(), LAST)
    with pytest.raises(ValueError, match="no training rows"):
        dataset.merge_examples(spark, store, FIRST.date(), LAST, CUTOFF, before)
    dataset.drop_stage(spark)
    assert not spark.catalog.tableExists("lake.ml.payment_risk_v1_examples")


def test_coverage_must_include_the_whole_range(prepared):
    """A range that starts before or ends after the stream history is rejected."""
    spark, _ = prepared
    dataset.check_coverage(spark, FIRST.date(), LAST)
    with pytest.raises(ValueError, match="stream history covers"):
        dataset.check_coverage(spark, FIRST.date() - timedelta(days=1), LAST)
    with pytest.raises(ValueError, match="stream history covers"):
        dataset.check_coverage(spark, FIRST.date(), LAST + timedelta(days=1))


def test_chunks_tile_the_range_without_gaps_or_overlap():
    """Chunks are at most the given length, adjacent, and cover the range; an empty range has none."""
    pieces = dataset.chunks(date(2026, 7, 29), date(2026, 8, 20), 10)
    assert pieces == [
        (date(2026, 7, 29), date(2026, 8, 8)),
        (date(2026, 8, 8), date(2026, 8, 18)),
        (date(2026, 8, 18), date(2026, 8, 20)),
    ]
    assert dataset.chunks(date(2026, 7, 29), date(2026, 7, 29), 10) == []
    assert dataset.chunks(date(2026, 7, 29), date(2026, 7, 30), 16) == [
        (date(2026, 7, 29), date(2026, 7, 30))
    ]


def test_the_stage_schema_lists_keys_then_the_service_features(feature_store):
    """Staged rows carry the attempt key, start time, label, bucket and every feature in order."""
    names = dataset.stage_schema(feature_store).names
    assert names[:6] == [*dataset.KEY, "started_at", "label", "bucket"]
    assert names[6:] == dataset.feature_names(feature_store)


def test_the_fence_path_is_inside_the_run_directory():
    """The fence file sits next to the run's other files."""
    assert dataset.fence_path("s3://b/runs/r1") == "s3://b/runs/r1/fence.json"
