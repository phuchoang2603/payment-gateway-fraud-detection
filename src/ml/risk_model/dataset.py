"""Builds the training set through Feast: plan, fence, stage chunks of days, then merge into Iceberg."""

import json
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta

import pyarrow as pa
from feast.type_map import feast_value_type_to_pa
from fraud_feast.repo import DAILY_TABLES, TABLES, epochs
from fraud_feast.training import entity_rows
from pyspark.sql import functions as F

from risk_model.data import BUCKETS, CHUNK_DAYS, KEY, TABLE

SERVICE = "payment_risk_v1"
STAGE = "ml.payment_risk_v1_stage"
SAMPLE_BUCKETS = 100
LABEL_TABLES = ("gold.label_attempt", "silver.attempts", "silver.payment_events")
PREFIX = "spark.sql.iceberg.snapshot-property."


@dataclass
class Build:
    """What one build wrote: the new snapshot, whether it changed anything, counts and provenance."""

    snapshot_id: int
    changed: bool
    rows: int
    positives: int
    sources: dict = field(default_factory=dict)
    features: list = field(default_factory=list)
    window: str = ""
    cutoff: str = ""


def sources():
    """Returns the Iceberg tables whose snapshots a build records and fences."""
    return [f"lake.{name}" for name in (*LABEL_TABLES, *DAILY_TABLES.values(), *TABLES.values())]


def current(spark, table):
    """Returns the current snapshot ID of a table, or None when it has none."""
    row = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first()
    return None if row is None else row[0]


def fence(spark):
    """Returns the current snapshot ID of every source table."""
    return {table: current(spark, table) for table in sources()}


def feature_names(store):
    """Returns the `<view>__<feature>` names of the feature service, in order."""
    return [
        f"{projection.name_to_use()}__{feature.name}"
        for projection in store.get_feature_service(SERVICE).feature_view_projections
        for feature in projection.features
    ]


def check_coverage(spark, start, end):
    """Raises ValueError unless the stream tables cover `[start, end)` for the configured epochs."""
    selected = ",".join(f"'{value}'" for value in epochs())
    first, last = None, None
    for table in TABLES.values():
        low, high = spark.sql(
            f"SELECT min(event_timestamp), max(event_timestamp) FROM lake.{table} "
            f"WHERE epoch IN ({selected})"
        ).first()
        if low is None:
            raise ValueError(f"lake.{table} has no rows for stream epochs {selected}")
        low, high = low.astimezone(UTC).date(), high.astimezone(UTC).date()
        first = low if first is None else max(first, low)
        last = high if last is None else min(last, high)
    if start < first or end > last + timedelta(days=1):
        raise ValueError(f"stream history covers {first}..{last}, not {start}..{end}")


def day_rows(store, rows, day):
    """Returns one day of entity rows joined with the feature service's values, as an Arrow table."""
    start = datetime.combine(day, time.min, UTC)
    job = store.get_historical_features(
        entity_df=rows.where(
            (F.col("started_at") >= F.lit(start))
            & (F.col("started_at") < F.lit(start + timedelta(days=1)))
        ),
        features=store.get_feature_service(SERVICE),
        full_feature_names=True,
    )
    return job.to_arrow()


def stage_schema(store):
    """Returns the Arrow schema of staged rows: keys, time, label, bucket and every feature."""
    features = {
        f"{projection.name_to_use()}__{feature.name}": feast_value_type_to_pa(
            feature.dtype.to_value_type()
        )
        for projection in store.get_feature_service(SERVICE).feature_view_projections
        for feature in projection.features
    }
    return pa.schema(
        [
            *[(name, pa.string()) for name in KEY],
            ("started_at", pa.timestamp("us", tz="UTC")),
            ("label", pa.bool_()),
            ("bucket", pa.int32()),
            *features.items(),
        ]
    )


def ensure_table(spark, frame):
    """Creates the examples table, merge-on-read and partitioned by day, if it does not exist."""
    if spark.catalog.tableExists(f"lake.{TABLE}"):
        return
    (
        frame.limit(0)
        .writeTo(f"lake.{TABLE}")
        .using("iceberg")
        .partitionedBy(F.days("started_at"))
        .tableProperty("format-version", "2")
        .tableProperty("write.merge.mode", "merge-on-read")
        .tableProperty("write.delete.mode", "merge-on-read")
        .tableProperty("write.update.mode", "merge-on-read")
        .create()
    )


def merge(spark, frame, properties):
    """Merges rows into the examples table, deleting rows that left the window, and records the properties.

    The properties are written into the new snapshot's summary through the session configuration.
    """
    keys = " AND ".join(f"t.{name} = s.{name}" for name in (*KEY, "row_hash"))
    frame.createOrReplaceTempView("examples")
    for name, value in properties.items():
        spark.conf.set(PREFIX + name, value)
    try:
        spark.sql(
            f"MERGE INTO lake.{TABLE} t USING examples s ON {keys} "
            "WHEN NOT MATCHED THEN INSERT * "
            "WHEN NOT MATCHED BY SOURCE THEN DELETE"
        )
    finally:
        for name in properties:
            spark.conf.unset(PREFIX + name)


def fence_path(directory):
    """Returns where a run keeps the source snapshots it fenced."""
    return f"{directory}/fence.json"


def chunks(first, last, days=CHUNK_DAYS):
    """Returns `(start, end)` pairs of at most `days` days that tile `[first, last)`."""
    starts = [first + timedelta(days=offset) for offset in range(0, (last - first).days, days)]
    return [(start, min(start + timedelta(days=days), last)) for start in starts]


def prepare(spark, store, first, last):
    """Checks coverage, fences the sources, creates an empty staging table and returns the fence."""
    check_coverage(spark, first, last)
    before = fence(spark)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.ml")
    drop_stage(spark)
    spark.createDataFrame(stage_schema(store).empty_table()).writeTo(f"lake.{STAGE}").using(
        "iceberg"
    ).partitionedBy(F.days("started_at")).create()
    return before


def drop_stage(spark):
    """Drops the staging table if it exists."""
    spark.sql(f"DROP TABLE IF EXISTS lake.{STAGE}")


def stage(spark, store, first, last, cutoff, sample_buckets=SAMPLE_BUCKETS):
    """Stages the sampled rows of `[first, last)` with their features and returns how many it staged.

    Restaging a range replaces its rows, so a retried chunk never duplicates them.
    """
    rows = entity_rows(spark, first.isoformat(), last.isoformat(), cutoff).withColumn(
        "bucket", F.pmod(F.xxhash64(*KEY), F.lit(BUCKETS)).cast("int")
    )
    rows = rows.where(F.col("label") | (F.col("bucket") < sample_buckets))
    schema = stage_schema(store)
    start = datetime.combine(first, time.min, UTC).isoformat()
    end = datetime.combine(last, time.min, UTC).isoformat()
    spark.sql(
        f"DELETE FROM lake.{STAGE} WHERE started_at >= TIMESTAMP '{start}' "
        f"AND started_at < TIMESTAMP '{end}'"
    )
    staged = 0
    with tempfile.TemporaryDirectory(dir=os.environ.get("SPARK_LOCAL_DIR")) as staging:
        store.config.offline_store.staging_location = staging
        for number in range((last - first).days):
            table = day_rows(store, rows, first + timedelta(days=number))
            if table.num_rows:
                spark.createDataFrame(table.select(schema.names).cast(schema)).writeTo(
                    f"lake.{STAGE}"
                ).append()
                staged += table.num_rows
            del table
    return staged


def merge_examples(spark, store, first, last, cutoff, before, sample_buckets=SAMPLE_BUCKETS):
    """Merges the staged rows into the examples table as a new version, then drops the staging table.

    `before` is the fence from `prepare`. Raises if nothing was staged or a source table moved since
    then, in which case nothing is written. Returns a Build.
    """
    features = feature_names(store)
    frame = spark.table(f"lake.{STAGE}").withColumn(
        "row_hash", F.xxhash64(F.col("label"), *[F.col(name) for name in features])
    )
    counts = frame.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("label").cast("int")).alias("positives"),
    ).first()
    if not counts.rows:
        raise ValueError(f"no training rows for {first}..{last} at cutoff {cutoff}")
    after = fence(spark)
    if after != before:
        moved = sorted(name for name in before if before[name] != after[name])
        raise RuntimeError(f"source tables changed during the build: {', '.join(moved)}")
    ensure_table(spark, frame)
    previous = current(spark, f"lake.{TABLE}")
    merge(
        spark,
        frame,
        {
            "ml.sources": json.dumps(before, sort_keys=True),
            "ml.window": f"{first}..{last}",
            "ml.cutoff": str(cutoff),
            "ml.sample_rate": str(sample_buckets / BUCKETS),
            "ml.feature_service": SERVICE,
            "ml.features": json.dumps(features),
        },
    )
    snapshot = current(spark, f"lake.{TABLE}")
    drop_stage(spark)
    return Build(
        snapshot_id=snapshot,
        changed=snapshot != previous,
        rows=counts.rows,
        positives=counts.positives or 0,
        sources=before,
        features=features,
        window=f"{first}..{last}",
        cutoff=str(cutoff),
    )


def session(store):
    """Returns the local Spark session configured from the Feast offline store."""
    from pyspark.sql import SparkSession

    configuration = {
        "spark.sql.shuffle.partitions": os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"),
        **store.config.offline_store.spark_conf,
    }
    if os.environ.get("SPARK_LOCAL_DIR"):
        configuration["spark.local.dir"] = os.environ["SPARK_LOCAL_DIR"]
    return SparkSession.builder.config(map=configuration).getOrCreate()


def feast_store():
    """Returns the Feast store with no online store, for reading training data."""
    from fraud_feast.runtime import store

    return store(redis=False)


@contextmanager
def spark_session():
    """Yields the Feast store and a Spark session on it, and stops the session on exit."""
    store = feast_store()
    spark = session(store)
    try:
        yield store, spark
    finally:
        spark.stop()
