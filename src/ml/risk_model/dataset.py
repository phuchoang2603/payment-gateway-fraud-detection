import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta

import pyarrow as pa
from feast.type_map import feast_value_type_to_pa
from fraud_feast.repo import DAILY_TABLES, TABLES, epochs
from fraud_feast.training import entity_rows
from pyspark.sql import functions as F

from risk_model.data import BUCKETS, KEY, TABLE

SERVICE = "payment_risk_v1"
STAGE = "ml.payment_risk_v1_stage"
SAMPLE_BUCKETS = 100
CHUNK_DAYS = 16
LABEL_TABLES = ("gold.label_attempt", "silver.attempts", "silver.payment_events")
PREFIX = "spark.sql.iceberg.snapshot-property."


@dataclass
class Build:
    snapshot_id: int
    changed: bool
    rows: int
    positives: int
    sources: dict = field(default_factory=dict)
    features: list = field(default_factory=list)
    window: str = ""
    cutoff: str = ""


def sources():
    return [f"lake.{name}" for name in (*LABEL_TABLES, *DAILY_TABLES.values(), *TABLES.values())]


def current(spark, table):
    row = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first()
    return None if row is None else row[0]


def fence(spark):
    return {table: current(spark, table) for table in sources()}


def feature_names(store):
    return [
        f"{projection.name_to_use()}__{feature.name}"
        for projection in store.get_feature_service(SERVICE).feature_view_projections
        for feature in projection.features
    ]


def check_coverage(spark, start, end):
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


def day_rows(store, rows, day, staging):
    start = datetime.combine(day, time.min, UTC)
    store.config.offline_store.staging_location = str(staging)
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
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.ml")
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
    return f"{directory}/fence.json"


def chunks(first, last, days=CHUNK_DAYS):
    starts = [first + timedelta(days=offset) for offset in range(0, (last - first).days, days)]
    return [(start, min(start + timedelta(days=days), last)) for start in starts]


def prepare(spark, store, first, last):
    check_coverage(spark, first, last)
    before = fence(spark)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.ml")
    spark.sql(f"DROP TABLE IF EXISTS lake.{STAGE}")
    spark.createDataFrame(stage_schema(store).empty_table()).writeTo(f"lake.{STAGE}").using(
        "iceberg"
    ).partitionedBy(F.days("started_at")).create()
    return before


def stage(spark, store, first, last, cutoff, sample_buckets=SAMPLE_BUCKETS):
    rows = entity_rows(spark, first.isoformat(), last.isoformat(), cutoff).withColumn(
        "bucket", F.pmod(F.xxhash64(*KEY), F.lit(BUCKETS)).cast("int")
    )
    rows = rows.where(F.col("label") | (F.col("bucket") < sample_buckets))
    schema = stage_schema(store)
    days = []
    with tempfile.TemporaryDirectory(dir=os.environ.get("SPARK_LOCAL_DIR")) as staging:
        for number in range((last - first).days):
            table = day_rows(store, rows, first + timedelta(days=number), staging)
            if table.num_rows:
                days.append(table.select(schema.names).cast(schema))
    start = datetime.combine(first, time.min, UTC).isoformat()
    end = datetime.combine(last, time.min, UTC).isoformat()
    spark.sql(
        f"DELETE FROM lake.{STAGE} WHERE started_at >= TIMESTAMP '{start}' "
        f"AND started_at < TIMESTAMP '{end}'"
    )
    if days:
        spark.createDataFrame(pa.concat_tables(days)).writeTo(f"lake.{STAGE}").append()
    return sum(table.num_rows for table in days)


def merge_examples(spark, store, first, last, cutoff, before, sample_buckets=SAMPLE_BUCKETS):
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


def build(spark, store, start, end, cutoff, sample_buckets=SAMPLE_BUCKETS):
    first, last = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    before = prepare(spark, store, first, last)
    try:
        for chunk_start, chunk_end in chunks(first, last):
            stage(spark, store, chunk_start, chunk_end, cutoff, sample_buckets)
        return merge_examples(spark, store, first, last, cutoff, before, sample_buckets)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS lake.{STAGE}")


def session(store):
    from pyspark.sql import SparkSession

    configuration = {
        "spark.sql.shuffle.partitions": os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"),
        **store.config.offline_store.spark_conf,
    }
    if os.environ.get("SPARK_LOCAL_DIR"):
        configuration["spark.local.dir"] = os.environ["SPARK_LOCAL_DIR"]
    return SparkSession.builder.config(map=configuration).getOrCreate()


def feast_store():
    from fraud_feast.runtime import store

    return store(redis=False)
