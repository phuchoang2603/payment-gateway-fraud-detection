import json
import os
import statistics
import time
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from batch import bronze, silver
from batch.benchmark import same_output
from batch.lakehouse import WRITE_PROPERTIES, file_stats
from batch.spark import ICEBERG_CATALOG, session
from batch.store import Store

NAMESPACE = f"{ICEBERG_CATALOG}.benchmark"
RUNS = 3
SMALL_APPENDS = 40
ZORDER = "zorder(customer_id,payment_method_fingerprint)"


def micros(spark: SparkSession, value: datetime):
    epoch_micros = int(value.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
    return spark._jvm.java.lang.Long(epoch_micros)


def day_filter(spark: SparkSession, column: str, day: date):
    expressions = spark._jvm.org.apache.iceberg.expressions.Expressions
    start = datetime(day.year, day.month, day.day)
    return getattr(expressions, "and")(
        expressions.greaterThanOrEqual(column, micros(spark, start)),
        expressions.lessThan(column, micros(spark, start + timedelta(days=1))),
    )


def customer_window(spark: SparkSession, customer: str, start: datetime):
    expressions = spark._jvm.org.apache.iceberg.expressions.Expressions
    return getattr(expressions, "and")(
        expressions.equal("customer_id", customer),
        expressions.greaterThanOrEqual("started_at", micros(spark, start)),
        expressions.lessThan("started_at", micros(spark, start + timedelta(days=30))),
    )


def planned(spark: SparkSession, name: str, expression):
    """Files and bytes Iceberg's planner selects for expression on the table's current snapshot."""
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    tasks = table.newScan().filter(expression).planFiles()
    files = size = 0
    iterator = tasks.iterator()
    while iterator.hasNext():
        files += 1
        size += iterator.next().file().fileSizeInBytes()
    tasks.close()
    return {"files_planned": files, "bytes_planned": size}


def profile(df: DataFrame):
    df.collect()
    times = []
    for _ in range(RUNS):
        started = time.perf_counter()
        df.collect()
        times.append(time.perf_counter() - started)
    return {"median_seconds": statistics.median(times)}


def create_copy(spark: SparkSession, name: str, source: DataFrame, partition=None) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {name} PURGE")
    creator = source.limit(0).writeTo(name).using("iceberg")
    for key, value in WRITE_PROPERTIES.items():
        creator = creator.tableProperty(key, value)
    if partition is not None:
        creator = creator.partitionedBy(partition)
    creator.create()
    source.writeTo(name).append()


def summary(df: DataFrame) -> DataFrame:
    return df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("event_id").alias("events"),
        F.max("recorded_at").alias("latest_recorded_at"),
    )


def compaction(spark: SparkSession, source: DataFrame):
    table = f"{NAMESPACE}.bronze_small_appends"
    create_copy(spark, table, source.limit(0))
    for piece in source.randomSplit([1.0] * SMALL_APPENDS, seed=7):
        piece.writeTo(table).append()
    before_df = summary(spark.table(table))
    before = {**profile(before_df), **file_stats(spark, table)}
    spark.sql(f"CALL {ICEBERG_CATALOG}.system.rewrite_data_files(table => '{table}', strategy => 'binpack')").collect()
    after_df = summary(spark.table(table))
    after = {**profile(after_df), **file_stats(spark, table)}
    same_output(before_df, after_df)
    return {"before": before, "after": after}


def partition_pruning(spark: SparkSession, source: DataFrame):
    partitioned = bronze.EVENTS
    unpartitioned = f"{NAMESPACE}.events_unpartitioned"
    snapshot = bronze.current_snapshot(spark, partitioned)
    copy = spark.read.option("versionAsOf", snapshot).table(partitioned)
    create_copy(spark, unpartitioned, copy.limit(0))
    copy.repartition(SMALL_APPENDS).writeTo(unpartitioned).append()

    day = source.agg(F.to_date(F.min("occurred_at")).alias("day")).first()["day"]

    def one_day(frame):
        return frame.where(
            (F.col("occurred_at") >= day) & (F.col("occurred_at") < F.lit(day) + F.expr("INTERVAL 1 DAY"))
        )

    partitioned_df = summary(one_day(spark.table(partitioned)))
    unpartitioned_df = summary(one_day(spark.table(unpartitioned)))
    same_output(partitioned_df, unpartitioned_df)
    expression = day_filter(spark, "occurred_at", day)
    return {
        "day": str(day),
        "partitioned": {**profile(partitioned_df), **planned(spark, partitioned, expression)},
        "unpartitioned": {**profile(unpartitioned_df), **planned(spark, unpartitioned, expression)},
    }


def zorder(spark: SparkSession, attempts: DataFrame):
    default = f"{NAMESPACE}.attempts_default"
    sorted_table = f"{NAMESPACE}.attempts_zorder"
    create_copy(spark, default, attempts, F.days("started_at"))
    create_copy(spark, sorted_table, attempts, F.days("started_at"))
    spark.sql(
        f"CALL {ICEBERG_CATALOG}.system.rewrite_data_files(table => '{sorted_table}', "
        f"strategy => 'sort', sort_order => '{ZORDER}')"
    ).collect()

    customer = attempts.groupBy("customer_id").count().orderBy(F.desc("count")).first()["customer_id"]
    start = attempts.agg(F.min("started_at")).first()[0]

    def one_customer(frame):
        return frame.where(
            (F.col("customer_id") == customer)
            & (F.col("started_at") >= F.lit(start))
            & (F.col("started_at") < F.lit(start) + F.expr("INTERVAL 30 DAYS"))
        )

    default_df = one_customer(spark.table(default)).agg(
        F.count("*").alias("attempts"), F.sum("amount_minor").alias("amount")
    )
    sorted_df = one_customer(spark.table(sorted_table)).agg(
        F.count("*").alias("attempts"), F.sum("amount_minor").alias("amount")
    )
    same_output(default_df, sorted_df)
    expression = customer_window(spark, customer, start)
    return {
        "customer_id": customer,
        "default": {**profile(default_df), **planned(spark, default, expression)},
        "zorder": {**profile(sorted_df), **planned(spark, sorted_table, expression)},
    }


def run(spark: SparkSession, run_id: str):
    events = spark.read.option("versionAsOf", bronze.current_snapshot(spark, bronze.EVENTS)).table(bronze.EVENTS)
    attempts = spark.read.option("versionAsOf", bronze.current_snapshot(spark, silver.ATTEMPTS)).table(silver.ATTEMPTS)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    return {
        "run_id": run_id,
        "compaction": compaction(spark, events),
        "partition_pruning": partition_pruning(spark, events),
        "zorder": zorder(spark, attempts),
    }


def main():
    run_id = os.environ["RUN_ID"]
    spark = session("dp2-storage-layout")
    try:
        result = run(spark, run_id)
        Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/storage-layout.json", result)
        print(json.dumps(result, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
