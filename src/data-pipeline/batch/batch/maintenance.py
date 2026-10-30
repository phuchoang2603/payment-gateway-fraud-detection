import json
import os
import time
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from batch import bronze, silver
from batch.lakehouse import TARGET_FILE_BYTES, file_stats
from batch.metrics import phase
from batch.spark import ICEBERG_CATALOG, session
from batch.store import Store

RETAIN_DAYS = 7
RETAIN_SNAPSHOTS = 10
PROBE_DAYS = 30
ZORDER = {silver.ATTEMPTS: "zorder(customer_id,payment_method_fingerprint)"}
TABLES = [
    bronze.EVENTS,
    bronze.LABELS,
    bronze.REJECTED,
    silver.PAYMENT_EVENTS,
    silver.ATTEMPTS,
    silver.LABELS,
    silver.REJECTED,
]


def rewrite_call(table):
    layout = f"strategy => 'sort', sort_order => '{ZORDER[table]}'" if table in ZORDER else "strategy => 'binpack'"
    return (
        f"CALL {ICEBERG_CATALOG}.system.rewrite_data_files(table => '{table}', {layout}, "
        f"options => map('target-file-size-bytes', '{TARGET_FILE_BYTES}'))"
    )


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
        getattr(expressions, "and")(
            expressions.equal("customer_id", customer),
            expressions.greaterThanOrEqual("started_at", micros(spark, start)),
        ),
        expressions.lessThan("started_at", micros(spark, start + timedelta(days=PROBE_DAYS))),
    )


def planned(spark: SparkSession, name: str, expression):
    """Files and bytes Iceberg's planner selects for expression on the table's current snapshot."""
    started = time.perf_counter()
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    tasks = table.newScan().filter(expression).planFiles()
    files = size = 0
    try:
        iterator = tasks.iterator()
        while iterator.hasNext():
            files += 1
            size += iterator.next().file().fileSizeInBytes()
    finally:
        tasks.close()
    return {
        "files_planned": files,
        "bytes_planned": size,
        "plan_seconds": round(time.perf_counter() - started, 3),
    }


def attempt_window(spark: SparkSession, table: str):
    attempts = spark.table(table)
    top = attempts.groupBy("customer_id").count().orderBy(F.desc("count"), F.asc("customer_id")).first()
    start = attempts.where(F.col("customer_id") == top["customer_id"]).agg(F.min("started_at")).first()[0]
    return top["customer_id"], start


def partition_probe(spark: SparkSession):
    events = bronze.EVENTS
    day = spark.table(events).agg(F.to_date(F.min("occurred_at")).alias("day")).first()["day"]
    expressions = spark._jvm.org.apache.iceberg.expressions.Expressions
    return {
        "day": str(day),
        "one_day": planned(spark, events, day_filter(spark, "occurred_at", day)),
        "no_filter": planned(spark, events, expressions.alwaysTrue()),
    }


def maintain(spark, tables=TABLES, now=None):
    cutoff = ((now or datetime.now(timezone.utc)) - timedelta(days=RETAIN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    report = {"tables": {}, "phases": {}, "probes": {}}
    window = None
    for table in tables:
        if not spark.catalog.tableExists(table):
            continue
        if table == silver.ATTEMPTS:
            customer, start = attempt_window(spark, table)
            window = customer_window(spark, customer, start)
            report["probes"]["attempts_customer_30_days"] = {
                "customer_id": customer,
                "before": planned(spark, table, window),
            }
        before = file_stats(spark, table)
        with phase(spark, report["phases"], f"rewrite {table}"):
            spark.sql(rewrite_call(table)).collect()
        with phase(spark, report["phases"], f"manifests {table}"):
            spark.sql(f"CALL {ICEBERG_CATALOG}.system.rewrite_manifests(table => '{table}')").collect()
        with phase(spark, report["phases"], f"expire {table}"):
            spark.sql(
                f"CALL {ICEBERG_CATALOG}.system.expire_snapshots(table => '{table}', "
                f"older_than => TIMESTAMP '{cutoff}', retain_last => {RETAIN_SNAPSHOTS})"
            ).collect()
        report["tables"][table] = {"before": before, "after": file_stats(spark, table)}
        if table == silver.ATTEMPTS:
            report["probes"]["attempts_customer_30_days"]["after"] = planned(spark, table, window)
    if spark.catalog.tableExists(bronze.EVENTS):
        report["probes"]["bronze_one_day"] = partition_probe(spark)
    return report


def main():
    run_id = os.environ["RUN_ID"]
    spark = session("lakehouse-maintenance")
    try:
        report = {"run_id": run_id, **maintain(spark)}
        Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/maintenance.json", report)
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
