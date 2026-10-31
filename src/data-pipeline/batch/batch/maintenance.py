"""Lakehouse upkeep.

`nightly` expires snapshots past the retention, drops audit branches that no run will publish (a branch pins every
file it references), and compacts the Bronze files that recent Kafka runs appended. Silver and Gold need no
compaction: every run rewrites whole day partitions at the target file size. Its cost does not grow with history.

`zorder` is on-demand storage evidence: planned files and bytes for one customer's month of silver.attempts before and
after a Z-order rewrite of that month only.
"""

import json
import os
import time
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from batch.bronze import ingest as bronze
from batch.days import day_range
from batch.gold import features as gold
from batch.lakehouse import TARGET_FILE_BYTES, file_stats
from batch.metrics import phase
from batch.silver import curate as silver
from batch.spark import ICEBERG_CATALOG, session
from batch.store import Store

RETAIN = timedelta(days=7)
RETAIN_SNAPSHOTS = 10
# Kafka runs append to Bronze; compacting the files ingested in these days keeps its partitions few and large.
COMPACT = timedelta(days=3)
AUDIT_PREFIX = "audit-"
BRONZE_TABLES = (bronze.EVENTS, bronze.LABELS, bronze.REJECTED)
TABLES = [*BRONZE_TABLES, *silver.TABLES, *gold.TABLES]
ZORDER = "zorder(customer_id,payment_method_fingerprint)"


def sql_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def compact_call(table: str, since: datetime) -> str:
    return (
        f"CALL {ICEBERG_CATALOG}.system.rewrite_data_files(table => '{table}', strategy => 'binpack', "
        f"where => \"_ingested_at >= TIMESTAMP '{sql_time(since)}'\", "
        f"options => map('target-file-size-bytes', '{TARGET_FILE_BYTES}'))"
    )


def stale_branches(spark: SparkSession, table: str, before: datetime) -> list[str]:
    """Audit branches whose head was committed before `before`: their runs failed or were abandoned."""
    rows = spark.sql(
        f"SELECT r.name FROM {table}.refs r JOIN {table}.snapshots s ON r.snapshot_id = s.snapshot_id "
        f"WHERE r.type = 'BRANCH' AND r.name LIKE '{AUDIT_PREFIX}%' "
        f"AND s.committed_at < TIMESTAMP '{sql_time(before)}'"
    ).collect()
    return sorted(row["name"] for row in rows)


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


def customer_window(spark: SparkSession, customer: str, first: date, last: date):
    """One customer's attempts over [first, last] payment days, as one Iceberg expression."""
    expressions = spark._jvm.org.apache.iceberg.expressions.Expressions
    return getattr(expressions, "and")(
        getattr(expressions, "and")(
            expressions.equal("customer_id", customer),
            expressions.greaterThanOrEqual(silver.PAYMENT_DAY, str(first)),
        ),
        expressions.lessThanOrEqual(silver.PAYMENT_DAY, str(last)),
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


def nightly(spark: SparkSession, now: datetime) -> dict:
    cutoff = now - RETAIN
    report = {"tables": {}, "phases": {}}
    for table in TABLES:
        if not spark.catalog.tableExists(table):
            continue
        entry = {"before": file_stats(spark, table)}
        if table in BRONZE_TABLES:
            with phase(spark, report["phases"], f"compact {table}"):
                spark.sql(compact_call(table, now - COMPACT)).collect()
        entry["dropped_branches"] = stale_branches(spark, table, cutoff)
        for branch in entry["dropped_branches"]:
            spark.sql(f"ALTER TABLE {table} DROP BRANCH `{branch}`")
        with phase(spark, report["phases"], f"expire {table}"):
            spark.sql(
                f"CALL {ICEBERG_CATALOG}.system.expire_snapshots(table => '{table}', "
                f"older_than => TIMESTAMP '{sql_time(cutoff)}', retain_last => {RETAIN_SNAPSHOTS})"
            ).collect()
        entry["after"] = file_stats(spark, table)
        report["tables"][table] = entry
    return report


def zorder(spark: SparkSession, first: date, last: date) -> dict:
    """Plan one customer's month before and after Z-ordering only that month of silver.attempts."""
    table = silver.ATTEMPTS
    month = spark.table(table).where((F.col(silver.PAYMENT_DAY) >= first) & (F.col(silver.PAYMENT_DAY) <= last))
    top = month.groupBy("customer_id").count().orderBy(F.desc("count"), F.asc("customer_id")).first()
    if top is None:
        raise RuntimeError(f"silver.attempts has no attempts between {first} and {last}")
    window = customer_window(spark, top["customer_id"], first, last)
    report = {"customer_id": top["customer_id"], "first": str(first), "last": str(last), "phases": {}}
    report["before"] = planned(spark, table, window)
    with phase(spark, report["phases"], "zorder"):
        spark.sql(
            f"CALL {ICEBERG_CATALOG}.system.rewrite_data_files(table => '{table}', strategy => 'sort', "
            f"sort_order => '{ZORDER}', where => \"{silver.PAYMENT_DAY} BETWEEN DATE '{first}' AND DATE '{last}'\", "
            f"options => map('target-file-size-bytes', '{TARGET_FILE_BYTES}', 'rewrite-all', 'true'))"
        ).collect()
    report["after"] = planned(spark, table, window)
    if spark.catalog.tableExists(bronze.EVENTS):
        report["bronze_one_day"] = partition_probe(spark)
    return report


def partition_probe(spark: SparkSession):
    events = bronze.EVENTS
    day = spark.table(events).agg(F.to_date(F.min("occurred_at")).alias("day")).first()["day"]
    expressions = spark._jvm.org.apache.iceberg.expressions.Expressions
    return {
        "day": str(day),
        "one_day": planned(spark, events, day_filter(spark, "occurred_at", day)),
        "no_filter": planned(spark, events, expressions.alwaysTrue()),
    }


def main():
    task = os.environ.get("MAINTENANCE_TASK", "nightly")
    if task not in ("nightly", "zorder"):
        raise SystemExit(f"MAINTENANCE_TASK must be nightly or zorder, not {task}")
    run_id = os.environ["RUN_ID"]
    now = datetime.now(timezone.utc)
    spark = session("lakehouse-maintenance")
    try:
        if task == "nightly":
            report = nightly(spark, now)
        else:
            report = zorder(spark, *day_range(os.environ, now.date()))
        report = {"run_id": run_id, "task": task, **report}
        Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/maintenance-{task}.json", report)
        print(json.dumps(report, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
