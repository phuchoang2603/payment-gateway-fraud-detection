import json
import os
from datetime import datetime, timedelta, timezone

from batch import bronze, silver
from batch.lakehouse import TARGET_FILE_BYTES, file_stats
from batch.metrics import phase
from batch.spark import ICEBERG_CATALOG, session
from batch.store import Store

RETAIN_DAYS = 7
RETAIN_SNAPSHOTS = 10
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


def maintain(spark, tables=TABLES, now=None):
    cutoff = ((now or datetime.now(timezone.utc)) - timedelta(days=RETAIN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    report = {"tables": {}, "phases": {}}
    for table in tables:
        if not spark.catalog.tableExists(table):
            continue
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
