import argparse
import os
from datetime import UTC, date, datetime, time, timedelta

from fraud_feast.lake import write_json
from fraud_feast.repo import DAILY_FIELDS, DAILY_TABLES, DAILY_TTL, KEYS
from fraud_feast.runtime import store


def run(spark, feature_store, now, lookback_days, since=None):
    from pyspark.sql import functions as F

    if lookback_days < 0:
        raise ValueError("lookback days must be nonnegative")
    feature_store.config.online_store.skip_dedup = True
    report = {
        "now": now.isoformat(),
        "since": str(since) if since else None,
        "windows": {},
        "samples": {},
    }
    for scope, table in DAILY_TABLES.items():
        view = f"{scope}_daily"
        previous = feature_store.get_feature_view(view).most_recent_end_time
        start = (
            datetime.combine(since, time.min, UTC)
            if since
            else previous - timedelta(days=lookback_days + 1)
            if previous
            else now - DAILY_TTL
        )
        if start >= now:
            raise ValueError("materialization start must precede now")
        feature_store.materialize(start, now, feature_views=[view])
        report["windows"][view] = {"start": start.isoformat(), "end": now.isoformat()}
        current = spark.table(f"lake.{table}").where(F.col("event_timestamp") <= F.lit(now))
        newest = current.agg(F.max("event_timestamp")).first()[0]
        report["samples"][scope] = (
            [
                row.asDict()
                for row in current.where(F.col("event_timestamp") == F.lit(newest))
                .select("integration_id", KEYS[scope], "event_timestamp", *DAILY_FIELDS[scope])
                .limit(200)
                .collect()
            ]
            if newest is not None
            else []
        )
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default=os.environ.get("SINCE_DAY") or None)
    args = parser.parse_args()
    since = date.fromisoformat(args.since) if args.since else None
    from pyspark.sql import SparkSession

    feature_store = store()
    spark = SparkSession.builder.config(
        map=feature_store.config.offline_store.spark_conf
    ).getOrCreate()
    try:
        report = run(
            spark, feature_store, datetime.now(UTC), int(os.environ["DP_LOOKBACK_DAYS"]), since
        )
        write_json(os.environ["RUN_REPORT"], report)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
