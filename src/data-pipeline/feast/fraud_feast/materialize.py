import argparse
import os
from datetime import UTC, date, datetime, time, timedelta

from feast import FeatureStore
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource

from fraud_feast.lake import catalog, read_json, write_json
from fraud_feast.repo import DAILY_FIELDS, DAILY_TABLES, DAILY_TTL, KEYS
from fraud_feast.runtime import config


class PinnedStore(FeatureStore):
    def __init__(self, pins, **kwargs):
        self.pins = pins
        super().__init__(**kwargs)

    def _get_feature_views_to_materialize(self, feature_views, version=None):
        views = super()._get_feature_views_to_materialize(feature_views, version=version)
        for view in views:
            scope = view.name.removesuffix("_daily")
            if scope not in self.pins:
                raise ValueError("only daily views may be materialized")
            view.batch_source = SparkSource(
                name=view.batch_source.name,
                query=f"SELECT * FROM lake.{DAILY_TABLES[scope]} VERSION AS OF {self.pins[scope]}",
                timestamp_field="event_timestamp",
                created_timestamp_column="created",
            )
        return views


def changed_ranges(table, consumed, pinned):
    if consumed is None or table.snapshot_by_id(consumed) is None:
        return None
    ranges = []
    snapshot = table.snapshot_by_id(pinned)
    while snapshot.snapshot_id != consumed:
        value = snapshot.summary.get("dp3.range") if snapshot.summary else None
        if value:
            start, end = value.split("/")
            ranges.append((date.fromisoformat(start), date.fromisoformat(end)))
        snapshot = table.snapshot_by_id(snapshot.parent_snapshot_id)
        if snapshot is None:
            return None
    return ranges


def at_day(day):
    return datetime.combine(day, time.min, UTC)


def range_condition(ranges):
    from functools import reduce
    from operator import or_

    from pyspark.sql import functions as F

    return reduce(
        or_,
        [
            (F.col("event_timestamp") >= at_day(start)) & (F.col("event_timestamp") < at_day(end))
            for start, end in ranges
        ],
        F.lit(False),
    )


def latest(frame, keys, now):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy(*keys).orderBy(
        F.col("event_timestamp").desc(), F.col("created").desc()
    )
    return (
        frame.where(F.col("event_timestamp") <= now)
        .withColumn("_latest", F.row_number().over(window))
        .where("_latest = 1")
        .drop("_latest")
    )


def refresh_frame(current, prior, scope, ranges, now):
    from pyspark.sql import functions as F

    keys = ["integration_id", KEYS[scope]]
    fields = DAILY_FIELDS[scope]
    affected = current.where(range_condition(ranges)).select(*keys)
    if prior is not None:
        affected = affected.unionByName(prior.where(range_condition(ranges)).select(*keys))
    affected = affected.distinct()
    chosen = latest(current, keys, now).select(*keys, "event_timestamp", "created", *fields)
    if prior is None:
        return affected.join(chosen, keys)
    deleted = latest(prior, keys, now).select(*keys, F.col("event_timestamp").alias("_deleted_at"))
    return (
        affected.join(chosen, keys, "left")
        .join(deleted, keys, "left")
        .withColumn("event_timestamp", F.coalesce("event_timestamp", "_deleted_at"))
        .withColumn("created", F.coalesce("created", F.lit(now)))
        .drop("_deleted_at")
    )


def ingest(frame, view, fields):
    from pyspark.sql import functions as F

    online = config(online=True).model_dump()
    online["online_store"]["skip_dedup"] = True

    def write(batches):
        import pyarrow as pa
        from feast import FeatureStore
        from feast.repo_config import RepoConfig

        from fraud_feast.push_job import write_online

        feature_store = FeatureStore(config=RepoConfig(**online))
        for batch in batches:
            rows = batch.to_pylist()
            write_online(feature_store, view, rows, fields)
            yield pa.RecordBatch.from_arrays([pa.array([len(rows)], type=pa.int64())], ["written"])

    return frame.mapInArrow(write, "written long").agg(F.sum("written")).first()[0] or 0


def unchanged(lake, pins):
    for scope, snapshot in pins.items():
        if lake.load_table(DAILY_TABLES[scope]).current_snapshot().snapshot_id != snapshot:
            raise ValueError("gold main moved during materialization")


def run(spark, feature_store, lake, now, state, start=None, end=None):
    from pyspark.sql import functions as F

    tables = {scope: lake.load_table(name) for scope, name in DAILY_TABLES.items()}
    pins = {scope: table.current_snapshot().snapshot_id for scope, table in tables.items()}
    feature_store.pins = pins
    feature_store.config.online_store.skip_dedup = True
    explicit = start is not None
    report = {
        "now": now.isoformat(),
        "start": str(start) if start else None,
        "end": str(end) if end else None,
        "pins": pins,
        "default": not explicit,
        "previous_state": state,
        "refreshed": 0,
        "samples": {},
    }
    ranges_by_scope = {
        scope: [(start, end)]
        if explicit
        else changed_ranges(table, (state or {}).get("pins", {}).get(scope), pins[scope])
        for scope, table in tables.items()
    }
    views = [f"{scope}_daily" for scope in KEYS]
    if not explicit:
        report["windows"] = {
            view: {
                "start": (
                    feature_store.get_feature_view(view).most_recent_end_time or now - DAILY_TTL
                ).isoformat(),
                "end": now.isoformat(),
            }
            for view in views
        }
        if any(ranges is None for ranges in ranges_by_scope.values()):
            feature_store.materialize(now - DAILY_TTL, now, feature_views=views)
            report["full_refresh"] = True
            for window in report["windows"].values():
                window["start"] = (now - DAILY_TTL).isoformat()
        feature_store.materialize_incremental(now, feature_views=views)
    for scope, table in tables.items():
        name = f"lake.{DAILY_TABLES[scope]}"
        current = spark.read.option("snapshot-id", pins[scope]).table(name)
        consumed = (state or {}).get("pins", {}).get(scope)
        prior = (
            spark.read.option("snapshot-id", consumed).table(name)
            if consumed is not None and table.snapshot_by_id(consumed)
            else None
        )
        ranges = ranges_by_scope[scope]
        if ranges is None:
            ranges = [((now - DAILY_TTL).date(), (now + timedelta(days=1)).date())]
            prior = None
        refreshed = refresh_frame(current, prior, scope, ranges, now).cache()
        fields = DAILY_FIELDS[scope]
        report["refreshed"] += ingest(refreshed, f"{scope}_daily", fields)
        keys = ["integration_id", KEYS[scope]]
        newest = (
            current.where(F.col("event_timestamp") <= now).agg(F.max("event_timestamp")).first()[0]
        )
        sample = refreshed.limit(200)
        if newest is not None:
            newest_keys = (
                current.where(F.to_date("event_timestamp") == newest.date())
                .select(*keys)
                .distinct()
                .limit(200)
            )
            newest_rows = latest(current.join(newest_keys, keys), keys, now).select(*sample.columns)
            sample = sample.unionByName(newest_rows).dropDuplicates(keys)
        report["samples"][scope] = [row.asDict() for row in sample.collect()]
        refreshed.unpersist()
    unchanged(lake, pins)
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=os.environ.get("START_DAY") or None)
    parser.add_argument("--end", default=os.environ.get("END_DAY") or None)
    args = parser.parse_args()
    if bool(args.start) != bool(args.end):
        parser.error("start and end must be supplied together")
    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None
    if start and start >= end:
        parser.error("start must precede end")
    from pyspark.sql import SparkSession

    configuration = config()
    spark = SparkSession.builder.config(map=configuration.offline_store.spark_conf).getOrCreate()
    lake = catalog()
    state = read_json(os.environ["FEAST_MATERIALIZE_STATE"])
    feature_store = PinnedStore({}, config=configuration)
    try:
        report = run(spark, feature_store, lake, datetime.now(UTC), state, start, end)
        write_json(os.environ["RUN_REPORT"], report)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
