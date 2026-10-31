"""DP3: gold features, the SCD2 customer dimension and attempt labels for a range of UTC days, from pinned Silver.

A run over [start, end] is a pure function of Silver: it overwrites only the rows of those days (and the label days and
customers they touch), so re-running a range changes nothing, the daily schedule runs yesterday plus a short lookback
for late outcomes, and a backfill is the same job over a longer range.
"""

import json
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from batch.gold.windows import (
    ANCHOR,
    Count,
    DailyAnchors,
    Distinct,
    ExactAnchors,
    Ratio,
    Total,
    as_of,
    changes,
    columns,
    merge_intervals,
    window_states,
)
from batch.lakehouse import create_table, ref_snapshot, snapshot_info, stage, write_ref
from batch.metrics import phase
from batch.silver import curate as silver
from batch.silver import incremental
from batch.spark import ICEBERG_CATALOG, session
from batch.store import Store

NAMESPACE = f"{ICEBERG_CATALOG}.gold"
DIM_CUSTOMER = f"{NAMESPACE}.dim_customer"
FEAT_CUSTOMER = f"{NAMESPACE}.feat_customer_daily"
FEAT_METHOD = f"{NAMESPACE}.feat_payment_method_daily"
FEAT_ATTEMPT_RT = f"{NAMESPACE}.feat_attempt_rt"
LABEL_ATTEMPT = f"{NAMESPACE}.label_attempt"
TABLES = (DIM_CUSTOMER, FEAT_CUSTOMER, FEAT_METHOD, FEAT_ATTEMPT_RT, LABEL_ATTEMPT)
# Bump when a change to DP3 would make earlier gold rows differ from a fresh run over the same days.
LOGIC_VERSION = "3"
LOOKBACK_ENV = "DP3_LOOKBACK_DAYS"
LOOKBACK_DEFAULT_DAYS = 2

RUN_PROPERTY = "dp3.run_id"
LOGIC_PROPERTY = "dp3.logic_version"
HISTORY_PROPERTY = "dp3.history_start"
RANGE_PROPERTY = "dp3.range"
SILVER_PROPERTIES = {
    silver.ATTEMPTS: "silver.attempts.snapshot_id",
    silver.LABELS: "silver.labels.snapshot_id",
    silver.PAYMENT_EVENTS: "silver.payment_events.snapshot_id",
}

ATTEMPT_KEY = silver.ATTEMPT_KEY
PAYMENT_KEY = silver.PAYMENT_KEY
CUSTOMER_KEY = ["integration_id", "customer_id"]
METHOD_KEY = ["integration_id", "payment_method_fingerprint"]
WINDOWS = {"7d": timedelta(days=7), "30d": timedelta(days=30)}
RT_WINDOWS = {"5m": timedelta(minutes=5), "1h": timedelta(hours=1), "24h": timedelta(hours=24)}
# fraud-snapshot-v2: an attempt whose time is more than this before its start is too late and contributes nothing.
RT_HORIZON = timedelta(hours=24)
# Reads assume an attempt's client time is at most a day after its start.
CLOCK_MARGIN = timedelta(days=1)
WINDOW_COLUMNS = [
    *silver.ATTEMPT_KEY,
    "customer_id",
    "payment_method_fingerprint",
    "merchant_id",
    "device_fingerprint",
    "amount_minor",
    "started_at",
    "attempted_at",
    "outcome",
    "outcome_at",
    "decline_source",
    "lifecycle_valid",
]
FAMILIAR_WINDOW = timedelta(days=90)
DAY = timedelta(days=1)

DECIDED = "outcome IN ('succeeded', 'declined')"
DECLINED = "outcome = 'declined'"
COMMON = [
    Count("attempt_count"),
    Total("amount_sum_minor", "amount_minor"),
    Count("decided_count", DECIDED, outcome=True),
    Count("declined_count", DECLINED, outcome=True),
    Count("risk_declined_count", f"{DECLINED} AND decline_source = 'DECLINE_SOURCE_RISK'", outcome=True),
    Ratio("decline_rate", "declined_count", "decided_count"),
    Distinct("distinct_merchant_count", "merchant_id"),
]


@dataclass(frozen=True)
class View:
    table: str
    keys: tuple
    measures: tuple

    @property
    def output(self) -> list[str]:
        return columns(self.measures, WINDOWS)

    @property
    def columns(self) -> list[str]:
        return [*self.keys, ANCHOR, "created", *self.output]


CUSTOMER = View(
    FEAT_CUSTOMER,
    tuple(CUSTOMER_KEY),
    (
        *COMMON,
        Distinct("distinct_device_count", "device_fingerprint"),
        Distinct("distinct_payment_method_count", "payment_method_fingerprint"),
    ),
)
METHOD = View(FEAT_METHOD, tuple(METHOD_KEY), (*COMMON, Distinct("distinct_customer_count", "customer_id")))
VIEWS = (CUSTOMER, METHOD)


@dataclass(frozen=True)
class Snapshot:
    """One entity view of fraud-snapshot-v2, as of each attempt's start."""

    keys: tuple
    measures: tuple
    output: tuple


RT_VIEWS = (
    Snapshot(
        tuple(CUSTOMER_KEY),
        (
            Count("customer_snapshot_attempt_count"),
            Distinct("customer_snapshot_distinct_devices", "device_fingerprint"),
        ),
        (
            "customer_snapshot_attempt_count_5m",
            "customer_snapshot_attempt_count_1h",
            "customer_snapshot_attempt_count_24h",
            "customer_snapshot_distinct_devices_24h",
        ),
    ),
    Snapshot(
        tuple(METHOD_KEY),
        (Count("method_snapshot_attempt_count"),),
        (
            "method_snapshot_attempt_count_5m",
            "method_snapshot_attempt_count_1h",
            "method_snapshot_attempt_count_24h",
        ),
    ),
)
RT_COLUMNS = [
    *ATTEMPT_KEY,
    "customer_id",
    "payment_method_fingerprint",
    ANCHOR,
    "created",
    *[name for view in RT_VIEWS for name in view.output],
]
PARTITIONS = {
    DIM_CUSTOMER: lambda: F.months("valid_from_ts"),
    FEAT_CUSTOMER: lambda: F.days(ANCHOR),
    FEAT_METHOD: lambda: F.days(ANCHOR),
    FEAT_ATTEMPT_RT: lambda: F.days(ANCHOR),
    LABEL_ATTEMPT: lambda: F.days(ANCHOR),
}
PROFILE_ATTRIBUTES = [
    "external_buyer_id",
    "customer_account_created_at",
    "shipping_country",
    "shipping_region",
    "shipping_postal_code",
    "shipping_address_fingerprint",
]
TRACKED = [*PROFILE_ATTRIBUTES, "familiar_device_fingerprints"]
START_PROFILE = {
    "external_buyer_id": "attempt_started.context.customer.external_buyer_id",
    "customer_account_created_at": "attempt_started.context.snapshot.customer_account_created_at",
    "shipping_country": "attempt_started.context.snapshot.shipping.country",
    "shipping_region": "attempt_started.context.snapshot.shipping.region",
    "shipping_postal_code": "attempt_started.context.snapshot.shipping.postal_code",
    "shipping_address_fingerprint": "attempt_started.context.snapshot.shipping.address_fingerprint",
}


def utc(micros) -> datetime:
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(micros))


def midnight(day: date) -> datetime:
    return datetime.combine(day, time(), tzinfo=timezone.utc)


def with_attempt_time(attempts: DataFrame) -> DataFrame:
    return attempts.withColumn("attempt_time", F.coalesce(F.col("attempted_at"), F.col("started_at")))


def history_start(attempts: DataFrame):
    """H, the UTC midnight of the earliest start; None without attempts."""
    micros = attempts.agg(F.min(F.unix_micros(F.date_trunc("day", "started_at")))).first()[0]
    return None if micros is None else utc(micros)


def commit_time(spark, table, snapshot) -> datetime:
    row = spark.sql(
        f"SELECT unix_micros(committed_at) AS micros FROM {table}.snapshots WHERE snapshot_id = {snapshot}"
    ).first()
    return utc(row["micros"])


def started_between(frame: DataFrame, first: datetime, after: datetime) -> DataFrame:
    return frame.where((F.col("started_at") >= F.lit(first)) & (F.col("started_at") < F.lit(after)))


def day_ranges(days: list[date]) -> list[tuple[date, date]]:
    """Consecutive days merged into [first, after) ranges."""
    ranges = []
    for day in sorted(set(days)):
        if ranges and ranges[-1][1] == day:
            ranges[-1] = (ranges[-1][0], day + DAY)
        else:
            ranges.append((day, day + DAY))
    return ranges


def on_days(column: str, days: list[date]):
    """A filter Iceberg can push down: the column falls on one of the days."""
    return reduce(
        lambda left, right: left | right,
        [(F.col(column) >= F.lit(midnight(a))) & (F.col(column) < F.lit(midnight(b))) for a, b in day_ranges(days)],
        F.lit(False),
    )


def daily_features(view: View, attempts: DataFrame, start, floor, cutoff, created) -> DataFrame:
    """Change rows at the daily anchors in (floor, cutoff], from every attempt still in a window at floor."""
    read = started_between(attempts, floor - max(WINDOWS.values()) - CLOCK_MARGIN, cutoff)
    states = window_states(
        read.where(F.col(view.keys[1]).isNotNull()),
        list(view.keys),
        list(view.measures),
        WINDOWS,
        DailyAnchors(),
        start,
        floor,
        cutoff,
    )
    return (
        changes(states, list(view.keys), view.output, floor)
        .withColumn("created", F.lit(created))
        .select(*view.columns)
    )


def attempt_snapshots(attempts: DataFrame, start, floor, cutoff, created) -> DataFrame:
    """Each attempt started in [floor, cutoff) with its customer and method snapshot values at its start: the batch
    view of fraud-snapshot-v2, counting every attempt start, valid or not, in time to be seen then."""
    read = started_between(attempts, floor - max(RT_WINDOWS.values()) - CLOCK_MARGIN, cutoff).where(
        F.col("attempt_time") >= F.col("started_at") - F.expr(f"INTERVAL {int(RT_HORIZON.total_seconds())} SECONDS")
    )
    points = started_between(attempts, floor, cutoff).select(
        *ATTEMPT_KEY, "customer_id", "payment_method_fingerprint", F.col("started_at").alias(ANCHOR)
    )
    for view in RT_VIEWS:
        states = window_states(
            read.where(F.col(view.keys[1]).isNotNull()),
            list(view.keys),
            list(view.measures),
            RT_WINDOWS,
            ExactAnchors(),
            start,
            floor,
            cutoff,
        )
        points = as_of(points, states, list(view.keys), list(view.output), ANCHOR)
    return points.withColumn("created", F.lit(created)).select(*RT_COLUMNS)


def profiles(events: DataFrame, attempts: DataFrame) -> DataFrame:
    """Each valid attempt's observed profile from its attempt_started context."""
    starts = events.where(F.col("payload_case") == "attempt_started").select(
        *PAYMENT_KEY,
        F.col("attempt_started.attempt_id").alias("attempt_id"),
        *[F.col(path).alias(name) for name, path in START_PROFILE.items()],
    )
    return attempts.select(*ATTEMPT_KEY, "customer_id", "device_fingerprint", "started_at").join(starts, ATTEMPT_KEY)


def familiar_intervals(uses: DataFrame) -> DataFrame:
    """When each device is familiar: from a use whose previous use lies within the window, until that previous use
    leaves it. Pairs of consecutive uses give [use, previous + window); their union is exact."""
    device = [*CUSTOMER_KEY, "device_fingerprint"]
    previous = F.lag("started_at").over(Window.partitionBy(*device).orderBy("started_at"))
    pairs = (
        uses.where(F.col("device_fingerprint").isNotNull())
        .select(
            *device,
            F.col("started_at").alias("start"),
            (previous + F.expr(f"INTERVAL {FAMILIAR_WINDOW.days} DAYS")).alias("end"),
        )
        .where(F.col("end") > F.col("start"))
    )
    return merge_intervals(pairs, device)


VERSION_COLUMNS = [
    "customer_sk",
    *CUSTOMER_KEY,
    "version_number",
    *TRACKED,
    "profile_hash",
    "valid_from_ts",
    "valid_to_ts",
    "is_current",
    "source_payment_id",
    "source_attempt_id",
]


def familiar_sets(profile: DataFrame, intervals: DataFrame) -> DataFrame:
    """Each attempt with `familiar_set`, an XOR fingerprint of the devices familiar at its start: one ordered pass per
    customer in which each interval's start and end toggle its device's hash, so no attempt joins every interval. A
    device's merged intervals never overlap, so its hash is in the running XOR exactly while one of them holds."""
    toggle = F.xxhash64("device_fingerprint")
    edges = intervals.select(*CUSTOMER_KEY, F.col("start").alias("_at"), toggle.alias("_toggle")).unionByName(
        intervals.select(*CUSTOMER_KEY, F.col("end").alias("_at"), toggle.alias("_toggle"))
    )
    points = profile.withColumns({"_at": F.col("started_at"), "_toggle": F.lit(0).cast("long")})
    # Edges at an attempt's instant apply first: intervals are [start, end).
    ordered = (
        Window.partitionBy(*CUSTOMER_KEY)
        .orderBy("_at", F.col("payment_id").asc_nulls_first(), F.col("attempt_id").asc_nulls_first())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    swept = points.unionByName(edges, allowMissingColumns=True).withColumn(
        "familiar_set", F.bit_xor("_toggle").over(ordered)
    )
    return swept.where(F.col("payment_id").isNotNull()).drop("_at", "_toggle")


def tracked_profiles(profile: DataFrame, intervals: DataFrame) -> DataFrame:
    """Each attempt's profile hash over its attributes and familiar-device fingerprint, one attempt per customer
    instant: the last in key order. `intervals` must cover the familiar window before each attempt."""
    hashed = familiar_sets(profile, intervals).withColumn(
        "profile_hash", F.sha2(F.to_json(F.struct(*PROFILE_ATTRIBUTES, "familiar_set")), 256)
    )
    instant = Window.partitionBy(*CUSTOMER_KEY, "started_at").orderBy(F.desc("payment_id"), F.desc("attempt_id"))
    return hashed.withColumn("_tie", F.row_number().over(instant)).where(F.col("_tie") == 1).drop("_tie")


def opened_versions(tracked: DataFrame, previous: DataFrame | None = None) -> DataFrame:
    """The versions the attempts open: each attempt whose hash differs from the previous attempt's. `previous` holds
    each customer's version open before the attempts (profile_hash, version_number), which seeds the comparison and
    the numbering."""
    ordered = Window.partitionBy(*CUSTOMER_KEY).orderBy("started_at")
    if previous is None:
        seeded = tracked.withColumns({"_seed": F.lit(None).cast("string"), "_base": F.lit(0)})
    else:
        seeds = previous.select(
            *CUSTOMER_KEY, F.col("profile_hash").alias("_seed"), F.col("version_number").alias("_base")
        )
        seeded = tracked.join(seeds, CUSTOMER_KEY, "left").fillna(0, subset=["_base"])
    before = F.coalesce(F.lag("profile_hash").over(ordered), F.col("_seed"))
    changes = seeded.withColumn("_previous", before).where(
        F.col("_previous").isNull() | (F.col("_previous") != F.col("profile_hash"))
    )
    return changes.select(
        F.sha2(
            F.concat_ws("|", "integration_id", "customer_id", F.unix_micros("started_at").cast("string")), 256
        ).alias("customer_sk"),
        *CUSTOMER_KEY,
        (F.col("_base") + F.row_number().over(ordered)).alias("version_number"),
        *PROFILE_ATTRIBUTES,
        "profile_hash",
        F.col("started_at").alias("valid_from_ts"),
        F.lit(None).cast("timestamp").alias("valid_to_ts"),
        F.lit(None).cast("boolean").alias("is_current"),
        F.col("payment_id").alias("source_payment_id"),
        F.col("attempt_id").alias("source_attempt_id"),
    )


def with_familiar_devices(versions: DataFrame, intervals: DataFrame) -> DataFrame:
    """The sorted familiar devices at each version's start; only versions, far fewer than attempts, join intervals."""
    devices = (
        versions.select("customer_sk", *CUSTOMER_KEY, "valid_from_ts")
        .join(intervals, CUSTOMER_KEY)
        .where((F.col("start") <= F.col("valid_from_ts")) & (F.col("valid_from_ts") < F.col("end")))
        .groupBy("customer_sk")
        .agg(F.array_sort(F.collect_set("device_fingerprint")).alias("familiar_device_fingerprints"))
    )
    return versions.join(devices, "customer_sk", "left").withColumn(
        "familiar_device_fingerprints",
        F.coalesce(F.col("familiar_device_fingerprints"), F.array().cast("array<string>")),
    )


def linked(versions: DataFrame, following: DataFrame | None = None) -> DataFrame:
    """valid_to_ts is the customer's next version, or `following`'s first later version (_next_from) after the last."""
    ordered = Window.partitionBy(*CUSTOMER_KEY).orderBy("valid_from_ts")
    if following is not None:
        versions = versions.join(following, CUSTOMER_KEY, "left")
    else:
        versions = versions.withColumn("_next_from", F.lit(None).cast("timestamp"))
    valid_to = F.coalesce(F.lead("valid_from_ts").over(ordered), F.col("_next_from"))
    return versions.withColumns({"valid_to_ts": valid_to, "is_current": valid_to.isNull()}).select(*VERSION_COLUMNS)


def dim_customer(profile: DataFrame) -> DataFrame:
    """SCD2 versions over a whole history: a version opens at each attempt whose tracked profile differs from the
    previous attempt's."""
    intervals = familiar_intervals(profile)
    return linked(with_familiar_devices(opened_versions(tracked_profiles(profile, intervals)), intervals))


def label_attempt(labels: DataFrame, attempts: DataFrame) -> DataFrame:
    """The earliest linked label per attempt and source; ties go to the smaller observation_id."""
    earliest = (
        labels.where(F.col("linkage") == "linked")
        .groupBy(*ATTEMPT_KEY, "source")
        .agg(
            F.min_by(
                F.struct("is_fraud", "label_available_at", "occurred_at"),
                F.struct("label_available_at", "observation_id"),
            ).alias("first")
        )
    )
    keyed = attempts.select(
        *ATTEMPT_KEY, "customer_id", "payment_method_fingerprint", F.col("started_at").alias(ANCHOR)
    )
    return earliest.join(keyed, ATTEMPT_KEY).select(
        *ATTEMPT_KEY,
        "customer_id",
        "payment_method_fingerprint",
        ANCHOR,
        F.col("first.is_fraud").alias("label"),
        F.col("source").alias("label_source"),
        F.col("first.label_available_at").alias("label_available_at"),
        F.col("first.occurred_at").alias("occurred_at"),
    )


def with_pins(writer, pins: dict):
    for key, value in pins.items():
        writer = writer.option(f"snapshot-property.{key}", value)
    return writer


def overwrite(spark, table: str, frame: DataFrame, condition, pins: dict) -> dict:
    """Replace the table's rows matching `condition` with `frame` on this run's branch."""
    create_table(spark, table, frame, PARTITIONS[table]())
    stage(spark, table)
    with_pins(frame.writeTo(table), pins).overwrite(condition)
    return recorded(spark, table, pins)


def recorded(spark, table: str, pins: dict) -> dict:
    info = snapshot_info(spark, table)
    summary = spark.sql(f"SELECT summary FROM {table}.snapshots WHERE snapshot_id = {info['snapshot_id']}").first()
    if (summary["summary"] or {}).get(RUN_PROPERTY) != pins[RUN_PROPERTY]:
        raise RuntimeError(f"{table}: the head commit does not record DP3 run {pins[RUN_PROPERTY]}")
    total = (summary["summary"] or {}).get("total-records")
    return {**info, "rows": int(total) if total is not None else None}


def silver_inputs(spark):
    snapshots = {table: ref_snapshot(spark, table) for table in SILVER_PROPERTIES}
    missing = [table for table, snapshot in snapshots.items() if snapshot is None]
    if missing:
        raise ValueError(f"Silver has no published snapshot for {', '.join(missing)}")
    frames = {table: incremental.read_at(spark, table, snapshot) for table, snapshot in snapshots.items()}
    return snapshots, frames


def label_days(labels: DataFrame, attempts: DataFrame, floor, cutoff) -> list[date]:
    """Start days of the attempts that gained a linked label available in [floor, cutoff)."""
    arrived = labels.where(
        (F.col("linkage") == "linked")
        & (F.col("label_available_at") >= F.lit(floor))
        & (F.col("label_available_at") < F.lit(cutoff))
    ).select(*ATTEMPT_KEY)
    rows = attempts.join(arrived, ATTEMPT_KEY, "left_semi").select(F.to_date("started_at").alias("day")).distinct()
    return [row["day"] for row in rows.collect()]


def replace_customers(spark, events, attempts, floor, cutoff, pins) -> tuple[dict, int]:
    """Replace the SCD2 versions opened in the range, carrying on from the version each customer had open at its start.

    Versions are sequential, so a range reads its customers' stored version before `floor` (the profile to compare
    the first attempt against, and the version number to continue) and their first stored version from `cutoff`
    (the end of its last version). Only device uses within the familiar window before the range are read. Re-running
    a range on unchanged Silver rewrites the same rows; after Silver changes, re-run from the first changed day on.
    """
    template = dim_customer(profiles(events.limit(0), attempts.limit(0)))
    create_table(spark, DIM_CUSTOMER, template, PARTITIONS[DIM_CUSTOMER]())
    stage(spark, DIM_CUSTOMER)
    # Pinned before the DELETE below, so recomputing a lost block never reads the rows this run removed.
    stored = incremental.read_at(spark, DIM_CUSTOMER, ref_snapshot(spark, DIM_CUSTOMER, write_ref()))
    in_range = started_between(attempts, floor, cutoff).where(F.col("customer_id").isNotNull())
    opened_in_range = (F.col("valid_from_ts") >= F.lit(floor)) & (F.col("valid_from_ts") < F.lit(cutoff))
    customers = (
        in_range.select(*CUSTOMER_KEY)
        .unionByName(stored.where(opened_in_range).select(*CUSTOMER_KEY))
        .distinct()
        # Persisted, not locally checkpointed: a lost spot executor's blocks are recomputed from the lineage.
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    mine = stored.join(customers, CUSTOMER_KEY, "left_semi")
    latest = Window.partitionBy(*CUSTOMER_KEY).orderBy(F.desc("valid_from_ts"))
    previous = (
        mine.where(F.col("valid_from_ts") < F.lit(floor))
        .withColumn("_n", F.row_number().over(latest))
        .where(F.col("_n") == 1)
        .drop("_n")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    following = (
        mine.where(F.col("valid_from_ts") >= F.lit(cutoff))
        .groupBy(*CUSTOMER_KEY)
        .agg(F.min("valid_from_ts").alias("_next_from"))
    )
    # attempt_started occurs at the attempt's start, so its events partition prunes to the range.
    starts = events.where((F.col("occurred_at") >= F.lit(floor)) & (F.col("occurred_at") < F.lit(cutoff)))
    uses = started_between(attempts, floor - FAMILIAR_WINDOW, cutoff).join(customers, CUSTOMER_KEY, "left_semi")
    intervals = familiar_intervals(uses).persist(StorageLevel.MEMORY_AND_DISK)
    opened = with_familiar_devices(
        opened_versions(tracked_profiles(profiles(starts, in_range), intervals), previous), intervals
    )
    rows = linked(previous.select(*VERSION_COLUMNS).unionByName(opened, allowMissingColumns=False), following)
    replaced = previous.select("customer_sk").unionByName(mine.where(opened_in_range).select("customer_sk"))
    replaced.createOrReplaceTempView("dp3_dim_customer_replaced")
    spark.sql(f"DELETE FROM {DIM_CUSTOMER} WHERE customer_sk IN (SELECT customer_sk FROM dp3_dim_customer_replaced)")
    with_pins(rows.writeTo(DIM_CUSTOMER), pins).append()
    count = customers.count()
    for frame in (customers, previous, intervals):
        frame.unpersist()
    return recorded(spark, DIM_CUSTOMER, pins), count


def run(spark, run_id: str, start_day: date, end_day: date) -> dict:
    if start_day > end_day:
        raise ValueError(f"start {start_day} is after end {end_day}")
    phases = {}
    snapshots, frames = silver_inputs(spark)
    attempts = with_attempt_time(frames[silver.ATTEMPTS]).where(F.col("lifecycle_valid"))
    start = history_start(attempts)
    if start is None:
        raise ValueError("silver.attempts has no lifecycle-valid attempts")
    created = commit_time(spark, silver.ATTEMPTS, snapshots[silver.ATTEMPTS])
    floor, cutoff = midnight(start_day), midnight(end_day + DAY)
    pins = {
        **{prop: str(snapshots[table]) for table, prop in SILVER_PROPERTIES.items()},
        RUN_PROPERTY: run_id,
        LOGIC_PROPERTY: LOGIC_VERSION,
        HISTORY_PROPERTY: start.isoformat(),
        RANGE_PROPERTY: f"{start_day}/{end_day}",
    }
    tables = {}
    # Both window engines read the same history: the range plus one longest window, pruned to their columns.
    history = max(*WINDOWS.values(), *RT_WINDOWS.values()) + CLOCK_MARGIN
    windowed = (
        started_between(frames[silver.ATTEMPTS], floor - history, cutoff)
        .select(*WINDOW_COLUMNS)
        .transform(with_attempt_time)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    for view in VIEWS:
        with phase(spark, phases, f"write {view.table.split('.')[-1]}"):
            rows = daily_features(view, windowed.where(F.col("lifecycle_valid")), start, floor, cutoff, created)
            within = (F.col(ANCHOR) > F.lit(floor)) & (F.col(ANCHOR) <= F.lit(cutoff))
            tables[view.table] = overwrite(spark, view.table, rows, within, pins)
    with phase(spark, phases, "write feat_attempt_rt"):
        rows = attempt_snapshots(windowed, start, floor, cutoff, created)
        within = (F.col(ANCHOR) >= F.lit(floor)) & (F.col(ANCHOR) < F.lit(cutoff))
        tables[FEAT_ATTEMPT_RT] = overwrite(spark, FEAT_ATTEMPT_RT, rows, within, pins)
    windowed.unpersist()
    with phase(spark, phases, "write label_attempt"):
        range_days = [start_day + n * DAY for n in range((end_day - start_day).days + 1)]
        days = sorted({*range_days, *label_days(frames[silver.LABELS], attempts, floor, cutoff)})
        scoped = attempts.where(on_days("started_at", days))
        labels = frames[silver.LABELS].join(scoped.select(*ATTEMPT_KEY), ATTEMPT_KEY, "left_semi")
        tables[LABEL_ATTEMPT] = overwrite(
            spark, LABEL_ATTEMPT, label_attempt(labels, scoped), on_days(ANCHOR, days), pins
        )
    with phase(spark, phases, "write dim_customer"):
        tables[DIM_CUSTOMER], customers = replace_customers(
            spark, frames[silver.PAYMENT_EVENTS], attempts, floor, cutoff, pins
        )
    report = {
        "run_id": run_id,
        "start": str(start_day),
        "end": str(end_day),
        "silver_snapshots": {table.split(".", 1)[1]: snapshot for table, snapshot in snapshots.items()},
        "created": created.isoformat(),
        "history_start": start.isoformat(),
        "days": [str(day) for day in days],
        "customers": customers,
        "tables": tables,
        "phases": phases,
    }
    Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/gold.json", report)
    return report


def day_range(environ, today: date) -> tuple[date, date]:
    """DP3_START and DP3_END, or yesterday and the lookback days before it."""
    end = date.fromisoformat(environ["DP3_END"]) if environ.get("DP3_END") else today - DAY
    if environ.get("DP3_START"):
        return date.fromisoformat(environ["DP3_START"]), end
    return end - timedelta(days=int(environ.get(LOOKBACK_ENV, LOOKBACK_DEFAULT_DAYS))), end


def main():
    run_id = os.environ["RUN_ID"]
    start_day, end_day = day_range(os.environ, datetime.now(timezone.utc).date())
    spark = session("dp3-gold")
    try:
        report = run(spark, run_id, start_day, end_day)
        print(json.dumps(report, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
