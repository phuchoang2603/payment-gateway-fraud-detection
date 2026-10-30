"""DP3: gold features, the SCD2 customer dimension and attempt labels from pinned Silver snapshots.

Historical mode recomputes every table. Incremental mode reads Silver's net changelog views since the snapshots the
last DP3 run consumed and recomputes only the affected entities, customers and attempts, which leaves the tables equal
to a historical run except for `created`.
"""

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from batch.gold.windows import (
    ANCHOR,
    SINCE,
    Count,
    DailyAnchors,
    Distinct,
    Ratio,
    Total,
    change_points,
    columns,
    merge_intervals,
)
from batch.lakehouse import create_table, ref_snapshot, snapshot_info, stage
from batch.metrics import phase
from batch.silver import curate as silver
from batch.silver import incremental
from batch.spark import ICEBERG_CATALOG, session
from batch.store import Store

NAMESPACE = f"{ICEBERG_CATALOG}.gold"
DIM_CUSTOMER = f"{NAMESPACE}.dim_customer"
FEAT_CUSTOMER = f"{NAMESPACE}.feat_customer_daily"
FEAT_METHOD = f"{NAMESPACE}.feat_payment_method_daily"
LABEL_ATTEMPT = f"{NAMESPACE}.label_attempt"
TABLES = (DIM_CUSTOMER, FEAT_CUSTOMER, FEAT_METHOD, LABEL_ATTEMPT)
MODES = ("historical", "incremental")
# Bump when a change to DP3 would make earlier gold rows differ from a fresh historical run.
LOGIC_VERSION = "1"

RUN_PROPERTY = "dp3.run_id"
MODE_PROPERTY = "dp3.mode"
LOGIC_PROPERTY = "dp3.logic_version"
HISTORY_PROPERTY = "dp3.history_start"
CAP_PROPERTY = "dp3.anchor_cap"
DP2_LOGIC_PROPERTY = "silver.dp2.logic_version"
SILVER_PROPERTIES = {
    silver.ATTEMPTS: "silver.attempts.snapshot_id",
    silver.LABELS: "silver.labels.snapshot_id",
    silver.PAYMENT_EVENTS: "silver.payment_events.snapshot_id",
}

ATTEMPT_KEY = silver.ATTEMPT_KEY
PAYMENT_KEY = silver.PAYMENT_KEY
CUSTOMER_KEY = ["integration_id", "customer_id"]
WINDOWS = {"7d": timedelta(days=7), "30d": timedelta(days=30)}
LONGEST = max(WINDOWS.values())
FAMILIAR_WINDOW = timedelta(days=90)
RULE = DailyAnchors()
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
    key: str
    measures: tuple

    @property
    def keys(self) -> list[str]:
        return ["integration_id", self.key]

    @property
    def columns(self) -> list[str]:
        return [*self.keys, ANCHOR, "created", *columns(self.measures, WINDOWS)]


CUSTOMER = View(
    FEAT_CUSTOMER,
    "customer_id",
    (
        *COMMON,
        Distinct("distinct_device_count", "device_fingerprint"),
        Distinct("distinct_payment_method_count", "payment_method_fingerprint"),
    ),
)
METHOD = View(FEAT_METHOD, "payment_method_fingerprint", (*COMMON, Distinct("distinct_customer_count", "customer_id")))
VIEWS = (CUSTOMER, METHOD)
PARTITIONS = {
    DIM_CUSTOMER: lambda: F.months("valid_from_ts"),
    FEAT_CUSTOMER: lambda: F.days(ANCHOR),
    FEAT_METHOD: lambda: F.days(ANCHOR),
    LABEL_ATTEMPT: lambda: F.days(ANCHOR),
}
TRACKED = [
    "external_buyer_id",
    "customer_account_created_at",
    "shipping_country",
    "shipping_region",
    "shipping_postal_code",
    "shipping_address_fingerprint",
    "familiar_device_fingerprints",
]
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


def iso(value: datetime) -> str:
    return value.isoformat()


def valid_attempts(attempts: DataFrame) -> DataFrame:
    return attempts.where(F.col("lifecycle_valid")).withColumn(
        "attempt_time", F.coalesce(F.col("attempted_at"), F.col("started_at"))
    )


def history_start(attempts: DataFrame):
    """H, the UTC midnight of the earliest start; None without attempts."""
    micros = attempts.agg(F.min(F.unix_micros(F.date_trunc("day", "started_at")))).first()[0]
    return None if micros is None else utc(micros)


def commit_time(spark, table, snapshot) -> datetime:
    row = spark.sql(
        f"SELECT unix_micros(committed_at) AS micros FROM {table}.snapshots WHERE snapshot_id = {snapshot}"
    ).first()
    return utc(row["micros"])


def midnight(value: datetime) -> datetime:
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def features(view: View, attempts: DataFrame, start, cap, created, scope=None, seeds=None) -> DataFrame:
    rows = change_points(
        attempts.where(F.col(view.key).isNotNull()),
        view.keys,
        list(view.measures),
        WINDOWS,
        RULE,
        start,
        cap,
        scope,
        seeds,
    )
    return rows.withColumn("created", F.lit(created)).select(*view.columns)


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


def dim_customer(profile: DataFrame) -> DataFrame:
    """SCD2 versions: a version opens at each attempt whose tracked profile differs from the previous attempt's."""
    intervals = familiar_intervals(profile.select(*CUSTOMER_KEY, "device_fingerprint", "started_at"))
    attempt = profile.select(*ATTEMPT_KEY, "customer_id", "started_at")
    familiar = (
        attempt.join(intervals, CUSTOMER_KEY)
        .where((F.col("start") <= F.col("started_at")) & (F.col("started_at") < F.col("end")))
        .groupBy(*ATTEMPT_KEY)
        .agg(F.array_sort(F.collect_set("device_fingerprint")).alias("familiar_device_fingerprints"))
    )
    tracked = profile.join(familiar, ATTEMPT_KEY, "left").withColumn(
        "familiar_device_fingerprints",
        F.coalesce(F.col("familiar_device_fingerprints"), F.array().cast("array<string>")),
    )
    hashed = tracked.withColumn("profile_hash", F.sha2(F.to_json(F.struct(*TRACKED)), 256))
    # Attempts at the same instant open at most one version: the last in key order.
    instant = Window.partitionBy(*CUSTOMER_KEY, "started_at").orderBy(F.desc("payment_id"), F.desc("attempt_id"))
    hashed = hashed.withColumn("_tie", F.row_number().over(instant)).where(F.col("_tie") == 1)
    ordered = Window.partitionBy(*CUSTOMER_KEY).orderBy("started_at")
    changes = hashed.withColumn("_previous", F.lag("profile_hash").over(ordered)).where(
        F.col("_previous").isNull() | (F.col("_previous") != F.col("profile_hash"))
    )
    valid_to = F.lead("started_at").over(ordered)
    return changes.select(
        F.sha2(
            F.concat_ws("|", "integration_id", "customer_id", F.unix_micros("started_at").cast("string")), 256
        ).alias("customer_sk"),
        *CUSTOMER_KEY,
        F.row_number().over(ordered).alias("version_number"),
        *TRACKED,
        "profile_hash",
        F.col("started_at").alias("valid_from_ts"),
        valid_to.alias("valid_to_ts"),
        valid_to.isNull().alias("is_current"),
        F.col("payment_id").alias("source_payment_id"),
        F.col("attempt_id").alias("source_attempt_id"),
    )


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
        *ATTEMPT_KEY, "customer_id", "payment_method_fingerprint", F.col("attempt_time").alias(ANCHOR)
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


def write(spark, table: str, frame: DataFrame, pins: dict) -> dict:
    create_table(spark, table, frame, PARTITIONS[table]())
    stage(spark, table)
    with_pins(frame.writeTo(table), pins).overwrite(F.lit(True))
    return recorded(spark, table, pins)


def replace(spark, table: str, frame: DataFrame, pins: dict, delete_sql: str) -> dict:
    """Delete the recomputed rows on this run's branch, then append them, recording this run on the last commit."""
    create_table(spark, table, frame, PARTITIONS[table]())
    stage(spark, table)
    spark.sql(delete_sql)
    with_pins(frame.writeTo(table), pins).append()
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


def run(spark, run_id, mode="historical"):
    if mode not in MODES:
        raise ValueError(f"mode must be one of {', '.join(MODES)}, not {mode}")
    phases = {}
    snapshots, frames = silver_inputs(spark)
    attempts = valid_attempts(frames[silver.ATTEMPTS])
    start = history_start(attempts)
    if start is None:
        raise ValueError("silver.attempts has no lifecycle-valid attempts")
    created = commit_time(spark, silver.ATTEMPTS, snapshots[silver.ATTEMPTS])
    cap = midnight(created)
    attempts_summary = spark.sql(
        f"SELECT summary FROM {silver.ATTEMPTS}.snapshots WHERE snapshot_id = {snapshots[silver.ATTEMPTS]}"
    ).first()["summary"]
    pins = {
        **{prop: str(snapshots[table]) for table, prop in SILVER_PROPERTIES.items()},
        DP2_LOGIC_PROPERTY: (attempts_summary or {}).get(incremental.LOGIC_PROPERTY, ""),
        RUN_PROPERTY: run_id,
        MODE_PROPERTY: mode,
        LOGIC_PROPERTY: LOGIC_VERSION,
        HISTORY_PROPERTY: iso(start),
        CAP_PROPERTY: iso(cap),
    }
    tables = {}
    scope_report = {}
    affected_days = None
    consumed = None
    if mode == "historical":
        for view in VIEWS:
            with phase(spark, phases, f"write {view.table.split('.')[-1]}"):
                tables[view.table] = write(spark, view.table, features(view, attempts, start, cap, created), pins)
        with phase(spark, phases, "write dim_customer"):
            profile = profiles(frames[silver.PAYMENT_EVENTS], attempts)
            tables[DIM_CUSTOMER] = write(spark, DIM_CUSTOMER, dim_customer(profile), pins)
        with phase(spark, phases, "write label_attempt"):
            tables[LABEL_ATTEMPT] = write(spark, LABEL_ATTEMPT, label_attempt(frames[silver.LABELS], attempts), pins)
    else:
        with phase(spark, phases, "scope"):
            changes = incremental_changes(spark, snapshots, start)
        for view in VIEWS:
            with phase(spark, phases, f"write {view.table.split('.')[-1]}"):
                tables[view.table], scope_report[view.table] = replace_features(
                    spark, view, attempts, changes, start, cap, created, pins
                )
        with phase(spark, phases, "write dim_customer"):
            tables[DIM_CUSTOMER], scope_report[DIM_CUSTOMER] = replace_customers(
                spark, frames[silver.PAYMENT_EVENTS], attempts, changes, pins
            )
        with phase(spark, phases, "write label_attempt"):
            tables[LABEL_ATTEMPT], scope_report[LABEL_ATTEMPT] = replace_labels(
                spark, frames[silver.LABELS], attempts, changes, pins
            )
        with phase(spark, phases, "affected days"):
            affected_days = changed_days(changes, attempts)
        consumed = {table.split(".", 1)[1]: snapshot for table, snapshot in changes["since"].items()}
        changes["images"].unpersist()
        changes["attempt_keys"].unpersist()
    report = {
        "run_id": run_id,
        "mode": mode,
        "silver_snapshots": {table.split(".", 1)[1]: snapshot for table, snapshot in snapshots.items()},
        "created": iso(created),
        "history_start": iso(start),
        "anchor_cap": iso(cap),
        "tables": tables,
        "scope": scope_report,
        "affected_days": affected_days,
        "consumed": consumed,
        "phases": phases,
    }
    Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/gold.json", report)
    return report


def previous_state(spark) -> dict:
    """The consumed state of the last published DP3 run, which every gold table must share."""
    summaries = {table: incremental.last_run(spark, table, RUN_PROPERTY) for table in TABLES}
    missing = [table for table, summary in summaries.items() if summary is None]
    if missing:
        raise incremental.IncrementalRefused(
            f"no published DP3 run recorded its Silver snapshots on {', '.join(missing)}"
        )
    runs = {summary.get(RUN_PROPERTY) for summary in summaries.values()}
    if len(runs) > 1:
        raise incremental.IncrementalRefused(
            f"the gold tables were last written by different DP3 runs: {sorted(runs)}"
        )
    state = summaries[FEAT_CUSTOMER]
    if state.get(LOGIC_PROPERTY) != LOGIC_VERSION:
        raise incremental.IncrementalRefused(
            f"the last DP3 run used logic version {state.get(LOGIC_PROPERTY)}, not {LOGIC_VERSION}"
        )
    return state


def silver_range(spark, table: str, since: int, current: int, dp2_logic: str) -> str | None:
    """Why Silver's main between two snapshots cannot be read as changes, or None."""
    snapshots = {
        row["snapshot_id"]: (row["parent_id"], row["summary"] or {})
        for row in spark.sql(f"SELECT snapshot_id, parent_id, summary FROM {table}.snapshots").collect()
    }
    if since not in snapshots:
        return f"{table} snapshot {since} has expired; DP3 must run within the snapshot retention"
    node = current
    while node != since:
        if node not in snapshots:
            return f"{table} main no longer descends from snapshot {since}"
        parent, summary = snapshots[node]
        if summary.get("dp2.mode") == "historical":
            return f"{table} snapshot {node} is a historical DP2 run, which rewrites every row"
        logic = summary.get(incremental.LOGIC_PROPERTY)
        if logic is not None and logic != dp2_logic:
            return f"{table} snapshot {node} used DP2 logic version {logic}, not {dp2_logic}"
        node = parent
    return None


def changelog(spark, table: str, since: int, current: int) -> DataFrame:
    """Silver's net row changes between two main snapshots, as DELETE (old) and INSERT (new) images."""
    if since == current:
        return (
            incremental.read_at(spark, table, current).limit(0).withColumn("_change_type", F.lit(None).cast("string"))
        )
    name = f"dp3_{table.split('.')[-1]}_changes"
    spark.sql(
        f"CALL {ICEBERG_CATALOG}.system.create_changelog_view("
        f"table => '{table.split('.', 1)[1]}', "
        f"options => map('start-snapshot-id', '{since}', 'end-snapshot-id', '{current}'), "
        f"net_changes => true, changelog_view => '{name}')"
    ).collect()
    return spark.table(name)


def incremental_changes(spark, snapshots: dict, start: datetime) -> dict:
    state = previous_state(spark)
    if state.get(HISTORY_PROPERTY) != iso(start):
        raise incremental.IncrementalRefused(
            f"the earliest Silver attempt day moved from {state.get(HISTORY_PROPERTY)} to {iso(start)}"
        )
    previous_cap = datetime.fromisoformat(state[CAP_PROPERTY])
    if previous_cap < start + LONGEST:
        raise incremental.IncrementalRefused("feature coverage was still incomplete at the last DP3 anchor")
    since = {table: int(state[prop]) for table, prop in SILVER_PROPERTIES.items()}
    dp2_logic = state.get(DP2_LOGIC_PROPERTY, "")
    for table in (silver.ATTEMPTS, silver.LABELS):
        reason = silver_range(spark, table, since[table], snapshots[table], dp2_logic)
        if reason:
            raise incremental.IncrementalRefused(f"{reason}; run dp3-gold with mode=historical")
    # Both images of every changed attempt, valid or not: a superset of the entities to recompute is still exact.
    images = (
        changelog(spark, silver.ATTEMPTS, since[silver.ATTEMPTS], snapshots[silver.ATTEMPTS])
        .withColumn("attempt_time", F.coalesce(F.col("attempted_at"), F.col("started_at")))
        .cache()
    )
    labelled = changelog(spark, silver.LABELS, since[silver.LABELS], snapshots[silver.LABELS])
    keys = images.select(*ATTEMPT_KEY).unionByName(labelled.select(*ATTEMPT_KEY)).distinct().cache()
    return {"since": since, "previous_cap": previous_cap, "images": images, "attempt_keys": keys}


def replace_features(spark, view: View, attempts, changes, start, cap, created, pins):
    next_anchor = changes["previous_cap"] + DAY
    entry = F.greatest(RULE.after(F.col("attempt_time")), RULE.after(F.col("started_at")))
    affected = changes["images"].where(F.col(view.key).isNotNull()).groupBy(*view.keys).agg(F.min(entry).alias(SINCE))
    # Entities whose values move between the previous and the new anchor cap without changed data: an attempt
    # still leaving the longest window, or one entering after the previous cap.
    still_leaving = changes["previous_cap"] - LONGEST
    expiring = (
        attempts.where(
            (F.col("attempt_time") >= F.lit(still_leaving))
            | (F.greatest("started_at", F.coalesce("outcome_at", "started_at")) >= F.lit(changes["previous_cap"]))
        )
        .where(F.col(view.key).isNotNull())
        .select(*view.keys)
        .distinct()
        .withColumn(SINCE, F.lit(next_anchor))
    )
    scope = (
        affected.unionByName(expiring)
        .groupBy(*view.keys)
        .agg(F.least(F.min(SINCE), F.lit(next_anchor)).alias(SINCE))
        .cache()
    )
    current = incremental.read_at(spark, view.table, ref_snapshot(spark, view.table))
    latest = Window.partitionBy(*view.keys).orderBy(F.desc(ANCHOR))
    seeds = (
        current.join(scope, view.keys)
        .where(F.col(ANCHOR) < F.col(SINCE))
        .withColumn("_latest", F.row_number().over(latest))
        .where(F.col("_latest") == 1)
        .drop("_latest", SINCE)
    )
    inputs = attempts.join(scope.select(*view.keys), view.keys, "left_semi")
    rows = features(view, inputs, start, cap, created, scope, seeds)
    scope_view = f"dp3_{view.table.split('.')[-1]}_scope"
    scope.createOrReplaceTempView(scope_view)
    on = " AND ".join(f"t.{key} = s.{key}" for key in view.keys)
    entities = scope.count()
    info = replace(
        spark,
        view.table,
        rows,
        pins,
        f"MERGE INTO {view.table} t USING {scope_view} s ON {on} AND t.{ANCHOR} >= s.{SINCE} WHEN MATCHED THEN DELETE",
    )
    scope.unpersist()
    return info, {"entities": entities}


def replace_customers(spark, events, attempts, changes, pins):
    customers = changes["images"].select(*CUSTOMER_KEY).where(F.col("customer_id").isNotNull()).distinct().cache()
    affected = attempts.join(customers, CUSTOMER_KEY, "left_semi")
    starts = events.join(F.broadcast(affected.select(*PAYMENT_KEY).distinct()), PAYMENT_KEY, "left_semi")
    rows = dim_customer(profiles(starts, affected))
    customers.createOrReplaceTempView("dp3_dim_customer_scope")
    count = customers.count()
    info = replace(
        spark,
        DIM_CUSTOMER,
        rows,
        pins,
        f"DELETE FROM {DIM_CUSTOMER} WHERE (integration_id, customer_id) IN "
        "(SELECT integration_id, customer_id FROM dp3_dim_customer_scope)",
    )
    customers.unpersist()
    return info, {"customers": count}


def replace_labels(spark, labels, attempts, changes, pins):
    keys = changes["attempt_keys"]
    scoped_attempts = attempts.join(keys, ATTEMPT_KEY, "left_semi")
    scoped_labels = labels.join(keys, ATTEMPT_KEY, "left_semi")
    keys.createOrReplaceTempView("dp3_label_attempt_scope")
    info = replace(
        spark,
        LABEL_ATTEMPT,
        label_attempt(scoped_labels, scoped_attempts),
        pins,
        f"DELETE FROM {LABEL_ATTEMPT} WHERE (integration_id, payment_id, attempt_id) IN "
        "(SELECT integration_id, payment_id, attempt_id FROM dp3_label_attempt_scope)",
    )
    return info, {"attempts": keys.count()}


def changed_days(changes, attempts) -> list[str]:
    """Start days whose marts rows can differ: both images of changed attempts and the attempts of changed labels."""
    days = (
        changes["images"]
        .select(F.to_date("started_at").alias("day"))
        .unionByName(
            attempts.join(changes["attempt_keys"], ATTEMPT_KEY, "left_semi").select(
                F.to_date("started_at").alias("day")
            )
        )
    )
    return sorted(str(row["day"]) for row in days.distinct().collect())


def main():
    run_id = os.environ["RUN_ID"]
    mode = os.environ.get("GOLD_MODE", "historical")
    spark = session("dp3-gold")
    try:
        report = run(spark, run_id, mode)
        print(json.dumps(report, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
