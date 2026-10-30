import json
import os
from functools import reduce

import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from batch import incremental
from batch.bronze import EVENTS as BRONZE_EVENTS
from batch.bronze import LABELS as BRONZE_LABELS
from batch.bronze import current_snapshot
from batch.lakehouse import LOCAL_WRITE_OPTIONS, WRITE_PROPERTIES, stage, write_ref
from batch.metrics import phase
from batch.schema import EVENT_COLUMNS, LABEL_COLUMNS
from batch.spark import ICEBERG_CATALOG, session
from batch.store import Store

NAMESPACE = f"{ICEBERG_CATALOG}.silver"
PAYMENT_EVENTS = f"{NAMESPACE}.payment_events"
ATTEMPTS = f"{NAMESPACE}.attempts"
LABELS = f"{NAMESPACE}.labels"
REJECTED = f"{NAMESPACE}.rejected_events"
MODES = ("historical", "incremental")
AFFECTED_VIEW = "dp2_affected_payments"

PAYMENT_KEY = ["integration_id", "payment_id"]
ATTEMPT_KEY = [*PAYMENT_KEY, "attempt_id"]
TERMINAL_KINDS = ("declined", "succeeded")
ATTEMPT_PAYLOADS = [
    ("start", "attempt_started"),
    ("decision", "risk_decision_recorded"),
    ("requested", "processor_operation_requested"),
    ("unknown", "processor_outcome_unknown"),
    ("declined", "attempt_declined"),
    ("succeeded", "payment_succeeded"),
]
EVENT_HASH_FIELDS = [column for column in EVENT_COLUMNS if column != "recorded_at"]
LABEL_HASH_FIELDS = list(LABEL_COLUMNS)
PERSIST = StorageLevel.MEMORY_AND_DISK


DEDUP_BROADCAST_ENV = "SILVER_DEDUP_BROADCAST_BYTES"
DEDUP_BROADCAST_DEFAULT = 256 * 1024 * 1024
# Duplicated IDs are matched on xxhash64(event_id). Under the threshold their keys are collected once as a sorted int64
# array (8 bytes each) and broadcast, and a vectorized membership test flags rows map-side, so unique events keep
# their layout and only duplicated rows are grouped. A broadcast join's hash relation costs the driver about 50 bytes
# per key and ran it out of heap on v19's 20M duplicated IDs. A collision only routes a unique event through the
# collapse path, which keeps its one copy, because both sides share the key.
KEY_BYTES = 8
DEDUP_COLUMNS = [*EVENT_COLUMNS.keys(), "event_hash", "_source"]


def dedup_broadcast_threshold() -> int:
    return int(os.environ.get(DEDUP_BROADCAST_ENV, DEDUP_BROADCAST_DEFAULT))


def duplicated_ids(hashed: DataFrame) -> DataFrame:
    return hashed.select("event_id").groupBy("event_id").agg(F.count("*").alias("copies")).where(F.col("copies") > 1)


def plan_dedup(hashed: DataFrame, threshold: int) -> dict:
    duplicated = duplicated_ids(hashed).persist(PERSIST)
    count = duplicated.count()
    estimated = count * KEY_BYTES
    return {
        "duplicated": duplicated,
        "duplicated_event_ids": count,
        "estimated_bytes": estimated,
        "strategy": "broadcast" if estimated < threshold else "shuffle",
        "broadcast_threshold_bytes": threshold,
    }


def duplicate_keys(plan: dict):
    """The duplicated IDs' keys as one sorted array, collected through Arrow once per plan and broadcast."""
    if "keys" not in plan:
        spark = plan["duplicated"].sparkSession
        arrow = "spark.sql.execution.arrow.pyspark.enabled"
        previous = spark.conf.get(arrow)
        spark.conf.set(arrow, "true")
        try:
            frame = plan["duplicated"].select(F.xxhash64("event_id").alias("event_key")).toPandas()
        finally:
            spark.conf.set(arrow, previous)
        keys = np.unique(frame["event_key"].to_numpy(dtype=np.int64))
        del frame
        # Raw bytes: PySpark's broadcast serializer cannot write the out-of-band buffers a large array pickles to.
        plan["keys"] = spark.sparkContext.broadcast(keys.tobytes())
    return plan["keys"]


def duplicated_flag(plan: dict):
    keys = duplicate_keys(plan)

    @F.pandas_udf("boolean")
    def member(event_key: pd.Series) -> pd.Series:
        sorted_keys = np.frombuffer(keys.value, dtype=np.int64)
        values = event_key.to_numpy(dtype=np.int64)
        if len(sorted_keys) == 0:
            return pd.Series(np.zeros(len(values), dtype=bool))
        found = np.minimum(np.searchsorted(sorted_keys, values), len(sorted_keys) - 1)
        return pd.Series(sorted_keys[found] == values)

    return member(F.xxhash64("event_id"))


def against_duplicates(frame: DataFrame, plan: dict, how: str) -> DataFrame:
    if plan["strategy"] == "broadcast":
        flag = duplicated_flag(plan)
        return frame.where(flag if how == "left_semi" else ~flag)
    keys = plan["duplicated"].select(F.xxhash64("event_id").alias("event_key"))
    keyed = frame.withColumn("event_key", F.xxhash64("event_id"))
    return keyed.join(keys, "event_key", how).drop("event_key")


def collapse_duplicates(hashed: DataFrame, plan: dict) -> DataFrame:
    return distinct_events(against_duplicates(hashed, plan, "left_semi"))


def narrow_dedup(hashed: DataFrame, plan: dict) -> tuple[DataFrame, DataFrame]:
    """Unique events keep their Bronze day layout; the collapsed duplicates, grouped by event_id, are re-clustered by
    day so a fanout write opens one file per task rather than one per day in every task."""
    collapsed = collapse_duplicates(hashed, plan)
    unique = against_duplicates(hashed, plan, "left_anti").select(*DEDUP_COLUMNS)
    by_day = collapsed.select(*DEDUP_COLUMNS).repartition(F.to_date("occurred_at"))
    return unique.unionByName(by_day), collapsed


def full_width_dedup(hashed: DataFrame) -> DataFrame:
    return distinct_events(hashed).select(*DEDUP_COLUMNS)


def run(spark, run_id, mode="historical"):
    if mode not in MODES:
        raise ValueError(f"mode must be one of {', '.join(MODES)}, not {mode}")
    events_snapshot = current_snapshot(spark, BRONZE_EVENTS)
    labels_snapshot = current_snapshot(spark, BRONZE_LABELS)
    pins = {
        incremental.EVENTS_PROPERTY: "" if events_snapshot is None else str(events_snapshot),
        incremental.LABELS_PROPERTY: "" if labels_snapshot is None else str(labels_snapshot),
        "dp2.mode": mode,
        incremental.LOGIC_PROPERTY: incremental.LOGIC_VERSION,
    }
    bronze_events = incremental.read_at(spark, BRONZE_EVENTS, events_snapshot)
    bronze_labels = incremental.read_at(spark, BRONZE_LABELS, labels_snapshot)
    cached = []
    phases = {}

    def cache(frame):
        cached.append(frame.persist(PERSIST))
        return cached[-1]

    scope = None
    if mode == "incremental":
        with phase(spark, phases, "scope"):
            scope = incremental_scope(spark, events_snapshot, labels_snapshot, cache)
        if scope["payments_count"] == 0:
            report = incremental_report(run_id, mode, scope, phases)
            Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/silver.json", report)
            return report
        # A semi join on named columns moves them first; keep Bronze's column order, which row_json serializes.
        affected = F.broadcast(scope["payments"])
        events = (
            bronze_events.where(F.col("occurred_at") >= F.lit(scope["cutoff"]))
            .join(affected, PAYMENT_KEY, "left_semi")
            .select(*bronze_events.columns)
        )
        labels_input = (
            bronze_labels.where(F.col("label_available_at") >= F.lit(scope["cutoff"]))
            .join(affected, PAYMENT_KEY, "left_semi")
            .select(*bronze_labels.columns)
        )
    else:
        events = bronze_events
        labels_input = bronze_labels
    hashed = with_event_hash(events)

    def save(name, frame, partition=None, options=None):
        if scope is None:
            return write_table(spark, name, frame, pins, run_id, partition, options)
        return replace_payments(spark, name, frame, pins, run_id, partition, options)

    try:
        plan = plan_dedup(hashed, dedup_broadcast_threshold())
        cached.append(plan["duplicated"])
        distinct, collapsed = narrow_dedup(hashed, plan)
        collapsed = cache(collapsed)
        conflicts = cache(collapsed.where(F.col("hashes") > 1).select("event_id", *PAYMENT_KEY))
        conflicting_payments = cache(conflicts.select(*PAYMENT_KEY).distinct())
        conflict_reason = cache(conflicts.groupBy(*PAYMENT_KEY).agg(F.min("event_id").alias("reason")))
        conflict_events = cache(
            distinct_events(hashed.join(F.broadcast(conflicting_payments), PAYMENT_KEY, "left_semi"))
        )
        accepted_events = distinct.join(F.broadcast(conflicting_payments), PAYMENT_KEY, "left_anti")

        snapshots = {}
        with phase(spark, phases, "write payment_events"):
            snapshots[PAYMENT_EVENTS] = save(
                PAYMENT_EVENTS, accepted_events, F.days("occurred_at"), LOCAL_WRITE_OPTIONS
            )
        accepted = spark.read.option("versionAsOf", snapshots[PAYMENT_EVENTS]["snapshot_id"]).table(PAYMENT_EVENTS)
        if scope is not None:
            accepted = accepted.join(F.broadcast(scope["payments"]), PAYMENT_KEY, "left_semi")
        payment_status = payments(accepted)
        accepted_attempts = attempts(accepted, payment_status)
        all_labels = label_observations(labels_input)
        linked = link_labels(all_labels, accepted_attempts, payment_status)
        accepted_labels = linked.join(conflicting_payments, PAYMENT_KEY, "left_anti")

        conflict_status = payments(conflict_events)
        conflict_attempts = attempts(conflict_events, conflict_status)
        historical = conflict_events.groupBy(*PAYMENT_KEY).agg(
            F.max(F.when(F.col("_source") == "historical", 1).otherwise(0)).alias("historical")
        )
        sourced_attempts = (
            conflict_attempts.join(historical, PAYMENT_KEY, "left")
            .withColumn("_source", F.when(F.col("historical") == 1, "historical").otherwise("kafka"))
            .drop("historical")
        )
        rejected = reduce(
            DataFrame.unionByName,
            [
                reject_payment_rows(hashed, "payment_events", conflicting_payments, conflict_reason),
                reject_payment_rows(sourced_attempts, "attempts", conflicting_payments, conflict_reason),
                reject_payment_rows(all_labels, "labels", conflicting_payments, conflict_reason),
            ],
        ).withColumn("_run_id", F.lit(run_id))

        for name, frame, partition in (
            (ATTEMPTS, accepted_attempts, F.days("started_at")),
            (LABELS, accepted_labels, F.days("label_available_at")),
            (REJECTED, rejected, None),
        ):
            with phase(spark, phases, f"write {name.split('.')[-1]}"):
                snapshots[name] = save(name, frame, partition)

        with phase(spark, phases, "report"):
            input_events = total_records(spark, BRONZE_EVENTS, events_snapshot) if scope is None else hashed.count()
            conflicting_events = conflicts.count()
            distinct = snapshots[PAYMENT_EVENTS]["added_records"] + conflict_events.count()
            report = {
                "run_id": run_id,
                "bronze_snapshots": {
                    "bronze.payment_events": events_snapshot,
                    "bronze.fraud_labels": labels_snapshot,
                },
                "dedup": {
                    "strategy": plan["strategy"],
                    "duplicated_event_ids": plan["duplicated_event_ids"],
                    "estimated_bytes": plan["estimated_bytes"],
                    "broadcast_threshold_bytes": plan["broadcast_threshold_bytes"],
                },
                "events": {
                    "input_rows": input_events,
                    "distinct_events": distinct,
                    "collapsed_deliveries": input_events - distinct,
                    "conflicting_events": conflicting_events,
                    "conflicting_payments": conflicting_payments.count(),
                },
                "labels": {
                    "input_rows": (
                        total_records(spark, BRONZE_LABELS, labels_snapshot) if scope is None else labels_input.count()
                    ),
                    "distinct_observations": snapshots[LABELS]["added_records"],
                    "linkage": counts_by(spark.table(LABELS), "linkage"),
                },
                "rejected_rows": {
                    "by_table": counts_by(spark.table(REJECTED), "rejected_table"),
                    "by_source": counts_by(spark.table(REJECTED), "source"),
                },
                "attempts": snapshots[ATTEMPTS]["added_records"],
                "lifecycle_invalid": spark.table(ATTEMPTS).where(~F.col("lifecycle_valid")).count(),
                "silver_snapshots": snapshots,
                "phases": phases,
                **incremental_report(run_id, mode, scope, phases),
            }
        Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/silver.json", report)
        return report
    finally:
        for frame in cached:
            frame.unpersist()


def incremental_scope(spark, events_snapshot, labels_snapshot, cache) -> dict:
    """The payments with Bronze rows appended since the last DP2 run, or IncrementalRefused when the result could
    differ from a historical run."""
    reason, since = incremental.refusal(
        spark,
        PAYMENT_EVENTS,
        {
            BRONZE_EVENTS: (incremental.EVENTS_PROPERTY, events_snapshot),
            BRONZE_LABELS: (incremental.LABELS_PROPERTY, labels_snapshot),
        },
    )
    if reason:
        raise incremental.IncrementalRefused(f"{reason}; run dp2-silver with mode=historical")
    new_events = incremental.appended(spark, BRONZE_EVENTS, since[BRONZE_EVENTS], events_snapshot)
    new_labels = incremental.appended(spark, BRONZE_LABELS, since[BRONZE_LABELS], labels_snapshot)
    payments, cutoff = incremental.affected_scope(new_events, new_labels)
    payments = cache(payments)
    count = payments.count()
    scope = {"payments": payments, "payments_count": count, "cutoff": cutoff, "bronze_since": since}
    if count == 0:
        return scope
    silver = {
        table: incremental.read_at(spark, table, current_snapshot(spark, table))
        for table in (PAYMENT_EVENTS, LABELS, REJECTED)
    }
    late = incremental.outside_lookback(silver[PAYMENT_EVENTS], silver[LABELS], silver[REJECTED], payments, cutoff)
    late_count = late.count()
    if late_count:
        days = incremental.lookback().days
        raise incremental.IncrementalRefused(
            f"{late_count} affected payments have Silver rows before the {days}-day lookback cutoff {cutoff}; "
            f"raise {incremental.LOOKBACK_ENV} or run dp2-silver with mode=historical"
        )
    payments.createOrReplaceTempView(AFFECTED_VIEW)
    return scope


def incremental_report(run_id, mode, scope, phases) -> dict:
    report = {"run_id": run_id, "mode": mode, "phases": phases}
    if scope is not None:
        report.update(
            {
                "bronze_since": scope["bronze_since"],
                "affected_payments": scope["payments_count"],
                "lookback_days": incremental.lookback().days,
                "cutoff": None if scope["cutoff"] is None else scope["cutoff"].isoformat(),
            }
        )
    return report


def create_table(spark, name, frame, partition=None, properties=WRITE_PROPERTIES) -> None:
    if spark.catalog.tableExists(name):
        return
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    creator = frame.limit(0).writeTo(name).using("iceberg")
    for key, value in properties.items():
        creator = creator.tableProperty(key, value)
    if partition is not None:
        creator = creator.partitionedBy(partition)
    creator.create()


def replace_payments(spark, name, frame, pins, run_id, partition=None, options=None) -> dict:
    """Replace the affected payments' rows: delete them, then append their recomputed rows, on this run's branch."""
    create_table(spark, name, frame, partition)
    stage(spark, name)
    spark.sql(
        f"DELETE FROM {name} WHERE (integration_id, payment_id) IN "
        f"(SELECT integration_id, payment_id FROM {AFFECTED_VIEW})"
    )
    writer = frame.writeTo(name).options(**(options or {}))
    for key, value in {**pins, "dp2.run_id": run_id}.items():
        writer = writer.option(f"snapshot-property.{key}", value)
    writer.append()
    return snapshot_info(spark, name)


def counts_by(frame: DataFrame, column: str) -> dict:
    return {row[column]: row["count"] for row in frame.groupBy(column).count().collect()}


def total_records(spark, table, snapshot) -> int:
    if snapshot is None:
        return 0
    row = spark.sql(
        f"SELECT summary['total-records'] AS total FROM {table}.snapshots WHERE snapshot_id = {snapshot}"
    ).first()
    return int(row["total"])


def snapshot_info(spark, table) -> dict:
    snapshot = current_snapshot(spark, table, write_ref())
    row = spark.sql(
        f"SELECT summary['added-records'] AS added FROM {table}.snapshots WHERE snapshot_id = {snapshot}"
    ).first()
    return {"snapshot_id": snapshot, "added_records": int(row["added"] or 0)}


def with_event_hash(events: DataFrame) -> DataFrame:
    return events.withColumn("event_hash", F.sha2(F.to_json(F.struct(*EVENT_HASH_FIELDS)), 256))


def distinct_events(hashed: DataFrame) -> DataFrame:
    """One shuffle on event_id: the number of distinct hashes per event and its earliest copy."""
    earliest = F.min_by(
        F.struct(*hashed.columns),
        F.struct(F.col("recorded_at"), F.col("_ingested_at"), F.col("_source")),
    )
    grouped = hashed.groupBy("event_id").agg(
        F.size(F.collect_set("event_hash")).alias("hashes"),
        earliest.alias("kept"),
    )
    return grouped.select("hashes", "kept.*")


START_FIELDS = [
    ("merchant_id", "attempt_started.context.merchant.merchant_id"),
    ("customer_id", "attempt_started.context.customer.customer_id"),
    ("amount_minor", "attempt_started.context.snapshot.total.minor_units"),
    ("currency", "attempt_started.context.snapshot.total.currency"),
    ("started_at", "occurred_at"),
    ("attempted_at", "attempt_started.signals.attempted_at"),
    ("device_fingerprint", "attempt_started.signals.device_fingerprint"),
    ("payment_method_type", "attempt_started.signals.payment_method.type"),
    ("payment_method_fingerprint", "attempt_started.signals.payment_method.fingerprint"),
]
DECISION_FIELDS = [
    ("risk_outcome", "risk_decision_recorded.assessment.outcome"),
    ("risk_reason_codes", "risk_decision_recorded.assessment.reason_codes"),
    ("policy_version", "risk_decision_recorded.assessment.policy_version"),
    ("model_version", "risk_decision_recorded.assessment.model_version"),
    ("feature_set_version", "risk_decision_recorded.assessment.feature_set_version"),
    ("risk_score", "risk_decision_recorded.assessment.risk_score"),
    ("assessment_id", "risk_decision_recorded.assessment.assessment_id"),
]


def attempt_events(events: DataFrame) -> DataFrame:
    """One row per attempt event from a single pass: its kind, version, and the start or decision fields it carries."""
    kind = reduce(
        lambda column, item: column.when(F.col(item[1]).isNotNull(), F.lit(item[0])),
        ATTEMPT_PAYLOADS[1:],
        F.when(F.col(ATTEMPT_PAYLOADS[0][1]).isNotNull(), F.lit(ATTEMPT_PAYLOADS[0][0])),
    )
    attempt_id = F.coalesce(*[F.col(f"{payload}.attempt_id") for _, payload in ATTEMPT_PAYLOADS])
    return events.where(attempt_id.isNotNull()).select(
        *PAYMENT_KEY,
        attempt_id.alias("attempt_id"),
        "aggregate_version",
        "occurred_at",
        kind.alias("kind"),
        F.col("attempt_declined.source").alias("detail"),
        F.when(
            F.col("attempt_started").isNotNull(), F.struct(*[F.col(path).alias(name) for name, path in START_FIELDS])
        ).alias("start"),
        F.when(
            F.col("risk_decision_recorded").isNotNull(),
            F.struct(*[F.col(path).alias(name) for name, path in DECISION_FIELDS]),
        ).alias("decision"),
    )


def attempts(events: DataFrame, payment_status: DataFrame) -> DataFrame:
    """Attempts from one aggregation per attempt: the first start and decision by aggregate_version, with the
    lifecycle counts, joined to the payment status. One shuffle replaces a window per payload and their joins."""
    kinds = attempt_events(events)
    per_attempt = (
        kinds.groupBy(*ATTEMPT_KEY)
        .agg(
            F.sum(F.when(F.col("kind") == "start", 1).otherwise(0)).alias("starts"),
            F.sum(F.when(F.col("kind").isin(*TERMINAL_KINDS), 1).otherwise(0)).alias("terminals"),
            F.min(F.when(F.col("kind") == "start", F.col("aggregate_version"))).alias("start_version"),
            F.min(F.when(F.col("kind").isin(*TERMINAL_KINDS), F.col("aggregate_version"))).alias("terminal_version"),
            F.max(F.when(F.col("kind") == "succeeded", F.col("occurred_at"))).alias("succeeded_at"),
            F.max(F.when(F.col("kind") == "declined", F.col("occurred_at"))).alias("declined_at"),
            F.max(F.when(F.col("kind") == "unknown", F.col("occurred_at"))).alias("unknown_at"),
            F.max(F.when(F.col("kind") == "declined", F.col("detail"))).alias("decline_source"),
            F.min_by("start", F.when(F.col("kind") == "start", F.col("aggregate_version"))).alias("start"),
            F.min_by("decision", F.when(F.col("kind") == "decision", F.col("aggregate_version"))).alias("decision"),
        )
        .select(
            "*",
            *[F.col(f"start.{name}").alias(name) for name, _ in START_FIELDS],
            *[F.col(f"decision.{name}").alias(name) for name, _ in DECISION_FIELDS],
        )
    )
    order = payment_status.select(*PAYMENT_KEY, "payment_status", "order_violation")
    joined = per_attempt.join(order, PAYMENT_KEY, "left")
    outcome = (
        F.when(F.col("succeeded_at").isNotNull(), F.lit("succeeded"))
        .when(F.col("declined_at").isNotNull(), F.lit("declined"))
        .when(F.col("unknown_at").isNotNull(), F.lit("unknown"))
        .otherwise(F.lit("pending"))
    )
    return joined.select(
        *ATTEMPT_KEY,
        "merchant_id",
        "customer_id",
        "payment_method_type",
        "payment_method_fingerprint",
        "device_fingerprint",
        "amount_minor",
        "currency",
        "started_at",
        "attempted_at",
        "risk_outcome",
        "risk_reason_codes",
        "policy_version",
        "model_version",
        "feature_set_version",
        "risk_score",
        "assessment_id",
        outcome.alias("outcome"),
        F.when(outcome == "declined", F.col("decline_source")).alias("decline_source"),
        F.coalesce(F.col("succeeded_at"), F.col("declined_at"), F.col("unknown_at")).alias("outcome_at"),
        F.coalesce(F.col("payment_status"), F.lit("open")).alias("payment_status"),
        (
            (F.coalesce(F.col("starts"), F.lit(0)) == 1)
            & (F.coalesce(F.col("terminals"), F.lit(0)) <= 1)
            & (F.col("terminal_version").isNull() | (F.col("terminal_version") > F.col("start_version")))
            & ~F.coalesce(F.col("order_violation"), F.lit(False))
        ).alias("lifecycle_valid"),
    )


def payments(events: DataFrame) -> DataFrame:
    ordered = Window.partitionBy(*PAYMENT_KEY).orderBy("aggregate_version")
    return (
        events.withColumn(
            "order_violation",
            F.coalesce(F.col("occurred_at") < F.lag("occurred_at").over(ordered), F.lit(False)),
        )
        .groupBy(*PAYMENT_KEY)
        .agg(
            F.max(F.col("payment_succeeded").isNotNull().cast("int")).alias("succeeded"),
            F.max(F.col("payment_expired").isNotNull().cast("int")).alias("expired"),
            F.max(F.col("order_violation").cast("int")).cast("boolean").alias("order_violation"),
        )
        .select(
            *PAYMENT_KEY,
            F.when(F.col("succeeded") == 1, F.lit("succeeded"))
            .when(F.col("expired") == 1, F.lit("expired"))
            .otherwise(F.lit("open"))
            .alias("payment_status"),
            "order_violation",
        )
    )


def label_observations(labels: DataFrame) -> DataFrame:
    hashed = labels.withColumn("observation_id", F.sha2(F.to_json(F.struct(*LABEL_HASH_FIELDS)), 256))
    per_observation = Window.partitionBy("observation_id").orderBy("_ingested_at", "_source")
    return (
        hashed.withColumn("_position", F.row_number().over(per_observation))
        .where(F.col("_position") == 1)
        .drop("_position")
    )


def link_labels(labels: DataFrame, attempts: DataFrame, payment_status: DataFrame) -> DataFrame:
    attempt = attempts.select(
        *ATTEMPT_KEY,
        F.col("started_at").alias("attempt_started_at"),
        F.col("outcome").alias("attempt_outcome"),
        F.col("outcome_at").alias("attempt_outcome_at"),
    )
    state = payment_status.select(*PAYMENT_KEY, F.col("payment_status").alias("payment_state"))
    joined = labels.join(attempt, ATTEMPT_KEY, "left").join(state, PAYMENT_KEY, "left")
    known = F.col("payment_state").isNotNull()
    terminal = F.col("payment_state").isin("succeeded", "expired")
    historical = F.col("_source") == "historical"
    missing = F.col("attempt_started_at").isNull()
    chargeback = F.col("source") == "LABEL_SOURCE_CHARGEBACK"
    linkage = (
        F.when(missing & ~known & historical, F.lit("orphan"))
        .when(missing & ~known, F.lit("pending"))
        .when(missing & (historical | terminal), F.lit("violation"))
        .when(missing, F.lit("pending"))
        .when(F.col("label_available_at") < F.col("attempt_started_at"), F.lit("violation"))
        .when(
            chargeback
            & (F.col("attempt_outcome") == "succeeded")
            & (F.col("label_available_at") < F.col("attempt_outcome_at")),
            F.lit("violation"),
        )
        .when(chargeback & (F.col("attempt_outcome") == "declined"), F.lit("violation"))
        .when(chargeback & (F.col("attempt_outcome") == "succeeded"), F.lit("linked"))
        .when(chargeback & terminal, F.lit("violation"))
        .when(chargeback, F.lit("pending"))
        .otherwise(F.lit("linked"))
    )
    return joined.withColumn("linkage", linkage).drop(
        "attempt_started_at", "attempt_outcome", "attempt_outcome_at", "payment_state"
    )


def reject_payment_rows(frame: DataFrame, table: str, conflicting_payments, conflict_reason) -> DataFrame:
    rejected = frame.join(conflicting_payments, PAYMENT_KEY, "left_semi").join(conflict_reason, PAYMENT_KEY, "left")
    return rejected.select(
        F.lit(table).alias("rejected_table"),
        F.col("reason"),
        F.col("_source").alias("source"),
        *PAYMENT_KEY,
        F.to_json(F.struct(*[F.col(c) for c in frame.columns if c != "_source"])).alias("row_json"),
    )


def write_table(spark, name, frame, pins, run_id, partition=None, options=None) -> dict:
    create_table(spark, name, frame, partition)
    stage(spark, name)
    writer = frame.writeTo(name).options(**(options or {}))
    for key, value in {**pins, "dp2.run_id": run_id}.items():
        writer = writer.option(f"snapshot-property.{key}", value)
    writer.overwrite(F.lit(True))
    return snapshot_info(spark, name)


def main():
    run_id = os.environ["RUN_ID"]
    mode = os.environ.get("SILVER_MODE", "historical")
    spark = session("dp2-silver")
    try:
        report = run(spark, run_id, mode)
        print(json.dumps(report, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
