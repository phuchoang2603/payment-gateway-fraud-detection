import json
import os
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from batch.bronze import EVENTS as BRONZE_EVENTS
from batch.bronze import LABELS as BRONZE_LABELS
from batch.bronze import current_snapshot
from batch.lakehouse import WRITE_PROPERTIES
from batch.metrics import phase
from batch.schema import EVENT_COLUMNS, LABEL_COLUMNS
from batch.spark import ICEBERG_CATALOG, session
from batch.store import Store

NAMESPACE = f"{ICEBERG_CATALOG}.silver"
PAYMENT_EVENTS = f"{NAMESPACE}.payment_events"
ATTEMPTS = f"{NAMESPACE}.attempts"
LABELS = f"{NAMESPACE}.labels"
REJECTED = f"{NAMESPACE}.rejected_events"

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


def run(spark, run_id):
    events_snapshot = current_snapshot(spark, BRONZE_EVENTS)
    labels_snapshot = current_snapshot(spark, BRONZE_LABELS)
    pins = {
        "bronze.payment_events.snapshot_id": str(events_snapshot),
        "bronze.fraud_labels.snapshot_id": str(labels_snapshot),
    }
    events = spark.read.option("versionAsOf", events_snapshot).table(BRONZE_EVENTS)
    labels = spark.read.option("versionAsOf", labels_snapshot).table(BRONZE_LABELS)

    cached = []
    phases = {}

    def cache(frame):
        cached.append(frame.persist(PERSIST))
        return cached[-1]

    try:
        copies = cache(with_event_hash(events))
        conflicts = cache(conflicting_events(copies))
        conflicting_payments = cache(conflicts.select(*PAYMENT_KEY).distinct())
        conflict_reason = cache(conflicts.groupBy(*PAYMENT_KEY).agg(F.min("event_id").alias("reason")))
        kept = cache(earliest_copies(copies))
        payment_status = cache(payments(kept))
        all_attempts = cache(attempts(kept, payment_status))
        all_labels = cache(label_observations(labels))
        linked = cache(link_labels(all_labels, all_attempts, payment_status))
        historical = kept.groupBy(*PAYMENT_KEY).agg(
            F.max(F.when(F.col("_source") == "historical", 1).otherwise(0)).alias("historical")
        )
        sourced_attempts = (
            all_attempts.join(historical, PAYMENT_KEY, "left")
            .withColumn("_source", F.when(F.col("historical") == 1, "historical").otherwise("kafka"))
            .drop("historical")
        )

        accepted_events = kept.join(conflicting_payments, PAYMENT_KEY, "left_anti").select(
            *EVENT_COLUMNS.keys(), "event_hash", "_source"
        )
        accepted_attempts = all_attempts.join(conflicting_payments, PAYMENT_KEY, "left_anti")
        accepted_labels = linked.join(conflicting_payments, PAYMENT_KEY, "left_anti")
        rejected = cache(
            reduce(
                DataFrame.unionByName,
                [
                    reject_payment_rows(copies, "payment_events", conflicting_payments, conflict_reason),
                    reject_payment_rows(sourced_attempts, "attempts", conflicting_payments, conflict_reason),
                    reject_payment_rows(all_labels, "labels", conflicting_payments, conflict_reason),
                ],
            ).withColumn("_run_id", F.lit(run_id))
        )

        with phase(spark, phases, "build"):
            input_events = copies.count()
            distinct_events = kept.count()
        report = {
            "run_id": run_id,
            "phases": phases,
            "bronze_snapshots": {"bronze.payment_events": events_snapshot, "bronze.fraud_labels": labels_snapshot},
            "events": {
                "input_rows": input_events,
                "distinct_events": distinct_events,
                "collapsed_deliveries": input_events - distinct_events,
                "conflicting_events": conflicts.select("event_id").distinct().count(),
                "conflicting_payments": conflicting_payments.count(),
            },
            "labels": {
                "input_rows": total_records(spark, BRONZE_LABELS, labels_snapshot),
                "distinct_observations": all_labels.count(),
            },
            "rejected_rows": {
                "by_table": counts_by(rejected, "rejected_table"),
                "by_source": counts_by(rejected, "source"),
            },
        }

        snapshots = {}
        for name, frame, partition in (
            (PAYMENT_EVENTS, accepted_events, F.days("occurred_at")),
            (ATTEMPTS, accepted_attempts, F.days("started_at")),
            (LABELS, accepted_labels, F.days("label_available_at")),
            (REJECTED, rejected, None),
        ):
            with phase(spark, phases, f"write {name.split('.')[-1]}"):
                snapshots[name] = write_table(spark, name, frame, pins, run_id, partition)
        report["labels"]["linkage"] = counts_by(accepted_labels, "linkage")
        report["attempts"] = snapshots[ATTEMPTS]["added_records"]
        report["lifecycle_invalid"] = accepted_attempts.where(~F.col("lifecycle_valid")).count()
        report["silver_snapshots"] = snapshots
        Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/silver.json", report)
        return report
    finally:
        for frame in cached:
            frame.unpersist()


def counts_by(frame: DataFrame, column: str) -> dict:
    return {row[column]: row["count"] for row in frame.groupBy(column).count().collect()}


def total_records(spark, table, snapshot) -> int:
    row = spark.sql(
        f"SELECT summary['total-records'] AS total FROM {table}.snapshots WHERE snapshot_id = {snapshot}"
    ).first()
    return int(row["total"])


def snapshot_info(spark, table) -> dict:
    row = spark.sql(
        f"SELECT snapshot_id, summary['added-records'] AS added FROM {table}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).first()
    return {"snapshot_id": row["snapshot_id"], "added_records": int(row["added"] or 0)}


def with_event_hash(events: DataFrame) -> DataFrame:
    return events.withColumn("event_hash", F.sha2(F.to_json(F.struct(*EVENT_HASH_FIELDS)), 256))


def conflicting_events(copies: DataFrame) -> DataFrame:
    per_event = Window.partitionBy("event_id")
    return copies.withColumn("hashes", F.size(F.collect_set("event_hash").over(per_event))).where(F.col("hashes") > 1)


def earliest_copies(copies: DataFrame) -> DataFrame:
    per_event = Window.partitionBy("event_id").orderBy("recorded_at", "_ingested_at", "_source")
    return (
        copies.withColumn("_position", F.row_number().over(per_event)).where(F.col("_position") == 1).drop("_position")
    )


def attempt_events(events: DataFrame) -> DataFrame:
    frames = []
    for kind, payload in ATTEMPT_PAYLOADS:
        detail = F.col("attempt_declined.source") if kind == "declined" else F.lit(None).cast("string")
        frames.append(
            events.where(F.col(payload).isNotNull()).select(
                *PAYMENT_KEY,
                F.col(f"{payload}.attempt_id").alias("attempt_id"),
                "aggregate_version",
                "occurred_at",
                F.lit(kind).alias("kind"),
                detail.alias("detail"),
            )
        )
    return reduce(DataFrame.unionByName, frames)


def first_per_attempt(events: DataFrame, payload: str, columns) -> DataFrame:
    keyed = events.where(F.col(payload).isNotNull()).withColumn("attempt_id", F.col(f"{payload}.attempt_id"))
    ordered = Window.partitionBy(*ATTEMPT_KEY).orderBy("aggregate_version")
    return (
        keyed.withColumn("_position", F.row_number().over(ordered))
        .where(F.col("_position") == 1)
        .select(*ATTEMPT_KEY, *columns)
    )


def attempts(events: DataFrame, payment_status: DataFrame) -> DataFrame:
    kinds = attempt_events(events)
    per_attempt = kinds.groupBy(*ATTEMPT_KEY).agg(
        F.sum(F.when(F.col("kind") == "start", 1).otherwise(0)).alias("starts"),
        F.sum(F.when(F.col("kind").isin(*TERMINAL_KINDS), 1).otherwise(0)).alias("terminals"),
        F.min(F.when(F.col("kind") == "start", F.col("aggregate_version"))).alias("start_version"),
        F.min(F.when(F.col("kind").isin(*TERMINAL_KINDS), F.col("aggregate_version"))).alias("terminal_version"),
        F.max(F.when(F.col("kind") == "succeeded", F.col("occurred_at"))).alias("succeeded_at"),
        F.max(F.when(F.col("kind") == "declined", F.col("occurred_at"))).alias("declined_at"),
        F.max(F.when(F.col("kind") == "unknown", F.col("occurred_at"))).alias("unknown_at"),
        F.max(F.when(F.col("kind") == "declined", F.col("detail"))).alias("decline_source"),
    )
    starts = first_per_attempt(
        events,
        "attempt_started",
        [
            F.col("attempt_started.context.merchant.merchant_id").alias("merchant_id"),
            F.col("attempt_started.context.customer.customer_id").alias("customer_id"),
            F.col("attempt_started.context.snapshot.total.minor_units").alias("amount_minor"),
            F.col("attempt_started.context.snapshot.total.currency").alias("currency"),
            F.col("occurred_at").alias("started_at"),
            F.col("attempt_started.signals.attempted_at").alias("attempted_at"),
            F.col("attempt_started.signals.device_fingerprint").alias("device_fingerprint"),
            F.col("attempt_started.signals.payment_method.type").alias("payment_method_type"),
            F.col("attempt_started.signals.payment_method.fingerprint").alias("payment_method_fingerprint"),
        ],
    )
    decisions = first_per_attempt(
        events,
        "risk_decision_recorded",
        [
            F.col("risk_decision_recorded.assessment.outcome").alias("risk_outcome"),
            F.col("risk_decision_recorded.assessment.reason_codes").alias("risk_reason_codes"),
            F.col("risk_decision_recorded.assessment.policy_version").alias("policy_version"),
            F.col("risk_decision_recorded.assessment.model_version").alias("model_version"),
            F.col("risk_decision_recorded.assessment.feature_set_version").alias("feature_set_version"),
            F.col("risk_decision_recorded.assessment.risk_score").alias("risk_score"),
            F.col("risk_decision_recorded.assessment.assessment_id").alias("assessment_id"),
        ],
    )
    keys = kinds.select(*ATTEMPT_KEY).distinct()
    order = payment_status.select(*PAYMENT_KEY, "payment_status", "order_violation")
    joined = (
        keys.join(starts, ATTEMPT_KEY, "left")
        .join(decisions, ATTEMPT_KEY, "left")
        .join(per_attempt, ATTEMPT_KEY, "left")
        .join(order, PAYMENT_KEY, "left")
    )
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


def write_table(spark, name, frame, pins, run_id, partition=None) -> dict:
    if not spark.catalog.tableExists(name):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
        creator = frame.limit(0).writeTo(name).using("iceberg")
        for key, value in WRITE_PROPERTIES.items():
            creator = creator.tableProperty(key, value)
        if partition is not None:
            creator = creator.partitionedBy(partition)
        creator.create()
    writer = frame.writeTo(name)
    for key, value in {**pins, "dp2.run_id": run_id}.items():
        writer = writer.option(f"snapshot-property.{key}", value)
    writer.overwrite(F.lit(True))
    return snapshot_info(spark, name)


def main():
    run_id = os.environ["RUN_ID"]
    spark = session("dp2-silver")
    try:
        report = run(spark, run_id)
        print(json.dumps(report, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
