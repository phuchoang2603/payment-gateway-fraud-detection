"""DP2: Silver payment events, attempts and labels for a range of UTC days, from pinned Bronze.

Every event of a payment occurs within its expiry, so a payment belongs to the UTC day of its first event, its
payment_day. A run over [start, end] reads Bronze from the day before start to the day after end, keeps the payments
whose payment_day falls in the range, and overwrites only those days; labels are overwritten by the day they became
available, and link to the attempts of the LABEL_LOOKBACK days before. A run is a pure function of Bronze: re-running a
range changes nothing, and a backfill is the same job over a longer range.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from batch.bronze.ingest import EVENTS as BRONZE_EVENTS
from batch.bronze.ingest import LABELS as BRONZE_LABELS
from batch.bronze.schema import EVENT_COLUMNS, LABEL_COLUMNS
from batch.days import DAY, between, day_range, midnight
from batch.lakehouse import create_table, overwrite, read_at, ref_snapshot, write_ref
from batch.metrics import phase
from batch.spark import ICEBERG_CATALOG, session
from batch.store import Store

NAMESPACE = f"{ICEBERG_CATALOG}.silver"
PAYMENT_EVENTS = f"{NAMESPACE}.payment_events"
ATTEMPTS = f"{NAMESPACE}.attempts"
LABELS = f"{NAMESPACE}.labels"
REJECTED = f"{NAMESPACE}.rejected_events"
TABLES = (PAYMENT_EVENTS, ATTEMPTS, LABELS, REJECTED)

PAYMENT_KEY = ["integration_id", "payment_id"]
ATTEMPT_KEY = [*PAYMENT_KEY, "attempt_id"]
PAYMENT_DAY = "payment_day"
# Payments expire 60 minutes after creation; a day of margin keeps every event of a payment inside the read.
LIFECYCLE = DAY
# Chargebacks become available 7 to 21 days after the payment.
LABEL_LOOKBACK = timedelta(days=45)
RUN_PROPERTY = "dp2.run_id"
RANGE_PROPERTY = "dp2.range"
BRONZE_PROPERTIES = {
    BRONZE_EVENTS: "bronze.payment_events.snapshot_id",
    BRONZE_LABELS: "bronze.fraud_labels.snapshot_id",
}
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
EVENT_OUTPUT = [*EVENT_COLUMNS.keys(), "event_hash", "_source", PAYMENT_DAY]
LAYOUT = {
    PAYMENT_EVENTS: (PAYMENT_DAY, "occurred_at"),
    ATTEMPTS: (PAYMENT_DAY, "started_at"),
    LABELS: ("days(label_available_at)", "label_available_at"),
    REJECTED: ("partition_day", None),
}


def table_layout(name: str):
    partition, order = LAYOUT[name]
    column = F.days(partition[len("days(") : -1]) if partition.startswith("days(") else F.col(partition)
    return column, order


def payment_days(events: DataFrame, first, last) -> DataFrame:
    """Each payment's day, the UTC day of its first event, for the payments that begin within [first, last]."""
    return (
        events.groupBy(*PAYMENT_KEY)
        .agg(F.to_date(F.min("occurred_at")).alias(PAYMENT_DAY))
        .where((F.col(PAYMENT_DAY) >= F.lit(first)) & (F.col(PAYMENT_DAY) <= F.lit(last)))
    )


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
    order = payment_status.select(*PAYMENT_KEY, PAYMENT_DAY, "payment_status", "order_violation")
    joined = per_attempt.join(order, PAYMENT_KEY, "left")
    outcome = (
        F.when(F.col("succeeded_at").isNotNull(), F.lit("succeeded"))
        .when(F.col("declined_at").isNotNull(), F.lit("declined"))
        .when(F.col("unknown_at").isNotNull(), F.lit("unknown"))
        .otherwise(F.lit("pending"))
    )
    return joined.select(
        *ATTEMPT_KEY,
        PAYMENT_DAY,
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
            F.min(PAYMENT_DAY).alias(PAYMENT_DAY),
            F.max(F.col("payment_succeeded").isNotNull().cast("int")).alias("succeeded"),
            F.max(F.col("payment_expired").isNotNull().cast("int")).alias("expired"),
            F.max(F.col("order_violation").cast("int")).cast("boolean").alias("order_violation"),
        )
        .select(
            *PAYMENT_KEY,
            PAYMENT_DAY,
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


def rejected_rows(frame: DataFrame, table: str, day, conflicting: DataFrame) -> DataFrame:
    """Rows of conflicting payments, kept whole as JSON with the conflicting event that rejected their payment."""
    rejected = frame.join(conflicting, PAYMENT_KEY)
    return rejected.select(
        F.lit(table).alias("rejected_table"),
        F.col("reason"),
        F.col("_source").alias("source"),
        *PAYMENT_KEY,
        day.alias("partition_day"),
        F.to_json(F.struct(*[F.col(c) for c in frame.columns if c != "_source"])).alias("row_json"),
    )


def write(spark, name: str, frame: DataFrame, condition, pins: dict) -> dict:
    partition, order = table_layout(name)
    create_table(spark, name, frame, partition, order=order)
    return overwrite(spark, name, frame, condition, pins)


def counts_by(frame: DataFrame, column: str) -> dict:
    return {row[column]: row["count"] for row in frame.groupBy(column).count().collect()}


def run(spark, run_id: str, start_day, end_day) -> dict:
    floor, cutoff = midnight(start_day), midnight(end_day + DAY)
    snapshots = {table: ref_snapshot(spark, table) for table in BRONZE_PROPERTIES}
    pins = {
        **{prop: str(snapshots[table] or "") for table, prop in BRONZE_PROPERTIES.items()},
        RUN_PROPERTY: run_id,
        RANGE_PROPERTY: f"{start_day}/{end_day}",
    }
    phases, written = {}, {}
    in_days = (F.col(PAYMENT_DAY) >= F.lit(start_day)) & (F.col(PAYMENT_DAY) <= F.lit(end_day))

    window = read_at(spark, BRONZE_EVENTS, snapshots[BRONZE_EVENTS]).where(
        between("occurred_at", floor - LIFECYCLE, cutoff + LIFECYCLE)
    )
    days = payment_days(window, start_day, end_day)
    hashed = with_event_hash(window.join(days, PAYMENT_KEY))
    collapsed = distinct_events(hashed).persist()
    conflicting = (
        collapsed.where(F.col("hashes") > 1).groupBy(*PAYMENT_KEY).agg(F.min("event_id").alias("reason")).persist()
    )
    accepted = collapsed.join(conflicting, PAYMENT_KEY, "left_anti").select(*EVENT_OUTPUT)
    with phase(spark, phases, "write payment_events"):
        written[PAYMENT_EVENTS] = write(spark, PAYMENT_EVENTS, accepted, in_days, pins)
    events = read_at(spark, PAYMENT_EVENTS, written[PAYMENT_EVENTS]["snapshot_id"]).where(in_days)
    status = payments(events)
    with phase(spark, phases, "write attempts"):
        written[ATTEMPTS] = write(spark, ATTEMPTS, attempts(events, status), in_days, pins)

    # Labels of the range's availability days link to the attempts and rejections of the lookback days before.
    lookback = (F.col(PAYMENT_DAY) >= F.lit(start_day - LABEL_LOOKBACK)) & (F.col(PAYMENT_DAY) <= F.lit(end_day))
    known = read_at(spark, ATTEMPTS, written[ATTEMPTS]["snapshot_id"]).where(lookback)
    known_status = known.select(*PAYMENT_KEY, "payment_status").distinct()
    stored_conflicts = (
        read_at(spark, REJECTED, ref_snapshot(spark, REJECTED, write_ref()))
        .where(
            (F.col("rejected_table") == "payment_events")
            & (F.col("partition_day") >= F.lit(start_day - LABEL_LOOKBACK))
            & (F.col("partition_day") < F.lit(start_day))
        )
        .groupBy(*PAYMENT_KEY)
        .agg(F.min("reason").alias("reason"))
        if spark.catalog.tableExists(REJECTED)
        else conflicting.limit(0)
    )
    conflicts = conflicting.unionByName(stored_conflicts)
    available = between("label_available_at", floor, cutoff)
    observations = label_observations(read_at(spark, BRONZE_LABELS, snapshots[BRONZE_LABELS]).where(available))
    linked = link_labels(observations, known, known_status)
    with phase(spark, phases, "write labels"):
        written[LABELS] = write(spark, LABELS, linked.join(conflicts, PAYMENT_KEY, "left_anti"), available, pins)

    conflict_events = hashed.join(conflicting.select(*PAYMENT_KEY), PAYMENT_KEY, "left_semi")
    conflict_distinct = distinct_events(conflict_events).drop("hashes")
    # A rejected attempt is attributed to the historical source when any of its payment's copies came from it.
    sources = conflict_events.groupBy(*PAYMENT_KEY).agg(
        F.when(F.max((F.col("_source") == "historical").cast("int")) == 1, "historical")
        .otherwise("kafka")
        .alias("_source")
    )
    conflict_attempts = attempts(conflict_distinct, payments(conflict_distinct)).join(sources, PAYMENT_KEY)
    payment_reasons = conflicting.join(days, PAYMENT_KEY)
    rejected = reduce(
        DataFrame.unionByName,
        [
            rejected_rows(conflict_events.drop(PAYMENT_DAY), "payment_events", F.col(PAYMENT_DAY), payment_reasons),
            rejected_rows(conflict_attempts.drop(PAYMENT_DAY), "attempts", F.col(PAYMENT_DAY), payment_reasons),
            rejected_rows(observations, "labels", F.to_date("label_available_at"), conflicts),
        ],
    ).withColumn("_run_id", F.lit(run_id))
    rejected_days = (F.col("partition_day") >= F.lit(start_day)) & (F.col("partition_day") <= F.lit(end_day))
    with phase(spark, phases, "write rejected_events"):
        written[REJECTED] = write(spark, REJECTED, rejected, rejected_days, pins)

    with phase(spark, phases, "report"):
        report = {
            "run_id": run_id,
            "start": str(start_day),
            "end": str(end_day),
            "bronze_snapshots": {table.split(".", 1)[1]: snapshot for table, snapshot in snapshots.items()},
            "silver_snapshots": written,
            "events": {
                "deliveries": hashed.count(),
                "distinct_events": collapsed.count(),
                "conflicting_payments": conflicting.count(),
            },
            "labels": {"linkage": counts_by(linked, "linkage")},
            "phases": phases,
        }
    collapsed.unpersist()
    conflicting.unpersist()
    Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/silver.json", report)
    return report


def main():
    run_id = os.environ["RUN_ID"]
    start_day, end_day = day_range(os.environ, datetime.now(timezone.utc).date())
    spark = session("dp2-silver")
    try:
        report = run(spark, run_id, start_day, end_day)
        print(json.dumps(report, sort_keys=True, default=str))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
