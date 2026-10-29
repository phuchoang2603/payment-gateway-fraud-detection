from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from fraud_batch.common.products import AUDIT

EVENT_KEY = ("integration_id", "event_id")
ATTEMPT_KEY = ("integration_id", "payment_id", "attempt_id")
PHYSICAL_SCHEMA = "_source_schema_version"


@dataclass(frozen=True)
class EventFrames:
    canonical: DataFrame
    provenance: DataFrame
    conflicts: DataFrame


@dataclass(frozen=True)
class LabelFrames:
    observations: DataFrame
    provenance: DataFrame


@dataclass(frozen=True)
class AttemptFrames:
    histories: DataFrame
    pending: DataFrame
    invalid: DataFrame
    linked_labels: DataFrame


def payment_context(events: DataFrame) -> DataFrame:
    return events.groupBy("integration_id", "payment_id").agg(
        F.sum(F.when(F.col("payment_created").isNotNull(), 1).otherwise(0)).alias("created_events"),
        F.sum(
            F.when(F.col("payment_succeeded").isNotNull() | F.col("payment_expired").isNotNull(), 1).otherwise(0)
        ).alias("terminal_events"),
        F.min("aggregate_version").alias("payment_first_version"),
        F.max("aggregate_version").alias("payment_last_version"),
        F.count(F.lit(1)).alias("payment_event_count"),
    )


def mark_pending_events(frames: EventFrames, context: DataFrame) -> EventFrames:
    events = frames.canonical
    pending = (
        (F.col("created_events") == 0)
        | (F.col("terminal_events") == 0)
        | (F.col("payment_first_version") != 1)
        | (F.col("payment_last_version") != F.col("payment_event_count"))
    )
    flags = events.select(*EVENT_KEY, "payment_id").join(context, ["integration_id", "payment_id"], "left")
    flags = flags.select(*EVENT_KEY, "payment_id", F.coalesce(pending, F.lit(True)).alias("pending_context"))
    provenance = frames.provenance.join(flags, list(EVENT_KEY), "left").withColumn(
        "pending_context", F.coalesce(F.col("pending_context"), F.lit(False))
    )
    provenance = provenance.withColumn(
        "pending_context_deliveries", F.when(F.col("pending_context"), F.col("representative_deliveries")).otherwise(0)
    ).withColumn(
        "representative_deliveries", F.when(F.col("pending_context"), 0).otherwise(F.col("representative_deliveries"))
    )
    return EventFrames(canonical=events, provenance=provenance, conflicts=frames.conflicts)


def attempt_histories(events: DataFrame, labels: DataFrame, context: DataFrame) -> AttemptFrames:
    attempt = F.coalesce(
        *[
            F.col(f"{payload}.attempt_id")
            for payload in (
                "attempt_started",
                "risk_decision_recorded",
                "processor_operation_requested",
                "processor_outcome_unknown",
                "attempt_declined",
                "payment_succeeded",
            )
        ]
    )
    selected = events.withColumn("attempt_id", attempt).where(F.col("attempt_id").isNotNull())
    history_event = F.struct(
        F.col("aggregate_version").alias("sequence"),
        F.col("event_id").alias("event_id"),
        F.col("occurred_at").alias("occurred_at"),
    )
    grouped = selected.groupBy(*ATTEMPT_KEY).agg(
        F.sort_array(F.collect_list(history_event)).alias("events"),
        F.sum(F.when(F.col("attempt_started").isNotNull(), 1).otherwise(0)).alias("started_events"),
        F.min(F.when(F.col("attempt_started").isNotNull(), F.col("occurred_at"))).alias("attempt_started_at"),
        F.min(F.col("attempt_started.signals.attempted_at")).alias("attempted_at"),
    )
    grouped = grouped.join(context, ["integration_id", "payment_id"], "left").fillna(
        {"created_events": 0, "terminal_events": 0}
    )
    invalid = grouped.where(
        (F.col("started_events") > 1) | (F.col("created_events") > 1) | (F.col("terminal_events") > 1)
    )
    complete_context = (F.col("payment_first_version") == 1) & (
        F.col("payment_last_version") == F.col("payment_event_count")
    )
    pending = grouped.where(
        (F.col("started_events") <= 1) & (F.col("created_events") <= 1) & (F.col("terminal_events") <= 1)
    ).where(
        (F.col("started_events") == 0)
        | (F.col("created_events") == 0)
        | (F.col("terminal_events") == 0)
        | ~complete_context
    )
    histories = grouped.where(
        (F.col("started_events") == 1)
        & (F.col("created_events") == 1)
        & (F.col("terminal_events") == 1)
        & complete_context
    )
    linked = labels.join(histories.select(*ATTEMPT_KEY, "attempt_started_at"), list(ATTEMPT_KEY), "left")
    succeeded = (
        events.where(F.col("payment_succeeded").isNotNull())
        .select(
            "integration_id", "payment_id", F.col("payment_succeeded.attempt_id").alias("attempt_id"), "occurred_at"
        )
        .groupBy(*ATTEMPT_KEY)
        .agg(F.min("occurred_at").alias("payment_succeeded_at"))
    )
    linked = linked.join(succeeded, list(ATTEMPT_KEY), "left")
    linked = linked.withColumn(
        "pending_context",
        F.col("attempt_started_at").isNull()
        | ((F.col("source") == "LABEL_SOURCE_CHARGEBACK") & F.col("payment_succeeded_at").isNull()),
    )
    return AttemptFrames(histories=histories, pending=pending, invalid=invalid, linked_labels=linked)


def label_observations(deliveries: DataFrame) -> LabelFrames:
    required = {
        "integration_id",
        "payment_id",
        "attempt_id",
        "occurred_at",
        "label_available_at",
        "source",
        "is_fraud",
    }
    missing = required.difference(deliveries.columns)
    if missing:
        raise ValueError(f"label deliveries lack required observation fields: {sorted(missing)}")
    business = [name for name in deliveries.columns if name not in AUDIT]
    provenance = [name for name in AUDIT if name in deliveries.columns]
    canonical_json = F.to_json(
        F.struct(*[F.col(name) for name in business]),
        {"ignoreNullFields": "false", "timeZone": "UTC", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"},
    )
    identified = deliveries.withColumn("observation_id", F.sha2(canonical_json, 256))
    observations = identified.select("observation_id", *business).distinct()
    provenance = identified.groupBy("observation_id", *provenance).count().withColumnRenamed("count", "deliveries")
    return LabelFrames(observations=observations, provenance=provenance)


def canonical_events(deliveries: DataFrame) -> EventFrames:
    required = set(EVENT_KEY) | set(AUDIT) | {PHYSICAL_SCHEMA}
    missing = required.difference(deliveries.columns)
    if missing:
        raise ValueError(f"event deliveries lack required provenance or identity: {sorted(missing)}")

    business = [name for name in deliveries.columns if name not in AUDIT and name != PHYSICAL_SCHEMA]
    variants = deliveries.select(*business, PHYSICAL_SCHEMA).distinct()
    conflicts = variants.groupBy(*EVENT_KEY).count().where(F.col("count") > 1).select(*EVENT_KEY)
    canonical = variants.join(conflicts, list(EVENT_KEY), "left_anti").select(*business, PHYSICAL_SCHEMA)

    provenance_columns = [*EVENT_KEY, *AUDIT]
    grouped = deliveries.groupBy(*provenance_columns).count().withColumnRenamed("count", "deliveries")
    grouped = grouped.join(conflicts.withColumn("conflict", F.lit(True)), list(EVENT_KEY), "left")
    grouped = grouped.withColumn("conflict", F.coalesce(F.col("conflict"), F.lit(False)))
    ordering = F.to_json(F.struct(*[F.col(name) for name in provenance_columns if name not in EVENT_KEY]))
    position = Window.partitionBy(*EVENT_KEY).orderBy(ordering)
    representative = (~F.col("conflict")) & (F.row_number().over(position) == 1)
    grouped = grouped.withColumn("representative_deliveries", F.when(representative, 1).otherwise(0))
    grouped = grouped.withColumn("conflicting_deliveries", F.when(F.col("conflict"), F.col("deliveries")).otherwise(0))
    grouped = grouped.withColumn("pending_context_deliveries", F.lit(0))
    grouped = grouped.withColumn(
        "identical_duplicate_deliveries",
        F.col("deliveries") - F.col("representative_deliveries") - F.col("conflicting_deliveries"),
    ).drop("conflict")
    return EventFrames(canonical=canonical, provenance=grouped, conflicts=conflicts)
