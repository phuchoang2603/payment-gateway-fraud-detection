from contextlib import contextmanager
from dataclasses import dataclass

from pyspark.sql import functions as F

from silver_pipeline.diagnostics import source_diagnostics
from silver_pipeline.silver import attempt_histories, canonical_events, label_observations, mark_pending_events
from silver_pipeline.silver_quality import (
    invalid_attempt_contexts,
    invalid_checkout_amounts,
    invalid_complete_lifecycles,
    invalid_events,
    invalid_recurring_lifecycles,
    missing_required_fields,
)
from silver_pipeline.silver_source import selected_bronze


@dataclass(frozen=True)
class CandidateFrames:
    events: object
    labels: object
    attempts: object
    diagnostics: dict


def build(source, relative_error=0.05):
    events = mark_pending_events(canonical_events(source["payment_events"]))
    labels = label_observations(source["fraud_labels"])
    attempts = attempt_histories(events.canonical, labels.observations)
    diagnostics = source_diagnostics(source["payment_events"], events.canonical, events.provenance, relative_error)
    return CandidateFrames(events=events, labels=labels, attempts=attempts, diagnostics=diagnostics)


def transform(
    spark, bronze_store, silver_store, candidate, namespace="bronze", silver_namespace="silver", relative_error=0.05
):
    from silver_pipeline.silver_incremental import recompute

    source = selected_bronze(spark, bronze_store, candidate, namespace)
    return recompute(
        spark, bronze_store, silver_store, candidate, source, build, namespace, silver_namespace, relative_error
    )


@contextmanager
def persist_intermediates(candidate):
    frames = [candidate.events.canonical, candidate.events.provenance, candidate.labels.observations]
    try:
        for frame in frames:
            frame.persist()
        yield candidate
    finally:
        for frame in reversed(frames):
            frame.unpersist()


def check_violations(checks):
    findings = [frame.limit(1).select(F.lit(message).alias("violation")) for frame, message in checks]
    combined = findings[0]
    for finding in findings[1:]:
        combined = combined.unionByName(finding)
    result = combined.limit(1).collect()
    if result:
        raise ValueError(result[0].violation)


def validate_basics(candidate, completeness):
    if completeness not in ("complete", "recurring"):
        raise ValueError("completeness must be complete or recurring")
    events = candidate.events.canonical
    lifecycle = (
        (invalid_complete_lifecycles(events), "complete payment lifecycle is invalid")
        if completeness == "complete"
        else (invalid_recurring_lifecycles(events), "observed recurring payment transitions are invalid")
    )
    check_violations(
        [
            (candidate.events.conflicts, "event identity has conflicting business content or physical schema"),
            (candidate.attempts.invalid, "attempt or payment identity has multiple start events"),
            (missing_required_fields(events), "event is missing conditionally required contract fields"),
            (invalid_events(events), "event violates envelope, payload or assessment reference rules"),
            (invalid_checkout_amounts(events), "checkout item quantities or amounts are inconsistent"),
            (invalid_attempt_contexts(events, completeness), "attempt context differs from its immutable checkout"),
            lifecycle,
        ]
    )
    if (
        events.where(
            F.col("integration_id").isNull()
            | (F.col("integration_id") == "")
            | F.col("event_id").isNull()
            | (F.col("event_id") == "")
            | F.col("payment_id").isNull()
            | (F.col("payment_id") == "")
        )
        .limit(1)
        .count()
    ):
        raise ValueError("event identity is incomplete")
    labels = candidate.labels.observations
    invalid_labels = labels.where(
        F.col("integration_id").isNull()
        | (F.col("integration_id") == "")
        | F.col("payment_id").isNull()
        | (F.col("payment_id") == "")
        | F.col("attempt_id").isNull()
        | (F.col("attempt_id") == "")
        | F.col("occurred_at").isNull()
        | F.col("label_available_at").isNull()
        | (F.col("label_available_at") < F.col("occurred_at"))
        | F.col("is_fraud").isNull()
        | (~F.col("source").isin("LABEL_SOURCE_SIMULATION_TRUTH", "LABEL_SOURCE_CHARGEBACK"))
        | F.col("source").isNull()
        | (
            (F.col("source") == "LABEL_SOURCE_SIMULATION_TRUTH")
            & (F.col("scenario").isNull() | (F.col("scenario") == ""))
        )
        | ((F.col("source") == "LABEL_SOURCE_CHARGEBACK") & ((~F.col("is_fraud")) | F.col("scenario").isNotNull()))
    )
    if invalid_labels.limit(1).count():
        raise ValueError("label observation violates availability or source semantics")
    linked = candidate.attempts.linked_labels
    if (
        linked.where(
            (F.col("attempt_started_at").isNotNull() & (F.col("label_available_at") < F.col("attempt_started_at")))
            | (
                (F.col("source") == "LABEL_SOURCE_CHARGEBACK")
                & F.col("payment_succeeded_at").isNotNull()
                & (F.col("label_available_at") < F.col("payment_succeeded_at"))
            )
        )
        .limit(1)
        .count()
    ):
        raise ValueError("label is available before its linked context")
    if completeness == "complete" and (
        candidate.attempts.pending.limit(1).count()
        or candidate.attempts.linked_labels.where(F.col("pending_context")).limit(1).count()
    ):
        raise ValueError("complete history has missing attempt context")
