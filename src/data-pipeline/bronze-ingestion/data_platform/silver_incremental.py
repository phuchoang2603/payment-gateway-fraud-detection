from pyspark.sql import functions as F

from data_platform.diagnostics import source_diagnostics
from data_platform.silver import AttemptFrames, EventFrames, LabelFrames
from data_platform.silver_runs import digest
from data_platform.silver_source import selected_bronze
from data_platform.silver_write import OUTPUTS, selected_snapshot, table_name

PAYMENT_KEY = ("integration_id", "payment_id")


def accepted_base(store, candidate):
    pointer = store.get("dp2/accepted.json")
    if not pointer:
        return None
    receipt = store.get(pointer["receipt_key"])
    if not receipt or digest(receipt) != pointer["receipt_sha256"] or receipt.get("outcome") != "passed":
        raise ValueError("accepted Silver receipt is missing or altered")
    if receipt.get("candidate_id") != pointer.get("candidate_id") or set(receipt.get("snapshots", {})) != set(OUTPUTS):
        raise ValueError("accepted Silver pointer does not identify complete snapshots")
    compatible = all(
        receipt.get(key) == candidate.get(key)
        for key in ("contracts", "product_sha256", "quality_sha256", "transformation_revision", "completeness")
    )
    current = {item["dataset_version"]: item for item in candidate["inputs"]}
    previous = {item["dataset_version"]: item for item in receipt["inputs"]}
    if candidate["completeness"] == "recurring" and not set(previous).issubset(current):
        raise ValueError("recurring Silver input cannot omit previously accepted Bronze receipts")
    if not compatible or any(current.get(version) != value for version, value in previous.items()):
        return None
    return receipt


def affected_payments(source, delta, boundaries):
    new_events = delta["payment_events"].select("integration_id", "event_id").distinct()
    related = source["payment_events"].join(new_events, ["integration_id", "event_id"], "inner").select(*PAYMENT_KEY)
    affected = related.unionByName(delta["fraud_labels"].select(*PAYMENT_KEY))
    if boundaries:
        start = F.to_timestamp(F.lit(boundaries["start"]))
        end = F.to_timestamp(F.lit(boundaries["end"]))
        events = source["payment_events"].where((F.col("occurred_at") >= start) & (F.col("occurred_at") < end))
        labels = source["fraud_labels"].where(
            (F.col("label_available_at") >= start) & (F.col("label_available_at") < end)
        )
        affected = affected.unionByName(events.select(*PAYMENT_KEY)).unionByName(labels.select(*PAYMENT_KEY))
    return affected.distinct()


def exclude_payments(frame, affected):
    columns = [column for column in frame.columns if column != "_candidate_id"]
    return frame.join(affected, list(PAYMENT_KEY), "left_anti").select(*columns)


def recompute(
    spark,
    bronze_store,
    silver_store,
    candidate,
    source,
    build,
    bronze_namespace="bronze",
    silver_namespace="silver",
    relative_error=0.05,
):
    from data_platform.silver_transform import CandidateFrames

    previous = accepted_base(silver_store, candidate)
    if previous is None:
        if candidate.get("boundaries") is not None:
            raise ValueError("bounded backfill requires a compatible accepted Silver base")
        return build(source, relative_error)
    previous_versions = {item["dataset_version"] for item in previous["inputs"]}
    additions = [item for item in candidate["inputs"] if item["dataset_version"] not in previous_versions]
    delta = (
        selected_bronze(spark, bronze_store, {"inputs": additions}, bronze_namespace)
        if additions
        else {table: frame.limit(0) for table, frame in source.items()}
    )
    affected = affected_payments(source, delta, candidate.get("boundaries"))
    changed = {table: frame.join(affected, list(PAYMENT_KEY), "left_semi") for table, frame in source.items()}
    updated = build(changed, relative_error)
    prior = {
        table: selected_snapshot(spark, table_name(table, silver_namespace), snapshot, previous["candidate_id"])
        for table, snapshot in previous["snapshots"].items()
    }
    events = EventFrames(
        canonical=exclude_payments(prior["payment_events"], affected).unionByName(updated.events.canonical),
        provenance=exclude_payments(prior["event_delivery_provenance"], affected).unionByName(
            updated.events.provenance
        ),
        conflicts=updated.events.conflicts,
    )
    prior_labels = exclude_payments(prior["label_observations"], affected)
    previous_label_provenance = prior["label_delivery_provenance"].join(
        prior["label_observations"].select("observation_id", *PAYMENT_KEY), "observation_id", "inner"
    )
    provenance_columns = [column for column in prior["label_delivery_provenance"].columns if column != "_candidate_id"]
    previous_label_provenance = previous_label_provenance.join(affected, list(PAYMENT_KEY), "left_anti").select(
        *provenance_columns
    )
    labels = LabelFrames(
        observations=prior_labels.select(*updated.labels.observations.columns).unionByName(
            updated.labels.observations
        ),
        provenance=previous_label_provenance.unionByName(updated.labels.provenance),
    )
    previous_attempts = exclude_payments(prior["attempt_histories"], affected)
    attempts = AttemptFrames(
        histories=previous_attempts.where(~F.col("pending_context"))
        .drop("pending_context")
        .unionByName(updated.attempts.histories),
        pending=previous_attempts.where(F.col("pending_context"))
        .drop("pending_context")
        .unionByName(updated.attempts.pending),
        invalid=updated.attempts.invalid,
        linked_labels=prior_labels.unionByName(updated.attempts.linked_labels),
    )
    diagnostics = source_diagnostics(source["payment_events"], events.canonical, events.provenance, relative_error)
    return CandidateFrames(events=events, labels=labels, attempts=attempts, diagnostics=diagnostics)
