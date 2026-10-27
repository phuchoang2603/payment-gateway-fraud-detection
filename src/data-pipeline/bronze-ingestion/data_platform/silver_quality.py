from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from data_platform.contracts import prepare
from data_platform.manifest import physical_schemas
from data_platform.products import AUDIT

PAYLOADS = (
    "payment_created",
    "attempt_started",
    "risk_decision_recorded",
    "processor_operation_requested",
    "processor_outcome_unknown",
    "attempt_declined",
    "payment_succeeded",
    "payment_expired",
)


def required_violations(column, data_type):
    if isinstance(data_type, T.StructType):
        invalid = F.lit(False)
        for field in data_type:
            child = column.getField(field.name)
            if not field.nullable:
                invalid = invalid | child.isNull()
            invalid = invalid | (child.isNotNull() & required_violations(child, field.dataType))
        return invalid
    if isinstance(data_type, T.ArrayType):
        return F.exists(
            column,
            lambda item: (
                (item.isNull() if not data_type.containsNull else F.lit(False))
                | (item.isNotNull() & required_violations(item, data_type.elementType))
            ),
        )
    return F.lit(False)


def missing_required_fields(events):
    expected = prepare("payment_events").schema
    invalid = F.lit(False)
    for field in expected:
        if field.name in AUDIT and field.name != "_source_schema_version":
            continue
        if field.name not in events.columns:
            raise ValueError(f"canonical events lack contract field: {field.name}")
        column = F.col(field.name)
        if not field.nullable:
            invalid = invalid | column.isNull()
        invalid = invalid | (column.isNotNull() & required_violations(column, field.dataType))
    return events.where(F.coalesce(invalid, F.lit(True)))


def invalid_events(events):
    definitions = physical_schemas()
    count = sum(F.col(payload).isNotNull().cast("int") for payload in PAYLOADS)
    assessment = "risk_decision_recorded.assessment"
    reference_id = F.col(f"{assessment}.assessment_id")
    reference_hash = F.col(f"{assessment}.evidence_sha256")
    invalid = (
        F.col("correlation_id").isNull()
        | (F.col("correlation_id") == "")
        | F.col("aggregate_version").isNull()
        | (F.col("aggregate_version") < 1)
        | F.col("occurred_at").isNull()
        | F.col("recorded_at").isNull()
        | (F.col("recorded_at") < F.col("occurred_at"))
        | F.col("schema_version").isNull()
        | ~F.col("schema_version").isin(*definitions["event_envelope_versions"])
        | F.col("_source_schema_version").isNull()
        | ~F.col("_source_schema_version").isin(*definitions["payment_events"])
        | (count != 1)
        | (F.col("payment_expired").isNotNull() & ~F.coalesce(F.col("payment_expired._present"), F.lit(False)))
        | (reference_id.isNotNull() & (F.length(reference_id) == 0))
        | (reference_hash.isNotNull() & ~reference_hash.rlike("^[a-f0-9]{64}$"))
    )
    for name, reference in (("assessment_id", reference_id), ("evidence_sha256", reference_hash)):
        supported = [
            version
            for version, definition in definitions["payment_events"].items()
            if name in definition["assessment_fields"]
        ]
        invalid = invalid | (reference.isNotNull() & ~F.col("_source_schema_version").isin(*supported))
    decision = F.col("risk_decision_recorded")
    invalid = invalid | (
        decision.isNotNull()
        & (
            F.col(f"{assessment}.outcome").isNull()
            | ~F.col(f"{assessment}.outcome").isin("RISK_OUTCOME_APPROVE", "RISK_OUTCOME_DECLINE")
            | F.col(f"{assessment}.policy_version").isNull()
            | (F.col(f"{assessment}.policy_version") == "")
            | F.col(f"{assessment}.reason_codes").isNull()
            | (
                (F.col(f"{assessment}.outcome") == "RISK_OUTCOME_DECLINE")
                & (F.size(F.col(f"{assessment}.reason_codes")) == 0)
            )
            | F.array_contains(F.col(f"{assessment}.reason_codes"), "")
        )
    )
    for payload in PAYLOADS:
        if payload not in ("payment_created", "payment_expired"):
            attempt_id = F.col(f"{payload}.attempt_id")
            invalid = invalid | (F.col(payload).isNotNull() & (attempt_id.isNull() | (attempt_id == "")))
    return events.where(F.coalesce(invalid, F.lit(True)))


def invalid_checkout_amounts(events):
    created = events.where(F.col("payment_created").isNotNull())
    total = F.col("payment_created.snapshot.total.minor_units")
    currency = F.col("payment_created.snapshot.total.currency")
    items = F.col("payment_created.snapshot.items")
    item_total = F.aggregate(
        items,
        F.lit(0).cast("decimal(38,0)"),
        lambda accumulated, item: (
            accumulated + item.quantity.cast("decimal(38,0)") * item.unit_price.minor_units.cast("decimal(38,0)")
        ).cast("decimal(38,0)"),
    )
    bad_item = F.exists(
        items,
        lambda item: (
            item.isNull()
            | item.quantity.isNull()
            | (item.quantity <= 0)
            | item.unit_price.minor_units.isNull()
            | (item.unit_price.minor_units < 0)
            | ~item.unit_price.currency.eqNullSafe(currency)
        ),
    )
    invalid = (
        total.isNull()
        | (total < 0)
        | currency.isNull()
        | ~currency.rlike("^[A-Z]{3}$")
        | items.isNull()
        | (F.size(items) == 0)
        | ~total.cast("decimal(38,0)").eqNullSafe(item_total)
        | bad_item
    )
    return created.where(F.coalesce(invalid, F.lit(True)))


def invalid_attempt_contexts(events, completeness):
    created = events.where(F.col("payment_created").isNotNull()).select(
        "integration_id", "payment_id", F.col("payment_created").alias("_created_context")
    )
    attempts = events.where(F.col("attempt_started").isNotNull()).join(
        created, ["integration_id", "payment_id"], "left"
    )
    context = F.col("attempt_started.context")
    mismatch = context.isNull() | (
        F.col("_created_context").isNotNull() & ~context.eqNullSafe(F.col("_created_context"))
    )
    if completeness == "complete":
        mismatch = mismatch | F.col("_created_context").isNull()
    return attempts.where(F.coalesce(mismatch, F.lit(True)))


def lifecycle_state(events):
    ordered = Window.partitionBy("integration_id", "payment_id").orderBy("aggregate_version", "event_id")
    history = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    whole = Window.partitionBy("integration_id", "payment_id")
    kind = F.coalesce(*[F.when(F.col(payload).isNotNull(), F.lit(payload)) for payload in PAYLOADS])
    active_attempt = F.last(F.col("attempt_started.attempt_id"), ignorenulls=True).over(history)
    attempt = F.coalesce(
        *[
            F.col(f"{payload}.attempt_id")
            for payload in PAYLOADS
            if payload not in ("payment_created", "payment_expired")
        ]
    )
    rows = (
        events.withColumn("_kind", kind)
        .withColumn("_ordinal", F.row_number().over(ordered))
        .withColumn("_total", F.count(F.lit(1)).over(whole))
        .withColumn("_previous_kind", F.lag("_kind").over(ordered))
        .withColumn("_previous_version", F.lag("aggregate_version").over(ordered))
        .withColumn("_previous_time", F.lag("occurred_at").over(ordered))
        .withColumn("_previous_correlation", F.lag("correlation_id").over(ordered))
        .withColumn("_previous_outcome", F.lag("risk_decision_recorded.assessment.outcome").over(ordered))
        .withColumn("_active_attempt", active_attempt)
        .withColumn("_attempt", attempt)
        .withColumn("_previous_attempt", F.lag("_attempt").over(ordered))
    )
    previous = F.col("_previous_kind")
    current = F.col("_kind")
    transition = (
        F.when(current == "payment_created", F.col("_ordinal") == 1)
        .when(current == "attempt_started", previous.isin("payment_created", "attempt_declined"))
        .when(current == "risk_decision_recorded", previous == "attempt_started")
        .when(
            current == "processor_operation_requested",
            (previous == "risk_decision_recorded") & (F.col("_previous_outcome") == "RISK_OUTCOME_APPROVE"),
        )
        .when(current == "processor_outcome_unknown", previous == "processor_operation_requested")
        .when(
            current == "attempt_declined",
            (
                (previous == "risk_decision_recorded")
                & (F.col("_previous_outcome") == "RISK_OUTCOME_DECLINE")
                & (F.col("attempt_declined.source") == "DECLINE_SOURCE_RISK")
            )
            | (
                previous.isin("processor_operation_requested", "processor_outcome_unknown")
                & (F.col("attempt_declined.source") == "DECLINE_SOURCE_PROCESSOR")
            ),
        )
        .when(
            current == "payment_succeeded", previous.isin("processor_operation_requested", "processor_outcome_unknown")
        )
        .when(
            current == "payment_expired",
            previous.isin("payment_created", "attempt_started", "risk_decision_recorded", "attempt_declined"),
        )
        .otherwise(F.lit(False))
    )
    return rows, transition


def invalid_complete_lifecycles(events):
    rows, transition = lifecycle_state(events)
    current = F.col("_kind")
    violation = (
        (F.col("aggregate_version") != F.col("_ordinal"))
        | ((F.col("_ordinal") == F.col("_total")) & ~current.isin("payment_succeeded", "payment_expired"))
        | (
            (F.col("_ordinal") > 1)
            & (
                (F.col("occurred_at") < F.col("_previous_time"))
                | (F.col("correlation_id") != F.col("_previous_correlation"))
            )
        )
        | (
            ~current.isin("payment_created", "payment_expired")
            & (
                F.col("_attempt").isNull()
                | (F.col("_attempt") == "")
                | ~F.col("_attempt").eqNullSafe(F.col("_active_attempt"))
            )
        )
        | ~F.coalesce(transition, F.lit(False))
    )
    return rows.where(F.coalesce(violation, F.lit(True)))


def invalid_recurring_lifecycles(events):
    rows, transition = lifecycle_state(events)
    current = F.col("_kind")
    previous_version = F.col("_previous_version")
    adjacent = previous_version.isNotNull() & (F.col("aggregate_version") == previous_version + 1)
    same_attempt = (
        ~current.isin("payment_created", "payment_expired", "attempt_started")
        & F.col("_previous_attempt").isNotNull()
        & ~F.col("_attempt").eqNullSafe(F.col("_previous_attempt"))
    )
    violation = (
        ((current == "payment_created") & (F.col("aggregate_version") != 1))
        | (previous_version.isNotNull() & (F.col("aggregate_version") <= previous_version))
        | (previous_version.isNotNull() & (F.col("occurred_at") < F.col("_previous_time")))
        | (previous_version.isNotNull() & (F.col("correlation_id") != F.col("_previous_correlation")))
        | F.coalesce(F.col("_previous_kind").isin("payment_succeeded", "payment_expired"), F.lit(False))
        | (adjacent & (~F.coalesce(transition, F.lit(False)) | same_attempt))
    )
    return rows.where(F.coalesce(violation, F.lit(True)))
