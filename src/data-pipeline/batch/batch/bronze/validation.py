from pathlib import Path

import yaml
from pyspark.sql import Column
from pyspark.sql import functions as F

LABEL_SOURCES = ("LABEL_SOURCE_SIMULATION_TRUTH", "LABEL_SOURCE_CHARGEBACK")
EVENT_PAYLOADS = (
    "payment_created",
    "attempt_started",
    "risk_decision_recorded",
    "processor_operation_requested",
    "processor_outcome_unknown",
    "attempt_declined",
    "payment_succeeded",
    "payment_expired",
)


def payload_case_of(event: Column) -> Column:
    """The one payload an event carries, named by its protobuf oneof member, or null when it carries none."""
    return F.coalesce(*[F.when(event.getField(name).isNotNull(), F.lit(name)) for name in EVENT_PAYLOADS])


VALUE_RULES = {
    "schema_version": lambda value: value.isin(2, 3),
    "aggregate_version": lambda value: value >= 1,
    "source": lambda value: value.isin(*LABEL_SOURCES),
}


def required_fields(contract: Path, physical_name: str, business_columns) -> dict[str, str]:
    spec = yaml.safe_load(contract.read_text())
    table = next(item for item in spec["schema"] if item["physicalName"] == physical_name)
    return {
        prop["name"]: prop["logicalType"]
        for prop in table["properties"]
        if prop.get("required") and prop["name"] in business_columns
    }


def rejection_reason(decoded: Column, value: Column, required: dict[str, str], payloads=()) -> Column:
    """The reason a Kafka record is rejected, or null when it is accepted.

    Protobuf decoding runs with default values emitted, so a valid message always has its string fields set
    (possibly empty) and an undecodable one has them null. Any string field therefore probes whether the bytes
    decoded at all; empty strings are missing values.
    """
    probe = next(name for name, logical in required.items() if logical == "string")
    decoded_ok = decoded.isNotNull() & decoded.getField(probe).isNotNull()
    checks = [
        F.when(value.isNull(), F.lit("empty record value")),
        F.when(value.isNotNull() & ~decoded_ok, F.lit("protobuf decoding failed")),
    ]
    for name, logical in required.items():
        field = decoded.getField(name)
        missing = F.nullif(field, F.lit("")).isNull() if logical == "string" else field.isNull()
        checks.append(F.when(decoded_ok & missing, F.lit(f"missing {name}")))
        if name in VALUE_RULES:
            checks.append(F.when(decoded_ok & ~VALUE_RULES[name](field), F.lit(f"invalid {name}")))
    if payloads:
        present = sum(F.when(decoded.getField(name).isNotNull(), 1).otherwise(0) for name in payloads)
        checks.append(
            F.when(
                decoded_ok & (present != 1),
                F.concat(F.lit("expected exactly one payload, found "), present.cast("string")),
            )
        )
    return F.nullif(F.concat_ws("; ", *checks), F.lit(""))
