from dataclasses import dataclass
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf.descriptor import FieldDescriptor

from simulator.arrow import LABEL_SCHEMA, PHYSICAL_SCHEMAS, TIMESTAMP, event_schema
from simulator.contracts.payments.v1.events_pb2 import PaymentEvent
from simulator.contracts.payments.v1.labels_pb2 import (
    LABEL_SOURCE_CHARGEBACK,
    LABEL_SOURCE_SIMULATION_TRUTH,
    FraudLabel,
)

TRANSPORT = pa.struct(
    [
        pa.field("topic", pa.string(), nullable=False),
        pa.field("partition", pa.int32(), nullable=False),
        pa.field("offset", pa.int64(), nullable=False),
        ("timestamp", TIMESTAMP),
        ("key", pa.binary()),
        pa.field("value", pa.binary(), nullable=False),
        (
            "headers",
            pa.list_(pa.struct([pa.field("key", pa.string(), nullable=False), ("value", pa.binary())])),
        ),
    ]
)


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes | None]]

    @classmethod
    def from_message(cls, message):
        kind, timestamp = message.timestamp()
        return cls(
            message.topic(),
            message.partition(),
            message.offset(),
            timestamp if kind and timestamp >= 0 else None,
            message.key(),
            message.value(),
            message.headers() or [],
        )

    @property
    def size(self):
        return (
            len(self.key or b"")
            + len(self.value or b"")
            + sum(len(key.encode()) + len(value or b"") for key, value in self.headers)
        )


def project(message, fields):
    result = {}
    for field in fields:
        name = "present" if field.name == "_present" else field.name
        descriptor = message.DESCRIPTOR.fields_by_name[name]
        if descriptor.has_presence and not message.HasField(name):
            result[field.name] = None
            continue
        value = getattr(message, name)
        if pa.types.is_struct(field.type):
            value = project(value, field.type)
        elif pa.types.is_list(field.type):
            value = (
                [project(item, field.type.value_type) for item in value]
                if pa.types.is_struct(field.type.value_type)
                else list(value)
            )
        elif pa.types.is_timestamp(field.type):
            if not 0 <= value.nanos < 1_000_000_000:
                raise ValueError("invalid protobuf timestamp")
            value = value.ToDatetime(tzinfo=UTC)
        elif descriptor.type == FieldDescriptor.TYPE_ENUM:
            value = descriptor.enum_type.values_by_number[value].name
        elif field.name in ("assessment_id", "evidence_sha256") and value == "":
            value = None
        result[field.name] = value
    return result


def decode(record, label_topic):
    if record.value is None:
        raise ValueError("tombstone is not a payment or label")
    if record.topic == label_topic:
        message = FraudLabel.FromString(record.value)
        if (
            not message.integration_id
            or not message.payment_id
            or not message.attempt_id
            or not message.HasField("occurred_at")
            or not message.HasField("label_available_at")
            or message.source not in (LABEL_SOURCE_CHARGEBACK, LABEL_SOURCE_SIMULATION_TRUTH)
        ):
            raise ValueError("incomplete fraud label")
        table, version, schema = "fraud_labels", 1, LABEL_SCHEMA
    else:
        message = PaymentEvent.FromString(record.value)
        if (
            not message.event_id
            or not message.payment_id
            or not message.integration_id
            or not 0 < message.aggregate_version < 2**63
            or not message.HasField("occurred_at")
            or not message.HasField("recorded_at")
            or message.WhichOneof("payload") is None
        ):
            raise ValueError("incomplete payment event")
        if message.schema_version not in PHYSICAL_SCHEMAS["event_envelope_versions"]:
            raise ValueError(f"unsupported payment event envelope version: {message.schema_version}")
        version = PHYSICAL_SCHEMAS["payment_events_output"]
        schema = event_schema(version)
        if message.WhichOneof("payload") == "attempt_started":
            attempt = message.attempt_started
            context = attempt.context
            if not (
                attempt.attempt_id
                and attempt.HasField("signals")
                and context.HasField("merchant")
                and context.HasField("customer")
                and context.HasField("snapshot")
                and context.merchant.merchant_id
                and context.customer.customer_id
                and context.snapshot.HasField("total")
            ):
                raise ValueError("incomplete attempt scoring context")
        table = "payment_events"
    row = project(message, schema)
    row["_transport"] = {
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "timestamp": datetime.fromtimestamp(record.timestamp_ms / 1000, UTC)
        if record.timestamp_ms is not None
        else None,
        "key": record.key,
        "value": record.value,
        "headers": [{"key": key, "value": value} for key, value in record.headers],
    }
    return table, version, row


def encode(table, version, rows):
    schema = LABEL_SCHEMA if table == "fraud_labels" else event_schema(version)
    schema = schema.append(pa.field("_transport", TRANSPORT, nullable=False))
    output = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), output, compression="zstd")
    return output.getvalue().to_pybytes()
