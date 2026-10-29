import os
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.types import ArrayType, DataType, StructType


def descriptor_path() -> Path:
    return Path(os.environ.get("PAYMENTS_DESCRIPTOR", Path(__file__).with_name("payments.pb")))


EVENT_MESSAGE = "payments.v1.PaymentEvent"
LABEL_MESSAGE = "payments.v1.FraudLabel"

MONEY = "struct<minor_units: bigint, currency: string>"
MERCHANT = "struct<merchant_id: string, external_seller_id: string>"
CUSTOMER = "struct<customer_id: string, external_buyer_id: string>"
SHIPPING = "struct<country: string, region: string, postal_code: string, address_fingerprint: string>"
LINE_ITEM = f"struct<product_id: string, category: string, condition: string, quantity: int, unit_price: {MONEY}>"
CHECKOUT = (
    "struct<external_order_id: string, total: "
    f"{MONEY}, items: array<{LINE_ITEM}>, merchant_account_created_at: timestamp, "
    f"customer_account_created_at: timestamp, shipping: {SHIPPING}>"
)
ATTEMPT_CONTEXT = f"struct<merchant: {MERCHANT}, customer: {CUSTOMER}, snapshot: {CHECKOUT}>"
SIGNALS = (
    "struct<attempted_at: timestamp, payment_method: struct<type: string, fingerprint: string>, "
    "device_fingerprint: string, ip_country: string, ip_network_fingerprint: string, "
    "user_agent_family: string>"
)
ASSESSMENT = (
    "struct<outcome: string, reason_codes: array<string>, policy_version: string, "
    "model_version: string, feature_set_version: string, risk_score: double, "
    "assessment_id: string, evidence_sha256: string>"
)

EVENT_COLUMNS = {
    "event_id": "string",
    "payment_id": "string",
    "aggregate_version": "bigint",
    "integration_id": "string",
    "occurred_at": "timestamp",
    "recorded_at": "timestamp",
    "schema_version": "int",
    "correlation_id": "string",
    "payment_created": ATTEMPT_CONTEXT,
    "attempt_started": f"struct<attempt_id: string, signals: {SIGNALS}, context: {ATTEMPT_CONTEXT}>",
    "risk_decision_recorded": f"struct<attempt_id: string, assessment: {ASSESSMENT}>",
    "processor_operation_requested": "struct<attempt_id: string, operation_key: string>",
    "processor_outcome_unknown": "struct<attempt_id: string>",
    "attempt_declined": "struct<attempt_id: string, source: string, reason: string>",
    "payment_succeeded": "struct<attempt_id: string, processor_reference: string>",
    "payment_expired": "struct<_present: boolean>",
}

LABEL_COLUMNS = {
    "integration_id": "string",
    "payment_id": "string",
    "attempt_id": "string",
    "is_fraud": "boolean",
    "occurred_at": "timestamp",
    "label_available_at": "timestamp",
    "source": "string",
    "scenario": "string",
}

PROVENANCE = {
    "_source": "string",
    "_dataset_version": "string",
    "_source_file": "string",
    "_kafka_topic": "string",
    "_kafka_partition": "int",
    "_kafka_offset": "bigint",
    "_kafka_timestamp": "timestamp",
    "_ingested_at": "timestamp",
    "_run_id": "string",
}

REJECTED_COLUMNS = {
    "_kafka_topic": "string",
    "_kafka_partition": "int",
    "_kafka_offset": "bigint",
    "_kafka_timestamp": "timestamp",
    "record_key": "binary",
    "record_value": "binary",
    "error": "string",
    "_ingested_at": "timestamp",
    "_run_id": "string",
}


def ddl(columns):
    return ", ".join(f"{name} {kind}" for name, kind in columns.items())


def event_ddl():
    return ddl({**EVENT_COLUMNS, **PROVENANCE})


def label_ddl():
    return ddl({**LABEL_COLUMNS, **PROVENANCE})


def rejected_ddl():
    return ddl(REJECTED_COLUMNS)


def business_schema(spark: SparkSession, columns) -> StructType:
    return spark.createDataFrame([], ddl(columns)).schema


def conform(expression: Column, source: DataType, target: DataType) -> Column:
    """Cast a column to the target type by field name, so a source may omit or reorder fields."""
    if isinstance(target, StructType):
        names = {field.name for field in source.fields} if isinstance(source, StructType) else set()
        children = [
            conform(expression.getField(field.name), source[field.name].dataType, field.dataType).alias(field.name)
            if field.name in names
            else F.lit(None).cast(field.dataType).alias(field.name)
            for field in target.fields
        ]
        return F.when(expression.isNull(), F.lit(None).cast(target)).otherwise(F.struct(*children))
    if isinstance(target, ArrayType):
        return F.transform(expression, lambda item: conform(item, source.elementType, target.elementType))
    return expression.cast(target)


def conform_frame(frame: DataFrame, target: StructType, keep=()) -> DataFrame:
    names = set(frame.columns)
    return frame.select(
        *[
            conform(F.col(field.name), frame.schema[field.name].dataType, field.dataType).alias(field.name)
            if field.name in names
            else F.lit(None).cast(field.dataType).alias(field.name)
            for field in target.fields
        ],
        *keep,
    )


def decode_events(value: Column) -> Column:
    decoded = _decode(value, EVENT_MESSAGE)
    expired = F.when(
        decoded["payment_expired"].isNotNull(),
        F.struct(decoded["payment_expired"]["present"].alias("_present")),
    )
    return decoded.withField("payment_expired", expired)


def decode_labels(value: Column) -> Column:
    return _decode(value, LABEL_MESSAGE)


def _decode(value: Column, message: str) -> Column:
    return from_protobuf(
        value,
        message,
        str(descriptor_path()),
        {"mode": "PERMISSIVE", "emit.default.values": "true", "upcast.unsigned.ints": "true"},
    )
