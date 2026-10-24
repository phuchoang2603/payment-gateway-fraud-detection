from typing import Any

import pyarrow as pa

Record = dict[str, Any]
TIMESTAMP = pa.timestamp("us", tz="UTC")
MONEY = pa.struct([("minor_units", pa.int64()), ("currency", pa.string())])
MERCHANT = pa.struct([("merchant_id", pa.string()), ("external_seller_id", pa.string())])
CUSTOMER = pa.struct([("customer_id", pa.string()), ("external_buyer_id", pa.string())])
SHIPPING = pa.struct(
    [
        ("country", pa.string()),
        ("region", pa.string()),
        ("postal_code", pa.string()),
        ("address_fingerprint", pa.string()),
    ]
)
LINE_ITEM = pa.struct(
    [
        ("product_id", pa.string()),
        ("category", pa.string()),
        ("condition", pa.string()),
        ("quantity", pa.int32()),
        ("unit_price", MONEY),
    ]
)
CHECKOUT = pa.struct(
    [
        ("external_order_id", pa.string()),
        ("total", MONEY),
        ("items", pa.list_(LINE_ITEM)),
        ("merchant_account_created_at", TIMESTAMP),
        ("customer_account_created_at", TIMESTAMP),
        ("shipping", SHIPPING),
    ]
)
PAYMENT_METHOD = pa.struct([("type", pa.string()), ("fingerprint", pa.string())])
ASSESSMENT = pa.struct(
    [
        ("outcome", pa.string()),
        ("reason_codes", pa.list_(pa.string())),
        ("policy_version", pa.string()),
        ("model_version", pa.string()),
        ("feature_set_version", pa.string()),
        ("risk_score", pa.float64()),
    ]
)


def event_schema(version: int) -> pa.Schema:
    if version not in (1, 2):
        raise ValueError(f"unsupported payment event schema version: {version}")
    signals = [
        ("attempted_at", TIMESTAMP),
        ("payment_method", PAYMENT_METHOD),
        ("device_fingerprint", pa.string()),
        ("ip_country", pa.string()),
        ("ip_network_fingerprint", pa.string()),
    ]
    if version == 2:
        signals.append(("user_agent_family", pa.string()))
    return pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("payment_id", pa.string(), nullable=False),
            pa.field("aggregate_version", pa.int64(), nullable=False),
            pa.field("integration_id", pa.string(), nullable=False),
            pa.field("occurred_at", TIMESTAMP, nullable=False),
            pa.field("recorded_at", TIMESTAMP, nullable=False),
            pa.field("schema_version", pa.uint32(), nullable=False),
            pa.field("correlation_id", pa.string(), nullable=False),
            ("payment_created", pa.struct([("merchant", MERCHANT), ("customer", CUSTOMER), ("snapshot", CHECKOUT)])),
            ("attempt_started", pa.struct([("attempt_id", pa.string()), ("signals", pa.struct(signals))])),
            ("risk_decision_recorded", pa.struct([("attempt_id", pa.string()), ("assessment", ASSESSMENT)])),
            (
                "processor_operation_requested",
                pa.struct([("attempt_id", pa.string()), ("operation_key", pa.string())]),
            ),
            ("processor_outcome_unknown", pa.struct([("attempt_id", pa.string())])),
            (
                "attempt_declined",
                pa.struct([("attempt_id", pa.string()), ("source", pa.string()), ("reason", pa.string())]),
            ),
            ("payment_succeeded", pa.struct([("attempt_id", pa.string()), ("processor_reference", pa.string())])),
            ("payment_expired", pa.struct([("_present", pa.bool_())])),
        ]
    )


LABEL_SCHEMA = pa.schema(
    [
        pa.field("integration_id", pa.string(), nullable=False),
        pa.field("payment_id", pa.string(), nullable=False),
        pa.field("attempt_id", pa.string(), nullable=False),
        pa.field("is_fraud", pa.bool_(), nullable=False),
        pa.field("occurred_at", TIMESTAMP, nullable=False),
        pa.field("label_available_at", TIMESTAMP, nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("scenario", pa.string()),
    ]
)
