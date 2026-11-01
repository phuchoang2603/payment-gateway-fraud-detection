import os
import re
from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, PushSource, ValueType
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.types import Float64, Int64, String

from fraud_feast.request import request_context, request_features_view

DAILY_TTL = timedelta(days=31, hours=12)
STREAM_TTL = timedelta(hours=24, minutes=5)
KEYS = {"customer": "customer_id", "payment_method": "payment_method_fingerprint"}
STREAM_FIELDS = {
    "customer": [
        "customer_snapshot_attempt_count_5m",
        "customer_snapshot_attempt_count_1h",
        "customer_snapshot_attempt_count_24h",
        "customer_snapshot_distinct_devices_24h",
    ],
    "payment_method": [
        "method_snapshot_attempt_count_5m",
        "method_snapshot_attempt_count_1h",
        "method_snapshot_attempt_count_24h",
    ],
}
SHARED = (
    "attempt_count",
    "amount_sum_minor",
    "decided_count",
    "declined_count",
    "risk_declined_count",
    "decline_rate",
    "distinct_merchant_count",
)
DAILY_FIELDS = {
    scope: [f"{measure}_{window}" for window in ("7d", "30d") for measure in (*SHARED, *extra)]
    for scope, extra in {
        "customer": ("distinct_device_count", "distinct_payment_method_count"),
        "payment_method": ("distinct_customer_count",),
    }.items()
}
TABLES = {scope: f"gold.feat_stream_{scope}" for scope in KEYS}
DAILY_TABLES = {scope: f"gold.feat_{scope}_daily" for scope in KEYS}


def epochs():
    values = os.environ["FEAST_STREAM_EPOCHS"].split(",")
    if not values or any(not re.fullmatch(r"[a-zA-Z0-9_-]+", value) for value in values):
        raise ValueError("invalid stream epochs")
    return values


def definitions():
    integration = Entity(
        name="integration", join_keys=["integration_id"], value_type=ValueType.STRING
    )
    entities = {
        scope: Entity(name=scope, join_keys=[key], value_type=ValueType.STRING)
        for scope, key in KEYS.items()
    }
    views = []
    model_views = []
    selected = ",".join(f"'{value}'" for value in epochs())
    for scope in KEYS:
        for kind, fields, ttl, definition in (
            ("daily", DAILY_FIELDS[scope], DAILY_TTL, "fraud-history-batch-v1"),
            ("stream", STREAM_FIELDS[scope], STREAM_TTL, "fraud-snapshot-v2"),
        ):
            options = (
                {"table": f"lake.{DAILY_TABLES[scope]}"}
                if kind == "daily"
                else {
                    "query": f"SELECT integration_id, {KEYS[scope]}, event_timestamp, created, epoch, {', '.join(fields)} FROM lake.{TABLES[scope]} WHERE epoch IN ({selected})"
                }
            )
            source = SparkSource(
                name=f"{scope}_{kind}_batch",
                timestamp_field="event_timestamp",
                created_timestamp_column="created",
                **options,
            )
            if kind == "stream":
                source = PushSource(name=f"{scope}_stream_push", batch_source=source)
            views.append(
                FeatureView(
                    name=f"{scope}_{kind}",
                    entities=[integration, entities[scope]],
                    ttl=ttl,
                    schema=[
                        Field(
                            name=name, dtype=Float64 if name.startswith("decline_rate_") else Int64
                        )
                        for name in fields
                    ]
                    + ([Field(name="epoch", dtype=String)] if kind == "stream" else []),
                    source=source,
                    online=True,
                    tags={"definition": definition},
                )
            )
            model_views.append(views[-1][fields])
    by_name = {view.name: view for view in views}
    request_features = request_features_view(
        by_name["customer_daily"], by_name["payment_method_daily"]
    )
    return [
        integration,
        *entities.values(),
        *views,
        request_context,
        request_features,
        FeatureService(
            name="payment_risk_v1",
            features=[*model_views, request_features],
        ),
    ]
