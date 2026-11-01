from feast import Field, RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Bool, Int64, String, UnixTimestamp

REQUEST_FIELDS = {
    "scored_at": UnixTimestamp,
    "amount_minor": Int64,
    "customer_account_created_at": UnixTimestamp,
    "merchant_account_created_at": UnixTimestamp,
    "shipping_country": String,
    "ip_country": String,
    "item_count": Int64,
    "has_device": Bool,
}
REQUEST_FEATURES = {
    "amount_minor": Int64,
    "customer_account_age_days": Int64,
    "merchant_account_age_days": Int64,
    "ip_shipping_country_mismatch": Bool,
    "item_count": Int64,
    "has_device": Bool,
}
request_context = RequestSource(
    name="request_context",
    schema=[Field(name=name, dtype=dtype) for name, dtype in REQUEST_FIELDS.items()],
)


@on_demand_feature_view(
    mode="python",
    sources=[request_context],
    schema=[Field(name=name, dtype=dtype) for name, dtype in REQUEST_FEATURES.items()],
)
def request_features(inputs):
    from datetime import datetime

    def instant(value):
        return (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            if isinstance(value, str)
            else value
        )

    scored = [instant(value) for value in inputs["scored_at"]]
    return {
        "amount_minor": inputs["amount_minor"],
        "customer_account_age_days": [
            (at - instant(created)).days
            for at, created in zip(scored, inputs["customer_account_created_at"])
        ],
        "merchant_account_age_days": [
            (at - instant(created)).days
            for at, created in zip(scored, inputs["merchant_account_created_at"])
        ],
        "ip_shipping_country_mismatch": [
            None if ip is None or shipping is None else ip != shipping
            for ip, shipping in zip(inputs["ip_country"], inputs["shipping_country"])
        ],
        "item_count": inputs["item_count"],
        "has_device": inputs["has_device"],
    }
