from feast import Field, RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Bool, Float64, Int64, String, UnixTimestamp

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
    "customer_amount_to_30d_avg": Float64,
    "payment_method_amount_to_30d_avg": Float64,
    "scored_hour_utc": Int64,
}
AVERAGE_FIELDS = ("amount_sum_minor_30d", "attempt_count_30d")
request_context = RequestSource(
    name="request_context",
    schema=[Field(name=name, dtype=dtype) for name, dtype in REQUEST_FIELDS.items()],
)


def request_features_view(customer_daily, payment_method_daily):
    @on_demand_feature_view(
        mode="python",
        sources=[
            request_context,
            customer_daily[list(AVERAGE_FIELDS)],
            payment_method_daily[list(AVERAGE_FIELDS)],
        ],
        schema=[Field(name=name, dtype=dtype) for name, dtype in REQUEST_FEATURES.items()],
    )
    def request_features(inputs):
        from datetime import UTC, datetime

        def instant(value):
            return (
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                if isinstance(value, str)
                else value
            )

        def relative(view):
            # Offline nulls arrive as NaN; a zero sum or count has no average to compare with.
            return [
                None
                if not count or not total or count != count or total != total
                else amount * count / total
                for amount, total, count in zip(
                    inputs["amount_minor"],
                    inputs[f"{view}__amount_sum_minor_30d"],
                    inputs[f"{view}__attempt_count_30d"],
                )
            ]

        scored = [instant(value) for value in inputs["scored_at"]]
        return {
            "amount_minor": inputs["amount_minor"],
            "customer_account_age_days": [
                None if created is None else (at - instant(created)).days
                for at, created in zip(scored, inputs["customer_account_created_at"])
            ],
            "merchant_account_age_days": [
                None if created is None else (at - instant(created)).days
                for at, created in zip(scored, inputs["merchant_account_created_at"])
            ],
            "ip_shipping_country_mismatch": [
                None if ip is None or shipping is None else ip != shipping
                for ip, shipping in zip(inputs["ip_country"], inputs["shipping_country"])
            ],
            "item_count": inputs["item_count"],
            "has_device": inputs["has_device"],
            "customer_amount_to_30d_avg": relative("customer_daily"),
            "payment_method_amount_to_30d_avg": relative("payment_method_daily"),
            "scored_hour_utc": [at.astimezone(UTC).hour if at.tzinfo else at.hour for at in scored],
        }

    return request_features
