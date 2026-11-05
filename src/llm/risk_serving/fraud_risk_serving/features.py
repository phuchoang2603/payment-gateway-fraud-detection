import json
import math
from datetime import UTC, datetime, timedelta

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from fraud_risk_serving.upstream import read


class RiskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    integration_id: str = Field(min_length=1, max_length=200)
    customer_id: str = Field(min_length=1, max_length=200)
    payment_method_fingerprint: str = Field(min_length=1, max_length=200)
    amount_minor: int = Field(ge=0, le=2**63 - 1, strict=True)
    customer_account_created_at: AwareDatetime
    merchant_account_created_at: AwareDatetime
    shipping_country: str = Field(max_length=2)
    ip_country: str = Field(max_length=2)
    item_count: int = Field(ge=0, le=2**63 - 1, strict=True)
    has_device: bool = Field(strict=True)


def daily_fields(scope):
    measures = [
        "attempt_count",
        "amount_sum_minor",
        "decided_count",
        "declined_count",
        "risk_declined_count",
        "decline_rate",
        "distinct_merchant_count",
    ]
    measures += (
        ["distinct_device_count", "distinct_payment_method_count"]
        if scope == "customer"
        else ["distinct_customer_count"]
    )
    return [f"{measure}_{window}" for window in ("7d", "30d") for measure in measures]


VIEW_FIELDS = {
    "customer_daily": daily_fields("customer"),
    "payment_method_daily": daily_fields("payment_method"),
    "customer_stream": [f"customer_snapshot_attempt_count_{window}" for window in ("5m", "1h", "24h")]
    + ["customer_snapshot_distinct_devices_24h"],
    "payment_method_stream": [f"method_snapshot_attempt_count_{window}" for window in ("5m", "1h", "24h")],
    "request_features": [
        "amount_minor",
        "customer_account_age_days",
        "merchant_account_age_days",
        "ip_shipping_country_mismatch",
        "item_count",
        "has_device",
        "customer_amount_to_30d_avg",
        "payment_method_amount_to_30d_avg",
        "scored_hour_utc",
    ],
}


def nullable(name):
    return name == "ip_shipping_country_mismatch" or name.endswith("_amount_to_30d_avg")


def evaluate(view, names, fields, scored_at):
    invalid = {"status": "error", "values": {}}
    values, timestamps = {}, []
    try:
        for name in names:
            field = fields[f"{view}__{name}"]
            if any(len(field[key]) != 1 for key in ("values", "statuses", "event_timestamps")):
                return invalid
            value, status = field["values"][0], field["statuses"][0]
            timestamp = datetime.fromisoformat(field["event_timestamps"][0])
            if timestamp.tzinfo is None:
                return invalid
            timestamps.append(timestamp)
            if status not in ("PRESENT", "NULL_VALUE", "NOT_FOUND") or (status != "PRESENT" and value is not None):
                return invalid
            if value is not None:
                if name in ("has_device", "ip_shipping_country_mismatch"):
                    valid = type(value) is bool
                elif name.startswith("decline_rate_") or name.endswith("_amount_to_30d_avg"):
                    valid = type(value) in (int, float) and math.isfinite(value) and value >= 0
                    if name.startswith("decline_rate_"):
                        valid = valid and value <= 1
                else:
                    valid = type(value) is int and 0 <= value <= 2**63 - 1
                if not valid:
                    return invalid
            values[name] = value
        if len(set(timestamps)) != 1:
            return invalid
        timestamp = timestamps[0]
        if view == "request_features":
            if any(value is None and not nullable(name) for name, value in values.items()):
                return invalid
            timestamp, status = scored_at, "available"
        elif all(value is None for value in values.values()) and timestamp == datetime(1970, 1, 1, tzinfo=UTC):
            status = "missing"
        elif timestamp > scored_at:
            status = "future"
        elif scored_at - timestamp > (
            timedelta(hours=24, minutes=5) if view.endswith("_stream") else timedelta(days=31, hours=12)
        ):
            status = "expired"
        else:
            status = "available"
        return {
            "status": status,
            "event_timestamp": timestamp.isoformat(),
            "values": values if status == "available" else {},
        }
    except (KeyError, TypeError, ValueError, OverflowError):
        return invalid


async def features(client, url, request: RiskRequest, scored_at: datetime):
    entities = request.model_dump(mode="json") | {"scored_at": scored_at.isoformat()}
    content, _ = await read(
        client,
        "feature store",
        "POST",
        url.rstrip("/") + "/get-online-features",
        5,
        json={
            "feature_service": "payment_risk_v1",
            "full_feature_names": True,
            "entities": {key: [value] for key, value in entities.items()},
        },
    )
    try:
        response = json.loads(content)
        names, results = response["metadata"]["feature_names"], response["results"]
        if len(names) != len(results) or len(names) != len(set(names)):
            raise ValueError("invalid field list")
        fields = dict(zip(names, results, strict=True))
        views = {view: evaluate(view, names, fields, scored_at) for view, names in VIEW_FIELDS.items()}
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("feature store returned a malformed response") from error
    for scope in ("customer", "payment_method"):
        if views[scope + "_daily"]["status"] != "available" and views["request_features"]["status"] == "available":
            views["request_features"]["values"][scope + "_amount_to_30d_avg"] = None
    return {"feature_service": "payment_risk_v1", "scored_at": scored_at.isoformat(), "views": views}
