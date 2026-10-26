import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

DEFINITION_PATH = (
    Path(__file__).resolve().parents[3] / "contracts/features/fraud-baseline-v1.yaml"
)


def definition_identity():
    content = DEFINITION_PATH.read_bytes()
    return {"version": "fraud-baseline-v1", "sha256": sha256(content).hexdigest()}


def instant(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("feature timestamps must be timezone-aware")
    return value.astimezone(UTC)


def identity(event, attempt):
    return event["integration_id"], event["payment_id"], attempt["attempt_id"]


def compute(current, events, *, cutoff, history_status, observed_availability=None):
    if history_status not in {"available", "missing", "stale"}:
        raise ValueError("unknown history status")
    t, q = instant(current["occurred_at"]), instant(cutoff)
    if q < t:
        raise ValueError("availability cutoff precedes current attempt")
    attempt = current["attempt_started"]
    context, signals = attempt["context"], attempt["signals"]
    snapshot = context["snapshot"]
    if snapshot["total"]["currency"] != "USD":
        raise ValueError("baseline features require USD")
    age = int(
        (t - instant(snapshot["customer_account_created_at"])).total_seconds() // 3600
    )
    if age < 0:
        raise ValueError("account creation follows attempt")
    definitions = json.loads(DEFINITION_PATH.read_text())
    values = {feature["name"]: None for feature in definitions["features"]}
    values.update(
        amount_minor=int(snapshot["total"]["minor_units"]),
        customer_account_age_hours=age,
        ip_shipping_country_mismatch=(
            signals["ip_country"] != snapshot["shipping"]["country"]
        )
        if signals.get("ip_country") is not None
        else None,
    )
    starts, decisions, seen = {}, {}, {}
    for event in events:
        encoded = json.dumps(
            event, sort_keys=True, default=lambda value: instant(value).isoformat()
        )
        event_id = event["event_id"]
        if event_id in seen:
            if seen[event_id] != encoded:
                raise ValueError("conflicting event identity/content")
            continue
        seen[event_id] = encoded
        available = instant(event["recorded_at"])
        if observed_availability is not None:
            if event_id not in observed_availability:
                raise ValueError("missing observed event availability")
            available = max(available, instant(observed_availability[event_id]))
        if available > q:
            continue
        for payload, target in (
            ("attempt_started", starts),
            ("risk_decision_recorded", decisions),
        ):
            if event.get(payload) is not None:
                key = identity(event, event[payload])
                if key in target:
                    raise ValueError("multiple lifecycle facts for one attempt")
                target[key] = event
    if history_status == "available":
        customer_history, method_history = [], []
        for key, event in starts.items():
            if (
                key not in decisions
                or key == identity(current, attempt)
                or key[0] != current["integration_id"]
            ):
                continue
            started = instant(event["occurred_at"])
            if not t - timedelta(days=7) <= started < t:
                continue
            prior = event["attempt_started"]
            if prior["context"]["snapshot"]["total"]["currency"] != "USD":
                raise ValueError("baseline history requires USD")
            if (
                prior["context"]["customer"]["customer_id"]
                == context["customer"]["customer_id"]
            ):
                customer_history.append(event)
            if (
                prior["signals"]["payment_method"]["fingerprint"]
                == signals["payment_method"]["fingerprint"]
            ):
                method_history.append(event)
        for scope, history in (
            ("customer", customer_history),
            ("method", method_history),
        ):
            for hours in (1, 24):
                values[f"{scope}_assessed_count_{hours}h"] = sum(
                    instant(event["occurred_at"]) >= t - timedelta(hours=hours)
                    for event in history
                )
        values["customer_assessed_amount_minor_24h"] = sum(
            int(event["attempt_started"]["context"]["snapshot"]["total"]["minor_units"])
            for event in customer_history
            if instant(event["occurred_at"]) >= t - timedelta(days=1)
        )
        values["customer_distinct_devices_7d"] = len(
            {
                device
                for event in customer_history
                if (
                    device := event["attempt_started"]["signals"].get(
                        "device_fingerprint"
                    )
                )
                is not None
            }
        )
        values["customer_seconds_since_last_assessed_7d"] = (
            int(
                (
                    t - max(instant(event["occurred_at"]) for event in customer_history)
                ).total_seconds()
            )
            if customer_history
            else None
        )
    return {
        "definition": definition_identity(),
        "anchor": t.isoformat(),
        "availability_cutoff": q.isoformat(),
        "availability_basis": "observed"
        if observed_availability is not None
        else "idealized_recorded_at",
        "history_status": history_status,
        "values": values,
    }
