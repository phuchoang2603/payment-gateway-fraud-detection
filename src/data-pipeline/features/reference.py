import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

DEFINITION_PATH = (
    Path(__file__).resolve().parents[3] / "contracts/features/fraud-snapshot-v2.yaml"
)
WINDOWS = {
    "5m": timedelta(minutes=5),
    "1h": timedelta(hours=1),
    "24h": timedelta(days=1),
}
PRECISION = timedelta(microseconds=1)


def definition_identity():
    content = DEFINITION_PATH.read_bytes()
    return {"version": "fraud-snapshot-v2", "sha256": sha256(content).hexdigest()}


def instant(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("feature timestamps must be timezone-aware")
    return value.astimezone(UTC)


def attempt_time(event):
    return instant(event["attempt_started"]["signals"]["attempted_at"])


def identity(event):
    return (
        event["integration_id"],
        event["payment_id"],
        event["attempt_started"]["attempt_id"],
    )


def available_starts(events, cutoff, observed_availability=None):
    q = instant(cutoff)
    starts, seen = [], {}
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
        if available <= q and event.get("attempt_started") is not None:
            starts.append(event)
    return starts


def entity_key(event, scope):
    attempt = event["attempt_started"]
    if scope == "customer":
        entity = attempt["context"]["customer"]["customer_id"]
    elif scope == "method":
        entity = attempt["signals"]["payment_method"]["fingerprint"]
    else:
        raise ValueError("unknown entity view")
    return event["integration_id"], entity


def expiration_anchors(starts, *, after):
    after = instant(after)
    return sorted(
        {
            attempt_time(event) + window + PRECISION
            for event in starts
            for window in WINDOWS.values()
            if attempt_time(event) + window + PRECISION > after
        }
    )


def snapshot(current, starts, *, scope, anchor, cutoff, coverage, status):
    if status not in {"available", "missing", "stale", "future", "incompatible"}:
        raise ValueError("unknown snapshot status")
    s, q = instant(anchor), instant(cutoff)
    if s > q:
        raise ValueError("snapshot anchor follows computation cutoff")
    if set(coverage) != set(WINDOWS) or any(
        value not in {"complete", "unknown"} for value in coverage.values()
    ):
        raise ValueError("invalid per-window coverage")
    fields = [f"{scope}_snapshot_attempt_count_{window}" for window in WINDOWS]
    if scope == "customer":
        fields.append("customer_snapshot_distinct_devices_24h")
    values = dict.fromkeys(fields)
    if s > attempt_time(current):
        status = "future"
    history = [
        (attempt_time(event), event["attempt_started"])
        for event in starts
        if identity(event) != identity(current)
        and entity_key(event, scope) == entity_key(current, scope)
        and s - WINDOWS["24h"] <= attempt_time(event) < s
    ]
    if status == "available":
        for window, length in WINDOWS.items():
            if coverage[window] == "complete":
                values[f"{scope}_snapshot_attempt_count_{window}"] = sum(
                    started >= s - length for started, _ in history
                )
        if scope == "customer" and coverage["24h"] == "complete":
            values["customer_snapshot_distinct_devices_24h"] = len(
                {
                    device
                    for _, attempt in history
                    if (device := attempt["signals"].get("device_fingerprint"))
                    is not None
                }
            )
    return {
        "entity_key": list(entity_key(current, scope)),
        "anchor": s.isoformat(),
        "status": status,
        "coverage": dict(coverage),
        "values": values,
    }


def compute(current, events, *, cutoff, views, observed_availability=None):
    t, q = attempt_time(current), instant(cutoff)
    if q < t:
        raise ValueError("availability cutoff precedes current attempt")
    attempt = current["attempt_started"]
    checkout, signals = attempt["context"]["snapshot"], attempt["signals"]
    if checkout["total"]["currency"] != "USD":
        raise ValueError("snapshot features require USD")
    age = int(
        (t - instant(checkout["customer_account_created_at"])).total_seconds() // 3600
    )
    if age < 0:
        raise ValueError("account creation follows attempt")
    if set(views) != {"customer", "method"}:
        raise ValueError("both independent snapshot views are required")
    values = {
        "amount_minor": int(checkout["total"]["minor_units"]),
        "customer_account_age_hours": age,
        "ip_shipping_country_mismatch": (
            signals["ip_country"] != checkout["shipping"]["country"]
        )
        if signals.get("ip_country") is not None
        else None,
    }
    starts = available_starts(events, q, observed_availability)
    snapshots = {
        scope: snapshot(current, starts, scope=scope, cutoff=q, **view)
        for scope, view in views.items()
    }
    for view in snapshots.values():
        values.update(view["values"])
    return {
        "definition": definition_identity(),
        "attempted_at": t.isoformat(),
        "availability_cutoff": q.isoformat(),
        "availability_basis": "observed"
        if observed_availability is not None
        else "idealized_recorded_at",
        "views": snapshots,
        "values": values,
    }
