import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

DEFINITION_PATH = (
    Path(__file__).resolve().parents[3] / "contracts/features/fraud-snapshot-v1.yaml"
)
WINDOWS = {"1h": timedelta(hours=1), "24h": timedelta(days=1), "7d": timedelta(days=7)}
PRECISION = timedelta(microseconds=1)


def definition_identity():
    content = DEFINITION_PATH.read_bytes()
    return {"version": "fraud-snapshot-v1", "sha256": sha256(content).hexdigest()}


def instant(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("feature timestamps must be timezone-aware")
    return value.astimezone(UTC)


def identity(event, attempt):
    return event["integration_id"], event["payment_id"], attempt["attempt_id"]


def eligible_starts(events, cutoff, observed_availability=None):
    q = instant(cutoff)
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
    return [event for key, event in starts.items() if key in decisions]


def entity_key(event, scope):
    attempt = event["attempt_started"]
    if scope == "customer":
        entity = attempt["context"]["customer"]["customer_id"]
    elif scope == "method":
        entity = attempt["signals"]["payment_method"]["fingerprint"]
    else:
        raise ValueError("unknown entity view")
    return event["integration_id"], entity


def elapsed_seconds(last_assessed_at, attempted_at):
    if last_assessed_at is None:
        return None
    last, t = instant(last_assessed_at), instant(attempted_at)
    return int((t - last).total_seconds()) if t - WINDOWS["7d"] <= last < t else None


def expiration_anchors(starts, *, after, scope):
    if scope not in {"customer", "method"}:
        raise ValueError("unknown entity view")
    windows = (
        WINDOWS.values() if scope == "customer" else (WINDOWS["1h"], WINDOWS["24h"])
    )
    after = instant(after)
    return sorted(
        {
            instant(event["occurred_at"]) + window + PRECISION
            for event in starts
            for window in windows
            if instant(event["occurred_at"]) + window + PRECISION > after
        }
    )


def snapshot(current, starts, *, scope, anchor, cutoff, coverage, status):
    if status not in {"available", "missing", "stale", "future", "incompatible"}:
        raise ValueError("unknown snapshot status")
    s, q = instant(anchor), instant(cutoff)
    if s > q:
        raise ValueError("snapshot anchor follows computation cutoff")
    windows = ("1h", "24h", "7d") if scope == "customer" else ("1h", "24h")
    if set(coverage) != set(windows) or any(
        value not in {"complete", "unknown"} for value in coverage.values()
    ):
        raise ValueError("invalid per-window coverage")
    fields = [
        f"{scope}_snapshot_assessed_count_1h",
        f"{scope}_snapshot_assessed_count_24h",
    ]
    if scope == "customer":
        fields.extend(
            [
                "customer_snapshot_assessed_amount_minor_24h",
                "customer_snapshot_distinct_devices_7d",
            ]
        )
    values = dict.fromkeys(fields)
    last = None
    if s > instant(current["occurred_at"]):
        status = "future"
    history = []
    for event in starts:
        started, prior = instant(event["occurred_at"]), event["attempt_started"]
        if (
            identity(event, prior) != identity(current, current["attempt_started"])
            and entity_key(event, scope) == entity_key(current, scope)
            and s - WINDOWS[windows[-1]] <= started < s
        ):
            if prior["context"]["snapshot"]["total"]["currency"] != "USD":
                raise ValueError("snapshot history requires USD")
            history.append((started, prior))
    if status == "available":
        for window in ("1h", "24h"):
            if coverage[window] == "complete":
                values[f"{scope}_snapshot_assessed_count_{window}"] = sum(
                    started >= s - WINDOWS[window] for started, _ in history
                )
        if scope == "customer":
            if coverage["24h"] == "complete":
                values["customer_snapshot_assessed_amount_minor_24h"] = sum(
                    int(prior["context"]["snapshot"]["total"]["minor_units"])
                    for started, prior in history
                    if started >= s - WINDOWS["24h"]
                )
            if coverage["7d"] == "complete":
                values["customer_snapshot_distinct_devices_7d"] = len(
                    {
                        device
                        for _, prior in history
                        if (device := prior["signals"].get("device_fingerprint"))
                        is not None
                    }
                )
                last = max((started for started, _ in history), default=None)
    return {
        "entity_key": list(entity_key(current, scope)),
        "anchor": s.isoformat(),
        "status": status,
        "coverage": dict(coverage),
        "values": values,
        **(
            {"last_assessed_at": last.isoformat() if last is not None else None}
            if scope == "customer"
            else {}
        ),
    }


def compute(current, events, *, cutoff, views, observed_availability=None):
    t, q = instant(current["occurred_at"]), instant(cutoff)
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
    starts = eligible_starts(events, q, observed_availability)
    snapshots = {
        scope: snapshot(current, starts, scope=scope, cutoff=q, **view)
        for scope, view in views.items()
    }
    for view in snapshots.values():
        values.update(view["values"])
    values["customer_seconds_since_snapshot_last_assessed_7d"] = elapsed_seconds(
        snapshots["customer"]["last_assessed_at"], t
    )
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
