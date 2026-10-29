import hashlib
import pickle
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from random import Random

import pytest
from features.reference_v2 import definition_identity
from features.reference_v2 import snapshot as reference_snapshot

from fraud_streaming.model import (
    DAY,
    HORIZON,
    HOUR,
    MINUTE,
    Entity,
    Fact,
    Packet,
    Rejected,
    dedup_status,
    lateness_bucket,
)
from fraud_streaming.wire import PaymentEvent, SnapshotUpdate, decode, snapshot

ANCHOR = 1_760_000_000_000_000


def start(identity="attempt", *, when=ANCHOR, device="device", customer="customer"):
    return Fact(
        identity,
        identity,
        "integration",
        "payment",
        identity,
        when,
        when,
        customer,
        "method",
        device,
    )


def packet(value, *, bootstrap=ANCHOR - 2 * DAY, now=ANCHOR):
    return Packet(value, now, bootstrap, ())


def test_horizon_conflicts_and_entity_binding():
    assert not packet(start(when=ANCHOR - HORIZON)).too_late
    assert packet(start(when=ANCHOR - HORIZON - 1)).too_late
    assert [lateness_bucket(value) for value in (0, MINUTE, HORIZON, HORIZON + 1)] == [
        "lt_1m",
        "1m_5m",
        "15m_24h",
        "too_late",
    ]
    assert [dedup_status(prior, "a") for prior in (None, "a", "b")] == [
        "new",
        "duplicate",
        "conflict",
    ]
    state = Entity("customer", "integration", "customer", ANCHOR - 2 * DAY)
    state.add(packet(start()))
    state.add(packet(start()))
    assert len(state.entries) == 1
    with pytest.raises(ValueError, match="conflicting attempt start"):
        state.add(packet(replace(start(), device="other")))
    with pytest.raises(ValueError, match="binding"):
        state.add(packet(start("other", customer="other")))
    with pytest.raises(ValueError, match="bootstrap"):
        state.add(packet(start("next"), bootstrap=ANCHOR))


def test_exact_window_expiration_repeated_devices_and_restored_revision():
    state = Entity("customer", "integration", "customer", ANCHOR - 2 * DAY)
    for value in (
        start("five", when=ANCHOR - 5 * MINUTE),
        start("hour", when=ANCHOR - HOUR),
        start("day", when=ANCHOR - DAY, device=None),
        start("again", when=ANCHOR - DAY + 1),
    ):
        state.add(packet(value))
    _, values = state.advance(ANCHOR)
    assert values == {
        "attempt_count_5m": 1,
        "attempt_count_1h": 2,
        "attempt_count_24h": 4,
        "distinct_devices_24h": 1,
    }
    state = pickle.loads(pickle.dumps(state))
    _, values = state.advance(ANCHOR + 1)
    assert values["attempt_count_5m"] == 0
    assert values["attempt_count_1h"] == 1
    assert values["attempt_count_24h"] == 3
    assert values["distinct_devices_24h"] == 1
    assert state.revision == 2
    _, values = state.advance(ANCHOR + 2)
    assert values["attempt_count_24h"] == 2 and values["distinct_devices_24h"] == 1
    _, values = state.advance(ANCHOR + DAY - 5 * MINUTE + 1)
    assert values["attempt_count_24h"] == 0 and values["distinct_devices_24h"] == 0
    assert state.entries == {}
    assert state.advance(ANCHOR + DAY) is None


def test_bootstrap_coverage_unknown_then_empty_zero_and_bounded_state():
    state = Entity("method", "integration", "method", ANCHOR, max_entries=1)
    state.add(packet(start(), bootstrap=ANCHOR))
    coverage, values = state.advance(ANCHOR)
    assert coverage == {5 * MINUTE: False, HOUR: False, DAY: False} and values == {}
    assert state.next_timer == ANCHOR + 1
    coverage, values = state.advance(ANCHOR + 1)
    assert values == {}
    assert state.advance(ANCHOR + 2) is None
    coverage, values = state.advance(ANCHOR + 5 * MINUTE)
    assert values == {"attempt_count_5m": 1}
    coverage, values = state.advance(ANCHOR + HOUR)
    assert values == {"attempt_count_5m": 0, "attempt_count_1h": 1}
    _, values = state.advance(ANCHOR + DAY + 1)
    assert values == {"attempt_count_5m": 0, "attempt_count_1h": 0, "attempt_count_24h": 0}
    assert state.next_timer is None
    state.add(packet(start("one"), bootstrap=ANCHOR))
    with pytest.raises(ValueError, match="state limit"):
        state.add(packet(start("two"), bootstrap=ANCHOR))


def test_snapshot_identity_and_store_and_forward_revision():
    state = Entity("customer", "integration", "customer", ANCHOR - 2 * DAY)
    state.add(packet(start(when=ANCHOR - HOUR)))
    first = SnapshotUpdate.FromString(
        snapshot(state, state.advance(ANCHOR), epoch="ci", now=ANCHOR)
    )
    state.add(packet(start("late", when=ANCHOR - 10 * MINUTE)))
    second = SnapshotUpdate.FromString(
        snapshot(state, state.advance(ANCHOR), epoch="ci", now=ANCHOR)
    )
    assert {
        "version": first.definition.version,
        "sha256": first.definition.sha256,
    } == definition_identity()
    assert first.anchor == second.anchor
    assert first.values.attempt_count_1h == 1 and second.values.attempt_count_1h == 2
    assert second.values.attempt_count_5m == 0
    assert second.coverage.five_minutes == 2
    assert second.revision == first.revision + 1
    assert second.online_write_at.ToMicroseconds() > first.online_write_at.ToMicroseconds()
    identity = second.snapshot_id
    second.ClearField("snapshot_id")
    assert identity == hashlib.sha256(second.SerializeToString(deterministic=True)).hexdigest()


@pytest.mark.parametrize("view", ["customer", "method"])
def test_rolling_state_matches_reference_after_late_input_and_restore(view):
    random = Random(42)
    state = Entity(view, "integration", view, ANCHOR - 2 * DAY)
    history = []
    observed = {}

    def instant(microseconds):
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=microseconds)

    def event(value):
        return {
            "integration_id": value.integration,
            "payment_id": value.payment,
            "attempt_started": {
                "attempt_id": value.attempt,
                "context": {"customer": {"customer_id": value.customer}},
                "signals": {
                    "attempted_at": instant(value.started_at),
                    "payment_method": {"fingerprint": value.method},
                    "device_fingerprint": value.device,
                },
            },
        }

    def compare(now):
        result = state.advance(now)
        if result is not None:
            observed.clear()
            observed.update(result[1])
        expected = reference_snapshot(
            event(start("current", when=now)),
            history,
            scope=view,
            anchor=instant(now),
            cutoff=instant(now),
            coverage={window: "complete" for window in ("5m", "1h", "24h")},
            status="available",
        )
        assert observed == {
            name.removeprefix(f"{view}_snapshot_"): value
            for name, value in expected["values"].items()
        }

    for index in range(128):
        now = ANCHOR + index * HOUR // 8
        value = start(
            str(index),
            when=now - random.choice((0, 5 * MINUTE, HOUR, DAY, random.randrange(DAY))),
            device=random.choice((None, "one", "two")),
        )
        state.add(packet(value, now=now))
        state.add(packet(value, now=now))
        history.append(event(value))
        if index == 64:
            state = pickle.loads(pickle.dumps(state))
        compare(now)
    compare(now + DAY + 1)


def test_decode_uses_client_attempt_time_and_ignores_other_payloads():
    event = PaymentEvent(
        event_id="event",
        integration_id="integration",
        payment_id="payment",
        aggregate_version=1,
        schema_version=3,
    )
    event.occurred_at.FromMicroseconds(ANCHOR)
    event.recorded_at.FromMicroseconds(ANCHOR)
    event.attempt_started.attempt_id = "attempt"
    event.attempt_started.context.customer.customer_id = "customer"
    event.attempt_started.signals.payment_method.fingerprint = "method"
    event.attempt_started.signals.attempted_at.FromMicroseconds(ANCHOR - 10 * MINUTE)
    fact, recorded_at = decode(event.SerializeToString())
    assert fact.device is None
    assert fact.started_at == ANCHOR - 10 * MINUTE and recorded_at == ANCHOR
    event.attempt_started.signals.ClearField("attempted_at")
    with pytest.raises(Rejected, match="context") as invalid:
        decode(event.SerializeToString())
    assert (invalid.value.kind, invalid.value.event_id) == ("invalid_context", "event")
    event.ClearField("attempt_started")
    event.risk_decision_recorded.attempt_id = "attempt"
    assert decode(event.SerializeToString()) == (None, ANCHOR)
    event.schema_version = 4
    with pytest.raises(Rejected, match="unsupported") as unsupported:
        decode(event.SerializeToString())
    assert unsupported.value.kind == "invalid_event"
    with pytest.raises(Rejected) as garbage:
        decode(b"\xff\xff\xff")
    assert (garbage.value.kind, garbage.value.event_id) == ("undecodable", None)
