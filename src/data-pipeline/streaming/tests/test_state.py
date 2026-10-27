import hashlib
import pickle
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from random import Random

import pytest
from features.reference import snapshot as reference_snapshot

from fraud_streaming.model import DAY, HOUR, RETENTION, Attempt, Contribution, Entity, Fact, Packet
from fraud_streaming.wire import PaymentEvent, SnapshotUpdate, decode, snapshot

ANCHOR = 1_760_000_000_000_000


def start(identity="attempt", *, when=ANCHOR, device="device", amount=100):
    return Fact(
        identity,
        identity,
        "integration",
        "payment",
        identity,
        "start",
        when,
        when,
        "customer",
        "method",
        amount,
        device,
    )


def contribution(value, *, bootstrap=ANCHOR - 8 * DAY, now=ANCHOR):
    return Contribution(value, now, bootstrap, ())


def test_reordered_matching_conflicts_and_expired_unmatched_facts():
    value = start()
    decision = replace(value, kind="decision", event_id="decision", digest="decision")
    state = Attempt()
    packet = Packet(decision, ANCHOR, ANCHOR, ())
    assert state.accept(packet) is None
    matched = state.accept(replace(packet, fact=value))
    assert matched.start == value
    assert state.accept(packet) is None
    with pytest.raises(ValueError, match="conflicting lifecycle"):
        state.accept(replace(packet, fact=replace(decision, digest="changed")))
    state.expire()
    with pytest.raises(ValueError, match="decision precedes"):
        Attempt(start=value).accept(replace(packet, fact=replace(decision, started_at=ANCHOR - 1)))
    with pytest.raises(ValueError, match="unmatched lifecycle"):
        Attempt(start=value).expire()
    with pytest.raises(ValueError, match="retained correction"):
        Attempt().accept(replace(packet, now=ANCHOR + RETENTION + 1))


def test_exact_window_expiration_repeated_devices_and_restored_revision():
    state = Entity("customer", "integration", "customer", ANCHOR - 8 * DAY)
    for value in (
        start("old", when=ANCHOR - 7 * DAY),
        start("hour", when=ANCHOR - HOUR, amount=2**53 + 1),
        start("day", when=ANCHOR - DAY, device=None),
    ):
        state.add(contribution(value))
    _, values = state.advance(ANCHOR)
    assert values == {
        "assessed_count_1h": 1,
        "assessed_count_24h": 2,
        "assessed_amount_minor_24h": 2**53 + 101,
        "distinct_devices_7d": 1,
        "last_assessed_start_at": ANCHOR - HOUR,
    }
    state = pickle.loads(pickle.dumps(state))
    _, values = state.advance(ANCHOR + 1)
    assert values["assessed_count_1h"] == 0
    assert values["assessed_count_24h"] == 1
    assert values["distinct_devices_7d"] == 1
    assert state.revision == 2
    _, values = state.advance(ANCHOR + 7 * DAY)
    assert values["distinct_devices_7d"] == 0
    assert values["last_assessed_start_at"] is None
    assert state.entries == {}
    assert state.advance(ANCHOR + 7 * DAY + 1) is None


def test_bootstrap_coverage_unknown_then_empty_zero_and_bounded_state():
    state = Entity("method", "integration", "method", ANCHOR, max_entries=1)
    state.add(contribution(start(), bootstrap=ANCHOR))
    coverage, values = state.advance(ANCHOR)
    assert coverage == {HOUR: False, DAY: False} and values == {}
    assert state.next_timer == ANCHOR + 1
    coverage, values = state.advance(ANCHOR + HOUR)
    assert coverage == {HOUR: True, DAY: False}
    assert values == {"assessed_count_1h": 1}
    _, values = state.advance(ANCHOR + DAY + 1)
    assert values == {"assessed_count_1h": 0, "assessed_count_24h": 0}
    assert state.next_timer is None
    state.add(contribution(start("one"), bootstrap=ANCHOR))
    with pytest.raises(ValueError, match="state limit"):
        state.add(contribution(start("two"), bootstrap=ANCHOR))


def test_snapshot_identity_and_same_anchor_late_correction():
    state = Entity("customer", "integration", "customer", ANCHOR - 8 * DAY)
    state.add(contribution(start(when=ANCHOR - HOUR)))
    first = SnapshotUpdate.FromString(
        snapshot(state, state.advance(ANCHOR), epoch="ci", now=ANCHOR)
    )
    state.add(contribution(start("late", when=ANCHOR - HOUR)))
    second = SnapshotUpdate.FromString(
        snapshot(state, state.advance(ANCHOR), epoch="ci", now=ANCHOR)
    )
    assert first.anchor == second.anchor
    assert first.values.assessed_count_1h == 1 and second.values.assessed_count_1h == 2
    assert second.revision == first.revision + 1
    assert second.online_write_at.ToMicroseconds() > first.online_write_at.ToMicroseconds()
    identity = second.snapshot_id
    second.ClearField("snapshot_id")
    assert identity == hashlib.sha256(second.SerializeToString(deterministic=True)).hexdigest()


@pytest.mark.parametrize("view", ["customer", "method"])
def test_rolling_state_matches_reference_after_late_input_and_restore(view):
    random = Random(42)
    state = Entity(view, "integration", view, ANCHOR - 8 * DAY)
    history = []
    observed = {}

    def instant(microseconds):
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=microseconds)

    def event(value):
        return {
            "integration_id": value.integration,
            "payment_id": value.payment,
            "occurred_at": instant(value.started_at),
            "attempt_started": {
                "attempt_id": value.attempt,
                "context": {
                    "customer": {"customer_id": value.customer},
                    "snapshot": {"total": {"minor_units": value.amount, "currency": "USD"}},
                },
                "signals": {
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
        current = event(start("current", when=now))
        expected = reference_snapshot(
            current,
            history,
            scope=view,
            anchor=instant(now),
            cutoff=instant(now),
            coverage={
                window: "complete"
                for window in ("1h", "24h", "7d")[: 3 if view == "customer" else 2]
            },
            status="available",
        )
        values = {
            name.removeprefix(f"{view}_snapshot_"): value
            for name, value in expected["values"].items()
        }
        if view == "customer":
            last = expected["last_assessed_at"]
            values["last_assessed_start_at"] = (
                (datetime.fromisoformat(last) - instant(0)) // timedelta(microseconds=1)
                if last is not None
                else None
            )
        assert observed == values

    for index in range(128):
        now = ANCHOR + index * HOUR // 8
        value = start(
            str(index),
            when=now - random.choice((0, HOUR, DAY, 7 * DAY, random.randrange(7 * DAY))),
            device=random.choice((None, "one", "two")),
            amount=random.randrange(10_000),
        )
        state.add(contribution(value, now=now))
        state.add(contribution(value, now=now))
        history.append(event(value))
        if index == 64:
            state = pickle.loads(pickle.dumps(state))
        compare(now)
    compare(now + 7 * DAY + 1)


def test_protobuf_context_and_finalized_decision_validation():
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
    event.attempt_started.context.snapshot.total.currency = "USD"
    event.attempt_started.context.snapshot.total.minor_units = 100
    event.attempt_started.signals.payment_method.fingerprint = "method"
    event.attempt_started.signals.attempted_at.CopyFrom(event.occurred_at)
    fact, _ = decode(event.SerializeToString())
    assert fact.device is None and fact.amount == 100
    event.attempt_started.context.snapshot.total.currency = "EUR"
    with pytest.raises(ValueError, match="context"):
        decode(event.SerializeToString())
    event.ClearField("attempt_started")
    event.risk_decision_recorded.attempt_id = "attempt"
    with pytest.raises(ValueError, match="finalized"):
        decode(event.SerializeToString())
    event.schema_version = 4
    with pytest.raises(ValueError, match="unsupported"):
        decode(event.SerializeToString())
