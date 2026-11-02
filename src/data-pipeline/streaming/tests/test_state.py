import hashlib
import pickle
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from random import Random

import pytest
from features.reference_v2 import definition_identity
from features.reference_v2 import snapshot as reference_snapshot
from hypothesis import given, settings
from hypothesis import strategies as st

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
    entity_key,
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


class Replay:
    """Feeds attempt starts to one entity's rolling state and checks it against the reference."""

    def __init__(self, view):
        self.view = view
        self.state = Entity(view, "integration", view, ANCHOR - 2 * DAY)
        self.history = []
        self.observed = {}

    def add(self, value, now, deliveries=2):
        for _ in range(deliveries):
            self.state.add(packet(value, now=now))
        self.history.append(event(value))

    def restore(self):
        self.state = pickle.loads(pickle.dumps(self.state))

    def compare(self, now):
        result = self.state.advance(now)
        if result is not None:
            self.observed.clear()
            self.observed.update(result[1])
        expected = reference_snapshot(
            event(start("current", when=now)),
            self.history,
            scope=self.view,
            anchor=instant(now),
            cutoff=instant(now),
            coverage={window: "complete" for window in ("5m", "1h", "24h")},
            status="available",
        )
        assert self.observed == {
            name.removeprefix(f"{self.view}_snapshot_"): value
            for name, value in expected["values"].items()
        }


@pytest.mark.parametrize("view", ["customer", "method"])
def test_rolling_state_matches_reference_after_late_input_and_restore(view):
    random = Random(42)
    replay = Replay(view)
    for index in range(128):
        now = ANCHOR + index * HOUR // 8
        value = start(
            str(index),
            when=now - random.choice((0, 5 * MINUTE, HOUR, DAY, random.randrange(DAY))),
            device=random.choice((None, "one", "two")),
        )
        replay.add(value, now)
        if index == 64:
            replay.restore()
        replay.compare(now)
    replay.compare(now + DAY + 1)


STEPS = st.lists(
    st.tuples(
        st.integers(0, 3 * HOUR),
        st.one_of(st.sampled_from((0, 5 * MINUTE, HOUR, DAY)), st.integers(0, DAY)),
        st.sampled_from((None, "one", "two")),
        st.integers(1, 3),
        st.booleans(),
    ),
    min_size=1,
    max_size=40,
)


@pytest.mark.parametrize("view", ["customer", "method"])
@settings(max_examples=60, deadline=None)
@given(steps=STEPS)
def test_rolling_state_matches_reference_for_any_arrivals_redeliveries_and_restores(view, steps):
    replay = Replay(view)
    now = ANCHOR
    for index, (gap, lateness, device, deliveries, restore) in enumerate(steps):
        now += gap
        replay.add(start(str(index), when=now - lateness, device=device), now, deliveries)
        if restore:
            replay.restore()
        replay.compare(now)
    replay.compare(now + DAY + 1)


@given(st.integers(0, 2 * DAY), st.integers(0, 2 * DAY))
def test_lateness_buckets_are_ordered_and_only_beyond_the_horizon_is_too_late(earlier, later):
    order = ["lt_1m", "1m_5m", "5m_15m", "15m_24h", "too_late"]
    earlier, later = sorted((earlier, later))

    assert order.index(lateness_bucket(earlier)) <= order.index(lateness_bucket(later))
    assert (lateness_bucket(later) == "too_late") == (later > HORIZON)


@given(st.lists(st.text(max_size=8), min_size=4, max_size=4), st.integers(1, 3))
def test_entity_keys_differ_when_a_boundary_between_components_moves(components, cut):
    if not components[cut - 1]:
        return
    moved = list(components)
    moved[cut - 1], moved[cut] = components[cut - 1][:-1], components[cut - 1][-1] + components[cut]

    assert entity_key(*components) != entity_key(*moved)


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


def test_replay_leaves_out_the_coverage_timers_but_still_reports_coverage_at_the_next_row():
    bootstrap = ANCHOR - 10 * MINUTE
    live = Entity("customer", "integration", "customer", bootstrap)
    replay = Entity("customer", "integration", "customer", bootstrap, coverage_timers=False)
    assert live.advance(ANCHOR) is not None and replay.advance(ANCHOR) is not None
    assert live.next_timer == bootstrap + HOUR
    assert replay.next_timer is None
    coverage, values = replay.advance(bootstrap + DAY)
    assert coverage == {5 * MINUTE: True, HOUR: True, DAY: True}
    assert values["attempt_count_24h"] == 0
