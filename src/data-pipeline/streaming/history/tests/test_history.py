import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from features.reference_v2 import compute
from fraud_streaming.history import Audit, encode
from fraud_streaming.model import DAY, Entity, Packet
from fraud_streaming.replay import OffsetBarrier
from fraud_streaming.wire import decode

FIXTURES = json.loads(
    (
        Path(__file__).resolve().parents[5] / "contracts/features/fraud-snapshot-v2.examples.json"
    ).read_text()
)


def bronze():
    row = copy.deepcopy(FIXTURES["event_templates"]["start"])
    row["schema_version"] = 2
    row["aggregate_version"] = 1
    row["recorded_at"] = datetime(2025, 9, 1, 11, 59, tzinfo=UTC)
    row["_dataset_version"] = "v19"
    row["_source_file"] = "part-0.parquet"
    row["attempt_started"]["signals"]["ip_country"] = None
    return row


def test_bronze_round_trip_normalizes_envelope_and_preserves_attempt():
    raw = encode(bronze())
    fact, recorded = decode(raw)
    assert fact.integration == "i"
    assert fact.customer == "c"
    assert fact.method == "card"
    assert recorded == int(datetime(2025, 9, 1, 11, 59, tzinfo=UTC).timestamp() * 1_000_000)
    assert fact.device == "prior-device"
    invalid = bronze()
    invalid["schema_version"] = 1
    with pytest.raises(ValueError, match="unsupported"):
        encode(invalid)


def test_clock_overtaking_replay_matches_reference_and_audits_duplicates():
    row = bronze()
    raw = encode(row)
    fact, now = decode(raw)
    barrier = OffsetBarrier("input", "clock")
    end = now + 60_000_000
    controls = [
        {"sequence": 0, "clock_us": now - 2 * DAY, "input_offsets": {"0": 0}},
        {"sequence": 1, "clock_us": end, "input_offsets": {"0": 1}},
    ]
    packets = []
    for index, control in enumerate(controls):
        packets.extend(
            barrier.accept((json.dumps(control).encode(), "clock", 0, index, 0), control)
        )
    packets.extend(barrier.accept((raw, "input", 0, 0, 0)))
    state = Entity("customer", fact.integration, fact.customer, now - 2 * DAY)
    quantities = None
    for payload, topic, *_ in packets:
        if topic == "input":
            value, instant = decode(payload)
            state.add(Packet(value, instant, now - 2 * DAY, ()))
        else:
            instant = json.loads(payload)["clock_us"]
        while state.next_timer is not None and state.next_timer <= instant:
            emitted = state.advance(state.next_timer)
            if emitted is not None:
                quantities = emitted
        emitted = state.advance(instant)
        if emitted is not None:
            quantities = emitted
    assert quantities is not None
    expected = compute(
        current=FIXTURES["defaults"]["current"],
        events=[row],
        cutoff=datetime.fromtimestamp(end / 1_000_000, UTC).isoformat(),
        views=FIXTURES["defaults"]["views"],
    )
    assert quantities[1] == {
        field: expected["values"][f"customer_snapshot_{field}"]
        for field in (
            "attempt_count_5m",
            "attempt_count_1h",
            "attempt_count_24h",
            "distinct_devices_24h",
        )
    }
    audit = Audit()
    audit.observe(raw, 0)
    audit.observe(raw, 1)
    assert audit.counts["duplicates"] == 1
    assert list(audit.accepted) == [0]


def test_audit_24_hour_lateness_boundary():
    for delta, late in ((-1, 0), (1, 1)):
        row = bronze()
        recorded = row["recorded_at"]
        row["attempt_started"]["signals"]["attempted_at"] = (
            recorded - timedelta(days=1, microseconds=delta)
        ).isoformat()
        audit = Audit()
        audit.observe(encode(row), 0)
        assert audit.counts["too_late"] == late
        assert len(audit.accepted) == 1 - late
