import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from features.reference_v2 import compute

from fraud_streaming.history import encode
from fraud_streaming.model import DAY, Entity, Packet
from fraud_streaming.replay import OffsetBarrier
from fraud_streaming.wire import decode

FIXTURES = json.loads(
    (
        Path(__file__).resolve().parents[4] / "contracts/features/fraud-snapshot-v2.examples.json"
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


def test_clock_overtaking_replay_matches_reference():
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


def test_units_split_the_range_into_week_runs():
    from datetime import date

    from fraud_streaming.history import units

    result = units(date(2026, 7, 29), date(2026, 8, 15), 7)
    assert [(unit["start"], unit["end"]) for unit in result] == [
        ("2026-07-29", "2026-08-05"),
        ("2026-08-05", "2026-08-12"),
        ("2026-08-12", "2026-08-15"),
    ]
    assert [unit["id"] for unit in result] == ["20260729", "20260805", "20260812"]
    with pytest.raises(ValueError):
        units(date(2026, 8, 1), date(2026, 8, 1), 7)
