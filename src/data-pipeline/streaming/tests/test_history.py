import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from fraud_streaming.contracts import PaymentEvent
from fraud_streaming.history import encode

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
    event = PaymentEvent.FromString(raw)
    assert event.schema_version == 3
    assert event.integration_id == "i"
    assert event.attempt_started.context.customer.customer_id == "c"
    assert event.attempt_started.signals.payment_method.fingerprint == "card"
    assert event.recorded_at.ToMicroseconds() == int(
        datetime(2025, 9, 1, 11, 59, tzinfo=UTC).timestamp() * 1_000_000
    )
    assert event.attempt_started.signals.device_fingerprint == "prior-device"
    invalid = bronze()
    invalid["schema_version"] = 1
    with pytest.raises(ValueError, match="unsupported"):
        encode(invalid)


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
