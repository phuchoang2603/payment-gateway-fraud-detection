from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from fraud_feast.training import entity_rows


def test_entities_join_valid_attempt_request_and_available_simulation_truth(spark):
    day = datetime(2026, 7, 29, tzinfo=UTC)
    names = ["truth-false", "truth-true", "unavailable", "chargeback", "invalid", "outside"]
    attempts = spark.createDataFrame(
        [
            (
                "i",
                name,
                name,
                day if name != "outside" else day - timedelta(days=1),
                name != "invalid",
            )
            for name in names
        ],
        "integration_id string, payment_id string, attempt_id string, started_at timestamp, lifecycle_valid boolean",
    )
    labels = spark.createDataFrame(
        [
            (
                "i",
                name,
                name,
                "customer",
                None,
                day,
                name != "truth-false",
                "LABEL_SOURCE_CHARGEBACK"
                if name == "chargeback"
                else "LABEL_SOURCE_SIMULATION_TRUTH",
                day + timedelta(days=2) if name == "unavailable" else day,
            )
            for name in names
        ],
        "integration_id string, payment_id string, attempt_id string, customer_id string, payment_method_fingerprint string, event_timestamp timestamp, label boolean, label_source string, label_available_at timestamp",
    )
    events = spark.createDataFrame(
        [
            {
                "integration_id": "i",
                "payment_id": name,
                "payload_case": "attempt_started",
                "attempt_started": {
                    "attempt_id": name,
                    "context": {
                        "snapshot": {
                            "total": {"minor_units": 500},
                            "customer_account_created_at": day - timedelta(days=10),
                            "merchant_account_created_at": day - timedelta(days=20),
                            "shipping": {"country": "US"},
                            "items": [{"sku": "a"}, {"sku": "b"}],
                        }
                    },
                    "signals": {"ip_country": None, "device_fingerprint": None},
                },
            }
            for name in names
        ],
        "integration_id string, payment_id string, payload_case string, attempt_started struct<attempt_id:string,context:struct<snapshot:struct<total:struct<minor_units:bigint>,customer_account_created_at:timestamp,merchant_account_created_at:timestamp,shipping:struct<country:string>,items:array<struct<sku:string>>>>,signals:struct<ip_country:string,device_fingerprint:string>>",
    )
    tables = {
        "lake.silver.attempts": attempts,
        "lake.gold.label_attempt": labels,
        "lake.silver.payment_events": events,
    }
    rows = entity_rows(
        SimpleNamespace(table=tables.__getitem__),
        "2026-07-29",
        "2026-07-30",
        day + timedelta(days=1),
    ).collect()
    assert {r.attempt_id: r.label for r in rows} == {"truth-false": False, "truth-true": True}
    for row in rows:
        assert row.event_timestamp == row.started_at == row.scored_at
        assert row.amount_minor == 500 and row.item_count == 2
        assert row.ip_country is None and row.payment_method_fingerprint is None
        assert row.has_device is False


# Boundary values of the cohort and cutoff: an empty or reversed range, and a cutoff without an offset.
@pytest.mark.parametrize(
    ("start", "end", "cutoff", "error"),
    [
        ("2026-08-01", "2026-08-01", "2026-09-01T00:00:00Z", "precede"),
        ("2026-08-02", "2026-08-01", "2026-09-01T00:00:00Z", "precede"),
        ("2026-08-01", "2026-08-02", "2026-09-01T00:00:00", "UTC offset"),
    ],
)
def test_entity_rows_rejects_bad_bounds(start, end, cutoff, error):
    with pytest.raises(ValueError, match=error):
        entity_rows(None, start, end, cutoff)
