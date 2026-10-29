from pathlib import Path

import pytest

from openmetadata_jobs import checks

CONTRACTS = Path(__file__).resolve().parents[4] / "contracts" / "data-products"


def test_every_contract_rule_becomes_a_native_count_check():
    found = checks.checks(CONTRACTS, catalog="fraud_lakehouse")

    assert {check.table for check in found} == {
        "bronze.payment_events",
        "bronze.fraud_labels",
        "bronze.rejected_records",
        "silver.payment_events",
        "silver.attempts",
        "silver.labels",
        "silver.rejected_events",
    }
    assert all(check.sql.startswith("SELECT count(*)") for check in found)
    assert all("{object}" not in check.sql for check in found)


def test_rule_that_is_not_a_zero_count_fails_preparation(tmp_path):
    product = tmp_path / "silver-payments"
    product.mkdir()
    (product / "labels.odcs.yaml").write_text(
        "schema:\n"
        "  - physicalName: silver.labels\n"
        "    quality:\n"
        "      - id: positive_rows\n"
        "        type: sql\n"
        "        query: SELECT count(*) FROM {object}\n"
        "        mustBe: 1\n"
    )

    with pytest.raises(ValueError, match="positive_rows"):
        checks.checks(tmp_path, catalog="fraud_lakehouse")


def test_a_check_counts_only_a_fresh_success():
    results = {
        "fresh": {"testCaseStatus": "Success", "timestamp": 10},
        "failed": {"testCaseStatus": "Failed", "timestamp": 10},
        "stale": {"testCaseStatus": "Success", "timestamp": 1},
    }

    assert checks.evaluate(["fresh", "failed", "stale", "missing"], results, prepared_at=5) == [
        "failed",
        "stale",
        "missing",
    ]
