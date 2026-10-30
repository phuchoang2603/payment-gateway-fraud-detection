from pathlib import Path

import pytest

from openmetadata_jobs import checks, warehouse

CONTRACTS = Path(__file__).resolve().parents[4] / "contracts" / "data-products"


def test_every_contract_rule_becomes_a_native_count_check():
    found = checks.checks(CONTRACTS)

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
        checks.checks(tmp_path)


def test_a_check_passes_only_when_its_latest_result_succeeded():
    results = {
        "success": {"testCaseStatus": "Success", "timestamp": 1},
        "failed": {"testCaseStatus": "Failed", "timestamp": 10},
        "aborted": {"testCaseStatus": "Aborted", "timestamp": 10},
    }

    assert checks.evaluate(["success", "failed", "aborted", "missing"], results) == [
        "failed",
        "aborted",
        "missing",
    ]


def test_table_names_include_the_service_and_database(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_DATABASE", "lakehouse")

    assert warehouse.table_fqn("bronze.fraud_labels") == 'clickhouse.default.lakehouse."bronze.fraud_labels"'
    assert warehouse.sql_table("bronze.fraud_labels") == "lakehouse.`bronze.fraud_labels`"
