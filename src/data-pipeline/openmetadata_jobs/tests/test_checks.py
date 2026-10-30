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


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_a_result_that_arrives_on_a_later_poll_passes():
    clock = Clock()
    answers = {"late": iter([[], [{"testCaseStatus": "Success", "timestamp": 10}]])}

    failed, missing = checks.await_results(
        ["late"], lambda name: next(answers[name]), prepared_at=5, timeout=60, poll=5, clock=clock, sleep=clock.sleep
    )

    assert (failed, missing) == ([], [])


def test_a_case_with_only_older_results_is_missing_after_the_timeout():
    clock = Clock()
    stale = [{"testCaseStatus": "Success", "timestamp": 1}]

    failed, missing = checks.await_results(
        ["stale"], lambda name: stale, prepared_at=5, timeout=10, poll=5, clock=clock, sleep=clock.sleep
    )

    assert (failed, missing) == ([], ["stale"])


def test_a_fresh_failed_result_fails_without_waiting():
    clock = Clock()
    calls = []

    def fetch(name):
        calls.append(name)
        return [{"testCaseStatus": "Failed", "timestamp": 10}]

    failed, missing = checks.await_results(
        ["bad"], fetch, prepared_at=5, timeout=60, poll=5, clock=clock, sleep=clock.sleep
    )

    assert (failed, missing) == (["bad"], [])
    assert calls == ["bad"]


def test_table_names_include_the_service_and_database(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_DATABASE", "lakehouse")

    assert warehouse.table_fqn("bronze.fraud_labels") == 'clickhouse.default.lakehouse."bronze.fraud_labels"'
    assert warehouse.sql_table("bronze.fraud_labels") == "lakehouse.`bronze.fraud_labels`"
