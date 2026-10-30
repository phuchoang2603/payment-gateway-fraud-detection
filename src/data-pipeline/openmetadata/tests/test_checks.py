import json
from pathlib import Path

import pytest

from openmetadata import checks, lineage, profile
from openmetadata.warehouse import LAKEHOUSE, MARTS

ROOT = Path(__file__).resolve().parents[4] / "contracts" / "data-products"


@pytest.fixture(scope="module")
def found():
    return {(check.table, check.name): check for check in checks.checks(ROOT)}


def test_every_contract_rule_resolves_to_its_server_database(found):
    assert {check.database for check in found.values()} == {LAKEHOUSE, MARTS}
    for check in found.values():
        for staging in (False, True):
            sql = checks.rendered(check, 42 if check.database == LAKEHOUSE else None, staging)
            assert "{object}" not in sql and "{table:" not in sql, check.name


def test_fqns_quote_only_lakehouse_names(found):
    assert (
        found[("gold.dim_customer", "gold_dim_one_current")].fqn == 'clickhouse.default.lakehouse."gold.dim_customer"'
    )
    assert found[("fact_payment_attempt", "marts_fact_unique_attempt")].fqn == (
        "clickhouse.default.marts.fact_payment_attempt"
    )


def test_a_single_table_rule_pins_the_whole_query(found):
    sql = checks.rendered(found[("silver.attempts", "silver_unique_attempt")], 7)

    assert "FROM lakehouse.`silver.attempts`" in sql
    assert sql.endswith(f"SETTINGS iceberg_snapshot_id = 7, {checks.SPILL_SETTINGS}")


def test_a_rule_reading_another_lakehouse_table_pins_only_its_own(found):
    sql = checks.rendered(found[("gold.label_attempt", "gold_label_attempt_exists")], 7)

    assert "(SELECT * FROM lakehouse.`gold.label_attempt` SETTINGS iceberg_snapshot_id = 7) AS l" in sql
    assert sql.endswith(f"SETTINGS {checks.SPILL_SETTINGS}")


def test_mart_rules_audit_staging_and_report_on_the_live_table(found):
    rule = found[("fact_merchant_day", "marts_merchant_day_sums")]

    audited = checks.rendered(rule, None, staging=True)
    live = checks.rendered(rule, None, staging=False)

    assert "marts.`fact_merchant_day__staging`" in audited
    assert "marts.`fact_payment_attempt__staging`" in audited and "marts.`build_info__staging`" in audited
    assert "__staging" not in live
    assert rule.fqn == "clickhouse.default.marts.fact_merchant_day"


def test_dp3_lineage_endpoints_are_contracted_tables(found):
    contracted = {check.fqn for check in found.values()}
    dp3 = [edge for edge in lineage.edges("v19") if edge.pipeline in lineage.DP3]

    assert {edge.pipeline for edge in dp3} == set(lineage.DP3) <= set(lineage.PIPELINES)
    for edge in dp3:
        assert edge.target.fqn in contracted, edge.target.fqn


def test_profiling_is_scoped_to_the_published_marts(tmp_path, monkeypatch):
    connectors = {
        "clickhouse": {
            "source": {
                "type": "clickhouse",
                "serviceName": "clickhouse",
                "serviceConnection": {"config": {"type": "Clickhouse", "password": "${CLICKHOUSE_PASSWORD}"}},
            }
        }
    }
    path = tmp_path / "connectors.json"
    path.write_text(json.dumps(connectors))
    monkeypatch.setenv("METADATA_CONNECTORS_FILE", str(path))

    config = profile.workflow()["source"]["sourceConfig"]["config"]

    assert config["type"] == "Profiler"
    assert config["schemaFilterPattern"] == {"includes": ["^marts$"]}
    assert config["tableFilterPattern"] == {"includes": ["^fact_payment_attempt$", "^fact_merchant_day$"]}
    assert config["profileSampleConfig"]["config"]["profileSample"] == 100
