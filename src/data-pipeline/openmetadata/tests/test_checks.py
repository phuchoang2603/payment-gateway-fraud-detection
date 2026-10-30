from pathlib import Path

import pytest

from openmetadata import checks, lineage, wap
from openmetadata.warehouse import LAKEHOUSE

ROOT = Path(__file__).resolve().parents[4] / "contracts" / "data-products"


@pytest.fixture(scope="module")
def found():
    return {(check.table, check.name): check for check in checks.checks(ROOT)}


def test_every_rule_of_every_product_renders_against_its_table(found):
    assert {check.table.split(".")[0] for check in found.values() if check.database == LAKEHOUSE} == {
        "bronze",
        "silver",
        "gold",
    }
    for check in found.values():
        for staging in (False, True):
            sql = checks.rendered(check, 42 if check.database == LAKEHOUSE else None, staging)
            assert "{object}" not in sql and "{table:" not in sql, check.name


def test_a_single_table_rule_is_pinned_to_the_audited_snapshot_query_wide(found):
    sql = checks.rendered(found[("silver.attempts", "silver_unique_attempt")], 7)

    assert "FROM lakehouse.`silver.attempts`" in sql
    assert sql.endswith(f"SETTINGS iceberg_snapshot_id = 7, {checks.SPILL_SETTINGS}")


def test_a_rule_reading_another_lakehouse_table_pins_only_its_own(found):
    sql = checks.rendered(found[("gold.label_attempt", "gold_label_attempt_exists")], 7)

    assert "(SELECT * FROM lakehouse.`gold.label_attempt` SETTINGS iceberg_snapshot_id = 7) AS l" in sql
    assert sql.endswith(f"SETTINGS {checks.SPILL_SETTINGS}")


def test_mart_rules_audit_staging_but_report_on_the_live_table(found):
    rule = found[("fact_merchant_day", "marts_merchant_day_sums")]

    audited = checks.rendered(rule, None, staging=True)

    assert "marts.`fact_merchant_day__staging`" in audited
    assert "marts.`fact_payment_attempt__staging`" in audited and "marts.`build_info__staging`" in audited
    assert "__staging" not in checks.rendered(rule, None, staging=False)
    assert rule.fqn == "clickhouse.default.marts.fact_merchant_day"


def test_every_lineage_edge_lands_on_a_contracted_table(found):
    contracted = {check.fqn for check in found.values()}
    targets = {edge.pipeline: [] for edge in lineage.edges("v19")}
    for edge in lineage.edges("v19"):
        targets[edge.pipeline].append(edge.target.fqn)

    assert set(targets) <= set(lineage.PIPELINES)
    for pipeline, fqns in targets.items():
        assert set(fqns) <= contracted, pipeline


def test_a_missing_or_failed_result_fails_the_check():
    results = {"passed": {"testCaseStatus": "Success"}, "failed": {"testCaseStatus": "Failed"}}

    assert checks.evaluate(["passed", "failed", "never-ran"], results) == ["failed", "never-ran"]


class FakeCatalog:
    def __init__(self, tables):
        self.tables = tables

    def metadata(self, table):
        return self.tables[table]


def table_metadata(main, branch=None, parents=None):
    refs = {"main": {"snapshot-id": main}} if main else {}
    if branch:
        refs["audit-run"] = {"snapshot-id": branch}
    snapshots = [{"snapshot-id": child, "parent-snapshot-id": parent} for child, parent in (parents or {}).items()]
    return {"refs": refs, "snapshots": snapshots}


def test_publish_fast_forwards_main_only_from_its_own_branch_base():
    catalog = FakeCatalog(
        {
            "silver.attempts": table_metadata(1, 2, {2: 1}),
            "silver.labels": table_metadata(5),
            "gold.dim_customer": table_metadata(None, 9),
        }
    )

    changes = wap.publish_changes(catalog, ["silver.attempts", "silver.labels", "gold.dim_customer"], "audit-run")

    assert [(change["identifier"]["name"], change["updates"][0]["snapshot-id"]) for change in changes] == [
        ("attempts", 2),
        ("dim_customer", 9),
    ]
    assert wap.audited_snapshots(catalog, ["silver.labels"], "audit-run") == {"silver.labels": 5}


def test_publish_refuses_when_main_moved_off_the_branch_base():
    catalog = FakeCatalog({"bronze.payment_events": table_metadata(3, 2, {2: 1, 3: 1})})

    with pytest.raises(RuntimeError, match="not a fast-forward"):
        wap.publish_changes(catalog, ["bronze.payment_events"], "audit-run")
