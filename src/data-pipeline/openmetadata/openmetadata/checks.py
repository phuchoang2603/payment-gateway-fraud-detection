import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from openmetadata.catalog import client, native, server
from openmetadata.integrations import workflow
from openmetadata.wap import Catalog, audited_snapshots
from openmetadata.warehouse import SERVICE, sql_table, table_fqn

PRODUCTS = ("bronze-payments", "silver-payments")
PAGE = 100


@dataclass(frozen=True)
class Check:
    table: str
    name: str
    description: str
    sql: str


def checks(root=None, products=None):
    root = Path(root or os.environ["DATA_PRODUCTS_ROOT"])
    found = []
    for product in products or PRODUCTS:
        for contract in sorted((root / product).glob("*.odcs.yaml")):
            for table in yaml.safe_load(contract.read_text())["schema"]:
                for rule in table.get("quality", []):
                    found.append(check(contract.name, table["physicalName"], rule))
    return found


def check(contract, table, rule):
    if rule.get("type") != "sql" or rule.get("mustBe") != 0 or not rule.get("query"):
        raise ValueError(f"{contract} rule {rule.get('id')} is not a scalar count that must be 0")
    sql = rule["query"].replace("{object}", sql_table(table)).strip()
    return Check(table=table, name=rule["id"], description=rule.get("description", rule["id"]), sql=sql)


# The join spill threshold must be below its hard size limit so full-load checks can spill before hitting the cap.
SPILL_SETTINGS = (
    "max_bytes_before_external_group_by = 800000000, max_bytes_before_external_sort = 800000000, "
    "join_algorithm = 'auto', max_bytes_before_external_join = 400000000, max_bytes_in_join = 800000000"
)


def audited(sql, snapshot):
    """Pin a rule to the audited Iceberg snapshot, with the spill settings; each rule reads only its own table."""
    settings = SPILL_SETTINGS if snapshot is None else f"iceberg_snapshot_id = {int(snapshot)}, {SPILL_SETTINGS}"
    return f"{sql.rstrip().rstrip(';')}\nSETTINGS {settings}"


def test_workflow(table, cases, snapshot=None):
    return {
        "source": {
            "type": "testsuite",
            "serviceName": SERVICE,
            "sourceConfig": {
                "config": {
                    "type": "TestSuite",
                    "entityFullyQualifiedName": table_fqn(table),
                    "testCases": [case.name for case in cases],
                }
            },
        },
        "processor": {
            "type": "orm-test-runner",
            "config": {
                "forceUpdate": True,
                "testCases": [
                    {
                        "name": case.name,
                        "description": case.description,
                        "testDefinitionName": "tableCustomSQLQuery",
                        "parameterValues": [
                            {"name": "sqlExpression", "value": audited(case.sql, snapshot)},
                            {"name": "strategy", "value": "COUNT"},
                            {"name": "operator", "value": "=="},
                            {"name": "threshold", "value": "0"},
                        ],
                    }
                    for case in cases
                ],
            },
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "INFO", "openMetadataServerConfig": server()},
    }


def evaluate(expected, results):
    """Return the expected test names whose latest result is missing or not Success.

    The native runner exits 0 when an assertion fails, so the stored result is the verdict.
    """
    return [name for name in expected if (results.get(name) or {}).get("testCaseStatus") != "Success"]


def readback(table):
    results, after = {}, None
    while True:
        query = {"entityLink": f"<#E::table::{table_fqn(table)}>", "fields": "testCaseResult", "limit": PAGE}
        if after:
            query["after"] = after
        page = client().client.get("/dataQuality/testCases", query)
        for case in page.get("data", []):
            results[case["name"]] = case.get("testCaseResult") or {}
        after = page.get("paging", {}).get("after")
        if not after:
            return results


def main():
    found = checks(products=[os.environ["CHECK_PRODUCT"]])
    tables = sorted({check.table for check in found})
    branch = os.environ.get("WAP_BRANCH")
    snapshots = audited_snapshots(Catalog(), tables, branch) if branch else {}
    for table, snapshot in snapshots.items():
        print(f"auditing {table} at snapshot {snapshot} ({branch} head, or main if this run did not write it)")
    native("ingest", workflow(SERVICE))
    for table in tables:
        cases = [check for check in found if check.table == table]
        native("test", test_workflow(table, cases, snapshots.get(table)))
    failures = []
    for table in tables:
        expected = [check.name for check in found if check.table == table]
        failures += [f"{table}.{name}" for name in evaluate(expected, readback(table))]
    if failures:
        print("checks did not succeed: " + ", ".join(sorted(failures)))
        return 1
    print(f"all {len(found)} checks succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
