import os
import time
from dataclasses import dataclass
from pathlib import Path

import yaml

from openmetadata_jobs.catalog import client, native, server
from openmetadata_jobs.integrations import workflow

PRODUCTS = ("bronze-payments", "silver-payments")
SERVICE = "starrocks"
PAGE = 100


@dataclass(frozen=True)
class Check:
    table: str
    name: str
    description: str
    sql: str


def checks(root=None, catalog=None, products=None):
    root = Path(root or os.environ["DATA_PRODUCTS_ROOT"])
    catalog = catalog or os.environ.get("STARROCKS_CATALOG", "lakehouse")
    found = []
    for product in products or PRODUCTS:
        for contract in sorted((root / product).glob("*.odcs.yaml")):
            for table in yaml.safe_load(contract.read_text())["schema"]:
                for rule in table.get("quality", []):
                    found.append(check(contract.name, table["physicalName"], rule, catalog))
    return found


def table_fqn(table):
    return f"{SERVICE}.{os.environ.get('STARROCKS_CATALOG', 'lakehouse')}.{table}"


def check(contract, table, rule, catalog):
    if rule.get("type") != "sql" or rule.get("mustBe") != 0 or not rule.get("query"):
        raise ValueError(f"{contract} rule {rule.get('id')} is not a scalar count that must be 0")
    sql = rule["query"].replace("{object}", f"{catalog}.{table}").strip()
    return Check(table=table, name=rule["id"], description=rule.get("description", rule["id"]), sql=sql)


def test_workflow(table, cases):
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
                            {"name": "sqlExpression", "value": case.sql},
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


def evaluate(expected, results, prepared_at):
    """Return the expected test names without a Success result newer than preparation.

    The native runner exits 0 when an assertion fails, so the stored result is the verdict.
    """
    failures = []
    for name in expected:
        result = results.get(name)
        if result is None or result.get("testCaseStatus") != "Success" or result.get("timestamp", 0) < prepared_at:
            failures.append(name)
    return failures


def readback(table, prepared_at):
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
    prepared_at = int(time.time() * 1000)
    found = checks(products=[os.environ["CHECK_PRODUCT"]])
    tables = sorted({check.table for check in found})
    native("ingest", workflow(SERVICE))
    for table in tables:
        cases = [check for check in found if check.table == table]
        native("test", test_workflow(table, cases))
    failures = []
    for table in tables:
        expected = [check.name for check in found if check.table == table]
        failures += [f"{table}.{name}" for name in evaluate(expected, readback(table, prepared_at), prepared_at)]
    if failures:
        print("checks did not succeed: " + ", ".join(sorted(failures)))
        return 1
    print(f"all {len(found)} checks succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
