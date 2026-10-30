import os
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

import yaml

from openmetadata_jobs.catalog import client, native, server
from openmetadata_jobs.integrations import workflow
from openmetadata_jobs.warehouse import SERVICE, sql_table, table_fqn

PRODUCTS = ("bronze-payments", "silver-payments")
PAGE = 100
RESULT_TIMEOUT_SECONDS = 120
RESULT_POLL_SECONDS = 5


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


def fresh_results(fqn, since):
    page = client().client.get(
        f"/dataQuality/testCases/testCaseResults/{quote(fqn, safe='')}",
        {"startTs": since, "limit": PAGE},
    )
    return [result for result in page.get("data", []) if result.get("timestamp", 0) >= since]


def await_results(
    expected,
    fetch,
    prepared_at,
    timeout=RESULT_TIMEOUT_SECONDS,
    poll=RESULT_POLL_SECONDS,
    clock=time.monotonic,
    sleep=time.sleep,
):
    """Return the expected cases that failed and those with no result from this run.

    The native runner writes results asynchronously, so each case is polled until it has a result written at or
    after preparation. Its newest such result decides it.
    """
    deadline = clock() + timeout
    pending, failed = list(expected), []
    while True:
        unresolved = []
        for name in pending:
            results = fetch(name)
            if not results:
                unresolved.append(name)
                continue
            newest = max(results, key=lambda result: result["timestamp"])
            if newest.get("testCaseStatus") != "Success":
                failed.append(name)
        pending = unresolved
        if not pending or clock() >= deadline:
            return failed, pending
        sleep(poll)


def main():
    prepared_at = int(time.time() * 1000)
    found = checks(products=[os.environ["CHECK_PRODUCT"]])
    tables = sorted({check.table for check in found})
    native("ingest", workflow(SERVICE))
    for table in tables:
        cases = [check for check in found if check.table == table]
        native("test", test_workflow(table, cases))
    failed, missing = [], []
    for table in tables:
        expected = [check.name for check in found if check.table == table]
        table_failed, table_missing = await_results(
            expected,
            lambda name, table=table: fresh_results(f"{table_fqn(table)}.{name}", prepared_at),
            prepared_at,
        )
        failed += [f"{table}.{name}" for name in table_failed]
        missing += [f"{table}.{name}" for name in table_missing]
    if failed:
        print("checks did not succeed: " + ", ".join(sorted(failed)))
    if missing:
        print("no result from this run: " + ", ".join(sorted(missing)))
    if failed or missing:
        return 1
    print(f"all {len(found)} checks succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
