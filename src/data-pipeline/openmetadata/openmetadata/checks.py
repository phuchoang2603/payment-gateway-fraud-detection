import os
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from openmetadata.catalog import client, native, server
from openmetadata.integrations import workflow
from openmetadata.wap import Catalog, audited_snapshots
from openmetadata.warehouse import LAKEHOUSE, MARTS, SERVICE, sql_table, table_fqn

PRODUCTS = ("bronze-payments", "silver-payments", "gold-features", "payment-marts")
PAGE = 100
STAGING = "__staging"
OTHER_TABLE = re.compile(r"\{table:([A-Za-z0-9_.]+)\}")


@dataclass(frozen=True)
class Check:
    table: str
    database: str
    name: str
    description: str
    query: str

    @property
    def fqn(self):
        return table_fqn(self.table, self.database)


def checks(root=None, products=None):
    root = Path(root or os.environ["DATA_PRODUCTS_ROOT"])
    found = []
    for product in products or PRODUCTS:
        for contract in sorted((root / product).glob("*.odcs.yaml")):
            document = yaml.safe_load(contract.read_text())
            database = document["servers"][0]["database"]
            for table in document["schema"]:
                for rule in table.get("quality", []):
                    found.append(check(contract.name, table["physicalName"], database, rule))
    return found


def check(contract, table, database, rule):
    if rule.get("type") != "sql" or rule.get("mustBe") != 0 or not rule.get("query"):
        raise ValueError(f"{contract} rule {rule.get('id')} is not a scalar count that must be 0")
    return Check(
        table=table,
        database=database,
        name=rule["id"],
        description=rule.get("description", rule["id"]),
        query=rule["query"].strip(),
    )


def products():
    return [product for product in os.environ["CHECK_PRODUCT"].split(",") if product]


# Full-load self-joins need disk-backed sorting rather than a hash table that grows with the payment count.
SPILL_SETTINGS = (
    "max_bytes_before_external_group_by = 800000000, max_bytes_before_external_sort = 400000000, "
    "join_algorithm = 'full_sorting_merge', "
    "max_threads = 2, max_block_size = 16384"
)


def rendered(check, snapshot=None, staging=False):
    """The rule's SQL against the audited data: an Iceberg table at its audited snapshot, or a mart's staging table.

    A query-level iceberg_snapshot_id applies to every Iceberg table the query reads, so a rule that also reads
    another lakehouse table pins only its own table, in a subquery.
    """
    sql = check.query.rstrip().rstrip(";")
    marts = staging and check.database == MARTS
    target = sql_table(f"{check.table}{STAGING}" if marts else check.table, check.database)
    sql = OTHER_TABLE.sub(lambda match: sql_table(f"{match[1]}{STAGING}" if marts else match[1], MARTS), sql)
    settings = SPILL_SETTINGS
    if snapshot is not None and check.database == LAKEHOUSE:
        if f"{LAKEHOUSE}." in sql:
            target = f"(SELECT * FROM {target} SETTINGS iceberg_snapshot_id = {int(snapshot)})"
        else:
            settings = f"iceberg_snapshot_id = {int(snapshot)}, {SPILL_SETTINGS}"
    return f"{sql.replace('{object}', target)}\nSETTINGS {settings}"


def test_workflow(fqn, cases, snapshot=None, staging=False):
    return {
        "source": {
            "type": "testsuite",
            "serviceName": SERVICE,
            "sourceConfig": {
                "config": {
                    "type": "TestSuite",
                    "entityFullyQualifiedName": fqn,
                    "testCases": [],
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
                            {"name": "sqlExpression", "value": rendered(case, snapshot, staging)},
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


def readback(fqn):
    results, after = {}, None
    while True:
        query = {"entityLink": f"<#E::table::{fqn}>", "fields": "testCaseResult", "limit": PAGE}
        if after:
            query["after"] = after
        page = client().client.get("/dataQuality/testCases", query)
        for case in page.get("data", []):
            results[case["name"]] = case.get("testCaseResult") or {}
        after = page.get("paging", {}).get("after")
        if not after:
            return results


def main():
    found = checks(products=products())
    branch = os.environ.get("WAP_BRANCH")
    iceberg = sorted({check.table for check in found if check.database == LAKEHOUSE})
    snapshots = audited_snapshots(Catalog(), iceberg, branch) if branch and iceberg else {}
    for table, snapshot in snapshots.items():
        print(f"auditing {table} at snapshot {snapshot} ({branch} head, or main if this run did not write it)")
    if branch and any(check.database == MARTS for check in found):
        print(f"auditing the marts in their {STAGING} tables")
    native("ingest", workflow(SERVICE))
    tables = sorted({(check.database, check.table) for check in found})
    for database, table in tables:
        cases = [check for check in found if (check.database, check.table) == (database, table)]
        native("test", test_workflow(cases[0].fqn, cases, snapshots.get(table), staging=bool(branch)))
    failures = []
    for database, table in tables:
        cases = [check for check in found if (check.database, check.table) == (database, table)]
        failures += [
            f"{database}.{table}.{name}" for name in evaluate([case.name for case in cases], readback(cases[0].fqn))
        ]
    if failures:
        print("checks did not succeed: " + ", ".join(sorted(failures)))
        return 1
    print(f"all {len(found)} checks succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
