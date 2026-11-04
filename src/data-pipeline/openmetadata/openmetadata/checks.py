"""Execute range-scoped contract assertions and publish native OpenMetadata test results."""

import os
import re
from dataclasses import dataclass
from datetime import date, timedelta
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
LAKEHOUSE_TABLE = re.compile(rf"{LAKEHOUSE}\.`([A-Za-z0-9_]+\.[A-Za-z0-9_]+)`")
# A Silver or Gold run records its day range on its commits; its rules then audit only the rows that run could have
# written, so check time follows the range, not the history. Bronze and the marts audit as before.
RANGE_PROPERTIES = ("dp2.range", "dp3.range")
LABEL_LOOKBACK_DAYS = 45


def instant(day: date) -> str:
    """Render UTC midnight for a date-scoped SQL boundary."""
    return f"toDateTime64('{day} 00:00:00', 6, 'UTC')"


def day_scope(column: str, start: date, end: date) -> str:
    """Build a half-open timestamp predicate for an inclusive date range."""
    return f"{column} BETWEEN toDate('{start}') AND toDate('{end}')"


SCOPES = {
    "silver.payment_events": lambda start, end: day_scope("payment_day", start, end),
    "silver.attempts": lambda start, end: day_scope("payment_day", start, end),
    "silver.rejected_events": lambda start, end: day_scope("partition_day", start, end),
    "silver.labels": lambda start, end: (
        f"label_available_at >= {instant(start)} AND label_available_at < {instant(end + timedelta(days=1))}"
    ),
    "gold.feat_customer_daily": lambda start, end: (
        f"event_timestamp > {instant(start)} AND event_timestamp <= {instant(end + timedelta(days=1))}"
    ),
    "gold.feat_payment_method_daily": lambda start, end: (
        f"event_timestamp > {instant(start)} AND event_timestamp <= {instant(end + timedelta(days=1))}"
    ),
    # A run also rewrites the label rows of earlier attempt days whose labels became available in its range.
    "gold.label_attempt": lambda start, end: (
        f"event_timestamp >= {instant(start - timedelta(days=LABEL_LOOKBACK_DAYS))} "
        f"AND event_timestamp < {instant(end + timedelta(days=1))}"
    ),
    # Every version still open at the range start: each customer's chain through its current version.
    "gold.dim_customer": lambda start, end: f"(valid_to_ts IS NULL OR valid_to_ts >= {instant(start)})",
}


def snapshot_range(metadata, snapshot):
    """The day range a Silver or Gold run recorded on its commit, or None."""
    for entry in metadata.get("snapshots", []):
        if entry.get("snapshot-id") == snapshot:
            summary = entry.get("summary") or {}
            for prop in RANGE_PROPERTIES:
                if summary.get(prop):
                    first, last = summary[prop].split("/")
                    return date.fromisoformat(first), date.fromisoformat(last)
    return None


def scopes_for(catalog, snapshots):
    """Each audited table's row filter from the range its audited commit recorded."""
    found = {}
    for table, snapshot in snapshots.items():
        recorded = snapshot_range(catalog.metadata(table), snapshot) if table in SCOPES else None
        if recorded:
            found[table] = SCOPES[table](*recorded)
    return found


@dataclass(frozen=True)
class Check:
    """Carry a contract assertion and its catalog test identity."""

    table: str
    database: str
    name: str
    description: str
    query: str

    @property
    def fqn(self):
        """Scope an assertion identity to its catalog table."""
        return table_fqn(self.table, self.database)


def checks(root=None, products=None):
    """Load executable quality assertions from selected data-product contracts."""
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
    """Reject unsupported assertions before constructing a native SQL test."""
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
    """Resolve the selected contract products from the run environment."""
    return [product for product in os.environ["CHECK_PRODUCT"].split(",") if product]


# Full-load self-joins need disk-backed sorting rather than a hash table that grows with the payment count.
SPILL_SETTINGS = (
    "max_bytes_before_external_group_by = 800000000, max_bytes_before_external_sort = 400000000, "
    "join_algorithm = 'full_sorting_merge', "
    "max_threads = 2, max_block_size = 16384"
)


def rendered(check, snapshot=None, staging=False, audited=None, scopes=None):
    """The rule's SQL against the audited data: an Iceberg table at its audited snapshot, or a mart's staging table.

    A query-level iceberg_snapshot_id applies to every Iceberg table the query reads, so a rule that also reads
    another lakehouse table pins only its own table, in a subquery. That other table is pinned to its own audited
    snapshot when this run audits it too: a pipeline run writes Silver and Gold on one unpublished branch.
    """
    sql = check.query.rstrip().rstrip(";")
    reads_another = f"{LAKEHOUSE}." in sql
    audited = audited or {}
    sql = LAKEHOUSE_TABLE.sub(
        lambda match: (
            f"(SELECT * FROM {match[0]} SETTINGS iceberg_snapshot_id = {int(audited[match[1]])})"
            if match[1] in audited
            else match[0]
        ),
        sql,
    )
    marts = staging and check.database == MARTS
    target = sql_table(f"{check.table}{STAGING}" if marts else check.table, check.database)
    sql = OTHER_TABLE.sub(lambda match: sql_table(f"{match[1]}{STAGING}" if marts else match[1], MARTS), sql)
    settings = SPILL_SETTINGS
    where = (scopes or {}).get(check.table) if check.database == LAKEHOUSE else None
    if where:
        # A table filter applies to every read of the table, a self-join's both sides included, and keeps column
        # pruning and filter pushdown that a SELECT * subquery would lose. Its key is database.table, unquoted.
        quoted = where.replace("\\", "\\\\").replace("'", "\\'")
        settings = f"additional_table_filters = {{'{LAKEHOUSE}.{check.table}': '{quoted}'}}, {settings}"
    if snapshot is not None and check.database == LAKEHOUSE:
        if reads_another:
            target = f"(SELECT * FROM {target} SETTINGS iceberg_snapshot_id = {int(snapshot)})"
        else:
            settings = f"iceberg_snapshot_id = {int(snapshot)}, {settings}"
    return f"{sql.replace('{object}', target)}\nSETTINGS {settings}"


def test_workflow(fqn, cases, snapshot=None, staging=False, audited=None, scopes=None):
    """Build native test execution against the audited snapshots or staging tables."""
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
                            {"name": "sqlExpression", "value": rendered(case, snapshot, staging, audited, scopes)},
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
    """Collect paginated native test results for the named suite."""
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
    """Audit the selected data products and fail the run on missing or failed results."""
    found = checks(products=products())
    branch = os.environ.get("WAP_BRANCH")
    iceberg = sorted({check.table for check in found if check.database == LAKEHOUSE})
    catalog = Catalog() if branch and iceberg else None
    snapshots = audited_snapshots(catalog, iceberg, branch) if catalog else {}
    scopes = scopes_for(catalog, snapshots) if catalog else {}
    for table, snapshot in snapshots.items():
        print(f"auditing {table} at snapshot {snapshot} ({branch} head, or main if this run did not write it)")
        if table in scopes:
            print(f"  only rows {scopes[table]}")
    if branch and any(check.database == MARTS for check in found):
        print(f"auditing the marts in their {STAGING} tables")
    native("ingest", workflow(SERVICE))
    tables = sorted({(check.database, check.table) for check in found})
    for database, table in tables:
        cases = [check for check in found if (check.database, check.table) == (database, table)]
        native("test", test_workflow(cases[0].fqn, cases, snapshots.get(table), bool(branch), snapshots, scopes))
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
