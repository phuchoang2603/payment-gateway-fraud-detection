import time
from dataclasses import dataclass
from hashlib import sha256

from data_platform.catalog import server
from data_platform.contracts import prepare
from data_platform.products import PORTS

CATALOG = "lakehouse"
SCHEMA = "bronze"
SERVICE = "starrocks"
TIME_COLUMN = {"payment_events": "occurred_at", "fraud_labels": "label_available_at"}
CONSERVATION = "source_row_counts"


@dataclass
class Case:
    table: str
    name: str
    description: str
    sql: str

    @property
    def fqn(self):
        return f"{entity(self.table)}.{self.name}"


def physical_table(table):
    return f"raw_{table}"


def entity(table):
    return f"{SERVICE}.{CATALOG}.{SCHEMA}.{physical_table(table)}"


def relation(table, run):
    return (
        f"(SELECT * FROM {CATALOG}.{SCHEMA}.{physical_table(table)} FOR VERSION AS OF {run['snapshots'][table]} "
        f"WHERE _source_dataset_version = '{run['dataset_version']}')"
    )


def conservation(table, manifest, rel):
    if manifest.get("input_mode") == "landing":
        return f"SELECT COUNT(*) FROM (SELECT COUNT(*) AS n FROM {rel} AS e) AS actual WHERE n <> {manifest['totals'][table]}"
    expected = ", ".join(f"('{day}', {rows})" for day, rows in sorted(manifest["partitions"][table].items()))
    column = TIME_COLUMN[table]
    return (
        "SELECT COUNT(*) FROM (\n"
        f"  SELECT 1 FROM (SELECT DATE(e.{column}) AS day, COUNT(*) AS n FROM {rel} AS e GROUP BY DATE(e.{column})) AS a\n"
        f"  FULL OUTER JOIN (SELECT CAST(t.day AS DATE) AS day, t.n FROM (VALUES {expected}) AS t(day, n)) AS x"
        " ON a.day = x.day\n"
        "  WHERE a.day IS NULL OR x.day IS NULL OR a.n <> x.n\n) AS violations"
    )


def cases(run, manifest):
    """Render every contract rule for the pinned snapshots; the Git contracts are the only source of rules."""
    tokens = {
        "events": relation("payment_events", run),
        "labels": relation("fraud_labels", run),
        "schema": SCHEMA,
    }
    result = []
    for table in PORTS:
        tokens["table"] = physical_table(table)
        tokens["relation"] = tokens[{"payment_events": "events", "fraud_labels": "labels"}[table]]
        rules = prepare(table).rules
        type_queries = [r.sql.format(**tokens) for r in rules if r.id.startswith("schema_type__")]
        result.append(
            Case(
                table,
                "schema_types",
                "All persisted physical types match the raw contract.",
                marker(run)
                + "SELECT COUNT(*) FROM (SELECT 1) AS anchor WHERE "
                + " OR ".join("(" + query + ") <> 0" for query in type_queries),
            )
        )
        required = [
            r.sql.format(**tokens).split(" AS e WHERE ", 1)[1] for r in rules if r.id.startswith("schema_required__")
        ]
        result.append(
            Case(
                table,
                "schema_required",
                "Required fields are present when their enclosing values are present.",
                marker(run)
                + f"SELECT COUNT(*) FROM {tokens['relation']} AS e WHERE "
                + " OR ".join(f"({predicate})" for predicate in required),
            )
        )
        for rule in rules:
            if not rule.id.startswith("schema_"):
                result.append(Case(table, rule.id, rule.description, marker(run) + rule.sql.format(**tokens)))
        result.append(
            Case(
                table,
                CONSERVATION,
                "Persisted selected delivery counts equal the immutable input inventory.",
                marker(run) + conservation(table, manifest, tokens["relation"]),
            )
        )
    return result


def marker(run):
    return f"/* dp1 run {run['run_id']} */ "


def expected(run, contracts, case_list):
    return {
        "run_id": run["run_id"],
        "manifest_sha256": run["manifest_sha256"],
        "snapshots": run["snapshots"],
        "contracts": contracts,
        "cases": {c.fqn: {"table": c.table, "sql_sha256": sha256(c.sql.encode()).hexdigest()} for c in case_list},
    }


def parameters(case):
    return [
        {"name": "sqlExpression", "value": case.sql},
        {"name": "strategy", "value": "COUNT"},
        {"name": "operator", "value": "=="},
        {"name": "threshold", "value": "0"},
    ]


def connection():
    return {
        "type": "StarRocks",
        "username": "openmetadata",
        "password": "${STARROCKS_PASSWORD}",
        "hostPort": "kube-starrocks-fe-service.starrocks.svc.cluster.local:9030",
        "databaseName": CATALOG,
        "connectionArguments": {"init_command": f"SET CATALOG {CATALOG}"},
    }


def metadata_workflow():
    return {
        "source": {
            "type": "starrocks",
            "serviceName": SERVICE,
            "serviceConnection": {"config": connection()},
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "schemaFilterPattern": {"includes": [f"^{SCHEMA}$"]},
                    "markDeletedTables": False,
                }
            },
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "INFO", "openMetadataServerConfig": server()},
    }


def profiler_workflow():
    workflow = metadata_workflow()
    workflow["source"]["sourceConfig"]["config"] = {
        "type": "Profiler",
        "schemaFilterPattern": {"includes": [f"^{SCHEMA}$"]},
        "tableFilterPattern": {"includes": [f"^{physical_table(table)}$" for table in PORTS]},
        "computeColumnMetrics": True,
        "metrics": ["rowCount", "columnCount", "nullCount", "distinctCount", "min", "max"],
        "threadCount": 2,
        "timeoutSeconds": 600,
        "useStatistics": False,
    }
    workflow["processor"] = {"type": "orm-profiler", "config": {}}
    return workflow


def test_workflow(table, case_list):
    return {
        "source": {
            "type": "testsuite",
            "serviceName": SERVICE,
            "sourceConfig": {
                "config": {
                    "type": "TestSuite",
                    "entityFullyQualifiedName": entity(table),
                    "testCases": [c.name for c in case_list if c.table == table],
                }
            },
        },
        "processor": {
            "type": "orm-test-runner",
            "config": {
                "forceUpdate": True,
                "testCases": [
                    {
                        "name": c.name,
                        "description": c.description,
                        "testDefinitionName": "tableCustomSQLQuery",
                        "parameterValues": parameters(c),
                    }
                    for c in case_list
                    if c.table == table
                ],
            },
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "INFO", "openMetadataServerConfig": server()},
    }


def prepare_run(store, run, contracts, manifest):
    from data_platform.runs import evidence_key

    key = evidence_key(run, contracts, "expected")
    rendered = expected(run, contracts, cases(run, manifest))
    prepared = store.get(key)
    if prepared is None:
        prepared = {**rendered, "prepared_at": int(time.time() * 1000)}
        store.put(key, prepared, create=True)
    elif prepared["cases"] != rendered["cases"]:
        raise ValueError("prepared native rule set differs from the contracts for this run")
    return prepared
