import json
from copy import deepcopy

import pytest

from data_platform import bronze_metadata, governance, quality, readback
from data_platform.products import hashes
from data_platform.runs import evidence_key
from data_platform.storage import Store

RUN = {
    "run_id": "run-1",
    "dataset_version": "v16",
    "manifest_sha256": "a" * 64,
    "source_root": "s3://source/historical/v16",
    "workflow": "workflow-1",
    "ingested_at": "2026-10-26T00:00:00+00:00",
    "snapshots": {"payment_events": 111, "fraud_labels": 222},
}
MANIFEST = {"partitions": {"payment_events": {"2026-07-01": 3}, "fraud_labels": {"2026-07-02": 2}}}


def rendered():
    return {case.fqn: case for case in quality.cases(RUN, MANIFEST)}


def test_every_contract_rule_is_rendered_against_independently_pinned_snapshots():
    cases = rendered()
    names = {case.name for case in cases.values()}
    assert names == {"schema_types", "schema_required", quality.CONSERVATION}
    for table, snapshot in RUN["snapshots"].items():
        checks = [c.sql for c in cases.values() if c.table == table]
        assert any(f"FOR VERSION AS OF {snapshot}" in sql for sql in checks)
        assert any("_source_dataset_version = 'v16'" in sql for sql in checks)
    required = cases[f"{quality.entity('payment_events')}.schema_required"].sql
    assert "ANY_MATCH" in required and " IS NOT NULL" in required
    assert not any("{" in case.sql or "}" in case.sql for case in cases.values())
    assert "('2026-07-01', 3)" in cases[f"{quality.entity('payment_events')}.{quality.CONSERVATION}"].sql


def test_preparation_is_stable_and_refuses_a_different_rule_set(tmp_path):
    store, contracts = Store(str(tmp_path)), hashes()
    first = quality.prepare_run(store, RUN, contracts, MANIFEST)
    assert quality.prepare_run(store, RUN, contracts, MANIFEST) == first
    changed = {"partitions": {**MANIFEST["partitions"], "fraud_labels": {"2026-07-02": 9}}}
    with pytest.raises(ValueError, match="differs from the contracts"):
        quality.prepare_run(store, RUN, contracts, changed)
    assert store.get(evidence_key(RUN, contracts, "expected")) == first


def test_only_fresh_matching_success_results_pass():
    cases = rendered()
    expected = quality.expected(RUN, hashes(), list(cases.values()))
    good = {
        fqn: {
            "fullyQualifiedName": fqn,
            "parameterValues": [{"name": "sqlExpression", "value": case.sql}],
            "testCaseResult": {
                "timestamp": 20,
                "testCaseStatus": "Success",
                "testResultValue": [{"name": "resultRowCount", "value": "0"}],
            },
        }
        for fqn, case in cases.items()
    }
    assert readback.verify(expected, good, 10) == {}
    names = list(cases)
    bad = json.loads(json.dumps(good))
    del bad[names[0]]
    bad[names[1]]["testCaseResult"]["testCaseStatus"] = "Failed"
    bad[names[2]]["testCaseResult"]["testCaseStatus"] = "Aborted"
    bad[names[3]]["testCaseResult"]["timestamp"] = 5
    bad[names[4]]["parameterValues"][0]["value"] += " "
    bad[names[5]]["testCaseResult"]["testResultValue"][0]["value"] = "7"
    bad[names[2]]["testCaseResult"] = {
        "timestamp": 20,
        "testCaseStatus": "Aborted",
        "testResultValue": [{"name": "resultRowCount"}],
    }
    assert readback.verify(expected, bad, 10) == {
        names[0]: "missing",
        names[1]: "Failed",
        names[2]: "Aborted",
        names[3]: "stale result",
        names[4]: "stale definition",
        names[5]: "nonzero violations",
    }


def test_native_contract_mapping_requires_owners_tests_and_preserves_nested_schema(monkeypatch):
    owners = [{"id": "owner", "type": "team"}]
    tests = [{"id": "raw-check", "type": "testCase"}]
    contract = governance.contract_body("payment_events", "events", owners, tests)
    assert hashes()["payment_events"] in contract["description"] and "```yaml" in contract["description"]
    assert contract["qualityExpectations"] == tests and contract["owners"] == owners
    assert contract["security"]["dataClassification"] == "synthetic"
    nested = next(c for c in contract["schema"] if c["name"] == "attempt_started")
    assert nested["dataType"] == "STRUCT" and nested["children"]
    event_id = next(c for c in contract["schema"] if c["name"] == "event_id")
    assert event_id["constraint"] == "NOT_NULL"
    with pytest.raises(ValueError, match="ownership"):
        governance.contract_body("payment_events", "events", [], tests)
    with pytest.raises(ValueError, match="quality"):
        governance.contract_body("payment_events", "events", owners, [])
    monkeypatch.setattr(governance, "product_document", lambda: {"team": {"name": "another owner"}})
    with pytest.raises(ValueError, match="separately resolved"):
        governance.contract_body("payment_events", "events", owners, tests)


def publication_fixture():
    owners = [{"id": "owner", "type": "team"}]
    tables = {t: {"id": t, "type": "table"} for t in RUN["snapshots"]}
    manifest = {
        "inventory": [
            {"table": t, "key": f"raw/{t}/part.parquet", "rows": n, "bytes": n * 10}
            for t, n in (("payment_events", 3), ("fraud_labels", 2))
        ]
    }
    paths = governance.source_paths(RUN, manifest)
    sources = {t: {"id": path["name"], "type": "container", "path": path["fullPath"]} for t, path in paths.items()}
    contracts = {
        t: {
            "id": f"contract-{t}",
            "body": governance.contract_body(t, t, owners, [{"id": f"test-{t}", "type": "testCase"}]),
        }
        for t in tables
    }
    stage = {"outcome": "passed", "started_at": 10, "ended_at": 20}
    status = governance.execution(
        {**RUN, "stages": {"select": stage, "ingest": stage}},
        manifest,
        paths,
        {name: stage for name in governance.TASKS},
        "passed",
    )
    profile = {
        **stage,
        "coverage": {
            t: {"scope": "latest-state-bounded-query", "max_rows": 100, "columns": ["event_id"]} for t in tables
        },
    }
    published = {
        "owners": owners,
        "tables": tables,
        "sources": sources,
        "contracts": contracts,
        "pipeline": {"id": "pipeline", "type": "pipeline"},
        "status": status,
        "source_product": "historical-payments",
        "profile": profile,
        "profile_required": True,
        "url": "https://workflows.example/run",
    }
    domain = [{"id": "domain"}]
    membership = {"owners": owners, "domains": domain}
    entities = {"domains/name/fraud-detection": {"id": "domain"}}
    for name in (governance.PRODUCT, "historical-payments"):
        entities[f"dataProducts/name/{name}"] = {"id": name, **membership}
    for name, relation, refs in (
        (governance.PRODUCT, "assets", [*tables.values(), published["pipeline"]]),
        (governance.PRODUCT, "inputPorts", list(sources.values())),
        (governance.PRODUCT, "outputPorts", list(tables.values())),
        ("historical-payments", "assets", list(sources.values())),
        ("historical-payments", "outputPorts", list(sources.values())),
    ):
        entities[f"dataProducts/name/{name}/{relation}"] = {"data": refs}
    for table in tables:
        fqn = quality.entity(table)
        entities[f"tables/name/{fqn}"] = {**tables[table], **membership, "dataProducts": [{"id": governance.PRODUCT}]}
        entities[f"containers/{sources[table]['id']}"] = {
            "fullPath": sources[table]["path"],
            **membership,
            "dataProducts": [{"id": "historical-payments"}],
        }
        entities[f"lineage/table/name/{fqn}"] = {
            "upstreamEdges": [
                {
                    "fromEntity": sources[table]["id"],
                    "toEntity": table,
                    "lineageDetails": {"pipeline": published["pipeline"]},
                }
            ]
        }
        entities[f"tables/{fqn}/tableProfile/latest"] = {
            "profile": {"timestamp": 15},
            "columns": [{"name": "event_id", "profile": {"nullCount": 0, "distinctCount": 2}}],
        }
    entities[f"pipelines/name/{governance.PIPELINE}"] = {
        **published["pipeline"],
        **membership,
        "dataProducts": [{"id": governance.PRODUCT}],
        "tasks": [
            {"name": name, "downstreamTasks": children, "sourceUrl": published["url"]}
            for name, children in governance.TASKS.items()
        ],
        "pipelineStatus": status,
        "sourceUrl": published["url"],
    }

    def fetch(path, query=None):
        if path == "dataContracts/entity":
            value = contracts[query["entityId"]]
            return {
                "id": value["id"],
                **value["body"],
                "termsOfUse": {"content": value["body"]["termsOfUse"]},
            }
        return entities[path]

    return published, entities, fetch


def test_readback_rejects_incomplete_contracts_ports_lineage_execution_and_profiles():
    published, entities, fetch = publication_fixture()
    entities[f"pipelines/name/{governance.PIPELINE}"]["pipelineStatus"] = {
        **published["status"],
        "taskStatus": list(reversed(published["status"]["taskStatus"])),
    }
    assert readback.governance_gaps(published, hashes(), fetch) == []
    entities[f"dataProducts/name/{governance.PRODUCT}/inputPorts"]["data"] = []
    entities[f"tables/name/{quality.entity('payment_events')}"]["owners"] = []
    entities[f"lineage/table/name/{quality.entity('fraud_labels')}"]["upstreamEdges"] = [
        {"fromEntity": "payment_events", "toEntity": "fraud_labels"}
    ]
    entities[f"pipelines/name/{governance.PIPELINE}"]["pipelineStatus"] = {"executionId": "stale"}
    entities[f"tables/{quality.entity('fraud_labels')}/tableProfile/latest"]["profile"]["timestamp"] = 0

    def incomplete_contract(path, query=None):
        value = fetch(path, query)
        return {**value, "schema": []} if path == "dataContracts/entity" else value

    assert set(readback.governance_gaps(published, hashes(), incomplete_contract)) == {
        "product:bronze-payments:inputPorts",
        "table:payment_events:membership",
        "contract:payment_events",
        "contract:fraud_labels",
        "lineage:fraud_labels:fabricated-table-dependency",
        "lineage:fraud_labels:source-pipeline-output",
        "pipeline:execution",
        "profile:fraud_labels:statistics",
    }


def test_pipeline_records_independent_source_ports_and_preserves_run_identity(monkeypatch):
    monkeypatch.setenv("WORKFLOW_ID", "metadata-recovery")
    published, _, _ = publication_fixture()
    status = published["status"]
    assert status["executionId"] == RUN["run_id"]
    assert status["parameters"]["workflow"] == RUN["workflow"]
    assert status["parameters"]["publication_workflow"] == "metadata-recovery"
    assert {s["version"] for s in status["outputs"]} == {"111", "222"}
    assert [s["rowCount"] for s in status["inputs"]] == [3, 2]
    landing = {**RUN, "input_mode": "landing", "source_root": "s3://source"}
    manifest = {
        "inventory": [
            {"table": "payment_events", "key": "streaming/payment_events/event.parquet", "rows": 4, "bytes": 40}
        ]
    }
    assert set(governance.source_paths(landing, manifest)) == {"payment_events"}
    assert (
        governance.execution(landing, manifest, governance.source_paths(landing, manifest), {}, "failed")["outputs"][
            1
        ]["rowCount"]
        == 0
    )


def test_daily_ingestion_can_skip_profiles_without_claiming_statistics():
    published, entities, fetch = publication_fixture()
    published.update(profile_required=False, profile={"outcome": "skipped", "reason": "recurring-disabled"})
    for table in published["tables"]:
        del entities[f"tables/{quality.entity(table)}/tableProfile/latest"]
    assert readback.governance_gaps(published, hashes(), fetch) == []
    published["profile_required"] = True
    assert "profile:coverage" in readback.governance_gaps(published, hashes(), fetch)


def test_replay_requires_matching_native_receipts():
    good = {
        "run_id": RUN["run_id"],
        "manifest_sha256": RUN["manifest_sha256"],
        "snapshots": RUN["snapshots"],
        "outcome": "passed",
    }
    assert bronze_metadata.stage_matches(good, RUN)
    stale = deepcopy(good)
    stale["snapshots"]["payment_events"] = 999
    assert not bronze_metadata.stage_matches(stale, RUN)


def test_query_cleanup_only_cancels_owned_work_and_waits_for_remaining_connections():
    class Cursor:
        statements = []
        rows = [
            {"Id": 1, "User": "metadata", "Command": "Query", "Info": "SHOW FULL PROCESSLIST"},
            {"Id": 2, "User": "metadata", "Command": "Query", "Info": "/* dp1 run current */ SELECT 1"},
            {"Id": 3, "User": "metadata", "Command": "Query", "Info": "SELECT unrelated"},
            {"Id": 4, "User": "another", "Command": "Query", "Info": "/* dp1 run other */ SELECT 1"},
            {"Id": 5, "User": "metadata", "Command": "Sleep", "Info": None},
        ]

        def execute(self, statement):
            self.statements.append(statement)

        def fetchall(self):
            return self.rows

    cursor = Cursor()
    assert bronze_metadata.cancel_owned_queries(cursor, "metadata", 1)
    assert cursor.statements == ["SHOW FULL PROCESSLIST", "KILL QUERY 2"]
    cursor.rows = [cursor.rows[2]]
    assert bronze_metadata.cancel_owned_queries(cursor, "metadata", 1)
    cursor.rows = []
    assert not bronze_metadata.cancel_owned_queries(cursor, "metadata", 1)
