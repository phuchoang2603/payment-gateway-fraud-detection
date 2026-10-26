import json

import pytest

from data_platform import cli, governance, quality, readback
from data_platform.products import hashes
from data_platform.runs import evidence_key
from data_platform.storage import Store

RUN = {
    "run_id": "run-1",
    "dataset_version": "v11",
    "manifest_sha256": "a" * 64,
    "source_root": "s3://source/historical/v11",
    "workflow": "workflow-1",
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
        assert any("_source_dataset_version = 'v11'" in sql for sql in checks)
    required = cases[f"{quality.entity('payment_events')}.schema_required"].sql
    assert "ANY_MATCH" in required and " IS NOT NULL" in required
    assert not any("{" in case.sql or "}" in case.sql for case in cases.values())
    assert "('2026-07-01', 3)" in cases[f"{quality.entity('payment_events')}.{quality.CONSERVATION}"].sql


def test_preparation_is_stable_and_refuses_a_different_rule_set(tmp_path):
    store, contracts = Store(str(tmp_path)), hashes()
    first = cli.prepare(store, RUN, contracts, MANIFEST, tmp_path / "out")
    assert cli.prepare(store, RUN, contracts, MANIFEST, tmp_path / "again") == first
    assert json.loads((tmp_path / "out/payment_events.json").read_text())["processor"]["config"]["testCases"]
    changed = {"partitions": {**MANIFEST["partitions"], "fraud_labels": {"2026-07-02": 9}}}
    with pytest.raises(ValueError, match="differs from the contracts"):
        cli.prepare(store, RUN, contracts, changed, tmp_path / "changed")
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


def test_governance_requests_carry_original_contract_provenance():
    ids = {"payment_events": "id-events", "fraud_labels": "id-labels", "pipeline": "id-pipeline"}
    requests = governance.requests(RUN, ids, "s3://evidence/result.json")
    contracts = [r["body"] for r in requests.values() if r["path"] == "/v1/dataContracts"]
    assert {c["entity"]["id"] for c in contracts} == {"id-events", "id-labels"}
    descriptions = [
        patch["value"]
        for request in requests.values()
        for patch in request.get("provenance", [])
        if patch["op"] == "replace" and patch["path"] == "/description"
    ]
    assert len(descriptions) == 2
    assert all("```yaml" in description for description in descriptions)
    assert {f"sha256={digest}" for digest in hashes().values()} <= {
        part for description in descriptions for part in description.split() if part.startswith("sha256=")
    }
    assert requests["lineage"]["path"] == "/v1/lineage"
