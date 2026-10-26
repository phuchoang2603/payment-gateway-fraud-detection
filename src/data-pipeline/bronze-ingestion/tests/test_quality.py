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
    linkage = cases[f"{quality.entity('fraud_labels')}.label_attempt_linkage"].sql
    assert "FOR VERSION AS OF 222" in linkage and "FOR VERSION AS OF 111" in linkage
    assert "_source_dataset_version = 'v11'" in linkage
    names = {case.name for case in cases.values()}
    assert {"payment_lifecycle", "label_availability", quality.CONSERVATION} <= names
    assert any(name.startswith("schema_type__payment_created") for name in names)
    assert any("_items__" in name for name in names if name.startswith("schema_required__"))
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
    bad[names[6]]["testCaseResult"] = {
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
        names[6]: "Aborted",
    }


def test_governance_requests_carry_original_contract_provenance():
    ids = {"payment_events": "id-events", "fraud_labels": "id-labels", "pipeline": "id-pipeline"}
    requests = governance.requests(RUN, ids, "s3://evidence/result.json")
    contracts = [r["body"] for r in requests.values() if r["path"] == "/v1/dataContracts"]
    assert {c["entity"]["id"] for c in contracts} == {"id-events", "id-labels"}
    for contract in contracts:
        assert "```yaml" in contract["description"]
    for request in requests.values():
        if request["path"] == "/v1/dataContracts":
            assert request["provenance"] == [
                {"op": "replace", "path": "/description", "value": request["body"]["description"]}
            ]
    assert {f"sha256={digest}" for digest in hashes().values()} <= {
        part for c in contracts for part in c["description"].split() if part.startswith("sha256=")
    }
    assert requests["lineage"]["path"] == "/v1/lineage"
