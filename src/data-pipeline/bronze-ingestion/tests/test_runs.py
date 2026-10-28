import pytest
from pipeline_common.storage import Store

from data_platform.runs import EXECUTION, accept, evidence_key, open_run, save_run

CONTRACTS = {"payment_events": "one", "fraud_labels": "two"}


def prepared(store, run):
    run["conservation"] = {
        "outcome": "passed",
        "snapshots": dict(run["snapshots"]),
        "manifest_sha256": run["manifest_sha256"],
    }
    store.put(
        evidence_key(run, CONTRACTS, "expected"),
        {"run_id": run["run_id"], "snapshots": run["snapshots"], "cases": {"a": {}, "b": {}}},
    )


def test_stable_context_and_manifest_guard(tmp_path):
    store = Store(str(tmp_path))
    run = open_run(store, "v10", "digest", CONTRACTS, "image", "workflow")
    assert open_run(store, "v10", "digest", CONTRACTS, "other-image", "retry") == run
    with pytest.raises(ValueError, match="different manifest"):
        open_run(store, "v10", "changed", CONTRACTS, "image", "retry")
    gate = {"result": "skipped", "gate": "dp1-starrocks-openmetadata"}
    declared = open_run(store, "v12", "digest", CONTRACTS, "image", "workflow", source_validation=gate)
    assert declared["source_validation"] == gate and declared["validation_execution"] == EXECUTION


def test_acceptance_requires_a_complete_passing_native_result_set(tmp_path):
    store = Store(str(tmp_path))
    run = open_run(store, "v10", "digest", CONTRACTS, "image", "workflow")
    with pytest.raises(ValueError, match="both committed"):
        accept(store, run, CONTRACTS, {}, [])
    run["snapshots"] = {"payment_events": 1, "fraud_labels": 2}
    save_run(store, run)
    with pytest.raises(ValueError, match="prepared native rule set"):
        accept(store, run, CONTRACTS, {}, [])
    prepared(store, run)
    with pytest.raises(ValueError, match="not a complete passing set"):
        accept(store, run, CONTRACTS, {"a": "Failed"}, [])
    assert store.get("accepted/v10.json") is None
    assert store.get(evidence_key(run, CONTRACTS, "result"))["problems"] == {"a": "Failed"}
    with pytest.raises(ValueError, match="not a complete passing set"):
        accept(store, run, CONTRACTS, {}, ["lineage:payment_events->fraud_labels"])
    conservation = run.pop("conservation")
    with pytest.raises(ValueError, match="content conservation"):
        accept(store, run, CONTRACTS, {}, [])
    run["conservation"] = conservation
    record = accept(store, run, CONTRACTS, {}, [])
    assert store.get("accepted/v10.json") == record and record["result"]["outcome"] == "passed"


def test_changed_contracts_need_separate_evidence(tmp_path):
    store = Store(str(tmp_path))
    run = open_run(store, "smoke", "digest", CONTRACTS, "image", "workflow")
    run["snapshots"] = {"payment_events": 1, "fraud_labels": 2}
    prepared(store, run)
    revised = {**CONTRACTS, "payment_events": "revised"}
    assert evidence_key(run, CONTRACTS, "expected") != evidence_key(run, revised, "expected")
    with pytest.raises(ValueError, match="prepared native rule set"):
        accept(store, run, revised, {}, [])
