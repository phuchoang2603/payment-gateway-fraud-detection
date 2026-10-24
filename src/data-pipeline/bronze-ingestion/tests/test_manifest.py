import json
import shutil
from copy import deepcopy

import pytest

from data_platform.manifest import selected_manifest, verify_file


def test_completed_inventory_and_integrity(dataset):
    root, expected = dataset
    actual, digest = selected_manifest(str(root))
    assert actual == expected and len(digest) == 64
    for entry in actual["inventory"]:
        assert verify_file(str(root), entry) == entry["key"]
    for field, value in [("sha256", "0" * 64), ("bytes", 1), ("rows", 999999)]:
        broken = {**actual["inventory"][0], field: value}
        with pytest.raises(ValueError):
            verify_file(str(root), broken)


def test_deferred_source_gate_reaches_preflight_but_requires_validated_snapshots(dataset, tmp_path, monkeypatch):
    from data_platform.cli import main
    from data_platform.runs import accept, open_run, report_key
    from data_platform.storage import Store

    root, manifest = dataset
    source = tmp_path / "source"
    shutil.copytree(root, source)
    manifest = deepcopy(manifest)
    manifest["validation"] = {"result": "skipped", "gate": "dp1-spark-gx", "reason": "validation deferred to DP1"}
    path = source / "manifest.json"
    path.write_text(json.dumps(manifest))
    selected, _ = selected_manifest(str(source))
    assert selected["validation"] == manifest["validation"]
    evidence = tmp_path / "evidence"
    monkeypatch.setenv("IMAGE_DIGEST", "image@sha256:" + "a" * 64)
    assert main(["preflight", "--source", str(source), "--evidence", str(evidence), "--version", "smoke"]) == 0
    store = Store(str(evidence))
    run = store.get("versions/smoke/run.json")
    assert run["source_validation"] == manifest["validation"]
    with pytest.raises(ValueError, match="different source validation gate"):
        open_run(store, "smoke", run["manifest_sha256"], run["ingestion_contracts"], "image", "retry")
    run["snapshots"] = {"payment_events": 1, "fraud_labels": 2}
    with pytest.raises(ValueError, match="persisted passing"):
        accept(store, run, run["ingestion_contracts"])
    report = {
        "data_outcome": "passed",
        "snapshots": run["snapshots"],
        "manifest_sha256": run["manifest_sha256"],
        "source_validation": run["source_validation"],
    }
    store.put(report_key(run, run["ingestion_contracts"]), report)
    with pytest.raises(ValueError, match="both passing Spark/GX"):
        accept(store, run, run["ingestion_contracts"])
    report["tables"] = {name: {"success": True} for name in run["snapshots"]}
    report["tables"]["fraud_labels"]["success"] = False
    store.put(report_key(run, run["ingestion_contracts"]), report)
    with pytest.raises(ValueError, match="both passing Spark/GX"):
        accept(store, run, run["ingestion_contracts"])
    report["tables"]["fraud_labels"]["success"] = True
    report.pop("source_validation")
    store.put(report_key(run, run["ingestion_contracts"]), report)
    with pytest.raises(ValueError, match="source gate differs"):
        accept(store, run, run["ingestion_contracts"])
    report["source_validation"] = run["source_validation"]
    store.put(report_key(run, run["ingestion_contracts"]), report)
    store.put(
        report_key(run, run["ingestion_contracts"]).replace("validation.json", "governance.json"),
        {
            "outcome": "passed",
            "readback_verified": True,
        },
    )
    assert accept(store, run, run["ingestion_contracts"])["source_validation"] == manifest["validation"]
    for declaration in (
        {"result": "skipped"},
        {**manifest["validation"], "gate": "unknown"},
        {**manifest["validation"], "reason": ""},
        {**manifest["validation"], "payment-events": "passed"},
    ):
        path.write_text(json.dumps({**manifest, "validation": declaration}))
        with pytest.raises(ValueError, match="source contract validation"):
            selected_manifest(str(source))


@pytest.mark.parametrize(
    "mutation", ["contract", "schema", "count", "escape", "table", "partition", "validation", "missing"]
)
def test_invalid_source_selection_is_rejected(dataset, tmp_path, mutation):
    _, source = dataset
    manifest = deepcopy(source)
    if mutation == "missing":
        with pytest.raises(FileNotFoundError):
            selected_manifest(str(tmp_path))
        return
    if mutation == "contract":
        manifest["data_product"]["contracts"]["payment-events"]["sha256"] = "0" * 64
    elif mutation == "schema":
        manifest["schema_versions"]["payment_events"] = [99]
    elif mutation == "table":
        manifest["inventory"][0]["table"] = "unknown"
    elif mutation == "partition":
        manifest["inventory"][0]["partition"] = "1900-01-01"
    elif mutation == "validation":
        manifest["validation"] = {"payment_events": "passed", "fraud_labels": "passed"}
    elif mutation == "count":
        manifest["inventory"][0]["rows"] += 1
    else:
        manifest["inventory"][0]["key"] = "../escape.parquet"
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        selected_manifest(str(tmp_path))
