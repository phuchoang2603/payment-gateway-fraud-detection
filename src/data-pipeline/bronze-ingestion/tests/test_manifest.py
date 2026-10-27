import json
import shutil
from copy import deepcopy

import pytest

from data_platform.manifest import selected_manifest, verify_file


def test_completed_inventory_and_integrity(dataset, tmp_path):
    root, expected = dataset
    actual, digest = selected_manifest(str(root))
    assert actual == expected and len(digest) == 64
    for entry in actual["inventory"]:
        assert verify_file(str(root), entry) == entry["key"]
    for field, value in [("sha256", "0" * 64), ("bytes", 1), ("rows", 999999)]:
        broken = {**actual["inventory"][0], field: value}
        with pytest.raises(ValueError):
            verify_file(str(root), broken)
    historical = deepcopy(expected)
    historical["feature_definition"] = {
        "version": "fraud-baseline-v1",
        "sha256": "2ee2a5153a248bdc98df3b2643e10a65bc5693ea111129b6c65a67352aa4ec0b",
    }
    historical["data_product"]["feature_definition"] = historical["feature_definition"]
    (tmp_path / "manifest.json").write_text(json.dumps(historical))
    selected, historical_digest = selected_manifest(str(tmp_path))
    assert selected == historical and historical_digest != digest


def test_deferred_source_gate_reaches_preflight_and_records_the_native_policy(dataset, tmp_path, monkeypatch):
    from data_platform.cli import main
    from data_platform.runs import EXECUTION, open_run
    from data_platform.storage import Store

    root, manifest = dataset
    source = tmp_path / "source"
    shutil.copytree(root, source)
    manifest = deepcopy(manifest)
    manifest["validation"] = {
        "result": "skipped",
        "gate": "dp1-starrocks-openmetadata",
        "reason": "validation deferred to DP1",
    }
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
    assert run["validation_execution"] == EXECUTION
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
    "mutation", ["contract", "schema", "count", "escape", "table", "partition", "validation", "feature", "missing"]
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
    elif mutation == "feature":
        manifest["data_product"]["feature_definition"]["sha256"] = "0" * 64
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


def test_manifest_without_embedded_inventory_is_rejected(dataset, tmp_path):
    _, source = dataset
    manifest = deepcopy(source)
    del manifest["inventory"]
    manifest["validation"] = {"payment-events": "passed", "fraud-labels": "passed"}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="lacks a file inventory"):
        selected_manifest(str(tmp_path))


@pytest.mark.parametrize("mutation", ["missing", "extra", "hash", "symlink"])
def test_verify_source_rejects_file_set_and_content_changes(dataset, tmp_path, mutation):
    from data_platform.inventory import verify_source

    root, manifest = dataset
    source = tmp_path / "source"
    shutil.copytree(root, source)
    assert verify_source(str(source), manifest)
    file = source / manifest["inventory"][0]["key"]
    if mutation == "missing":
        file.unlink()
    elif mutation == "extra":
        file.with_name("extra.parquet").write_bytes(file.read_bytes())
    elif mutation == "symlink":
        file.with_name("link.parquet").symlink_to(file)
    else:
        content = bytearray(file.read_bytes())
        content[10] ^= 1
        file.write_bytes(content)
    with pytest.raises(ValueError):
        verify_source(str(source), manifest)
