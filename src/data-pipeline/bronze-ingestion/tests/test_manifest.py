import json
import shutil
from copy import deepcopy
from hashlib import sha256

import pipeline_common.manifest as source_manifest
import pytest
import yaml
from pipeline_common.manifest import selected_manifest, source_contracts, verify_file
from pipeline_common.products import PRODUCTS


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


def test_prior_source_contract_identity_remains_selectable(dataset, tmp_path):
    _, manifest = dataset
    manifest = deepcopy(manifest)
    product, paths = source_contracts("6.0.1")
    contract = paths["payment_events"].read_bytes()
    manifest["data_product"]["version"] = product["version"]
    del manifest["data_product"]["physical_schema_definitions"]
    manifest["data_product"]["contracts"]["payment-events"] = {
        "id": "urn:datacontract:fraud:historical-payment-events",
        "version": "4.0.1",
        "sha256": sha256(contract).hexdigest(),
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    selected, _ = selected_manifest(str(tmp_path))
    assert selected == manifest
    manifest["schema_versions"]["payment_events"] = [4]
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="unsupported source schemas"):
        selected_manifest(str(tmp_path))


def test_inventory_accepts_only_a_fully_declared_later_physical_version(tmp_path, monkeypatch):
    products = tmp_path / "data-products"
    shutil.copytree(PRODUCTS, products)
    source = products / "historical-payments"
    contract_path = source / "payment-events.odcs.yaml"
    contract = yaml.safe_load(contract_path.read_text())
    versions = next(item for item in contract["schema"][0]["customProperties"] if item["property"] == "schemaVersions")
    versions["value"].append(5)
    contract_path.write_text(yaml.safe_dump(contract))
    definitions_path = source / "physical-schemas-v1.yaml"
    definitions = yaml.safe_load(definitions_path.read_text())
    definitions["payment_events"][5] = definitions["payment_events"][4]
    definitions_path.write_text(yaml.safe_dump(definitions))
    monkeypatch.setattr(source_manifest, "PRODUCTS", products)
    source_manifest.supported_versions.cache_clear()
    key = "raw/payment_events/day-00000-kind-0-block-00000000/v5.parquet"
    try:
        assert source_manifest.inventory_location(key) == "payment_events"
        del definitions["payment_events"][5]
        definitions_path.write_text(yaml.safe_dump(definitions))
        source_manifest.supported_versions.cache_clear()
        with pytest.raises(ValueError, match="escapes the declared layout"):
            source_manifest.inventory_location(key)
    finally:
        source_manifest.supported_versions.cache_clear()


def test_deferred_source_gate_reaches_preflight_and_records_the_native_policy(dataset, tmp_path, monkeypatch):
    from pipeline_common.storage import Store

    from data_platform.runs import EXECUTION, open_run
    from data_platform.runtime import main

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
    monkeypatch.setenv("PIPELINE_STAGE", "preflight")
    monkeypatch.setenv("SOURCE_ROOT", str(source))
    monkeypatch.setenv("EVIDENCE_ROOT", str(evidence))
    monkeypatch.setenv("DATASET_VERSION", "smoke")
    monkeypatch.setenv("PIPELINE_OUTPUT", str(tmp_path / "out"))
    assert main() == 0
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


def test_physical_definitions_are_resolved_from_pinned_product(tmp_path, monkeypatch):
    old = tmp_path / "historical-payments/physical-old.yaml"
    new = tmp_path / "historical-payments/physical-new.yaml"
    old.parent.mkdir(parents=True)
    old.write_text("payment_events: {3: {}}\n")
    new.write_text("payment_events: {4: {}}\n")
    products = {
        "old": {
            "customProperties": [
                {"property": "physicalSchemaDefinitions", "value": "historical-payments/physical-old.yaml"}
            ]
        },
        "new": {
            "customProperties": [
                {"property": "physicalSchemaDefinitions", "value": "historical-payments/physical-new.yaml"}
            ]
        },
    }
    monkeypatch.setattr(source_manifest, "PRODUCTS", tmp_path)
    monkeypatch.setattr(source_manifest, "source_contracts", lambda version: (products[version], {}))
    assert set(source_manifest.physical_schemas("old")["payment_events"]) == {3}
    assert set(source_manifest.physical_schemas("new")["payment_events"]) == {4}
