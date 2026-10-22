import json
import shutil
from copy import deepcopy
from hashlib import sha256

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from data_platform.cli import main
from data_platform.inventory import create_companion, verify_source
from data_platform.manifest import selected_manifest
from data_platform.products import PORTS, PRODUCTS, contract_path
from data_platform.runs import open_run
from data_platform.storage import Store


@pytest.fixture
def legacy_source(tmp_path):
    root = tmp_path / "source"
    product = yaml.safe_load((PRODUCTS / "historical-payments/product.odps.yaml").read_text())
    manifest = {
        "dataset_version": "v7",
        "data_product": {"id": product["id"], "version": product["version"], "contracts": {}},
        "validation": {port: "passed" for port in PORTS.values()},
        "schema_versions": {"payment_events": [1, 2], "fraud_labels": [1]},
        "partitions": {},
        "storage": {},
    }
    for table, port in PORTS.items():
        contract_bytes = contract_path(table, "historical-payments").read_bytes()
        contract = yaml.safe_load(contract_bytes)
        manifest["data_product"]["contracts"][port] = {
            "id": contract["id"],
            "version": contract["version"],
            "sha256": sha256(contract_bytes).hexdigest(),
        }
        partition = "occurred_date" if table == "payment_events" else "label_available_date"
        file = root / table / f"{partition}=2026-07-01" / "part-0.parquet"
        file.parent.mkdir(parents=True)
        pq.write_table(pa.table({"id": [1, 2, 2]}), file)
        manifest["partitions"][table] = {"2026-07-01": 3}
        manifest["storage"][table] = {"files": 1, "bytes": file.stat().st_size}
    (root / "manifest.json").write_text(json.dumps(manifest))
    return root


def write_companion(path, companion):
    body = json.dumps(companion, sort_keys=True, indent=2).encode()
    path.write_bytes(body)
    return sha256(body).hexdigest()


def test_create_select_verify_and_preflight_pin(legacy_source, tmp_path, monkeypatch):
    root = legacy_source
    (root / "validation").mkdir()
    (root / "validation" / "payment-events.json").write_text("{}")
    before = {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    target = tmp_path / "evidence" / "source-inventories" / "v7.json"
    result = create_companion(str(root), str(target))
    manifest, digest = selected_manifest(str(root), str(target), result["sha256"])
    assert digest == result["manifest_sha256"]
    assert all(entry["key"].endswith(".parquet") for entry in manifest["inventory"])
    assert verify_source(str(root), manifest) == result["totals"]
    assert {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()} == before
    with pytest.raises(FileExistsError):
        create_companion(str(root), str(target))
    with pytest.raises(ValueError, match="lacks a file inventory"):
        selected_manifest(str(root))
    monkeypatch.setenv("IMAGE_DIGEST", "image@sha256:" + "a" * 64)
    arguments = [
        "preflight",
        "--source",
        str(root),
        "--evidence",
        str(tmp_path / "runs"),
        "--inventory-uri",
        str(target),
        "--inventory-sha256",
        result["sha256"],
    ]
    assert main(arguments) == 0
    store = Store(str(tmp_path / "runs"))
    run = store.get("versions/v7/run.json")
    assert run["source_inventory"] == {"uri": str(target), "sha256": result["sha256"]}
    assert run["manifest_sha256"] == digest
    with pytest.raises(ValueError, match="different source inventory pin"):
        open_run(store, "v7", digest, {}, "image", "retry", source_inventory={"uri": str(target), "sha256": "0" * 64})


def test_companion_rejects_invalid_evidence_and_cross_version_reuse(legacy_source, tmp_path):
    target = tmp_path / "inventory.json"
    result = create_companion(str(legacy_source), str(target))
    companion = json.loads(target.read_bytes())
    for uri, pin in [
        (str(target), None),
        (None, result["sha256"]),
        (str(target), "0" * 64),
        (str(tmp_path / "absent.json"), result["sha256"]),
    ]:
        with pytest.raises((ValueError, FileNotFoundError)):
            selected_manifest(str(legacy_source), uri, pin)
    original = target.read_bytes()
    target.write_bytes(original + b" ")
    with pytest.raises(ValueError, match="differs from pin"):
        selected_manifest(str(legacy_source), str(target), result["sha256"])
    target.write_bytes(original)
    for field in [
        "source_root",
        "dataset_version",
        "manifest_sha256",
        "data_product",
        "validation",
        "inventory_format_version",
    ]:
        changed = {**companion, field: "different"}
        path = tmp_path / f"{field}.json"
        pin = write_companion(path, changed)
        with pytest.raises(ValueError, match="binding differs"):
            selected_manifest(str(legacy_source), str(path), pin)
    path = legacy_source / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["dataset_version"] = "v8"
    path.write_text(json.dumps(manifest))
    newer = tmp_path / "v8.json"
    newer_result = create_companion(str(legacy_source), str(newer))
    selected, digest = selected_manifest(str(legacy_source), str(newer), newer_result["sha256"])
    assert selected["dataset_version"] == "v8"
    assert digest == newer_result["manifest_sha256"]
    with pytest.raises(ValueError, match="binding differs"):
        selected_manifest(str(legacy_source), str(target), result["sha256"])
    manifest["inventory"] = companion["inventory"]
    path.write_text(json.dumps(manifest))
    assert selected_manifest(str(legacy_source))[0] == manifest
    with pytest.raises(ValueError, match="cannot be replaced"):
        selected_manifest(str(legacy_source), str(target), result["sha256"])


def test_companion_rejects_file_set_mutations(legacy_source, tmp_path):
    create_companion(str(legacy_source), str(tmp_path / "base.json"))
    companion = json.loads((tmp_path / "base.json").read_bytes())
    entry = companion["inventory"][0]
    for mutation in ["missing", "extra", "duplicate", "escape", "rows", "files", "bytes", "hash", "symlink"]:
        case = tmp_path / mutation
        root = case / "source"
        shutil.copytree(legacy_source, root)
        target = case / "inventory.json"
        pin = create_companion(str(root), str(target))["sha256"]
        changed = deepcopy(companion)
        file = root / entry["key"]
        if mutation == "missing":
            file.unlink()
        elif mutation == "extra":
            file.with_name("extra.parquet").write_bytes(file.read_bytes())
        elif mutation == "duplicate":
            changed["inventory"].append(deepcopy(entry))
        elif mutation == "escape":
            changed["inventory"][0]["key"] = "../outside.parquet"
        elif mutation == "rows":
            changed["inventory"][0]["rows"] += 1
        elif mutation in ("files", "bytes"):
            changed["totals"][entry["table"]][mutation] += 1
        elif mutation == "hash":
            content = bytearray(file.read_bytes())
            content[10] ^= 1
            file.write_bytes(content)
        elif mutation == "symlink":
            file.with_name("link.parquet").symlink_to(file)
        if mutation in ("missing", "extra", "symlink"):
            scan = case / "scan.json"
            with pytest.raises(ValueError):
                create_companion(str(root), str(scan))
            assert not scan.exists()
        if mutation not in ("missing", "extra", "symlink", "hash"):
            pin = write_companion(target, changed)
        with pytest.raises(ValueError):
            manifest, _ = selected_manifest(str(root), str(target), pin)
            verify_source(str(root), manifest)
    with pytest.raises(ValueError, match="outside"):
        create_companion(str(legacy_source), str(legacy_source / "inventory.json"))
    for bounds in [{"max_files": 1}, {"max_bytes": 1}, {"timeout": 0}]:
        target = tmp_path / "bounded.json"
        with pytest.raises(ValueError, match="bound"):
            create_companion(str(legacy_source), str(target), **bounds)
        assert not target.exists()
