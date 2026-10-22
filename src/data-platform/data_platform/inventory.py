import json
import os
import re
import time
from hashlib import sha256
from pathlib import Path

from data_platform.manifest import file_metadata, inventory_location, read_manifest, validate_inventory, verify_file
from data_platform.products import PORTS
from data_platform.storage import Store

MAX_FILES = 10000
MAX_BYTES = 1024**3
MAX_JSON_BYTES = 8 * 1024**2


def source_root(root):
    store = Store(root, "source")
    if store.bucket:
        if not store.prefix or any(part in ("", ".", "..") for part in store.prefix.split("/")):
            raise ValueError("source root must identify a canonical dataset prefix")
        return f"s3://{store.bucket}/{store.prefix}"
    return str(Path(store.prefix).resolve())


def companion_store(uri):
    if not uri or not uri.endswith(".json"):
        raise ValueError("an explicit companion JSON URI is required")
    parent, name = uri.rsplit("/", 1) if "/" in uri else (".", uri)
    return Store(parent), name


def check_deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError("source inventory deadline exceeded")


def source_listing(root, manifest, max_files=MAX_FILES, max_bytes=MAX_BYTES, deadline=None):
    store = Store(root, "source")
    deadline = deadline if deadline is not None else time.monotonic() + 900
    files = {}
    total_bytes = 0

    def add(key, size):
        nonlocal total_bytes
        check_deadline(deadline)
        if not key.endswith(".parquet"):
            return
        inventory_location(key, manifest)
        if key in files:
            raise ValueError("duplicate source file")
        files[key] = size
        total_bytes += size
        if len(files) > max_files or total_bytes > max_bytes:
            raise ValueError("source inventory exceeds file/byte bounds")

    if store.client:
        prefix = store.prefix.rstrip("/") + "/"
        options = {"Bucket": store.bucket, "Prefix": prefix, "MaxKeys": min(max_files + 2, 1000)}
        while True:
            check_deadline(deadline)
            page = store.client.list_objects_v2(**options)
            for item in page.get("Contents", []):
                if not item["Key"].startswith(prefix):
                    raise ValueError("source listing escaped dataset prefix")
                add(item["Key"][len(prefix) :], item["Size"])
            if not page.get("IsTruncated"):
                break
            token = page.get("NextContinuationToken")
            if not token or token == options.get("ContinuationToken"):
                raise ValueError("source listing did not advance")
            options["ContinuationToken"] = token
    else:
        base = Path(store.prefix).resolve()
        directories = 0
        for directory, names, filenames in os.walk(base, followlinks=False):
            check_deadline(deadline)
            directories += 1
            if directories > max_files * 4 + 1:
                raise ValueError("source inventory exceeds directory bound")
            for name in names + filenames:
                if (Path(directory) / name).is_symlink():
                    raise ValueError("source listing contains a symbolic link")
            for name in filenames:
                path = Path(directory) / name
                add(path.relative_to(base).as_posix(), path.stat().st_size)
    return dict(sorted(files.items()))


def inventory_totals(manifest):
    totals = {table: {"files": 0, "bytes": 0, "rows": 0} for table in PORTS}
    for entry in manifest["inventory"]:
        total = totals[entry["table"]]
        total["files"] += 1
        total["bytes"] += entry["bytes"]
        total["rows"] += entry["rows"]
    for table, total in totals.items():
        declared = manifest.get("storage", {}).get(table, {})
        if any(type(declared.get(key)) is not int or total[key] != declared[key] for key in ("files", "bytes")):
            raise ValueError(f"inventory differs from declared storage totals: {table}")
    return totals


def binding(root, manifest, digest):
    return {
        "inventory_format_version": 1,
        "source_root": source_root(root),
        "dataset_version": manifest["dataset_version"],
        "manifest_sha256": digest,
        "data_product": manifest["data_product"],
        "validation": manifest["validation"],
    }


def load_companion(root, manifest, digest, uri, pin):
    if not pin or not re.fullmatch(r"[a-f0-9]{64}", pin):
        raise ValueError("an explicit companion SHA256 pin is required")
    store, key = companion_store(uri)
    body = store.read(key, max_bytes=MAX_JSON_BYTES)
    if sha256(body).hexdigest() != pin:
        raise ValueError("companion SHA256 differs from pin")
    companion = json.loads(body)
    expected = binding(root, manifest, digest)
    if any(companion.get(key) != value for key, value in expected.items()):
        raise ValueError("companion binding differs from selected source manifest")
    selected = {**manifest, "inventory": companion.get("inventory")}
    validate_inventory(selected)
    if inventory_totals(selected) != companion.get("totals"):
        raise ValueError("companion totals differ from inventory")
    actual = source_listing(root, manifest)
    if actual != {entry["key"]: entry["bytes"] for entry in selected["inventory"]}:
        raise ValueError("source files differ from pinned companion inventory")
    return selected


def create_companion(root, uri, max_files=MAX_FILES, max_bytes=MAX_BYTES, timeout=900):
    if not 0 < max_files <= MAX_FILES or not 0 < max_bytes <= MAX_BYTES or not 0 < timeout <= 3600:
        raise ValueError("inventory bounds must be positive and within supported limits")
    destination, key = companion_store(uri)
    canonical = source_root(root)
    target = source_root(uri)
    if target == canonical or target.startswith(canonical + "/"):
        raise ValueError("companion must be stored outside the immutable source dataset")
    deadline = time.monotonic() + timeout
    manifest, digest = read_manifest(root)
    if "inventory" in manifest:
        raise ValueError("native manifest already contains an inventory")
    bindings = binding(root, manifest, digest)
    listing = source_listing(root, manifest, max_files, max_bytes, deadline)
    entries = []
    for path, size in listing.items():
        check_deadline(deadline)
        table, day = inventory_location(path, manifest)
        metadata = file_metadata(root, path, size, deadline)
        if metadata["bytes"] != size:
            raise ValueError("source size changed during inventory scan")
        entries.append({"key": path, "table": table, "partition": day, **metadata})
    selected = {**manifest, "inventory": entries}
    validate_inventory(selected)
    companion = {**bindings, "inventory": entries, "totals": inventory_totals(selected)}
    if read_manifest(root)[1] != digest or source_listing(root, manifest, max_files, max_bytes, deadline) != listing:
        raise ValueError("source changed during inventory scan")
    check_deadline(deadline)
    destination.put(key, companion, create=True)
    body = destination.read(key, max_bytes=MAX_JSON_BYTES)
    return {"uri": uri, "sha256": sha256(body).hexdigest(), "manifest_sha256": digest, "totals": companion["totals"]}


def verify_source(root, manifest, timeout=900):
    if not 0 < timeout <= 3600:
        raise ValueError("inventory verification deadline must be within 3600 seconds")
    deadline = time.monotonic() + timeout
    expected = {entry["key"]: entry["bytes"] for entry in manifest["inventory"]}
    if source_listing(root, manifest, deadline=deadline) != expected:
        raise ValueError("source files differ from selected inventory")
    for entry in manifest["inventory"]:
        check_deadline(deadline)
        verify_file(root, entry, deadline)
    if source_listing(root, manifest, deadline=deadline) != expected:
        raise ValueError("source changed during integrity verification")
    return inventory_totals(manifest)
