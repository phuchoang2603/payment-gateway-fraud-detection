import os
import time
from pathlib import Path

from fraud_batch.common.manifest import inventory_location, verify_file
from fraud_batch.common.products import PORTS
from fraud_batch.common.storage import Store

MAX_FILES = 10000
MAX_BYTES = 1024**3


def check_deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError("source inventory deadline exceeded")


def source_listing(root, manifest, max_files=MAX_FILES, max_bytes=MAX_BYTES, deadline=None):
    store = Store(root)
    deadline = deadline if deadline is not None else time.monotonic() + 900
    files = {}
    total_bytes = 0

    def add(key, size):
        nonlocal total_bytes
        check_deadline(deadline)
        if not key.endswith(".parquet"):
            return
        if not key.startswith("raw/"):
            return
        inventory_location(key)
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


def verify_source(root, manifest, timeout=900):
    if not 0 < timeout <= 3600:
        raise ValueError("inventory verification deadline must be within 3600 seconds")
    deadline = time.monotonic() + timeout
    expected = {entry["key"]: entry["bytes"] for entry in manifest["inventory"]}
    limits = {"max_files": max(MAX_FILES, len(expected)), "max_bytes": max(MAX_BYTES, sum(expected.values()))}
    if source_listing(root, manifest, deadline=deadline, **limits) != expected:
        raise ValueError("source files differ from selected inventory")
    for entry in manifest["inventory"]:
        check_deadline(deadline)
        verify_file(root, entry, deadline)
    if source_listing(root, manifest, deadline=deadline, **limits) != expected:
        raise ValueError("source changed during integrity verification")
    return inventory_totals(manifest)
