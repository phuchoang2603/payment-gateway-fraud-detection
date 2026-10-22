import json
import re
import time
from collections import Counter
from hashlib import sha256
from pathlib import PurePosixPath

import certifi
import pyarrow.fs as fs
import pyarrow.parquet as pq
import yaml

from data_platform.products import PORTS, PRODUCTS, contract_path
from data_platform.storage import Store


def read_manifest(root):
    body = Store(root, "source").read("manifest.json", max_bytes=1024 * 1024)
    manifest = json.loads(body)
    version = manifest.get("dataset_version", "")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", version):
        raise ValueError("invalid dataset version")
    product = yaml.safe_load((PRODUCTS / "historical-payments/product.odps.yaml").read_text())
    declared = manifest.get("data_product", {})
    if (declared.get("id"), declared.get("version")) != (product["id"], product["version"]):
        raise ValueError("unsupported source product identity")
    validation = manifest.get("validation", {})
    deferred = (
        validation.get("result") == "skipped"
        and validation.get("gate") == "dp1-spark-gx"
        and isinstance(validation.get("reason"), str)
        and bool(validation["reason"].strip())
        and isinstance(manifest.get("inventory"), list)
        and not any(port in validation for port in PORTS.values())
    )
    if "result" in validation and not deferred:
        raise ValueError("source contract validation has no supported downstream gate")
    for table, port in PORTS.items():
        contract_bytes = contract_path(table, "historical-payments").read_bytes()
        contract = yaml.safe_load(contract_bytes)
        if declared.get("contracts", {}).get(port) != {
            "id": contract["id"],
            "version": contract["version"],
            "sha256": sha256(contract_bytes).hexdigest(),
        }:
            raise ValueError(f"unsupported source contract identity: {port}")
        if validation.get(port) != "passed" and not deferred:
            raise ValueError(f"source contract validation is incomplete: {port}")
        supported = [1, 2] if table == "payment_events" else [1]
        versions = manifest.get("schema_versions", {}).get(table, [])
        if not versions or any(v not in supported for v in versions):
            raise ValueError(f"unsupported source schemas: {table}")
    return manifest, sha256(body).hexdigest()


def selected_manifest(root, inventory_uri=None, inventory_sha256=None):
    manifest, digest = read_manifest(root)
    if inventory_uri or inventory_sha256:
        from data_platform.inventory import load_companion

        if "inventory" in manifest:
            raise ValueError("native manifest inventory cannot be replaced by a companion")
        manifest = load_companion(root, manifest, digest, inventory_uri, inventory_sha256)
    validate_inventory(manifest)
    return manifest, digest


def validate_inventory(manifest):
    files = manifest.get("inventory")
    if not isinstance(files, list):
        raise ValueError("completed manifest lacks a file inventory with SHA-256, byte sizes and row counts")
    paths = set()
    totals = {table: 0 for table in PORTS}
    partitions = {table: Counter() for table in PORTS}
    for entry in files:
        table, day = inventory_location(entry["key"], manifest)
        path = PurePosixPath(entry["key"])
        if str(path) in paths:
            raise ValueError("duplicate inventory path")
        paths.add(str(path))
        if not re.fullmatch("[0-9a-f]{64}", entry.get("sha256", "")):
            raise ValueError("file inventory requires SHA-256")
        if (
            type(entry.get("bytes")) is not int
            or entry["bytes"] <= 0
            or type(entry.get("rows")) is not int
            or entry["rows"] < 0
        ):
            raise ValueError("invalid inventory size/count")
        if entry.get("table") != table:
            raise ValueError("inventory table identity differs from its key")
        if entry.get("partition") != day:
            raise ValueError("inventory partition differs from its key")
        partitions[table][day] += entry["rows"]
        totals[table] += entry["rows"]
    for table in PORTS:
        if dict(partitions[table]) != manifest["partitions"][table] or totals[table] != sum(
            manifest["partitions"][table].values()
        ):
            raise ValueError(f"inventory/manifest row counts differ: {table}")


def inventory_location(key, manifest):
    path = PurePosixPath(key)
    if (
        path.is_absolute()
        or ".." in path.parts
        or not path.parts
        or str(path) != key
        or path.parts[0] not in PORTS
        or path.suffix != ".parquet"
    ):
        raise ValueError("file inventory path escapes the selected dataset")
    table = path.parts[0]
    partition_name = "occurred_date" if table == "payment_events" else "label_available_date"
    if len(path.parts) != 3 or not path.parts[1].startswith(partition_name + "="):
        raise ValueError("file inventory must identify a declared date partition")
    day = path.parts[1].split("=", 1)[1]
    if day not in manifest["partitions"][table]:
        raise ValueError("file inventory references an undeclared partition")
    return table, day


def source_filesystem(root):
    if root.startswith("s3://"):
        import os

        return fs.S3FileSystem(
            access_key=os.environ["SOURCE_ACCESS_KEY_ID"],
            secret_key=os.environ["SOURCE_SECRET_ACCESS_KEY"],
            endpoint_override=os.environ.get("S3_ENDPOINT", "https://minio.home.phuchoang.sbs"),
            region=os.environ.get("AWS_REGION", "us-east-1"),
            force_virtual_addressing=False,
            tls_ca_file_path=certifi.where(),
        ), root[5:]
    return fs.LocalFileSystem(), root.removeprefix("file://")


def file_metadata(root, key, max_bytes, deadline=None):
    filesystem, prefix = source_filesystem(root)
    path = prefix.rstrip("/") + "/" + key
    digest = sha256()
    size = 0
    with filesystem.open_input_stream(path) as stream:
        while chunk := stream.read(1024 * 1024):
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("source inventory deadline exceeded")
            size += len(chunk)
            if size > max_bytes:
                raise ValueError(f"source integrity mismatch: {key} exceeds byte bound")
            digest.update(chunk)
    with filesystem.open_input_file(path) as stream:
        metadata = pq.ParquetFile(stream).metadata
        return {
            "sha256": digest.hexdigest(),
            "bytes": size,
            "rows": metadata.num_rows,
            "row_groups": metadata.num_row_groups,
        }


def verify_file(root, entry, deadline=None):
    actual = file_metadata(root, entry["key"], entry["bytes"], deadline)
    if actual["bytes"] != entry["bytes"] or actual["sha256"] != entry["sha256"]:
        raise ValueError(f"source integrity mismatch: {entry['key']}")
    if actual["rows"] != entry["rows"]:
        raise ValueError(f"source row count mismatch: {entry['key']}")
    return entry["key"]


def verify_files(spark, root, manifest):
    entries = manifest["inventory"]
    if entries:
        spark.sparkContext.parallelize(entries, min(len(entries), 8)).map(
            lambda entry: verify_file(root, entry)
        ).count()
