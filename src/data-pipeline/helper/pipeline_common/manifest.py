import json
import re
import time
from collections import Counter
from datetime import date
from functools import lru_cache
from hashlib import sha256
from pathlib import PurePosixPath

import certifi
import pyarrow.fs as fs
import pyarrow.parquet as pq
import yaml

from pipeline_common.products import PORTS, PRODUCTS
from pipeline_common.storage import Store


def source_contracts(product_version):
    source = PRODUCTS / "historical-payments"
    products = [source / "product.odps.yaml", *sorted((source / "versions").glob("product-*.odps.yaml"))]
    for product_path in products:
        product = yaml.safe_load(product_path.read_text())
        if product["version"] == product_version:
            ports = {port["name"]: port for port in product["outputPorts"]}
            paths = {}
            for table, port in PORTS.items():
                candidates = [source / f"{port}.odcs.yaml", *sorted((source / "versions").glob(f"{port}-*.odcs.yaml"))]
                matches = [
                    path
                    for path in candidates
                    if (document := yaml.safe_load(path.read_text()))["id"] == ports[port]["contractId"]
                    and document["version"] == ports[port]["version"]
                ]
                if len(matches) != 1:
                    raise ValueError(f"ambiguous or unsupported source contract: {port}")
                paths[table] = matches[0]
            return product, paths
    raise ValueError("unsupported source product identity")


def declared_versions(contract):
    return next(
        item["value"]
        for item in contract["schema"][0].get("customProperties", [])
        if item["property"] == "schemaVersions"
    )


def physical_schemas():
    product = yaml.safe_load((PRODUCTS / "historical-payments/product.odps.yaml").read_text())
    path = next(
        item["value"] for item in product["customProperties"] if item["property"] == "physicalSchemaDefinitions"
    )
    return yaml.safe_load((PRODUCTS / path).read_text())


@lru_cache(maxsize=None)
def supported_versions(table):
    versions = set()
    source = PRODUCTS / "historical-payments"
    products = [source / "product.odps.yaml", *sorted((source / "versions").glob("product-*.odps.yaml"))]
    for path in products:
        product = yaml.safe_load(path.read_text())
        _, contracts = source_contracts(product["version"])
        versions.update(declared_versions(yaml.safe_load(contracts[table].read_text())))
    return versions.intersection(physical_schemas()[table])


def read_manifest(root):
    body = Store(root).read("manifest.json", max_bytes=64 * 1024 * 1024)
    manifest = json.loads(body)
    version = manifest.get("dataset_version", "")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", version):
        raise ValueError("invalid dataset version")
    declared = manifest.get("data_product", {})
    product, contracts = source_contracts(declared.get("version"))
    if declared.get("id") != product["id"] or declared.get("version") != product["version"]:
        raise ValueError("unsupported source product identity")
    schema_path = next(
        (
            entry["value"]
            for entry in product.get("customProperties", [])
            if entry["property"] == "physicalSchemaDefinitions"
        ),
        None,
    )
    if schema_path is not None:
        expected = {"sha256": sha256((PRODUCTS / schema_path).read_bytes()).hexdigest()}
        if declared.get("physical_schema_definitions") != expected:
            raise ValueError("unsupported physical schema definition identity")
    elif "physical_schema_definitions" in declared:
        raise ValueError("unexpected physical schema definition identity")
    if manifest.get("source_layout") != "task-fragments-v1" or manifest.get("manifest_version") != 3:
        raise ValueError("unsupported source layout")
    definition = PRODUCTS.parent / "features/fraud-snapshot-v1.yaml"
    supported_features = [
        {
            "version": "fraud-baseline-v1",
            "sha256": "2ee2a5153a248bdc98df3b2643e10a65bc5693ea111129b6c65a67352aa4ec0b",
        },
        {"version": "fraud-snapshot-v1", "sha256": sha256(definition.read_bytes()).hexdigest()},
    ]
    feature = manifest.get("feature_definition")
    if feature not in supported_features or declared.get("feature_definition") != feature:
        raise ValueError("unsupported source feature definition")
    validation = manifest.get("validation", {})
    deferred = (
        validation.get("result") == "skipped"
        and validation.get("gate") == "dp1-starrocks-openmetadata"
        and isinstance(validation.get("reason"), str)
        and bool(validation["reason"].strip())
        and isinstance(manifest.get("inventory"), list)
        and not any(port in validation for port in PORTS.values())
    )
    if "result" in validation and not deferred:
        raise ValueError("source contract validation has no supported downstream gate")
    for table, port in PORTS.items():
        contract_bytes = contracts[table].read_bytes()
        contract = yaml.safe_load(contract_bytes)
        if declared.get("contracts", {}).get(port) != {
            "id": contract["id"],
            "version": contract["version"],
            "sha256": sha256(contract_bytes).hexdigest(),
        }:
            raise ValueError(f"unsupported source contract identity: {port}")
        if validation.get(port) != "passed" and not deferred:
            raise ValueError(f"source contract validation is incomplete: {port}")
        supported = set(declared_versions(contract)).intersection(physical_schemas()[table])
        versions = manifest.get("schema_versions", {}).get(table, [])
        if not versions or any(v not in supported for v in versions):
            raise ValueError(f"unsupported source schemas: {table}")
    return manifest, sha256(body).hexdigest()


def selected_manifest(root):
    manifest, digest = read_manifest(root)
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
        table = inventory_location(entry["key"])
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
        counts = entry.get("partitions")
        version = int(path.stem.removeprefix("v"))
        if type(entry.get("schema_version")) is not int or entry["schema_version"] != version:
            raise ValueError("inventory schema version differs from its key")
        if version not in manifest.get("schema_versions", {}).get(table, []):
            raise ValueError("inventory schema version is not declared by the manifest")
        if "partition" in entry or not isinstance(counts, dict) or not counts:
            raise ValueError("raw inventory requires per-date counts")
        for partition, rows in counts.items():
            try:
                canonical = date.fromisoformat(partition).isoformat()
            except (ValueError, TypeError):
                raise ValueError("invalid raw partition date") from None
            if canonical != partition or type(rows) is not int or rows <= 0:
                raise ValueError("invalid raw partition count")
        if sum(counts.values()) != entry["rows"]:
            raise ValueError("raw partition counts differ from file rows")
        partitions[table].update(counts)
        totals[table] += entry["rows"]
    for table in PORTS:
        if dict(partitions[table]) != manifest["partitions"][table] or totals[table] != sum(
            manifest["partitions"][table].values()
        ):
            raise ValueError(f"inventory/manifest row counts differ: {table}")


def inventory_location(key):
    match = re.fullmatch(
        r"raw/(payment_events|fraud_labels)/day-[0-9]{5}-kind-[01]-block-[0-9]{8}/v([1-9][0-9]*)\.parquet", key
    )
    if not match or int(match[2]) not in supported_versions(match[1]):
        raise ValueError("raw inventory path escapes the declared layout")
    return match[1]


def source_filesystem(root):
    if root.startswith("s3://"):
        import os

        return fs.S3FileSystem(
            access_key=os.environ["AWS_ACCESS_KEY_ID"],
            secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            endpoint_override=os.environ["S3_ENDPOINT"],
            region=os.environ.get("AWS_REGION", "us-east-1"),
            force_virtual_addressing=os.environ.get("S3_PATH_STYLE", "false").lower() != "true",
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
