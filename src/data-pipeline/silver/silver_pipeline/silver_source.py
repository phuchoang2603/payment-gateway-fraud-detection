from pathlib import Path

from data_platform.contracts import check_physical, prepare
from data_platform.ingest import normalize_events, table_name
from data_platform.landing import selection
from data_platform.manifest import selected_manifest
from data_platform.products import AUDIT, PORTS
from pyspark.sql import functions as F
from pyspark.sql import types as T


def selected_inventory(bronze_store, pinned):
    if pinned["input_mode"] == "landing":
        manifest, manifest_hash = selection(bronze_store, pinned["dataset_version"])
        if (
            manifest.get("source_epoch") != pinned.get("source_epoch")
            or manifest.get("source_root") != pinned["source_root"]
        ):
            raise ValueError("accepted landing source or epoch changed")
    else:
        manifest, manifest_hash = selected_manifest(pinned["source_root"])
    if manifest_hash != pinned["manifest_sha256"] or manifest.get("dataset_version") != pinned["dataset_version"]:
        raise ValueError("accepted Bronze source selection changed")
    return manifest["inventory"]


def source_versions(spark, frame, pinned, inventory, table):
    root = pinned["source_root"].rstrip("/")
    entries = []
    for entry in inventory:
        if entry["table"] != table:
            continue
        path = root + "/" + entry["key"]
        if not path.startswith("s3://"):
            path = str(Path(path.removeprefix("file://")).absolute())
        entries.append((path, entry["schema_version"]))
    schema = T.StructType(
        [
            T.StructField("_lookup_file", T.StringType(), False),
            T.StructField("_inventory_version", T.IntegerType(), False),
        ]
    )
    versions = spark.createDataFrame(entries, schema)
    frame = frame.withColumn("_lookup_file", F.regexp_replace(F.url_decode(F.col("_source_file")), "^file:/+", "/"))
    frame = frame.join(F.broadcast(versions), "_lookup_file", "left")
    selected = F.col("_inventory_version")
    if "_source_schema_version" in frame.columns:
        selected = F.when(
            F.col("_source_schema_version").isNull() | (F.col("_source_schema_version") == selected), selected
        )
    frame = frame.withColumn(
        "_source_schema_version",
        F.when(selected.isNotNull(), selected).otherwise(
            F.raise_error("source physical schema provenance differs").cast("int")
        ),
    )
    return frame.drop("_lookup_file", "_inventory_version")


def selected_bronze(spark, bronze_store, candidate, namespace="bronze"):
    frames = {table: [] for table in PORTS}
    for pinned in candidate["inputs"]:
        inventory = selected_inventory(bronze_store, pinned)
        for table in PORTS:
            expected = prepare(table).schema
            frame = (
                spark.read.format("iceberg")
                .option("versionAsOf", str(pinned["snapshots"][table]))
                .load(table_name(table, namespace))
                .where(F.col("_source_dataset_version") == pinned["dataset_version"])
            )
            frame = source_versions(spark, frame, pinned, inventory, table)
            if table == "payment_events":
                frame = normalize_events(frame, frame.schema, expected)
            for field in expected:
                if field.name not in frame.columns:
                    if field.name not in AUDIT or not field.nullable:
                        raise ValueError(f"accepted Bronze snapshot lacks required field: {table}.{field.name}")
                    frame = frame.withColumn(field.name, F.lit(None).cast(field.dataType))
            frame = frame.select(*expected.fieldNames())
            check_physical(frame.schema, expected)
            frames[table].append(frame)
    result = {}
    for table, parts in frames.items():
        if not parts:
            raise ValueError("Silver requires accepted Bronze inputs")
        result[table] = parts[0]
        for part in parts[1:]:
            result[table] = result[table].unionByName(part)
    return result
