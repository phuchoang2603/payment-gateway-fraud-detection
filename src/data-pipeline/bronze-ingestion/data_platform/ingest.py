import re
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import partitioning

from data_platform.contracts import check_physical, prepare
from data_platform.manifest import verify_files
from data_platform.products import AUDIT, PORTS
from data_platform.runs import save_run


def table_name(table, namespace="bronze"):
    if not namespace.replace("_", "").isalnum():
        raise ValueError("invalid namespace")
    return f"lake.{namespace}.raw_{table}"


def source_frames(spark, root, manifest, run):
    result = {}
    for table in PORTS:
        prepared = prepare(table)
        paths = [root.rstrip("/") + "/" + entry["key"] for entry in manifest["inventory"] if entry["table"] == table]
        if paths:
            frame = spark.read.option("mergeSchema", "true").parquet(*[p.replace("s3://", "s3a://", 1) for p in paths])
            columns = prepared.business_columns + (["_transport"] if manifest.get("input_mode") == "landing" else [])
            check_physical(frame.schema, T.StructType([prepared.schema[c] for c in columns]))
            frame = frame.select(*columns)
            frame = frame.withColumn("_source_file", F.regexp_replace(F.input_file_name(), "^s3a://", "s3://"))
        else:
            frame = spark.createDataFrame([], T.StructType([f for f in prepared.schema if f.name not in AUDIT]))
            frame = frame.withColumn("_source_file", F.lit("").cast("string"))
        if manifest.get("input_mode") == "landing":
            files = {
                root.rstrip("/") + "/" + e["key"]: e["batch"] for e in manifest["inventory"] if e["table"] == table
            }
            files = {
                key if key.startswith("s3://") else Path(key.removeprefix("file://")).absolute().as_uri(): value
                for key, value in files.items()
            }
            batch_map = F.create_map(*[F.lit(v) for pair in files.items() for v in pair]) if files else None
            frame = frame.withColumn("_source_epoch", F.lit(manifest["source_epoch"]))
            frame = frame.withColumn(
                "_source_batch", batch_map[F.col("_source_file")] if files else F.lit(None).cast("string")
            )
            if "_transport" not in frame.columns:
                frame = frame.withColumn("_transport", F.lit(None).cast(prepared.schema["_transport"].dataType))
        else:
            for column in ("_source_epoch", "_source_batch", "_transport"):
                frame = frame.withColumn(column, F.lit(None).cast(prepared.schema[column].dataType))
        frame = (
            frame.withColumn("_source_dataset_version", F.lit(run["dataset_version"]))
            .withColumn("_source_manifest_sha256", F.lit(run["manifest_sha256"]))
            .withColumn("_ingestion_run_id", F.lit(run["run_id"]))
            .withColumn("_ingested_at", F.lit(datetime.fromisoformat(run["ingested_at"])))
            .select(*prepared.schema.fieldNames())
        )
        check_physical(frame.schema, prepared.schema)
        result[table] = frame
    return result


def committed_snapshot(spark, name, run):
    rows = (
        spark.table(name + ".snapshots")
        .where(F.col("summary")["dp1.run-id"] == run["run_id"])
        .orderBy(F.col("committed_at").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    row = rows[0]
    if row.summary.get("dp1.manifest-sha256") != run["manifest_sha256"]:
        raise ValueError("committed snapshot source digest differs")
    return row.snapshot_id


def check_date_partition(spark, name, table):
    column = "occurred_at" if table == "payment_events" else "label_available_at"
    definition = spark.sql(f"SHOW CREATE TABLE {name}").first()[0]
    canonical = re.sub(r"\s+", "", definition.replace("`", "")).lower()
    if f"days({column})" not in canonical:
        raise ValueError(f"table lacks the required date partition: {name}")


def ingest(spark, root, manifest, store, run, namespace="bronze", after_commit=None):
    conservation = run.get("conservation", {})
    if (
        set(run["snapshots"]) == set(PORTS)
        and conservation.get("outcome") == "passed"
        and conservation.get("snapshots") == run["snapshots"]
        and conservation.get("manifest_sha256") == run["manifest_sha256"]
    ):
        return run
    verify_files(spark, root, manifest)
    frames = source_frames(spark, root, manifest, run)
    if manifest.get("input_mode") == "landing":
        verify_transport(frames, manifest)
    if not spark.catalog.databaseExists(f"lake.{namespace}"):
        spark.sql(f"CREATE NAMESPACE lake.{namespace}")
    for table, frame in frames.items():
        name = table_name(table, namespace)
        if table in run["snapshots"]:
            continue
        if not spark.catalog.tableExists(name):
            (
                frame.limit(0)
                .writeTo(name)
                .using("iceberg")
                .partitionedBy(
                    "_source_dataset_version",
                    partitioning.days("occurred_at" if table == "payment_events" else "label_available_at"),
                )
                .tableProperty("format-version", "2")
                .tableProperty("write.target-file-size-bytes", "134217728")
                .tableProperty("history.expire.max-snapshot-age-ms", "9223372036854775807")
                .create()
            )
        existing = spark.table(name).schema
        for column in ("_source_epoch", "_source_batch", "_transport"):
            if column not in existing.fieldNames():
                spark.sql(f"ALTER TABLE {name} ADD COLUMN `{column}` {frame.schema[column].dataType.simpleString()}")
        check_date_partition(spark, name, table)
        if manifest.get("input_mode") == "landing" and not any(e["table"] == table for e in manifest["inventory"]):
            run["snapshots"][table] = (
                spark.table(name + ".snapshots").orderBy(F.col("committed_at").desc()).first().snapshot_id
            )
            save_run(store, run)
            continue
        snapshot = committed_snapshot(spark, name, run)
        if snapshot is None:
            (
                frame.writeTo(name)
                .option("snapshot-property.dp1.run-id", run["run_id"])
                .option("snapshot-property.dp1.manifest-sha256", run["manifest_sha256"])
                .overwrite(F.col("_source_dataset_version") == run["dataset_version"])
            )
            if after_commit:
                after_commit(table)
            snapshot = committed_snapshot(spark, name, run)
            if snapshot is None:
                raise RuntimeError("commit did not produce a recoverable snapshot")
        run["snapshots"][table] = snapshot
        save_run(store, run)
    verify_content(spark, frames, run, namespace)
    run["conservation"] = {
        "outcome": "passed",
        "method": "spark-exceptAll-bidirectional",
        "snapshots": dict(run["snapshots"]),
        "manifest_sha256": run["manifest_sha256"],
    }
    save_run(store, run)
    return run


def verify_content(spark, frames, run, namespace):
    persisted = selected_snapshots(spark, run, namespace)
    for table, expected in frames.items():
        actual = persisted[table].select(*expected.columns)
        if actual.exceptAll(expected).limit(1).count() or expected.exceptAll(actual).limit(1).count():
            raise ValueError(f"persisted content differs from source deliveries: {table}")


def selected_snapshots(spark, run, namespace="bronze"):
    if set(run["snapshots"]) != set(PORTS):
        raise ValueError("validation requires a complete recorded snapshot pair")
    return {
        table: spark.read.format("iceberg")
        .option("versionAsOf", str(snapshot))
        .load(table_name(table, namespace))
        .where(F.col("_source_dataset_version") == run["dataset_version"])
        for table, snapshot in run["snapshots"].items()
    }


def verify_transport(frames, manifest):
    for table, frame in frames.items():
        entries = [e for e in manifest["inventory"] if e["table"] == table]
        if not entries:
            continue
        valid = F.lit(False)
        for entry in entries:
            topic, partition, first, last = entry["range"]
            valid = valid | (
                (F.col("_source_batch") == entry["batch"])
                & (F.col("_transport.topic") == topic)
                & (F.col("_transport.partition") == partition)
                & F.col("_transport.offset").between(first, last)
            )
        if frame.where(~F.coalesce(valid, F.lit(False)) | F.col("_transport.value").isNull()).limit(1).count():
            raise ValueError("landing transport provenance differs from completion manifest")
        if (
            frame.groupBy("_source_epoch", "_transport.topic", "_transport.partition", "_transport.offset")
            .count()
            .where("count > 1")
            .limit(1)
            .count()
        ):
            raise ValueError("landing repeats transport positions")
