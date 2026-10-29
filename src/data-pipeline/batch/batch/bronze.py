import json
import os
from hashlib import sha256

from confluent_kafka import Consumer, TopicPartition
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from batch.lakehouse import TABLE_PROPERTIES
from batch.schema import (
    EVENT_COLUMNS,
    LABEL_COLUMNS,
    business_schema,
    conform,
    conform_frame,
    decode_events,
    decode_labels,
    event_ddl,
    label_ddl,
    rejected_ddl,
)
from batch.spark import ICEBERG_CATALOG, session
from batch.store import Store, parquet_rows, read_object

NAMESPACE = f"{ICEBERG_CATALOG}.bronze"
EVENTS = f"{NAMESPACE}.payment_events"
LABELS = f"{NAMESPACE}.fraud_labels"
REJECTED = f"{NAMESPACE}.rejected_records"


def kafka_sources():
    return {
        "payment_events": {
            "topic": os.environ.get("PAYMENT_EVENTS_TOPIC", "payments.events.v1"),
            "decode": decode_events,
            "key": "event_id",
            "table": EVENTS,
            "columns": EVENT_COLUMNS,
        },
        "fraud_labels": {
            "topic": os.environ.get("FRAUD_LABELS_TOPIC", "fraud.labels.v1"),
            "decode": decode_labels,
            "key": "integration_id",
            "table": LABELS,
            "columns": LABEL_COLUMNS,
        },
    }


def run_historical(spark, root, version, run_id):
    store = Store(root)
    manifest = store.read_json("manifest.json")
    if manifest["dataset_version"] != version:
        raise ValueError("manifest dataset_version differs from DATASET_VERSION")
    verify_parts(spark, root, manifest["files"])
    ensure_tables(spark)
    report = {"mode": "historical", "dataset_version": version, "run_id": run_id, "tables": {}}
    for table, (name, columns) in {
        "payment_events": (EVENTS, EVENT_COLUMNS),
        "fraud_labels": (LABELS, LABEL_COLUMNS),
    }.items():
        parts = [part for part in manifest["files"] if part["table"] == table]
        frame = conform_frame(
            read_parts(spark, root, table, parts),
            business_schema(spark, columns),
            keep=("_source_file",),
        ).withColumns(
            {
                "_source": F.lit("historical"),
                "_dataset_version": F.lit(version),
                "_kafka_topic": F.lit(None).cast("string"),
                "_kafka_partition": F.lit(None).cast("int"),
                "_kafka_offset": F.lit(None).cast("bigint"),
                "_kafka_timestamp": F.lit(None).cast("timestamp"),
                "_ingested_at": F.current_timestamp(),
                "_run_id": F.lit(run_id),
            }
        )
        same_version = (F.col("_source") == "historical") & (F.col("_dataset_version") == version)
        frame.writeTo(name).overwrite(same_version)
        written = spark.table(name).where(same_version).count()
        if written != manifest["rows"][table]:
            raise ValueError(f"{name} holds {written} rows for {version}; manifest lists {manifest['rows'][table]}")
        report["tables"][name] = {
            "rows": written,
            "files": len(parts),
            "snapshot_id": current_snapshot(spark, name),
        }
    return report


def verify_parts(spark, root, parts):
    problems = (
        spark.sparkContext.parallelize(parts, max(1, min(len(parts), 256)))
        .map(lambda part: verify_part(root, part))
        .filter(lambda problem: problem is not None)
        .collect()
    )
    if problems:
        raise ValueError(f"{len(problems)} source files failed verification, first: {problems[0]}")


def verify_part(root, part):
    body = read_object(f"{root.rstrip('/')}/{part['path']}")
    if len(body) != part["bytes"] or sha256(body).hexdigest() != part["sha256"]:
        return f"checksum mismatch: {part['path']}"
    if parquet_rows(body) != part["rows"]:
        return f"row count mismatch: {part['path']}"
    return None


def read_parts(spark, root, table, parts):
    paths = [hadoop_uri(f"{root.rstrip('/')}/{part['path']}") for part in parts]
    return (
        spark.read.option("mergeSchema", "true")
        .parquet(*paths)
        .withColumn("_source_file", F.regexp_extract(F.col("_metadata.file_path"), rf"({table}/date=.+)$", 1))
    )


def run_kafka(spark, bootstrap, run_id):
    ensure_tables(spark)
    starting = os.environ.get("KAFKA_STARTING_OFFSETS", "earliest")
    if starting not in ("earliest", "latest"):
        raise ValueError("KAFKA_STARTING_OFFSETS must be earliest or latest")
    report = {"mode": "kafka", "run_id": run_id, "topics": {}}
    for source in kafka_sources().values():
        topic, table = source["topic"], source["table"]
        lows, highs = watermarks(bootstrap, topic)
        fallback = lows if starting == "earliest" else highs
        accepted_starts = next_starts(spark, table, topic, fallback, kafka_rows=True)
        rejected_starts = next_starts(spark, REJECTED, topic, fallback, kafka_rows=False)
        if all(highs[p] <= accepted_starts[p] for p in highs):
            report["topics"][topic] = {"table": table, "accepted": 0, "rejected": 0}
            continue
        records = read_records(spark, bootstrap, topic, accepted_starts, highs).cache()
        decoded = records.withColumn("decoded", source["decode"](F.col("value")))
        malformed = F.col("value").isNull() | F.col("decoded").isNull() | F.col("decoded")[source["key"]].isNull()
        accepted = decoded.where(~malformed & _from_offsets(accepted_starts))
        rejected = decoded.where(malformed & _from_offsets(rejected_starts))
        accepted_rows = accepted_frame(accepted, source["columns"], run_id).cache()
        rejected_rows = rejected_frame(rejected, run_id).cache()
        report["topics"][topic] = {
            "table": table,
            "accepted": accepted_rows.count(),
            "rejected": rejected_rows.count(),
            "accepted_starts": _partition_map(accepted_starts),
            "rejected_starts": _partition_map(rejected_starts),
            "ends": _partition_map(highs),
            "snapshot_ids": {},
        }
        # Rejected rows commit first, so the rejected table never lags the accepted one and the read window can start from accepted rows alone.
        rejected_rows.writeTo(REJECTED).append()
        accepted_rows.writeTo(table).append()
        report["topics"][topic]["snapshot_ids"] = {
            table: current_snapshot(spark, table),
            REJECTED: current_snapshot(spark, REJECTED),
        }
        records.unpersist()
        accepted_rows.unpersist()
        rejected_rows.unpersist()
    return report


def next_starts(spark, table, topic, fallback, kafka_rows):
    """Next offset per partition, from the rows the table already holds for this topic.

    Progress lives in the rows, so compaction and snapshot expiry cannot move it. Partitions
    without rows start at the fallback.
    """
    frame = spark.table(table).where(F.col("_kafka_topic") == topic)
    if kafka_rows:
        frame = frame.where(F.col("_source") == "kafka")
    last = {
        row["_kafka_partition"]: row["last"]
        for row in frame.groupBy("_kafka_partition")
        .agg(F.max("_kafka_offset").alias("last"))
        .select("_kafka_partition", "last")
        .collect()
    }
    return {p: last[p] + 1 if p in last else fallback[p] for p in fallback}


def _partition_map(offsets):
    return {str(p): offsets[p] for p in sorted(offsets)}


def _from_offsets(starts):
    condition = F.lit(False)
    for partition, offset in starts.items():
        condition = condition | ((F.col("partition") == partition) & (F.col("offset") >= offset))
    return condition


def watermarks(bootstrap, topic):
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": "dp1-bronze-watermarks", "enable.auto.commit": False}
    )
    try:
        partitions = consumer.list_topics(topic, timeout=30).topics[topic].partitions
        lows, highs = {}, {}
        for partition in partitions:
            lows[partition], highs[partition] = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=30
            )
        return lows, highs
    finally:
        consumer.close()


def read_records(spark, bootstrap, topic, starts, ends):
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", json.dumps({topic: _partition_map(starts)}))
        .option("endingOffsets", json.dumps({topic: _partition_map(ends)}))
        .option("failOnDataLoss", "true")
        .load()
    )


def accepted_frame(decoded: DataFrame, columns, run_id):
    target = business_schema(decoded.sparkSession, columns)
    business = conform(F.col("decoded"), decoded.schema["decoded"].dataType, target)
    return (
        decoded.select(business.alias("business"), "topic", "partition", "offset", "timestamp")
        .select("business.*", "topic", "partition", "offset", "timestamp")
        .withColumns(
            {
                "_source": F.lit("kafka"),
                "_dataset_version": F.lit(None).cast("string"),
                "_source_file": F.lit(None).cast("string"),
                "_kafka_topic": F.col("topic"),
                "_kafka_partition": F.col("partition"),
                "_kafka_offset": F.col("offset"),
                "_kafka_timestamp": F.col("timestamp"),
                "_ingested_at": F.current_timestamp(),
                "_run_id": F.lit(run_id),
            }
        )
        .drop("topic", "partition", "offset", "timestamp")
    )


def rejected_frame(rejects: DataFrame, run_id):
    return rejects.select(
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.col("key").alias("record_key"),
        F.col("value").alias("record_value"),
        F.lit("protobuf decoding failed").alias("error"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(run_id).alias("_run_id"),
    )


def ensure_tables(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {EVENTS} ({event_ddl()}) USING iceberg "
        f"PARTITIONED BY (_source, days(occurred_at)) {TABLE_PROPERTIES}"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {LABELS} ({label_ddl()}) USING iceberg "
        f"PARTITIONED BY (_source, days(label_available_at)) {TABLE_PROPERTIES}"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {REJECTED} ({rejected_ddl()}) USING iceberg "
        f"PARTITIONED BY (days(_ingested_at)) {TABLE_PROPERTIES}"
    )


def current_snapshot(spark, table):
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    return rows[0]["snapshot_id"] if rows else None


def hadoop_uri(uri):
    return "s3a://" + uri[len("s3://") :] if uri.startswith("s3://") else uri


def main():
    mode = os.environ["BRONZE_SOURCE"]
    run_id = os.environ["RUN_ID"]
    spark = session(f"dp1-bronze-{mode}")
    try:
        if mode == "historical":
            report = run_historical(spark, os.environ["HISTORICAL_ROOT"], os.environ["DATASET_VERSION"], run_id)
        elif mode == "kafka":
            report = run_kafka(spark, os.environ["KAFKA_BOOTSTRAP_SERVERS"], run_id)
        else:
            raise ValueError("BRONZE_SOURCE must be historical or kafka")
        Store(os.environ["RUN_REPORT_ROOT"]).write_json(f"{run_id}/bronze-{mode}.json", report)
        print(json.dumps(report, sort_keys=True))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
