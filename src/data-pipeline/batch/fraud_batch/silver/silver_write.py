import os
import re
import time
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import functions as F

from fraud_batch.common.contracts import check_physical, ensure_contract_columns
from fraud_batch.common.products import SILVER_PORTS
from fraud_batch.silver.silver_contracts import hashes, product_hash, quality_hash, schema
from fraud_batch.silver.silver_runs import verify_candidate
from fraud_batch.silver.silver_transform import validate_basics

OUTPUTS = SILVER_PORTS


def table_name(table, namespace="silver"):
    if table not in OUTPUTS or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", namespace):
        raise ValueError("invalid Silver table or namespace")
    return f"lake.{namespace}.{table}"


def candidate_outputs(frames):
    histories = frames.attempts.histories.withColumn("pending_context", F.lit(False))
    pending = frames.attempts.pending.withColumn("pending_context", F.lit(True))
    return {
        "payment_events": frames.events.canonical,
        "event_delivery_provenance": frames.events.provenance,
        "attempt_histories": histories.unionByName(pending),
        "label_observations": frames.attempts.linked_labels,
        "label_delivery_provenance": frames.labels.provenance,
    }


def committed_snapshot(spark, name, candidate_id):
    rows = (
        spark.table(name + ".snapshots")
        .where(F.col("summary")["dp2.candidate-id"] == candidate_id)
        .orderBy(F.col("committed_at").desc())
        .select("snapshot_id")
        .limit(1)
        .collect()
    )
    return rows[0].snapshot_id if rows else None


def selected_snapshot(spark, name, snapshot):
    return spark.read.format("iceberg").option("versionAsOf", str(snapshot)).load(name)


def write_candidate(spark, store, candidate, frames, completeness, namespace="silver"):
    if candidate.get("state") != "candidate" or not re.fullmatch(r"[a-f0-9]{64}", candidate.get("candidate_id", "")):
        raise ValueError("Silver output requires an identified unaccepted candidate")
    verify_candidate(candidate)
    if candidate.get("completeness") != completeness:
        raise ValueError("Silver completeness differs from the candidate identity")
    if (
        candidate.get("contracts") != hashes()
        or candidate.get("product_sha256") != product_hash()
        or candidate.get("quality_sha256") != quality_hash()
    ):
        raise ValueError("Silver contracts differ from the candidate identity")
    table_name(OUTPUTS[0], namespace)
    receipt = {
        "candidate_id": candidate["candidate_id"],
        "attempt_id": str(uuid4()),
        "started_at": int(time.time() * 1000),
        "workflow_id": os.environ.get("WORKFLOW_ID"),
        "spark_application_id": spark.sparkContext.applicationId,
        "outcome": "error",
    }
    try:
        validate_basics(frames, completeness)
        if not spark.catalog.databaseExists(f"lake.{namespace}"):
            spark.sql(f"CREATE NAMESPACE lake.{namespace}")
        for table, frame in candidate_outputs(frames).items():
            name = table_name(table, namespace)
            expected = frame.withColumn("_candidate_id", F.lit(candidate["candidate_id"])).persist(
                StorageLevel.DISK_ONLY
            )
            try:
                check_physical(expected.schema, schema(table))
                if not spark.catalog.tableExists(name):
                    expected.limit(0).writeTo(name).using("iceberg").create()
                ensure_contract_columns(spark, name, schema(table))
                check_physical(spark.table(name).select(*expected.columns).schema, expected.schema)
                snapshot = candidate["snapshots"].get(table) or committed_snapshot(
                    spark, name, candidate["candidate_id"]
                )
                if snapshot is None:
                    (
                        expected.writeTo(name)
                        .option("snapshot-property.dp2.candidate-id", candidate["candidate_id"])
                        .overwrite(F.lit(True))
                    )
                    snapshot = committed_snapshot(spark, name, candidate["candidate_id"])
                    if snapshot is None:
                        raise RuntimeError("Silver commit did not produce a recoverable snapshot")
                actual = selected_snapshot(spark, name, snapshot)
                columns = [column for column in expected.columns if column != "_candidate_id"]
                actual_content = actual.select(*columns)
                expected_content = expected.select(*columns)
                if (
                    actual_content.exceptAll(expected_content).limit(1).count()
                    or expected_content.exceptAll(actual_content).limit(1).count()
                ):
                    raise ValueError(f"Silver candidate snapshot differs from deterministic output: {table}")
                candidate["snapshots"][table] = snapshot
                store.put(f"dp2/candidates/{candidate['candidate_id']}.json", candidate)
            finally:
                expected.unpersist()
        receipt["outcome"] = "passed"
        return candidate
    except Exception as error:
        receipt["error_type"] = type(error).__name__
        raise
    finally:
        receipt.update(ended_at=int(time.time() * 1000), snapshots=dict(candidate["snapshots"]))
        store.put(
            f"dp2/candidates/{candidate['candidate_id']}/transform/{receipt['attempt_id']}.json", receipt, create=True
        )
        candidate["transform"] = receipt
        store.put(f"dp2/candidates/{candidate['candidate_id']}.json", candidate)
