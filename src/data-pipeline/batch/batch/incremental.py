"""Incremental DP2: the last run's state, the refusal rules and the affected-payment scope.

Every Silver rule is local to one payment, so recomputing each affected payment from all of its Bronze rows and
replacing its Silver rows gives the result a historical run over the same Bronze would. New Bronze rows are an Iceberg
incremental append read since the Bronze snapshots the last DP2 run recorded on its Silver commits; consumers read
Silver's changes with Iceberg's changelog view.
"""

import os
from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOOKBACK_ENV = "SILVER_LOOKBACK_DAYS"
LOOKBACK_DEFAULT_DAYS = 35
# Bump when a change to curation would make earlier Silver rows differ from a fresh historical run.
LOGIC_VERSION = "1"
PAYMENT_KEY = ["integration_id", "payment_id"]
EVENTS_PROPERTY = "bronze.payment_events.snapshot_id"
LABELS_PROPERTY = "bronze.fraud_labels.snapshot_id"
RUN_PROPERTY = "dp2.run_id"
LOGIC_PROPERTY = "dp2.logic_version"


class IncrementalRefused(RuntimeError):
    """Incremental mode cannot be exact from the recorded state; run dp2-silver mode=historical instead."""


def lookback() -> timedelta:
    return timedelta(days=int(os.environ.get(LOOKBACK_ENV, LOOKBACK_DEFAULT_DAYS)))


def read_at(spark: SparkSession, table: str, snapshot) -> DataFrame:
    """The table at a snapshot; a table without one is empty."""
    if snapshot is None:
        return spark.table(table).limit(0)
    return spark.read.option("versionAsOf", snapshot).table(table)


def last_run(spark: SparkSession, table: str):
    """The summary of the newest DP2 commit in the table's main history; maintenance commits carry no run ID."""
    if not spark.catalog.tableExists(table):
        return None
    snapshots = {
        row["snapshot_id"]: (row["parent_id"], row["summary"])
        for row in spark.sql(f"SELECT snapshot_id, parent_id, summary FROM {table}.snapshots").collect()
    }
    head = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    node = head[0]["snapshot_id"] if head else None
    while node in snapshots:
        parent, summary = snapshots[node]
        if summary and summary.get(RUN_PROPERTY):
            return summary
        node = parent
    return None


def rewritten_since(spark: SparkSession, table: str, since, current) -> str | None:
    """Why the table's main changed rows other than by appending or compaction since the recorded snapshot, or None."""
    if since == current:
        return None
    snapshots = {
        row["snapshot_id"]: (row["parent_id"], row["operation"])
        for row in spark.sql(f"SELECT snapshot_id, parent_id, operation FROM {table}.snapshots").collect()
    }
    node = current
    while node is not None and node != since:
        if node not in snapshots:
            return f"{table} snapshot {node} is not in its history"
        parent, operation = snapshots[node]
        if operation not in ("append", "replace"):
            return f"{table} snapshot {node} is a {operation}, not an append"
        node = parent
    return None if node == since else f"{table} no longer descends from snapshot {since}"


def refusal(spark: SparkSession, state_table: str, bronze: dict):
    """The reason incremental mode would not be exact, or None, with the Bronze snapshots the last run read.

    `bronze` maps each Bronze table to (its snapshot property, its current main snapshot)."""
    summary = last_run(spark, state_table)
    if summary is None:
        return "no published Silver run recorded the Bronze snapshots it read", None
    if summary.get(LOGIC_PROPERTY) != LOGIC_VERSION:
        return f"the last Silver run used logic version {summary.get(LOGIC_PROPERTY)}, not {LOGIC_VERSION}", None
    since = {}
    for table, (prop, current) in bronze.items():
        recorded = summary.get(prop)
        if recorded is None:
            return f"the last Silver run did not record {prop}", None
        since[table] = int(recorded) if recorded else None
        reason = rewritten_since(spark, table, since[table], current)
        if reason:
            return f"Bronze changed historical rows since the last Silver run: {reason}", None
    return None, since


def appended(spark: SparkSession, table: str, since, current) -> DataFrame:
    """Rows appended to the table's main between two snapshots, by Iceberg's incremental append read."""
    if since == current:
        return spark.table(table).limit(0)
    if since is None:
        return read_at(spark, table, current)
    return (
        spark.read.format("iceberg")
        .option("start-snapshot-id", str(since))
        .option("end-snapshot-id", str(current))
        .load(table)
    )


def affected_scope(new_events: DataFrame, new_labels: DataFrame):
    """Payments with new Bronze rows, and the earliest time their full history is reread from."""
    new_rows = new_events.select(*PAYMENT_KEY, F.col("occurred_at").alias("anchor")).unionByName(
        new_labels.select(*PAYMENT_KEY, F.col("label_available_at").alias("anchor"))
    )
    payments = new_rows.select(*PAYMENT_KEY).distinct()
    earliest = new_rows.agg(F.min("anchor")).first()[0]
    cutoff = None if earliest is None else earliest - lookback()
    return payments, cutoff


def outside_lookback(silver_events: DataFrame, silver_labels: DataFrame, rejected: DataFrame, payments, cutoff):
    """Affected payments whose earlier Silver rows are older than the cutoff, so their Bronze reread would miss rows."""
    earliest = (
        silver_events.select(*PAYMENT_KEY, F.col("occurred_at").alias("at"))
        .unionByName(silver_labels.select(*PAYMENT_KEY, F.col("label_available_at").alias("at")))
        .unionByName(
            rejected.select(
                *PAYMENT_KEY,
                F.coalesce(
                    F.get_json_object("row_json", "$.occurred_at"),
                    F.get_json_object("row_json", "$.label_available_at"),
                )
                .cast("timestamp")
                .alias("at"),
            )
        )
        .join(F.broadcast(payments), PAYMENT_KEY, "left_semi")
        .groupBy(*PAYMENT_KEY)
        .agg(F.min("at").alias("at"))
    )
    return earliest.where(F.col("at") < F.lit(cutoff))
