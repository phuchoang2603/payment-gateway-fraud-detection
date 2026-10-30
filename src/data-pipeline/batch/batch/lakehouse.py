import os

from pyspark.sql import SparkSession

TARGET_FILE_BYTES = 256 * 1024 * 1024
TABLE_PROPERTIES = (
    f"TBLPROPERTIES ('write.distribution-mode' = 'hash', 'write.target-file-size-bytes' = '{TARGET_FILE_BYTES}', "
    "'write.wap.enabled' = 'true')"
)
WRITE_PROPERTIES = {
    "write.distribution-mode": "hash",
    "write.target-file-size-bytes": str(TARGET_FILE_BYTES),
    "write.wap.enabled": "true",
}
# For input already laid out by the target's day partition (one historical file per purchase day, or Bronze files
# read back for Silver), each task touches only a few partitions, so the hash shuffle adds no clustering: write in
# place with one open file per partition.
LOCAL_WRITE_OPTIONS = {"distribution-mode": "none", "fanout-enabled": "true"}

# Write-audit-publish: a pipeline run writes to this branch (spark.wap.branch), its checks audit the branch heads, and
# only a passing run fast-forwards main. Readers of another pipeline's tables use main, so they see audited data only.
WAP_BRANCH_ENV = "WAP_BRANCH"


def write_ref() -> str:
    return os.environ.get(WAP_BRANCH_ENV) or "main"


def ref_snapshot(spark: SparkSession, table: str, ref: str = "main"):
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = '{ref}'").collect()
    return rows[0]["snapshot_id"] if rows else None


def stage(spark: SparkSession, table: str) -> None:
    """Enable WAP on the table and branch it from main before this run writes.

    Writing to a branch that does not exist would start it empty, so an overwrite would drop main's other rows when
    published. A table without a main snapshot has nothing to keep and gets its branch on first write.
    """
    branch = os.environ.get(WAP_BRANCH_ENV)
    if not branch:
        return
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.wap.enabled' = 'true')")
    if ref_snapshot(spark, table) is not None:
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS `{branch}`")


def file_stats(spark: SparkSession, table: str):
    row = spark.sql(
        f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes FROM {table}.files"
    ).first()
    return {"files": row["files"], "bytes": int(row["bytes"])}


def create_table(spark: SparkSession, name: str, frame, partition=None, properties=WRITE_PROPERTIES) -> None:
    if spark.catalog.tableExists(name):
        return
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name.rsplit('.', 1)[0]}")
    creator = frame.limit(0).writeTo(name).using("iceberg")
    for key, value in properties.items():
        creator = creator.tableProperty(key, value)
    if partition is not None:
        creator = creator.partitionedBy(partition)
    creator.create()


def snapshot_info(spark: SparkSession, table: str) -> dict:
    """The head of this run's write ref and the records its commit added."""
    snapshot = ref_snapshot(spark, table, write_ref())
    row = spark.sql(
        f"SELECT summary['added-records'] AS added FROM {table}.snapshots WHERE snapshot_id = {snapshot}"
    ).first()
    return {"snapshot_id": snapshot, "added_records": int(row["added"] or 0)}
