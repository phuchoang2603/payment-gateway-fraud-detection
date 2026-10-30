from pyspark.sql import SparkSession

TARGET_FILE_BYTES = 256 * 1024 * 1024
TABLE_PROPERTIES = (
    f"TBLPROPERTIES ('write.distribution-mode' = 'hash', 'write.target-file-size-bytes' = '{TARGET_FILE_BYTES}')"
)
WRITE_PROPERTIES = {
    "write.distribution-mode": "hash",
    "write.target-file-size-bytes": str(TARGET_FILE_BYTES),
}
# For input already laid out by the target's day partition (one historical file per purchase day, or Bronze files
# read back for Silver), each task touches only a few partitions, so the hash shuffle adds no clustering: write in
# place with one open file per partition.
LOCAL_WRITE_OPTIONS = {"distribution-mode": "none", "fanout-enabled": "true"}


def file_stats(spark: SparkSession, table: str):
    row = spark.sql(
        f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes FROM {table}.files"
    ).first()
    return {"files": row["files"], "bytes": int(row["bytes"])}
