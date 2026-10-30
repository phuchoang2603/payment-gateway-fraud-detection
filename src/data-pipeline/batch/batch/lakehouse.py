from pyspark.sql import SparkSession

TARGET_FILE_BYTES = 256 * 1024 * 1024
TABLE_PROPERTIES = (
    f"TBLPROPERTIES ('write.distribution-mode' = 'hash', 'write.target-file-size-bytes' = '{TARGET_FILE_BYTES}')"
)
WRITE_PROPERTIES = {
    "write.distribution-mode": "hash",
    "write.target-file-size-bytes": str(TARGET_FILE_BYTES),
}


def file_stats(spark: SparkSession, table: str):
    row = spark.sql(
        f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes FROM {table}.files"
    ).first()
    return {"files": row["files"], "bytes": int(row["bytes"])}
