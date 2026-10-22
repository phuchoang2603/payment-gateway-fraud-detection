import json
import os
import uuid

from pyiceberg.catalog import load_catalog
from pyspark.sql import functions as F

from data_platform.spark import session


def smoke():
    name = "dp1_smoke_" + uuid.uuid4().hex[:12]
    namespace = "bronze"
    table = f"lake.{namespace}.{name}"
    spark = None
    created = False
    report = {
        "table": name,
        "catalog": os.environ["ICEBERG_REST_URI"],
        "catalog_signing": os.environ.get("ICEBERG_SIGV4_ENABLED", "true"),
        "s3_endpoint": os.environ.get("S3_ENDPOINT"),
        "outcome": "error",
    }

    def stage(value):
        report["stage"] = value
        print(json.dumps({"smoke": name, "stage": value}), flush=True)

    try:
        stage("session")
        spark = session()
        stage("namespace")
        if not spark.catalog.databaseExists(f"lake.{namespace}"):
            raise RuntimeError("bronze namespace is not provisioned")
        stage("create")
        frame = spark.createDataFrame([("first", 1), ("other", 2)], "version string, value long")
        frame.writeTo(table).using("iceberg").partitionedBy("version").create()
        created = True
        stage("snapshot")
        snapshot = spark.table(table + ".snapshots").select("snapshot_id").first()[0]
        stage("overwrite")
        spark.createDataFrame([("first", 3)], "version string, value long").writeTo(table).overwrite(
            F.col("version") == "first"
        )
        stage("pinned-read")
        assert spark.table(table).where("version='other'").first().value == 2
        pinned = spark.read.format("iceberg").option("snapshot-id", snapshot).load(table)
        assert pinned.where("version='first'").first().value == 1
        stage("metadata")
        from data_platform.catalog import iceberg_config

        catalog = load_catalog("lake", **iceberg_config()["catalog"]["lake"])
        assert catalog.load_table((namespace, name)).snapshot_by_id(snapshot) is not None
        report["outcome"] = "passed"
        return 0
    except Exception as error:
        report["error_type"] = type(error).__name__
        return 1
    finally:
        if spark:
            if created:
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
                    report["cleanup"] = "dropped"
                except Exception as error:
                    report["cleanup"] = "failed:" + type(error).__name__
            spark.stop()
        print(json.dumps(report), flush=True)
