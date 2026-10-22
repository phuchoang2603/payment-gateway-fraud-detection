import os
import uuid

from pyiceberg.catalog import load_catalog
from pyspark.sql import functions as F

from data_platform.catalog import iceberg_config
from data_platform.spark import session
from data_platform.storage import Store


def probe():
    name = "dp1_probe_" + uuid.uuid4().hex[:12]
    namespace = os.environ.get("ICEBERG_PROBE_NAMESPACE", "bronze")
    if not namespace.replace("_", "").isalnum():
        raise ValueError("invalid probe namespace")
    spark = session()
    report = {
        "namespace": namespace,
        "table": name,
        "catalog": os.environ["ICEBERG_REST_URI"],
        "catalog_signing": os.environ.get("ICEBERG_SIGV4_ENABLED", "true"),
        "s3_endpoint": os.environ.get("S3_ENDPOINT"),
        "path_style": True,
        "outcome": "error",
    }
    try:
        if not spark.catalog.databaseExists(f"lake.{namespace}"):
            spark.sql(f"CREATE NAMESPACE lake.{namespace}")
        table = f"lake.{namespace}.{name}"
        frame = spark.createDataFrame([("first", 1), ("other", 2)], "version string, value long")
        frame.writeTo(table).using("iceberg").partitionedBy("version").create()
        snapshot = spark.table(table + ".snapshots").select("snapshot_id").first()[0]
        spark.createDataFrame([("first", 3)], "version string, value long").writeTo(table).overwrite(
            F.col("version") == "first"
        )
        assert spark.table(table).where("version='other'").first().value == 2
        pinned = spark.read.format("iceberg").option("snapshot-id", snapshot).load(table)
        assert pinned.where("version='first'").first().value == 1
        properties = iceberg_config()["catalog"]["lake"]
        catalog = load_catalog("lake", **properties)
        metadata = catalog.load_table((namespace, name))
        assert metadata.snapshot_by_id(snapshot) is not None
        report.update(outcome="passed", snapshot_id=snapshot, current_snapshot=metadata.current_snapshot().snapshot_id)
        return 0
    except Exception as error:
        report["error_type"] = type(error).__name__
        return 1
    finally:
        Store(os.environ["EVIDENCE_ROOT"]).put(f"probes/{name}.json", report, create=True)
        spark.stop()
