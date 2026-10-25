import os
import sys
from urllib.parse import urlsplit

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.12.0"
ICEBERG_RUNTIME = f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{ICEBERG_VERSION}"
PACKAGES = (
    f"{ICEBERG_RUNTIME},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:3.4.2"
)


def session(local_warehouse=None):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName("historical-dp1-bronze")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.ui.retainedExecutions", "20")
        .config("spark.ui.retainedJobs", "20")
        .config("spark.ui.retainedStages", "40")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.redaction.regex", "(?i)secret|password|token|access.?key|credential")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    )
    if not os.environ.get("DP1_BUNDLED_JARS"):
        builder = builder.config("spark.jars.packages", ICEBERG_RUNTIME if local_warehouse else PACKAGES)
    if local_warehouse:
        builder = (
            builder.master("local[2]")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", str(local_warehouse))
        )
    else:
        prefix = "spark.sql.catalog.lake."
        options = {
            "type": "rest",
            "uri": os.environ["ICEBERG_REST_URI"],
            "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "fraud-lakehouse-prod"),
            "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "rest.access-key-id": os.environ["LAKE_ACCESS_KEY_ID"],
            "rest.secret-access-key": os.environ["LAKE_SECRET_ACCESS_KEY"],
            "rest.sigv4-enabled": os.environ.get("ICEBERG_SIGV4_ENABLED", "true"),
            "rest.signing-region": os.environ.get("AWS_REGION", "us-east-1"),
            "rest.signing-name": os.environ.get("ICEBERG_SIGNING_NAME", "s3"),
            "s3.endpoint": os.environ.get("S3_ENDPOINT", "https://minio.home.phuchoang.sbs"),
            "s3.path-style-access": "true",
            "s3.access-key-id": os.environ["LAKE_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["LAKE_SECRET_ACCESS_KEY"],
            "client.region": os.environ.get("AWS_REGION", "us-east-1"),
        }
        for key, value in options.items():
            builder = builder.config(prefix + key, value)
        source_bucket = urlsplit(os.environ.get("SOURCE_ROOT", "s3://synthetic-source/historical/v10")).netloc
        hadoop = f"spark.hadoop.fs.s3a.bucket.{source_bucket}."
        for key, value in {
            "access.key": os.environ["SOURCE_ACCESS_KEY_ID"],
            "secret.key": os.environ["SOURCE_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("S3_ENDPOINT", "https://minio.home.phuchoang.sbs"),
            "path.style.access": "true",
            "bucket.probe": "0",
            "endpoint.region": os.environ.get("AWS_REGION", "us-east-1"),
            "connection.ssl.enabled": "true",
            "aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        }.items():
            builder = builder.config(hadoop + key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
