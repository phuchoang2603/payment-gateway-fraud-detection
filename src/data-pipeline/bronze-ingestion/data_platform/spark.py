import os
import sys
from urllib.parse import urlsplit

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.10.1"
ICEBERG_RUNTIME = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}"
PACKAGES = (
    f"{ICEBERG_RUNTIME},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:3.3.4"
)


def kubernetes(builder):
    return (
        builder.master("k8s://https://kubernetes.default.svc")
        .config("spark.submit.deployMode", "client")
        .config("spark.kubernetes.namespace", os.environ["POD_NAMESPACE"])
        .config("spark.kubernetes.container.image", os.environ["SPARK_IMAGE"])
        .config("spark.kubernetes.driver.pod.name", os.environ["HOSTNAME"])
        .config("spark.kubernetes.executor.podTemplateFile", "/etc/dp1/executor-pod.yaml")
        .config("spark.kubernetes.executor.label.dp1-workflow", os.environ.get("WORKFLOW_ID", "local"))
        .config("spark.driver.host", os.environ["POD_IP"])
        .config("spark.driver.memory", "2g")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.kubernetes.executor.podNamePrefix", os.environ["HOSTNAME"])
        .config("spark.driver.port", "7078")
        .config("spark.blockManager.port", "7079")
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "1")
        .config("spark.kubernetes.executor.request.cores", "500m")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.memoryOverhead", "512m")
    )


def session(local_warehouse=None):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName("historical-dp1-bronze")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
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
        source_bucket = urlsplit(os.environ.get("SOURCE_ROOT", "s3://synthetic-source/historical/v7")).netloc
        hadoop = f"spark.hadoop.fs.s3a.bucket.{source_bucket}."
        for key, value in {
            "access.key": os.environ["SOURCE_ACCESS_KEY_ID"],
            "secret.key": os.environ["SOURCE_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("S3_ENDPOINT", "https://minio.home.phuchoang.sbs"),
            "path.style.access": "true",
            "bucket.probe": "0",
            "connection.ssl.enabled": "true",
            "aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        }.items():
            builder = builder.config(hadoop + key, value)
    if not local_warehouse and os.environ.get("SPARK_IMAGE"):
        builder = kubernetes(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
