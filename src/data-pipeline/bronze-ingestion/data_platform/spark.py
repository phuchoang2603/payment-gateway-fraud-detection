import os
import sys

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
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "1024"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "67108864")
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
        access_key = os.environ["AWS_ACCESS_KEY_ID"]
        secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        prefix = "spark.sql.catalog.lake."
        options = {
            "type": "rest",
            "uri": os.environ["ICEBERG_REST_URI"],
            "warehouse": os.environ["ICEBERG_WAREHOUSE"],
            "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "rest.auth.type": "oauth2",
            "credential": f"{os.environ['ICEBERG_CLIENT_ID']}:{os.environ['ICEBERG_CLIENT_SECRET']}",
            "oauth2-server-uri": os.environ["ICEBERG_OAUTH2_URI"],
            "scope": "PRINCIPAL_ROLE:ALL",
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.path-style-access": os.environ["S3_PATH_STYLE"],
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "client.region": os.environ.get("AWS_REGION", "us-east-1"),
        }
        for key, value in options.items():
            builder = builder.config(prefix + key, value)
        hadoop = "spark.hadoop.fs.s3a."
        for key, value in {
            "access.key": access_key,
            "secret.key": secret_key,
            "endpoint": os.environ["S3_ENDPOINT"],
            "path.style.access": os.environ["S3_PATH_STYLE"],
            "bucket.probe": "0",
            "endpoint.region": os.environ.get("AWS_REGION", "us-east-1"),
            "connection.ssl.enabled": "true",
            "aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        }.items():
            builder = builder.config(hadoop + key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
