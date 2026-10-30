import os

from pyspark.sql import SparkSession

ICEBERG_CATALOG = "lake"


def session(app_name):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "1024"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.redaction.regex", "(?i)secret|password|token|access.?key|credential")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    )
    if os.environ.get("WAP_BRANCH"):
        builder = builder.config("spark.wap.branch", os.environ["WAP_BRANCH"])
    if os.environ.get("SPARK_JARS_PACKAGES"):
        builder = builder.config("spark.jars.packages", os.environ["SPARK_JARS_PACKAGES"])
    local_warehouse = os.environ.get("BATCH_LOCAL_WAREHOUSE")
    if local_warehouse:
        builder = (
            builder.master("local[2]")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.enabled", "false")
            .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", local_warehouse)
        )
    else:
        builder = _rest_catalog(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _rest_catalog(builder):
    access_key = os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    region = os.environ.get("AWS_REGION", "us-east-1")
    prefix = f"spark.sql.catalog.{ICEBERG_CATALOG}."
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
        "client.region": region,
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
        "endpoint.region": region,
        "connection.ssl.enabled": "true",
        "aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    }.items():
        builder = builder.config(hadoop + key, value)
    return builder
