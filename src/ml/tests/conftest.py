"""Shared fixtures: a local Spark session with Iceberg, and a Feast test repository."""

import os

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Returns a local Spark session with an Iceberg warehouse and the `gold`, `silver` and `ml` namespaces."""
    from fraud_feast.spark_jars import manifest
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    os.environ["PYSPARK_PYTHON"] = os.sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .appName("risk-model-ci")
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{manifest()['versions']['iceberg']}",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    for namespace in ("gold", "silver", "ml"):
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{namespace}")
    yield session
    session.stop()


@pytest.fixture
def feature_store(tmp_path, monkeypatch):
    """Returns a Feast store on local files with the fraud feature definitions applied."""
    from feast import FeatureStore
    from feast.repo_config import RepoConfig
    from fraud_feast.repo import definitions

    monkeypatch.setenv("FEAST_STREAM_EPOCHS", "history-ci,live-ci")
    store = FeatureStore(
        config=RepoConfig(
            project="ci",
            provider="local",
            registry=str(tmp_path / "registry.pb"),
            entity_key_serialization_version=3,
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
            offline_store={"type": "spark", "spark_conf": {"spark.sql.session.timeZone": "UTC"}},
        )
    )
    store.apply(definitions())
    return store
