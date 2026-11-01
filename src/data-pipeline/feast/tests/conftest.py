import os
from pathlib import Path
from uuid import uuid4

import pytest


@pytest.fixture(scope="session")
def redis_address():
    from testcontainers.redis import RedisContainer

    with RedisContainer("redis:7.2.16") as redis:
        yield f"{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}"


@pytest.fixture
def lake(tmp_path):
    from pyiceberg.catalog import load_catalog

    from fraud_feast.apply import ensure_tables

    catalog = load_catalog(
        "test",
        type="sql",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"file://{tmp_path}/warehouse",
    )
    catalog.create_namespace("gold")
    ensure_tables(catalog)
    return catalog


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    from fraud_feast.spark_jars import manifest

    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    os.environ["PYSPARK_PYTHON"] = os.sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .appName("feast-ci")
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{manifest()['versions']['iceberg']}",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    yield session
    session.stop()


@pytest.fixture
def feature_store(tmp_path, redis_address, monkeypatch):
    from feast import FeatureStore
    from feast.repo_config import RepoConfig

    from fraud_feast.repo import definitions

    monkeypatch.setenv("FEAST_STREAM_EPOCHS", "history-ci,live-ci")
    configuration = RepoConfig(
        project=f"ci_{uuid4().hex}",
        provider="local",
        registry=str(tmp_path / "registry.pb"),
        entity_key_serialization_version=3,
        online_store={
            "type": "redis",
            "connection_string": redis_address,
            "key_ttl_seconds": 2764800,
        },
        offline_store={"type": "spark", "spark_conf": {"spark.sql.session.timeZone": "UTC"}},
        materialization={"pull_latest_features": True},
    )
    store = FeatureStore(config=configuration)
    store.apply(definitions())
    return store


CONTRACTS = Path(__file__).resolve().parents[4] / "contracts" / "features"
