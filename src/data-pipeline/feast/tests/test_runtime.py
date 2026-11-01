import pytest

from fraud_feast.runtime import config


@pytest.mark.parametrize("master", [None, "local[*]"])
def test_spark_master_defaults_to_local_and_allows_environment_override(master, monkeypatch):
    values = {
        "FEAST_REDIS_PASSWORD": "test",
        "FEAST_REDIS_ADDR": "localhost:6379",
        "FEAST_REGISTRY": "registry.pb",
        "ICEBERG_REST_URI": "http://localhost:8181/api/catalog",
        "ICEBERG_WAREHOUSE": "warehouse",
        "ICEBERG_CLIENT_ID": "test",
        "ICEBERG_CLIENT_SECRET": "test",
        "ICEBERG_OAUTH2_URI": "http://localhost:8181/api/catalog/v1/oauth/tokens",
        "S3_ENDPOINT": "http://localhost:9000",
        "S3_PATH_STYLE": "true",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": "us-east-1",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    if master is None:
        monkeypatch.delenv("SPARK_MASTER", raising=False)
    else:
        monkeypatch.setenv("SPARK_MASTER", master)
    configuration = config()
    assert configuration.offline_store.spark_conf.get("spark.master") == (master or "local[*]")
    assert config(online=True).offline_store.type == "file"


def test_offline_store_needs_no_redis(monkeypatch):
    for name, value in {
        "FEAST_REGISTRY": "registry.pb",
        "ICEBERG_REST_URI": "http://localhost:8181/api/catalog",
        "ICEBERG_WAREHOUSE": "warehouse",
        "ICEBERG_CLIENT_ID": "test",
        "ICEBERG_CLIENT_SECRET": "test",
        "ICEBERG_OAUTH2_URI": "http://localhost:8181/api/catalog/v1/oauth/tokens",
        "S3_ENDPOINT": "http://localhost:9000",
        "S3_PATH_STYLE": "true",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("FEAST_REDIS_ADDR", raising=False)
    monkeypatch.delenv("FEAST_REDIS_PASSWORD", raising=False)
    configuration = config(redis=False)
    assert configuration.online_store is None
    assert configuration.offline_store.type == "spark"
    with pytest.raises(KeyError):
        config()
