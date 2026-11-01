import pytest

from fraud_feast.runtime import config


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
