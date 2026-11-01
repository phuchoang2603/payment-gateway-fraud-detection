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


def test_registry_reads_the_object_without_head_bucket(monkeypatch):
    from unittest.mock import MagicMock

    from botocore.exceptions import ClientError
    from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto

    from fraud_feast.registry import PrefixS3RegistryStore

    store = PrefixS3RegistryStore.__new__(PrefixS3RegistryStore)
    store._bucket, store._key = "bucket", "runs/feast/registry.pb"
    store._uri = MagicMock()
    store.s3_client = MagicMock()
    expected = RegistryProto(version_id="v1")
    store.s3_client.Object.return_value.get.return_value = {
        "Body": MagicMock(read=lambda: expected.SerializeToString())
    }
    assert store.get_registry_proto() == expected
    store.s3_client.meta.client.head_bucket.assert_not_called()
    store.s3_client.Object.return_value.get.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey"}}, "GetObject"
    )
    with pytest.raises(FileNotFoundError):
        store.get_registry_proto()
