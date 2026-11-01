import pytest

from fraud_feast.runtime import config


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


ENV = {
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


# Partitions of the Redis password: plain, containing a comma or quote (escaped for Feast's JSON decoding), empty.
@pytest.mark.parametrize(
    ("password", "encoded"),
    [("plain", '"plain"'), ("a,b", '"a\\u002cb"'), ('q"uote', '"q\\"uote"'), ("", None)],
)
def test_redis_connection_encodes_the_password(monkeypatch, password, encoded):
    monkeypatch.setenv("FEAST_REDIS_ADDR", "redis:6379")
    monkeypatch.setenv("FEAST_REDIS_PASSWORD", password)
    from fraud_feast.runtime import redis_connection

    if encoded is None:
        with pytest.raises(ValueError):
            redis_connection()
        return
    assert (
        redis_connection()
        == f"redis:6379,password={encoded},socket_timeout=2,socket_connect_timeout=2"
    )


# Partitions of the store mode: offline Spark with Redis, online with a file offline store, offline without Redis.
@pytest.mark.parametrize(
    ("kwargs", "offline", "online"),
    [
        ({}, "spark", "redis"),
        ({"online": True}, "file", "redis"),
        ({"redis": False}, "spark", None),
    ],
)
def test_config_modes(monkeypatch, kwargs, offline, online):
    for name, value in {
        **ENV,
        "FEAST_REDIS_ADDR": "redis:6379",
        "FEAST_REDIS_PASSWORD": "p",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("SPARK_MASTER", raising=False)
    configuration = config(**kwargs)
    assert configuration.offline_store.type == offline
    assert (configuration.online_store.type if configuration.online_store else None) == online
    if offline == "spark":
        assert configuration.offline_store.spark_conf["spark.master"] == "local[*]"
