from types import SimpleNamespace

import pytest
from botocore.awsrequest import AWSResponse

from simulator.s3 import client


def put(monkeypatch, endpoint, **options):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_REGION", "auto")
    monkeypatch.setenv("S3_ENDPOINT", endpoint)
    monkeypatch.setenv("S3_PATH_STYLE", "true")
    sent = []

    def capture(request, **_):
        sent.append(request)
        return AWSResponse(request.url, 200, {}, SimpleNamespace(stream=lambda: iter(())))

    s3 = client()
    s3.meta.events.register("before-send.s3.PutObject", capture)
    s3.put_object(Bucket="bucket", Key="key", Body=b"body", **options)
    request = sent[0]
    headers = {name: value.decode() if isinstance(value, bytes) else value for name, value in request.headers.items()}
    return SimpleNamespace(url=request.url, headers=headers)


def test_gcs_create_only_write_uses_generation_precondition(monkeypatch):
    request = put(monkeypatch, "https://storage.googleapis.com", IfNoneMatch="*")
    assert request.url == "https://storage.googleapis.com/bucket/key"
    assert request.headers["x-goog-if-generation-match"] == "0"
    assert "If-None-Match" not in request.headers


def test_gcs_unconditional_write_has_no_precondition(monkeypatch):
    request = put(monkeypatch, "https://storage.googleapis.com")
    assert "x-goog-if-generation-match" not in request.headers


@pytest.mark.parametrize("endpoint", ["https://minio.example.com", "https://s3.us-east-1.amazonaws.com"])
def test_other_stores_keep_if_none_match(monkeypatch, endpoint):
    request = put(monkeypatch, endpoint, IfNoneMatch="*")
    assert request.headers["If-None-Match"] == "*"
    assert "x-goog-if-generation-match" not in request.headers


def test_writes_carry_no_default_checksum(monkeypatch):
    request = put(monkeypatch, "https://storage.googleapis.com")
    assert not [name for name in request.headers if name.lower().startswith(("x-amz-checksum", "x-amz-sdk-checksum"))]
