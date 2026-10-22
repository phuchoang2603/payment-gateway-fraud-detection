import json
import os
import tempfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def s3_client(identity):
    prefix = identity.upper()
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "https://minio.home.phuchoang.sbs"),
        aws_access_key_id=os.environ[f"{prefix}_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ[f"{prefix}_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(
            connect_timeout=10, read_timeout=30, retries={"max_attempts": 3}, s3={"addressing_style": "path"}
        ),
    )


class Store:
    def __init__(self, root, identity="lake"):
        parsed = urlsplit(root)
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("storage location cannot contain credentials or query parameters")
        self.root = root.rstrip("/")
        self.bucket = parsed.netloc if parsed.scheme == "s3" else None
        if parsed.scheme not in ("s3", "", "file"):
            raise ValueError("only local or S3 storage is supported")
        self.prefix = parsed.path.strip("/") if self.bucket else parsed.path
        self.client = s3_client(identity) if self.bucket else None

    def key(self, relative):
        path = PurePosixPath(relative)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError("object path must stay inside its scope")
        return str(PurePosixPath(self.prefix) / path)

    def read(self, relative, max_bytes=None):
        key = self.key(relative)
        if self.client:
            stream = self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
        else:
            stream = Path(key).open("rb")
        with stream:
            body = stream.read() if max_bytes is None else stream.read(max_bytes + 1)
        if max_bytes is not None and len(body) > max_bytes:
            raise ValueError("stored JSON exceeds byte bound")
        return body

    def get(self, relative):
        try:
            return json.loads(self.read(relative))
        except FileNotFoundError:
            return None
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

    def put(self, relative, value, create=False):
        body = json.dumps(value, sort_keys=True, indent=2, default=str).encode()
        key = self.key(relative)
        if self.client:
            options = {"IfNoneMatch": "*"} if create else {}
            self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json", **options)
        else:
            target = Path(key)
            target.parent.mkdir(parents=True, exist_ok=True)
            if create:
                with target.open("xb") as stream:
                    stream.write(body)
                    stream.flush()
                    os.fsync(stream.fileno())
            else:
                descriptor, temporary = tempfile.mkstemp(dir=target.parent)
                try:
                    with os.fdopen(descriptor, "wb") as stream:
                        stream.write(body)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, target)
                finally:
                    Path(temporary).unlink(missing_ok=True)
