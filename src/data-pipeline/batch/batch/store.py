import json
import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit

import boto3
from botocore.config import Config


class Store:
    def __init__(self, uri):
        parsed = urlsplit(uri)
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("storage location cannot carry credentials or query parameters")
        if parsed.scheme not in ("s3", "s3a", "file", ""):
            raise ValueError("storage must be a local path or an S3 URI")
        self.bucket = parsed.netloc if parsed.scheme in ("s3", "s3a") else None
        self.prefix = parsed.path.strip("/") if self.bucket else uri.rstrip("/")
        self.uri = uri.rstrip("/")
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = s3_client()
        return self._client

    def read(self, relative):
        if self.bucket:
            key = f"{self.prefix}/{relative}"
            return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return Path(self.prefix, relative).read_bytes()

    def read_json(self, relative):
        return json.loads(self.read(relative))

    def list_sizes(self) -> dict[str, int]:
        """Size of every object under this root, keyed by path relative to it. Metadata only."""
        if self.bucket:
            base = f"{self.prefix}/" if self.prefix else ""
            sizes = {}
            for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=base):
                for item in page.get("Contents", []):
                    sizes[item["Key"][len(base) :]] = item["Size"]
            return sizes
        root = Path(self.prefix)
        return {path.relative_to(root).as_posix(): path.stat().st_size for path in root.rglob("*") if path.is_file()}

    def write(self, relative, body):
        if self.bucket:
            self.client.put_object(Bucket=self.bucket, Key=f"{self.prefix}/{relative}", Body=body)
            return
        target = Path(self.prefix, relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)

    def write_json(self, relative, value):
        self.write(relative, json.dumps(value, sort_keys=True, indent=2).encode())


@lru_cache(maxsize=1)
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT") or None,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(
            connect_timeout=10,
            read_timeout=60,
            retries={"max_attempts": 5},
            s3={"addressing_style": "path" if os.environ.get("S3_PATH_STYLE", "false") == "true" else "virtual"},
        ),
    )
