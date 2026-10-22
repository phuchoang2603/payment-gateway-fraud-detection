import fcntl
import io
import json
import os
import shutil
import time
from contextlib import contextmanager
from hashlib import file_digest, sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUFFER_BYTES = 1024 * 1024


def digest_stream(stream):
    return file_digest(stream, "sha256").hexdigest()


def digest_file(path):
    with Path(path).open("rb") as stream:
        return digest_stream(stream)


class ConditionalWriteFailed(FileExistsError):
    pass


class CleanupFailed(OSError):
    def __init__(self, message, removed):
        super().__init__(message)
        self.removed = removed.copy()


class ObjectStore:
    def __init__(self, prefix: str, *, remote: bool = False):
        self.prefix = prefix.rstrip("/")
        self.remote = remote
        self.capabilities = None
        self.metrics = {"uploaded_file_bytes": 0, "upload_file_seconds": 0.0, "checksum_read_bytes": 0}
        self.metrics_lock = Lock()
        if remote:
            self.bucket, _, self.key_prefix = self.prefix.partition("/")
            self.client = boto3.client(
                "s3",
                endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3", "https://minio.home.phuchoang.sbs"),
                aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
                region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                config=Config(
                    s3={"addressing_style": "path"},
                    connect_timeout=15,
                    read_timeout=120,
                    retries={"max_attempts": 3},
                    request_checksum_calculation="when_required",
                ),
            )

    def uri(self, key):
        return f"s3://{self.prefix}/{key}" if self.remote else f"{self.prefix}/{key}"

    def _key(self, key):
        return f"{self.key_prefix}/{key}".lstrip("/")

    @contextmanager
    def read(self, key):
        if not self.remote:
            with Path(self.uri(key)).open("rb") as stream:
                yield stream
            return
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise FileNotFoundError(key) from None
            raise OSError(f"S3 read failed ({error.response['Error']['Code']})") from None
        with response["Body"] as stream:
            yield stream

    def json(self, key):
        with self.read(key) as stream:
            return json.load(stream)

    def exists(self, key):
        try:
            with self.read(key):
                return True
        except FileNotFoundError:
            return False

    def put_json(self, key, value, *, expected=None, absent=False):
        payload = json.dumps(value, sort_keys=True, indent=2).encode()
        return self.put(key, io.BytesIO(payload), len(payload), expected=expected, absent=absent)

    def put_file(self, key, path):
        size = Path(path).stat().st_size
        start = time.monotonic()
        with Path(path).open("rb") as stream:
            self.put(key, stream, size)
        with self.metrics_lock:
            self.metrics["uploaded_file_bytes"] += size
            self.metrics["upload_file_seconds"] += time.monotonic() - start

    def transfer_report(self):
        with self.metrics_lock:
            values = self.metrics.copy()
        duration = values["upload_file_seconds"]
        values["upload_bytes_per_transfer_second"] = values["uploaded_file_bytes"] / duration if duration else 0
        return values

    def has_objects(self):
        if not self.remote:
            root = Path(self.prefix)
            return root.exists() and any(path.is_file() for path in root.rglob("*"))
        result = self.client.list_objects_v2(Bucket=self.bucket, Prefix=self._key(""), MaxKeys=1)
        return bool(result.get("Contents"))

    def purge(self, prefix, *, keep=()):
        removed = {"objects": 0, "bytes": 0}
        if not self.remote:
            root = Path(self.uri(prefix.rstrip("/")))
            if root.is_dir():
                files = [
                    path
                    for path in root.rglob("*")
                    if path.is_file() and str(path.relative_to(self.prefix)) not in keep
                ]
                for path in files:
                    try:
                        size = path.stat().st_size
                        path.unlink()
                    except OSError as error:
                        raise CleanupFailed(str(error), removed) from error
                    removed["objects"] += 1
                    removed["bytes"] += size
                try:
                    for path in sorted(root.rglob("*"), reverse=True):
                        if path.is_dir() and not any(path.iterdir()):
                            path.rmdir()
                    if not any(root.iterdir()):
                        root.rmdir()
                except OSError as error:
                    raise CleanupFailed(str(error), removed) from error
            elif root.exists():
                try:
                    size = root.stat().st_size
                    root.unlink()
                except OSError as error:
                    raise CleanupFailed(str(error), removed) from error
                removed.update(objects=1, bytes=size)
            return removed
        pending = []
        retained = {self._key(key) for key in keep}
        try:
            for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=self._key(prefix.rstrip("/") + "/")
            ):
                for item in page.get("Contents", []):
                    if item["Key"] in retained:
                        continue
                    pending.append(item)
                    if len(pending) == 1000:
                        removed = self._delete_objects(pending, removed)
                        pending = []
        except ClientError as error:
            raise CleanupFailed(f"S3 listing failed ({error.response['Error']['Code']})", removed) from None
        if pending:
            removed = self._delete_objects(pending, removed)
        return removed

    def _delete_objects(self, items, removed):
        try:
            response = self.client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": item["Key"]} for item in items], "Quiet": True},
            )
        except ClientError as error:
            raise CleanupFailed(f"S3 delete failed ({error.response['Error']['Code']})", removed) from None
        errors = response.get("Errors", [])
        failed = {item["Key"] for item in errors}
        deleted = [item for item in items if item["Key"] not in failed]
        removed["objects"] += len(deleted)
        removed["bytes"] += sum(item["Size"] for item in deleted)
        if errors:
            codes = ", ".join(sorted({item["Code"] for item in errors}))
            raise CleanupFailed(f"S3 delete failed for {len(failed)} objects ({codes})", removed)
        return removed

    def tag(self, key, tags):
        if not self.remote:
            return
        try:
            current = self.client.get_object_tagging(Bucket=self.bucket, Key=self._key(key))
            existing = {item["Key"]: item["Value"] for item in current["TagSet"]}
            if all(existing.get(name) == value for name, value in tags.items()):
                return
            tags = {**existing, **tags}
            self.client.put_object_tagging(
                Bucket=self.bucket,
                Key=self._key(key),
                Tagging={"TagSet": [{"Key": name, "Value": value} for name, value in sorted(tags.items())]},
            )
        except ClientError as error:
            raise OSError(f"S3 tagging failed ({error.response['Error']['Code']})") from None

    def put(self, key, stream, size, *, expected=None, absent=False):
        if self.remote:
            conditions = {"IfNoneMatch": "*"} if absent else ({"IfMatch": expected} if expected else {})
            try:
                result = self.client.put_object(
                    Bucket=self.bucket, Key=self._key(key), Body=stream, ContentLength=size, **conditions
                )
                return result["ETag"]
            except ClientError as error:
                code = error.response["Error"]["Code"]
                if code in ("PreconditionFailed", "ConditionalRequestConflict", "412", "409"):
                    raise ConditionalWriteFailed(f"conditional write refused: {key}") from None
                raise OSError(f"S3 write failed ({code})") from None
        path = Path(self.uri(key))
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = Path(self.prefix) / "_run" / "locks" / (sha256(key.encode()).hexdigest() + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if absent and path.exists():
                raise ConditionalWriteFailed(f"conditional create refused: {key}")
            if expected and (not path.exists() or digest_file(path) != expected):
                raise ConditionalWriteFailed(f"conditional update refused: {key}")
            with NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
                name = temporary.name
                try:
                    shutil.copyfileobj(stream, temporary, BUFFER_BYTES)
                    temporary.flush()
                    os.fsync(temporary.fileno())
                    os.replace(name, path)
                finally:
                    Path(name).unlink(missing_ok=True)
            return digest_file(path)

    def etag(self, key):
        if not self.remote:
            return digest_file(self.uri(key))
        try:
            return self.client.head_object(Bucket=self.bucket, Key=self._key(key))["ETag"]
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise FileNotFoundError(key) from None
            raise OSError(f"S3 metadata failed ({error.response['Error']['Code']})") from None

    def verify(self, item):
        try:
            with self.read(item["key"]) as stream:
                digest = sha256()
                length = 0
                while chunk := stream.read(BUFFER_BYTES):
                    digest.update(chunk)
                    length += len(chunk)
            with self.metrics_lock:
                self.metrics["checksum_read_bytes"] += length
            return length == item["bytes"] and digest.hexdigest() == item["sha256"]
        except FileNotFoundError:
            return False

    def probe(self):
        if self.capabilities:
            return self.capabilities
        key = f"_run/primitives/{uuid4().hex}.json"
        first = self.put_json(key, {"probe": 1}, absent=True)
        for options in ({"absent": True}, {"expected": '"00000000000000000000000000000000"'}):
            try:
                self.put_json(key, {"probe": "must-not-write"}, **options)
            except ConditionalWriteFailed:
                pass
            else:
                raise OSError("storage does not enforce conditional writes; safe resume/publication unavailable")
        self.put_json(key, {"probe": 2}, expected=first)
        if self.json(key) != {"probe": 2}:
            raise OSError("storage conditional update verification failed")
        self.capabilities = {
            "create_if_absent": True,
            "etag_update": True,
            "integrity": "streamed-sha256",
            "probe_key": key,
        }
        return self.capabilities
