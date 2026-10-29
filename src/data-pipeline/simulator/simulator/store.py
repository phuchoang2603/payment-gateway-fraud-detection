import os
import shutil
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from botocore.exceptions import ClientError

from simulator.s3 import client

ABSENT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
PRECONDITION_CODES = frozenset({"PreconditionFailed", "ConditionalRequestConflict", "412", "409"})


class ObjectExists(Exception):
    pass


class LocalStore:
    def __init__(self, root: Path):
        self.root = root

    def _path(self, key: str) -> Path:
        return self.root / key

    def read(self, key: str) -> bytes | None:
        path = self._path(key)
        return path.read_bytes() if path.is_file() else None

    def write(self, key: str, data: bytes, *, if_absent: bool = False) -> None:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_bytes(data)
        try:
            if if_absent:
                try:
                    os.link(temporary, target)
                except FileExistsError as error:
                    raise ObjectExists(key) from error
            else:
                os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    def publish_file(self, key: str, source: Path) -> None:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        shutil.copyfile(source, temporary)
        os.replace(temporary, target)


class S3Store:
    def __init__(self, bucket: str, prefix: str, s3):
        self.bucket = bucket
        self.prefix = prefix
        self.s3 = s3

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def read(self, key: str) -> bytes | None:
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self._key(key))
        except ClientError as error:
            if error.response["Error"]["Code"] in ABSENT_CODES:
                return None
            raise
        return response["Body"].read()

    def write(self, key: str, data: bytes, *, if_absent: bool = False) -> None:
        options = {"IfNoneMatch": "*"} if if_absent else {}
        try:
            self.s3.put_object(Bucket=self.bucket, Key=self._key(key), Body=data, **options)
        except ClientError as error:
            if if_absent and error.response["Error"]["Code"] in PRECONDITION_CODES:
                raise ObjectExists(key) from error
            raise

    def publish_file(self, key: str, source: Path) -> None:
        self.s3.upload_file(str(source), self.bucket, self._key(key))


def open_store(output: str, dataset: str):
    if not output.startswith("s3://"):
        return LocalStore(Path(output).expanduser().resolve() / "historical" / dataset)
    parsed = urlsplit(output)
    prefix = "/".join(part for part in (parsed.path.strip("/"), "historical", dataset) if part)
    return S3Store(parsed.netloc, prefix, client())
