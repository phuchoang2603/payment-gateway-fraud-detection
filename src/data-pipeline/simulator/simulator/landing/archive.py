import base64
import json
import re
from collections import defaultdict
from hashlib import sha256

from botocore.exceptions import ClientError

from simulator.landing.parquet import decode, encode

FORMAT = "decoded-payments-parquet-v1"


def digest(body):
    return sha256(body).hexdigest()


class LandingStore:
    def __init__(self, client, bucket, source_epoch):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", source_epoch):
            raise ValueError("invalid streaming source epoch")
        self.source_epoch = source_epoch
        self.client = client
        self.bucket = bucket

    def create(self, key, body):
        if not key.startswith("streaming/"):
            raise ValueError("object outside streaming prefix")
        checksum = digest(body)
        metadata = {"sha256": checksum, "landing-format": FORMAT}
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentLength=len(body),
                IfNoneMatch="*",
                ChecksumSHA256=base64.b64encode(sha256(body).digest()).decode(),
                Metadata=metadata,
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in (
                "PreconditionFailed",
                "ConditionalRequestConflict",
                "409",
                "412",
            ):
                raise RuntimeError("conditional publication failed") from None
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        actual = {key.lower(): value for key, value in head.get("Metadata", {}).items()}
        if head["ContentLength"] != len(body) or any(actual.get(key) != value for key, value in metadata.items()):
            raise RuntimeError("immutable landing object collision")


def publish(store, records, label_topic):
    partitions = defaultdict(list)
    for record in records:
        partitions[record.topic, record.partition].append(record)
    manifests = []
    for (topic, partition), batch in sorted(partitions.items()):
        first, last = batch[0].offset, batch[-1].offset
        if any(right.offset <= left.offset for left, right in zip(batch, batch[1:])):
            raise ValueError("non-increasing partition offsets")
        groups = defaultdict(list)
        for record in batch:
            table, version, row = decode(record, label_topic)
            groups[table, version].append(row)
        manifest = {
            "format": FORMAT,
            "source_epoch": store.source_epoch,
            "topic": topic,
            "partition": partition,
            "first_offset": first,
            "last_offset": last,
            "rows": len(batch),
            "files": [],
        }
        for (table, version), rows in sorted(groups.items()):
            body = encode(table, version, rows)
            checksum = digest(body)
            key = f"streaming/{table}/epoch={store.source_epoch}/topic={topic}/partition={partition}/{first:020d}-{last:020d}-v{version}-{checksum}.parquet"
            store.create(key, body)
            manifest["files"].append(
                {
                    "key": key,
                    "sha256": checksum,
                    "bytes": len(body),
                    "rows": len(rows),
                    "table": table,
                    "schema_version": version,
                }
            )
        body = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        key = f"streaming/_batches/epoch={store.source_epoch}/topic={topic}/partition={partition}/{first:020d}-{last:020d}-{digest(body)}.json"
        store.create(key, body)
        manifests.append(key)
    return manifests
