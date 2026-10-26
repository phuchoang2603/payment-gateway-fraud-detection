import json
from hashlib import sha256
from urllib.parse import urlsplit

import boto3

from simulator.live.model import HistoricalModel


def load_model(config):
    import os

    parsed = urlsplit(config.manifest)
    if parsed.scheme != "s3" or parsed.netloc != "synthetic-source" or not parsed.path.startswith("/historical/"):
        raise ValueError("live input requires an explicit historical manifest")
    client = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT", "https://minio.home.phuchoang.sbs"))
    response = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    stream = response["Body"]
    try:
        body = stream.read(64 * 1024**2 + 1)
    finally:
        stream.close()
    if len(body) > 64 * 1024**2 or sha256(body).hexdigest() != config.manifest_sha256:
        raise ValueError("historical manifest digest mismatch")
    manifest = json.loads(body)
    return HistoricalModel(manifest)
