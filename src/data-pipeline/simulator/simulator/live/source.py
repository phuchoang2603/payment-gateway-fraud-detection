import json
from hashlib import sha256
from urllib.parse import urlsplit

from simulator.live.model import HistoricalModel
from simulator.s3 import client


def load_model(config):
    parsed = urlsplit(config.manifest)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.endswith("/manifest.json")
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or ".." in parsed.path.split("/")
    ):
        raise ValueError("live input requires an explicit S3 manifest URI")
    response = client().get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    stream = response["Body"]
    try:
        body = stream.read(64 * 1024**2 + 1)
    finally:
        stream.close()
    if len(body) > 64 * 1024**2 or sha256(body).hexdigest() != config.manifest_sha256:
        raise ValueError("historical manifest digest mismatch")
    manifest = json.loads(body)
    return HistoricalModel(manifest)
