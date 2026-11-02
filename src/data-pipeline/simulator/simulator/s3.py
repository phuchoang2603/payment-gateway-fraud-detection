import os
from urllib.parse import urlsplit

import boto3
from botocore.config import Config

GCS_HOST = "storage.googleapis.com"


def _create_only(params, **_):
    # GCS has no If-None-Match on writes; its create-only precondition is generation 0.
    headers = params["headers"]
    if headers.pop("If-None-Match", None) == "*":
        headers["x-goog-if-generation-match"] = "0"


def client():
    endpoint = os.environ.get("S3_ENDPOINT")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(
            s3={
                "addressing_style": "path" if os.environ.get("S3_PATH_STYLE", "false").lower() == "true" else "virtual"
            },
            connect_timeout=15,
            read_timeout=120,
            retries={"max_attempts": 3},
            request_checksum_calculation="when_required",
        ),
    )
    if endpoint and urlsplit(endpoint).hostname == GCS_HOST:
        s3.meta.events.register("before-call.s3.PutObject", _create_only)
    return s3
