"""Configure S3-compatible clients, including signed GCS create-only preconditions."""

import os
from urllib.parse import urlsplit

import boto3
from botocore.auth import AUTH_TYPE_MAPS, S3SigV4QueryAuth
from botocore.config import Config

GCS_HOST = "storage.googleapis.com"


class _GCSCreateOnlyAuth(S3SigV4QueryAuth):
    def _modify_request_before_signing(self, request):
        # The query signer normally moves form bodies into the URL; S3 object bodies must stay intact.
        body, request.data = request.data, None
        try:
            super()._modify_request_before_signing(request)
        finally:
            request.data = body


AUTH_TYPE_MAPS["gcs-create-only"] = _GCSCreateOnlyAuth


def _create_only_signer(context, **_):
    if context.get("gcs_create_only"):
        return "gcs-create-only"


def _create_only(params, **_):
    # GCS rejects mixed x-amz/x-goog headers; query authentication keeps its generation precondition usable.
    headers = params["headers"]
    if headers.pop("If-None-Match", None) == "*":
        headers["x-goog-if-generation-match"] = "0"
        params["context"]["gcs_create_only"] = True


def client():
    """Create the endpoint-specific S3 client with static credentials and compatible transfer settings."""
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
        s3.meta.events.register("choose-signer.s3.PutObject", _create_only_signer)
    return s3
