import json
import os
from pathlib import Path
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from pyiceberg.catalog import load_catalog


def catalog():
    lake = load_catalog(
        "lake",
        type="rest",
        uri=os.environ["ICEBERG_REST_URI"],
        warehouse=os.environ["ICEBERG_WAREHOUSE"],
        credential=f"{os.environ['ICEBERG_CLIENT_ID']}:{os.environ['ICEBERG_CLIENT_SECRET']}",
        **{
            "oauth2-server-uri": os.environ["ICEBERG_OAUTH2_URI"],
            "scope": "PRINCIPAL_ROLE:ALL",
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
            "s3.force-virtual-addressing": "false"
            if os.environ.get("S3_PATH_STYLE") == "true"
            else "true",
        },
    )
    # Polaris runs with STS disabled and rejects PyIceberg's default request for vended credentials; the writer
    # brings its own storage credentials, so the request is dropped. PyIceberg has no public switch for it.
    lake._session.headers.pop("X-Iceberg-Access-Delegation", None)
    return lake


def s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(
            s3={
                "addressing_style": "path"
                if os.environ.get("S3_PATH_STYLE") == "true"
                else "virtual"
            },
            request_checksum_calculation="when_required",
        ),
    )


def read_json(uri):
    path = urlsplit(uri)
    if path.scheme == "s3":
        client = s3()
        try:
            return json.loads(
                client.get_object(Bucket=path.netloc, Key=path.path.lstrip("/"))["Body"].read()
            )
        except client.exceptions.NoSuchKey:
            return None
    file = Path(uri)
    return json.loads(file.read_text()) if file.exists() else None


def write_json(uri, value):
    body = json.dumps(value, sort_keys=True, default=str).encode()
    path = urlsplit(uri)
    if path.scheme == "s3":
        s3().put_object(
            Bucket=path.netloc, Key=path.path.lstrip("/"), Body=body, ContentType="application/json"
        )
    else:
        target = Path(uri)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".pending")
        temporary.write_bytes(body)
        temporary.replace(target)
