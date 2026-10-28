import os

import boto3
from botocore.config import Config


def client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT"),
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
