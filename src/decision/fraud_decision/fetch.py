import os
from pathlib import Path

import boto3


def fetch(client, bucket: str, prefix: str, target: Path) -> int:
    prefix = prefix.rstrip("/") + "/"
    copied = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            path = target / item["Key"][len(prefix) :]
            if path.exists() and path.stat().st_size == item["Size"]:
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, item["Key"], str(path))
            copied += 1
    if not (target / "rl_agent_config.json").is_file():
        raise SystemExit(f"s3://{bucket}/{prefix} holds no laya checkpoint")
    return copied


def main() -> None:
    target = Path(os.environ.get("MODEL_DIR", "/models/laya"))
    copied = fetch(boto3.client("s3"), os.environ["MODEL_BUCKET"], os.environ["MODEL_PREFIX"], target)
    print(f"{copied} files copied to {target}", flush=True)


if __name__ == "__main__":  # pragma: no cover
    main()
