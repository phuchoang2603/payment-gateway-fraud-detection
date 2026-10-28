import json
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

import pyarrow.fs as fs

from simulator.config import load_config


def destination(output: str) -> tuple[fs.FileSystem, str]:
    if not output.startswith("s3://"):
        return fs.LocalFileSystem(), str(Path(output).expanduser().resolve())
    parsed = urlsplit(output)
    if (
        not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or ".." in parsed.path.split("/")
    ):
        raise ValueError("S3 output must be a bucket and optional prefix, without credentials or query")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise ValueError("S3 output requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment")
    filesystem = fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        session_token=os.environ.get("AWS_SESSION_TOKEN"),
        region=os.environ.get("AWS_REGION", "us-east-1"),
        scheme="https",
        endpoint_override=os.environ.get("S3_ENDPOINT"),
        force_virtual_addressing=os.environ.get("S3_PATH_STYLE", "false").lower() != "true",
        allow_bucket_creation=False,
        allow_bucket_deletion=False,
    )
    return filesystem, f"{parsed.netloc}{parsed.path}".rstrip("/")


CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs/historical-smoke.yaml"


def main() -> int:
    try:
        from simulator.storage import generate

        config = load_config(Path(os.environ.get("SIMULATOR_CONFIG", str(CONFIG_PATH))))
        filesystem, root = destination(config.output)
        manifest = generate(
            config,
            filesystem,
            root,
            resume=True,
            prepare_only=config.execution.prepare_only,
            takeover_stopped_writer=config.execution.takeover_stopped_writer,
            cleanup_intermediates=True,
            allow_small_dataset=config.execution.allow_small_dataset,
        )
    except (ValueError, OSError) as error:
        message = str(error)
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
            if value := os.environ.get(name):
                message = message.replace(value, "[redacted]")
        print(f"historical generation failed: {message}", file=sys.stderr)
        return 2
    print(json.dumps({key: manifest[key] for key in ("dataset_version", "config_sha256", "history_end")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
