import argparse
import json
import os
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="simulator", description="Synthetic payment data generator")
    commands = parser.add_subparsers(dest="command", required=True)
    historical = commands.add_parser("historical", help="Generate and publish an immutable historical dataset")
    historical.add_argument("--config", type=Path, required=True)
    historical.add_argument("--output", help="Override the configured s3:// bucket or local directory")
    historical.add_argument(
        "--prepare-only", action="store_true", help="Generate raw files without publishing a manifest"
    )
    historical.add_argument(
        "--takeover-stopped-writer",
        action="store_true",
        help="Replace crash ownership only after stopping the previous writer",
    )
    historical.add_argument(
        "--allow-small-dataset",
        action="store_true",
        help="Publish a dataset smaller than one customer block, for contract probes only",
    )
    live = commands.add_parser("live", help="Drive current-time gateway traffic and optional delivery faults")
    live.add_argument("--config", type=Path, required=True)
    commands.add_parser("archive", help="Land committed Kafka protobuf as immutable historical-compatible Parquet")
    args = parser.parse_args(argv)
    if args.command == "archive":
        from simulator.landing.runtime import main as archive

        try:
            archive()
        except Exception as error:
            parser.exit(2, f"stream archiver failed: {type(error).__name__}\n")
        return 0
    if args.command == "live":
        import asyncio

        from simulator.live.config import load_live_config
        from simulator.live.runtime import run_live

        try:
            asyncio.run(run_live(load_live_config(args.config)))
        except Exception as error:
            parser.exit(2, f"live simulation failed: {type(error).__name__}\n")
        return 0
    try:
        from simulator.storage import generate

        config = load_config(args.config)
        filesystem, root = destination(args.output or config.output)
        manifest = generate(
            config,
            filesystem,
            root,
            resume=True,
            prepare_only=args.prepare_only,
            takeover_stopped_writer=args.takeover_stopped_writer,
            cleanup_intermediates=True,
            allow_small_dataset=args.allow_small_dataset,
        )
    except (ValueError, OSError) as error:
        message = str(error)
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
            if value := os.environ.get(name):
                message = message.replace(value, "[redacted]")
        parser.exit(2, f"historical generation failed: {message}\n")
    print(json.dumps({key: manifest[key] for key in ("dataset_version", "config_sha256", "history_end")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
