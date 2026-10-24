import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone

from data_platform.manifest import selected_manifest
from data_platform.products import hashes
from data_platform.runs import accept, open_run, save_run
from data_platform.storage import Store


def main(argv=None):
    parser = argparse.ArgumentParser(prog="dp1")
    parser.add_argument(
        "stage",
        choices=[
            "preflight",
            "ingest",
            "validate",
            "publish",
            "accept",
            "smoke",
            "source-inventory",
            "verify-source",
        ],
    )
    parser.add_argument("--source", default=os.environ.get("SOURCE_ROOT", "s3://synthetic-source/historical/v7"))
    parser.add_argument(
        "--evidence", default=os.environ.get("EVIDENCE_ROOT", "s3://fraud-lakehouse-prod/dp1-evidence")
    )
    parser.add_argument("--version", default=os.environ.get("DATASET_VERSION", "v7"))
    parser.add_argument("--workflow", default=os.environ.get("WORKFLOW_ID", "local"))
    parser.add_argument("--inventory-uri", default=os.environ.get("SOURCE_INVENTORY_URI") or None)
    parser.add_argument("--inventory-sha256", default=os.environ.get("SOURCE_INVENTORY_SHA256") or None)
    parser.add_argument("--inventory-max-files", type=int, default=10000)
    parser.add_argument("--inventory-max-bytes", type=int, default=1024**3)
    parser.add_argument("--inventory-timeout", type=int, default=900)
    parser.add_argument("--local-warehouse")
    args = parser.parse_args(argv)
    spark = None
    try:
        if args.stage == "smoke":
            from data_platform.smoke import smoke

            return smoke()
        if args.stage == "source-inventory":
            from data_platform.inventory import create_companion

            result = create_companion(
                args.source,
                args.inventory_uri,
                args.inventory_max_files,
                args.inventory_max_bytes,
                args.inventory_timeout,
            )
            print(json.dumps(result, sort_keys=True))
            return 0
        if args.stage == "verify-source":
            from data_platform.inventory import verify_source

            manifest, digest = selected_manifest(args.source, args.inventory_uri, args.inventory_sha256)
            if manifest["dataset_version"] != args.version:
                raise ValueError("selected manifest differs from requested version")
            totals = verify_source(args.source, manifest, args.inventory_timeout)
            print(json.dumps({"manifest_sha256": digest, "totals": totals, "outcome": "passed"}, sort_keys=True))
            return 0
        store = Store(args.evidence)
        contracts = hashes()
        if args.stage in ("preflight", "ingest"):
            manifest, digest = selected_manifest(args.source, args.inventory_uri, args.inventory_sha256)
            if manifest["dataset_version"] != args.version:
                raise ValueError("selected manifest differs from requested version")
            image = os.environ.get("IMAGE_DIGEST", "")
            if not re.fullmatch(r".+@sha256:[a-f0-9]{64}", image):
                raise ValueError("IMAGE_DIGEST must identify the immutable job image")
            source_inventory = (
                {"uri": args.inventory_uri, "sha256": args.inventory_sha256} if args.inventory_uri else None
            )
            run = open_run(
                store,
                args.version,
                digest,
                contracts,
                image,
                args.workflow,
                source_inventory=source_inventory,
                source_validation=manifest["validation"]
                if manifest["validation"].get("result") == "skipped"
                else None,
            )
            run["source_root"] = args.source
            save_run(store, run)
            if args.stage == "preflight":
                print(json.dumps({"run_id": run["run_id"], "state": run["state"]}))
                return 0
        else:
            run = store.get(f"versions/{args.version}/run.json")
            if not run:
                raise ValueError("run context is missing")
        if args.stage in ("ingest", "validate"):
            from data_platform.spark import session

            spark = session(args.local_warehouse)
        if args.stage == "ingest":
            from data_platform.ingest import ingest

            ingest(spark, args.source, manifest, store, run)
        elif args.stage == "validate":
            from data_platform.validate import validate

            inventory = run.get("source_inventory") or {}
            manifest, digest = selected_manifest(run["source_root"], inventory.get("uri"), inventory.get("sha256"))
            if digest != run["manifest_sha256"]:
                raise ValueError("source manifest changed since ingestion")
            report = validate(
                spark, run["source_root"], manifest, store, run, contracts, os.environ.get("IMAGE_DIGEST", "")
            )
            return 0 if report["data_outcome"] == "passed" else 2
        elif args.stage == "publish":
            from data_platform.governance import publish

            result = publish(store, run, contracts)
            return 0 if result["outcome"] == "passed" else 3
        elif args.stage == "accept":
            accept(store, run, contracts)
        return 0
    except Exception as error:
        if args.stage in ("preflight", "ingest", "validate") and "run" in locals():
            from data_platform.runs import report_key

            failure = {
                "run_id": run["run_id"],
                "dataset_version": run["dataset_version"],
                "manifest_sha256": run["manifest_sha256"],
                "source_inventory": run.get("source_inventory"),
                "source_validation": run.get("source_validation"),
                "contracts": contracts,
                "snapshots": run["snapshots"],
                "data_outcome": "error",
                "tables": {},
                "run_time": datetime.now(timezone.utc).isoformat(),
                "execution_errors": [{"stage": args.stage, "type": type(error).__name__}],
            }
            store.put(report_key(run, contracts), failure)
        print(
            json.dumps({"stage": args.stage, "error_type": type(error).__name__, "outcome": "error"}), file=sys.stderr
        )
        return 1
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
