import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from data_platform.manifest import selected_manifest
from data_platform.products import hashes
from data_platform.runs import VALIDATION_POLICY, evidence_key, open_run, save_run
from data_platform.storage import Store

STAGES = ["preflight", "ingest", "verify-source", "select"]


def main(argv=None):
    logging.disable(logging.INFO)
    parser = argparse.ArgumentParser(prog="dp1")
    parser.add_argument("stage", choices=STAGES)
    parser.add_argument("--source", default=os.environ.get("SOURCE_ROOT"), required=not os.environ.get("SOURCE_ROOT"))
    parser.add_argument(
        "--evidence", default=os.environ.get("EVIDENCE_ROOT"), required=not os.environ.get("EVIDENCE_ROOT")
    )
    parser.add_argument(
        "--version", default=os.environ.get("DATASET_VERSION"), required=not os.environ.get("DATASET_VERSION")
    )
    parser.add_argument("--workflow", default=os.environ.get("WORKFLOW_ID", "local"))
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--local-warehouse")
    parser.add_argument("--out", default="/tmp/out")
    parser.add_argument(
        "--input-mode", choices=["historical", "landing"], default=os.environ.get("INPUT_MODE", "historical")
    )
    parser.add_argument("--epoch", default=os.environ.get("SOURCE_EPOCH"))
    parser.add_argument("--max-batches", type=int, default=int(os.environ.get("LANDING_MAX_BATCHES", "100")))
    args = parser.parse_args(argv)
    spark = None
    started_at = int(time.time() * 1000)
    try:
        if args.stage == "verify-source":
            from data_platform.inventory import verify_source

            manifest, digest = selected_manifest(args.source)
            if manifest["dataset_version"] != args.version:
                raise ValueError("selected manifest differs from requested version")
            totals = verify_source(args.source, manifest, args.timeout)
            print(json.dumps({"manifest_sha256": digest, "totals": totals, "outcome": "passed"}, sort_keys=True))
            return 0
        store = Store(args.evidence)
        contracts = hashes()
        if args.stage == "select" and args.input_mode == "landing":
            from data_platform.landing import select

            if args.max_batches < 1:
                raise ValueError("landing batch limit must be positive")
            manifest = select(Store(args.source), store, args.epoch, args.max_batches)
            if manifest is None:
                out = Path(args.out)
                out.mkdir(parents=True, exist_ok=True)
                for name, value in {"version": "none", "ingest": "false", "native": "false"}.items():
                    (out / name).write_text(value)
                return 0
            args.version = manifest["dataset_version"]
        if args.stage in ("preflight", "ingest", "select"):
            if args.input_mode == "landing":
                from data_platform.landing import selection

                manifest, digest = selection(store, args.version)
                if manifest["source_root"] != args.source or manifest["source_epoch"] != args.epoch:
                    raise ValueError("landing selection source/epoch differs")
            else:
                manifest, digest = selected_manifest(args.source)
            if manifest["dataset_version"] != args.version:
                raise ValueError("selected manifest differs from requested version")
            image = os.environ.get("IMAGE_DIGEST", "")
            if not re.fullmatch(r".+@sha256:[a-f0-9]{64}", image):
                raise ValueError("IMAGE_DIGEST must identify the immutable job image")
            run = open_run(
                store,
                args.version,
                digest,
                contracts,
                image,
                args.workflow,
                source_validation=manifest["validation"]
                if manifest.get("validation", {}).get("result") == "skipped"
                else None,
            )
            if args.input_mode == "landing":
                run["input_mode"] = "landing"
                run["selection_uri"] = f"{store.root}/selections/{args.version}.json"
                run["source_epoch"] = manifest["source_epoch"]
                run["changed_ports"] = [t for t, rows in manifest["totals"].items() if rows]
                run["batches"] = [{k: b[k] for k in ("key", "sha256", "range")} for b in manifest["batches"]]
            else:
                run["source_product"] = manifest["data_product"]
            run["source_root"] = args.source
            if args.stage == "select":
                run.setdefault("stages", {}).setdefault(
                    "select", {"outcome": "passed", "started_at": started_at, "ended_at": int(time.time() * 1000)}
                )
            save_run(store, run)
            if args.stage in ("preflight", "select"):
                out = Path(args.out)
                out.mkdir(parents=True, exist_ok=True)
                conservation = run.get("conservation", {})
                ingested = (
                    set(run["snapshots"]) == set(contracts)
                    and conservation.get("outcome") == "passed"
                    and conservation.get("snapshots") == run["snapshots"]
                    and conservation.get("manifest_sha256") == run["manifest_sha256"]
                )
                from data_platform.bronze_metadata import stage_matches

                native = ingested and all(
                    stage_matches(store.get(evidence_key(run, contracts, stage)), run)
                    for stage in ("discover", "validate", "profile")
                )
                for name, value in {
                    "version": args.version,
                    "ingest": str(not ingested).lower(),
                    "native": str(not native).lower(),
                }.items():
                    (out / name).write_text(value)
                print(json.dumps({"run_id": run["run_id"], "state": run["state"]}))
                return 0
        else:
            run = store.get(f"versions/{args.version}/run.json")
            if not run:
                raise ValueError("run context is missing")
        if args.stage == "ingest":
            application = os.environ.get("SPARK_APPLICATION_NAME")
            if application:
                attempts = run.setdefault("spark_applications", {}).setdefault(args.stage, [])
                if application not in attempts:
                    attempts.append(application)
                save_run(store, run)
            from data_platform.ingest import ingest
            from data_platform.spark import session

            spark = session(args.local_warehouse)
            ingest(spark, args.source, manifest, store, run)
            run.setdefault("stages", {}).setdefault(
                "ingest", {"outcome": "passed", "started_at": started_at, "ended_at": int(time.time() * 1000)}
            )
            save_run(store, run)
        return 0
    except Exception as error:
        if args.stage in ("preflight", "ingest", "select") and "run" in locals():
            failure = {
                "validation_policy": VALIDATION_POLICY,
                "run_id": run["run_id"],
                "dataset_version": run["dataset_version"],
                "manifest_sha256": run["manifest_sha256"],
                "contracts": contracts,
                "snapshots": run["snapshots"],
                "outcome": "error",
                "run_time": datetime.now(timezone.utc).isoformat(),
                "execution_errors": [{"stage": args.stage, "type": type(error).__name__}],
            }
            store.put(evidence_key(run, contracts, f"error-{args.stage}"), failure)
        print(
            json.dumps({"stage": args.stage, "error_type": type(error).__name__, "outcome": "error"}), file=sys.stderr
        )
        return 1
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
