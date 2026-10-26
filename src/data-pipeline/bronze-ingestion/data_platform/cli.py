import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from data_platform import quality
from data_platform.manifest import selected_manifest
from data_platform.products import hashes
from data_platform.runs import VALIDATION_POLICY, accept, evidence_key, open_run, save_run
from data_platform.storage import Store

STAGES = ["preflight", "ingest", "prepare", "governance", "accept", "verify-source", "select"]


def prepare(store, run, contracts, manifest, out):
    key = evidence_key(run, contracts, "expected")
    cases = quality.cases(run, manifest)
    prepared = store.get(key)
    if prepared is None:
        prepared = {**quality.expected(run, contracts, cases), "prepared_at": int(time.time() * 1000)}
        store.put(key, prepared, create=True)
    elif prepared["cases"] != quality.expected(run, contracts, cases)["cases"]:
        raise ValueError("prepared native rule set differs from the contracts for this run")
    out.mkdir(parents=True, exist_ok=True)
    (out / "metadata.json").write_text(quality.dumps(quality.metadata_workflow()))
    (out / "profiler.json").write_text(quality.dumps(quality.profiler_workflow()))
    for table in ("payment_events", "fraud_labels"):
        (out / f"{table}.json").write_text(quality.dumps(quality.test_workflow(table, cases)))
    return prepared


def main(argv=None):
    logging.disable(logging.INFO)
    parser = argparse.ArgumentParser(prog="dp1")
    parser.add_argument("stage", choices=STAGES)
    parser.add_argument("--source", default=os.environ.get("SOURCE_ROOT", "s3://synthetic-source/historical/v16"))
    parser.add_argument(
        "--evidence", default=os.environ.get("EVIDENCE_ROOT", "s3://fraud-lakehouse-prod/dp1-evidence")
    )
    parser.add_argument("--version", default=os.environ.get("DATASET_VERSION", "v16"))
    parser.add_argument("--workflow", default=os.environ.get("WORKFLOW_ID", "local"))
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--local-warehouse")
    parser.add_argument("--out", default="/tmp/out")
    parser.add_argument(
        "--input-mode", choices=["historical", "landing"], default=os.environ.get("INPUT_MODE", "historical")
    )
    parser.add_argument("--epoch", default=os.environ.get("SOURCE_EPOCH", "v16-20261026"))
    parser.add_argument("--max-batches", type=int, default=int(os.environ.get("LANDING_MAX_BATCHES", "100")))
    parser.add_argument("--table-ids", default=os.environ.get("TABLE_IDS", "{}"))
    args = parser.parse_args(argv)
    spark = None
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
        if args.stage == "select":
            from data_platform.landing import select

            if args.max_batches < 1:
                raise ValueError("landing batch limit must be positive")
            manifest = select(Store(args.source), store, args.epoch, args.max_batches)
            out = Path(args.out)
            out.mkdir(parents=True, exist_ok=True)
            (out / "version").write_text(manifest["dataset_version"] if manifest else "none")
            print(json.dumps({"input": bool(manifest), "version": manifest["dataset_version"] if manifest else None}))
            return 0
        if args.stage in ("preflight", "ingest"):
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
                run["source_epoch"] = manifest["source_epoch"]
                run["changed_ports"] = [t for t, rows in manifest["totals"].items() if rows]
                run["batches"] = [{k: b[k] for k in ("key", "sha256", "range")} for b in manifest["batches"]]
            else:
                run["source_product"] = manifest["data_product"]
            run["source_root"] = args.source
            save_run(store, run)
            if args.stage == "preflight":
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
        elif args.stage == "prepare":
            if run.get("input_mode") == "landing":
                from data_platform.landing import selection

                manifest, digest = selection(store, args.version)
            else:
                manifest, digest = selected_manifest(run["source_root"])
            if digest != run["manifest_sha256"]:
                raise ValueError("source manifest changed since ingestion")
            prepare(store, run, contracts, manifest, Path(args.out))
        elif args.stage == "governance":
            from data_platform.governance import requests

            report = f"{store.root}/{evidence_key(run, contracts, 'result')}"
            writes = requests(run, json.loads(args.table_ids), report)
            print(json.dumps({name: {**write, "body": json.dumps(write["body"])} for name, write in writes.items()}))
        elif args.stage == "accept":
            from data_platform import readback

            expected = store.get(evidence_key(run, contracts, "expected"))
            found = {}
            for table in ("payment_events", "fraud_labels"):
                found.update(readback.observed(table))
            problems = readback.verify(expected, found, expected["prepared_at"])
            gaps = readback.governance_gaps(("payment_events", "fraud_labels"), contracts)
            accept(store, run, contracts, problems, gaps)
        return 0
    except Exception as error:
        if args.stage in ("preflight", "ingest", "prepare", "accept") and "run" in locals():
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
            if args.stage != "accept" or not isinstance(error, ValueError):
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
