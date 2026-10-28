import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from fraud_batch.bronze.runs import VALIDATION_POLICY, evidence_key, open_run, save_run
from fraud_batch.common.images import immutable_image
from fraud_batch.common.manifest import selected_manifest
from fraud_batch.common.products import hashes
from fraud_batch.common.storage import Store

STAGES = ["preflight", "ingest", "verify-source", "select"]


def main():
    logging.disable(logging.INFO)
    stage = os.environ["PIPELINE_STAGE"]
    if stage not in STAGES:
        raise ValueError("unsupported Bronze pipeline stage")
    source = os.environ["SOURCE_ROOT"]
    evidence = os.environ["EVIDENCE_ROOT"]
    version = os.environ["DATASET_VERSION"]
    workflow = os.environ.get("WORKFLOW_ID", "local")
    timeout = int(os.environ.get("SOURCE_VERIFY_TIMEOUT", "900"))
    local_warehouse = os.environ.get("LOCAL_WAREHOUSE")
    out = os.environ.get("PIPELINE_OUTPUT", "/tmp/out")
    input_mode = os.environ.get("INPUT_MODE", "historical")
    if input_mode not in ("historical", "landing"):
        raise ValueError("INPUT_MODE must be historical or landing")
    epoch = os.environ.get("SOURCE_EPOCH", "")
    max_batches = int(os.environ.get("LANDING_MAX_BATCHES", "100"))
    spark = None
    started_at = int(time.time() * 1000)
    try:
        if stage == "verify-source":
            from fraud_batch.bronze.inventory import verify_source

            manifest, digest = selected_manifest(source)
            if manifest["dataset_version"] != version:
                raise ValueError("selected manifest differs from requested version")
            totals = verify_source(source, manifest, timeout)
            print(json.dumps({"manifest_sha256": digest, "totals": totals, "outcome": "passed"}, sort_keys=True))
            return 0
        store = Store(evidence)
        contracts = hashes()
        if stage == "select" and input_mode == "landing":
            from fraud_batch.bronze.landing import select

            if max_batches < 1:
                raise ValueError("landing batch limit must be positive")
            manifest = select(Store(source), store, epoch, max_batches)
            if manifest is None:
                out = Path(out)
                out.mkdir(parents=True, exist_ok=True)
                for name, value in {"version": "none", "ingest": "false", "native": "false"}.items():
                    (out / name).write_text(value)
                return 0
            version = manifest["dataset_version"]
        if stage in ("preflight", "ingest", "select"):
            if input_mode == "landing":
                from fraud_batch.bronze.landing import selection

                manifest, digest = selection(store, version)
                if manifest["source_root"] != source or manifest["source_epoch"] != epoch:
                    raise ValueError("landing selection source/epoch differs")
            else:
                manifest, digest = selected_manifest(source)
            if manifest["dataset_version"] != version:
                raise ValueError("selected manifest differs from requested version")
            image = immutable_image(os.environ.get("IMAGE_REFERENCE", ""))
            run = open_run(
                store,
                version,
                digest,
                contracts,
                image,
                workflow,
                source_validation=manifest["validation"]
                if manifest.get("validation", {}).get("result") == "skipped"
                else None,
            )
            if input_mode == "landing":
                run["input_mode"] = "landing"
                run["selection_uri"] = f"{store.root}/selections/{version}.json"
                run["source_epoch"] = manifest["source_epoch"]
                run["changed_ports"] = [t for t, rows in manifest["totals"].items() if rows]
                run["batches"] = [{k: b[k] for k in ("key", "sha256", "range")} for b in manifest["batches"]]
            else:
                run["source_product"] = manifest["data_product"]
            run["source_root"] = source
            if stage == "select":
                run.setdefault("stages", {}).setdefault(
                    "select", {"outcome": "passed", "started_at": started_at, "ended_at": int(time.time() * 1000)}
                )
            save_run(store, run)
            if stage in ("preflight", "select"):
                out = Path(out)
                out.mkdir(parents=True, exist_ok=True)
                conservation = run.get("conservation", {})
                ingested = (
                    set(run["snapshots"]) == set(contracts)
                    and conservation.get("outcome") == "passed"
                    and conservation.get("snapshots") == run["snapshots"]
                    and conservation.get("manifest_sha256") == run["manifest_sha256"]
                )
                from fraud_batch.bronze.bronze_metadata import stage_matches

                native = ingested and all(
                    stage_matches(store.get(evidence_key(run, contracts, stage)), run)
                    for stage in ("discover", "validate", "profile")
                )
                for name, value in {
                    "version": version,
                    "ingest": str(not ingested).lower(),
                    "native": str(not native).lower(),
                }.items():
                    (out / name).write_text(value)
                print(json.dumps({"run_id": run["run_id"], "state": run["state"]}))
                return 0
        else:
            run = store.get(f"versions/{version}/run.json")
            if not run:
                raise ValueError("run context is missing")
        if stage == "ingest":
            application = os.environ.get("SPARK_APPLICATION_NAME")
            if application:
                attempts = run.setdefault("spark_applications", {}).setdefault(stage, [])
                if application not in attempts:
                    attempts.append(application)
                save_run(store, run)
            from fraud_batch.bronze.ingest import ingest
            from fraud_batch.common.spark import session

            spark = session(local_warehouse)
            ingest(spark, source, manifest, store, run)
            run.setdefault("stages", {}).setdefault(
                "ingest", {"outcome": "passed", "started_at": started_at, "ended_at": int(time.time() * 1000)}
            )
            save_run(store, run)
        return 0
    except Exception as error:
        if stage in ("preflight", "ingest", "select") and "run" in locals():
            failure = {
                "validation_policy": VALIDATION_POLICY,
                "run_id": run["run_id"],
                "dataset_version": run["dataset_version"],
                "manifest_sha256": run["manifest_sha256"],
                "contracts": contracts,
                "snapshots": run["snapshots"],
                "outcome": "error",
                "run_time": datetime.now(timezone.utc).isoformat(),
                "execution_errors": [{"stage": stage, "type": type(error).__name__}],
            }
            store.put(evidence_key(run, contracts, f"error-{stage}"), failure)
        print(json.dumps({"stage": stage, "error_type": type(error).__name__, "outcome": "error"}), file=sys.stderr)
        return 1
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
