from datetime import datetime, timezone
from hashlib import sha256
from uuid import uuid4

from data_platform.products import PORTS


def validation_key(contracts):
    return sha256("".join(f"{k}:{v}" for k, v in sorted(contracts.items())).encode()).hexdigest()


def open_run(store, version, digest, contracts, image, workflow, source_inventory=None, source_validation=None):
    key = f"versions/{version}/run.json"
    run = store.get(key)
    if run:
        if run["manifest_sha256"] != digest:
            raise ValueError("dataset version already has a different manifest digest")
        if run.get("source_inventory") != source_inventory:
            raise ValueError("dataset version already has a different source inventory pin")
        if run.get("source_validation") != source_validation:
            raise ValueError("dataset version already has a different source validation gate")
        return run
    run = {
        "dataset_version": version,
        "manifest_sha256": digest,
        "run_id": str(uuid4()),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "image": image,
        "workflow": workflow,
        "ingestion_contracts": contracts,
        "snapshots": {},
        "state": "candidate",
    }
    if source_inventory:
        run["source_inventory"] = source_inventory
    if source_validation:
        run["source_validation"] = source_validation
    store.put(key, run, create=True)
    return run


def save_run(store, run):
    store.put(f"versions/{run['dataset_version']}/run.json", run)


def report_key(run, contracts):
    return f"runs/{run['run_id']}/{validation_key(contracts)}/validation.json"


def accept(store, run, contracts):
    if set(run["snapshots"]) != set(PORTS):
        raise ValueError("acceptance requires both committed snapshots")
    report = store.get(report_key(run, contracts))
    if report is None or report.get("data_outcome") != "passed" or report.get("execution_errors"):
        raise ValueError("acceptance requires a persisted passing report")
    if report.get("snapshots") != run["snapshots"] or report.get("manifest_sha256") != run["manifest_sha256"]:
        raise ValueError("validation identity differs from candidate")
    if report.get("source_inventory") != run.get("source_inventory"):
        raise ValueError("validation source inventory differs from candidate")
    if report.get("source_validation") != run.get("source_validation"):
        raise ValueError("validation source gate differs from candidate")
    if run.get("source_validation") and (
        set(report.get("tables", {})) != set(PORTS)
        or not all(table.get("success") is True for table in report["tables"].values())
    ):
        raise ValueError("deferred source validation requires both passing Spark/GX table results")
    governance = store.get(report_key(run, contracts).replace("validation.json", "governance.json"))
    if not governance or governance.get("outcome") != "passed" or not governance.get("readback_verified"):
        raise ValueError("acceptance requires verified governance readback")
    record = {
        **run,
        "contracts": contracts,
        "validation_report": report_key(run, contracts),
        "governance": governance,
        "state": "accepted",
    }
    store.put(f"accepted/{run['dataset_version']}.json", record)
    run["state"] = "accepted"
    save_run(store, run)
    return record
