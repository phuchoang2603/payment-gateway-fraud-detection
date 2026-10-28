from datetime import datetime, timezone
from hashlib import sha256
from uuid import uuid4

from fraud_batch.common.products import PORTS

VALIDATION_POLICY = "bronze-catalog-v4"
EXECUTION = {"gate": "dp1-starrocks-openmetadata", "policy": VALIDATION_POLICY, "engine": "starrocks"}


def validation_key(contracts):
    return sha256((VALIDATION_POLICY + "".join(f"{k}:{v}" for k, v in sorted(contracts.items()))).encode()).hexdigest()


def open_run(store, version, digest, contracts, image, workflow, source_validation=None):
    key = f"versions/{version}/run.json"
    run = store.get(key)
    if run:
        if run["manifest_sha256"] != digest:
            raise ValueError("dataset version already has a different manifest digest")
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
    if source_validation:
        run["source_validation"] = source_validation
        run["validation_execution"] = EXECUTION
    store.put(key, run, create=True)
    return run


def save_run(store, run):
    store.put(f"versions/{run['dataset_version']}/run.json", run)


def evidence_key(run, contracts, name):
    return f"runs/{run['run_id']}/{validation_key(contracts)}/{name}.json"


def accept(store, run, contracts, problems, gaps):
    if set(run["snapshots"]) != set(PORTS):
        raise ValueError("acceptance requires both committed snapshots")
    expected = store.get(evidence_key(run, contracts, "expected"))
    if expected is None or expected["snapshots"] != run["snapshots"] or expected["run_id"] != run["run_id"]:
        raise ValueError("acceptance requires the prepared native rule set for this run")
    conservation = run.get("conservation", {})
    if (
        conservation.get("outcome") != "passed"
        or conservation.get("snapshots") != run["snapshots"]
        or conservation.get("manifest_sha256") != run["manifest_sha256"]
    ):
        raise ValueError("acceptance requires matching persisted content conservation")
    outcome = {
        "conservation": conservation,
        "validation_policy": VALIDATION_POLICY,
        "run_id": run["run_id"],
        "manifest_sha256": run["manifest_sha256"],
        "snapshots": run["snapshots"],
        "contracts": contracts,
        "expected_cases": len(expected["cases"]),
        "problems": dict(sorted(problems.items())[:50]),
        "problem_count": len(problems),
        "governance_gaps": gaps,
        "outcome": "passed" if not problems and not gaps else "failed",
    }
    store.put(evidence_key(run, contracts, "result"), outcome)
    run["state"] = "accepted" if outcome["outcome"] == "passed" else "validation_failed"
    save_run(store, run)
    if outcome["outcome"] != "passed":
        raise ValueError("native results are not a complete passing set")
    record = {**run, "validation_policy": VALIDATION_POLICY, "contracts": contracts, "result": outcome}
    store.put(f"accepted/{run['dataset_version']}.json", record)
    if run.get("input_mode") == "landing":
        from fraud_batch.bronze.landing import advance

        advance(store, run)
    return record
