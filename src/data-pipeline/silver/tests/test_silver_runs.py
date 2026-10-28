from copy import deepcopy
from io import BytesIO
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError
from pipeline_common.landing import state_key
from pipeline_common.products import SILVER_PORTS
from pipeline_common.storage import Store

from silver_pipeline.silver_contracts import hashes, product_hash, quality_hash
from silver_pipeline.silver_governance import publish
from silver_pipeline.silver_runs import IDENTITY_FIELDS, digest, open_candidate, pin_input


def accepted(version, snapshot):
    snapshots = {"payment_events": snapshot, "fraud_labels": snapshot + 1}
    manifest = "a" * 64
    run_id = f"run-{version}"
    conservation = {"outcome": "passed", "snapshots": snapshots, "manifest_sha256": manifest}
    return {
        "dataset_version": version,
        "state": "accepted",
        "run_id": run_id,
        "manifest_sha256": manifest,
        "source_root": "s3://example/historical",
        "snapshots": snapshots,
        "conservation": conservation,
        "result": {
            "outcome": "passed",
            "run_id": run_id,
            "manifest_sha256": manifest,
            "snapshots": snapshots,
            "expected_cases": 2,
            "problem_count": 0,
            "governance_gaps": [],
        },
    }


def test_candidate_pins_accepted_snapshots_and_replays_idempotently(tmp_path):
    bronze = Store(str(tmp_path / "bronze"))
    silver = Store(str(tmp_path / "silver"))
    bronze.put("accepted/first.json", accepted("first", 10))
    bronze.put("accepted/older.json", accepted("older", 20))
    candidate = open_candidate(bronze, silver, ["first"], "revision", "complete")
    assert candidate == open_candidate(bronze, silver, ["first"], "revision", "complete")
    assert candidate["inputs"][0]["snapshots"] == {"payment_events": 10, "fraud_labels": 11}
    older = open_candidate(bronze, silver, ["first", "older"], "revision", "complete")
    assert older["candidate_id"] != candidate["candidate_id"]
    assert [item["dataset_version"] for item in older["inputs"]] == ["first", "older"]
    assert silver.get("dp2/accepted.json") is None
    bad = deepcopy(accepted("bad", 30))
    bad["result"]["snapshots"] = {**bad["result"]["snapshots"], "fraud_labels": 999}
    bronze.put("accepted/bad.json", bad)
    with pytest.raises(ValueError, match="matching accepted snapshots"):
        pin_input(bronze, silver, "bad")
    with pytest.raises(ValueError, match="timezone-aware"):
        open_candidate(bronze, silver, ["first"], "revision", "complete", {"start": "2025-01-02", "end": "2025-01-01"})


def test_landing_ledger_version_remains_pinned_after_progress_advances(tmp_path):
    bronze = Store(str(tmp_path / "bronze"))
    silver = Store(str(tmp_path / "silver"))
    receipt = accepted("landing-first", 30)
    batch = {"key": "batch-1", "sha256": "b" * 64, "range": ["payments.events.v1", 0, 1, 2]}
    receipt.update(input_mode="landing", source_epoch="epoch", source_root="s3://example/landing", batches=[batch])
    bronze.put("accepted/landing-first.json", receipt)
    ledger = {
        "source_root": receipt["source_root"],
        "accepted": {batch["key"]: batch["sha256"]},
        "ranges": [batch["range"]],
    }
    bronze.put(state_key("epoch"), ledger)
    first = pin_input(bronze, silver, "landing-first")
    ledger["accepted"]["batch-2"] = "c" * 64
    ledger["ranges"].append(["payments.events.v1", 0, 3, 4])
    bronze.put(state_key("epoch"), ledger)
    assert pin_input(bronze, silver, "landing-first") == first
    assert silver.get(first["landing_ledger_key"])["accepted"] == {"batch-1": "b" * 64}


def test_acceptance_pointer_rejects_stale_updates(tmp_path):
    store = Store(str(tmp_path))
    first = {"candidate_id": "first"}
    second = {"candidate_id": "second"}
    assert store.compare_and_swap("dp2/accepted.json", None, first)
    assert not store.compare_and_swap("dp2/accepted.json", None, second)
    assert store.get("dp2/accepted.json") == first
    assert store.compare_and_swap("dp2/accepted.json", first, second)
    assert not store.compare_and_swap("dp2/accepted.json", first, {"candidate_id": "stale"})
    assert store.get("dp2/accepted.json") == second


def test_remote_pointer_uses_conditional_writes_and_rejects_conflicts():
    store = Store.__new__(Store)
    store.bucket, store.prefix, store.client = "bucket", "evidence", Mock()
    store.client.get_object.return_value = {"Body": BytesIO(b'{"candidate_id":"first"}'), "ETag": '"etag"'}
    assert store.compare_and_swap("dp2/accepted.json", {"candidate_id": "first"}, {"candidate_id": "second"})
    assert store.client.put_object.call_args.kwargs["IfMatch"] == '"etag"'
    store.client.get_object.return_value = {"Body": BytesIO(b'{"candidate_id":"first"}'), "ETag": '"etag"'}
    store.client.put_object.side_effect = ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
    assert not store.compare_and_swap("dp2/accepted.json", {"candidate_id": "first"}, {"candidate_id": "second"})
    store.client.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    store.client.put_object.side_effect = None
    assert store.compare_and_swap("dp2/accepted.json", None, {"candidate_id": "first"})
    assert store.client.put_object.call_args.kwargs["IfNoneMatch"] == "*"


def test_missing_native_evidence_cannot_replace_accepted_pointer(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_ID", "workflow")
    store = Store(str(tmp_path))
    previous = {"candidate_id": "previous", "receipt_key": "previous.json", "receipt_sha256": "old"}
    store.put("dp2/accepted.json", previous)
    candidate = {
        "candidate_id": "a" * 64,
        "state": "candidate",
        "contracts": hashes(),
        "product_sha256": product_hash(),
        "quality_sha256": quality_hash(),
        "completeness": "complete",
        "boundaries": None,
        "transformation_revision": "revision",
        "snapshots": {table: index + 10 for index, table in enumerate(SILVER_PORTS)},
        "inputs": [{"dataset_version": "old", "snapshots": {"payment_events": 100, "fraud_labels": 101}}],
    }
    candidate["candidate_id"] = digest({field: candidate[field] for field in IDENTITY_FIELDS})
    with pytest.raises(ValueError, match="native rule identities"):
        publish(store, candidate)
    assert store.get("dp2/accepted.json") == previous
    assert store.get("dp2/publication-lock.json") is None
