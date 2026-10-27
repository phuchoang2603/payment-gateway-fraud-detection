import json
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from hashlib import sha256
from uuid import uuid4

from data_platform.landing import state_key
from data_platform.products import PORTS
from data_platform.silver_contracts import hashes, product_hash, quality_hash

IDENTITY_FIELDS = (
    "inputs",
    "transformation_revision",
    "contracts",
    "product_sha256",
    "quality_sha256",
    "completeness",
    "boundaries",
)


def digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return sha256(encoded).hexdigest()


def verify_candidate(candidate):
    if any(field not in candidate for field in IDENTITY_FIELDS):
        raise ValueError("Silver candidate lacks immutable identity fields")
    if digest({field: candidate[field] for field in IDENTITY_FIELDS}) != candidate.get("candidate_id"):
        raise ValueError("Silver candidate identity differs from its pinned inputs")


@contextmanager
def publication_lock(store, candidate_id, workflow_id, takeover_stopped=False):
    key = "dp2/publication-lock.json"
    owner = {"token": str(uuid4()), "candidate_id": candidate_id, "workflow": workflow_id}
    previous = store.get(key)
    if previous is not None and not takeover_stopped:
        raise ValueError("another Silver publication owns the lock; stop it before explicit takeover")
    if not store.compare_and_swap(key, previous, owner):
        raise ValueError("Silver publication lock changed")
    try:
        yield
    finally:
        if not store.compare_and_swap(key, owner, None):
            raise RuntimeError("Silver publication lock ownership changed")


def pin_input(bronze_store, silver_store, version):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", version):
        raise ValueError("invalid accepted input version")
    receipt = bronze_store.get(f"accepted/{version}.json")
    if not receipt or receipt.get("dataset_version") != version or receipt.get("state") != "accepted":
        raise ValueError("Silver requires an accepted Bronze receipt")
    snapshots = receipt.get("snapshots", {})
    result = receipt.get("result", {})
    conservation = receipt.get("conservation", {})
    if (
        set(snapshots) != set(PORTS)
        or any(type(value) is not int or value <= 0 for value in snapshots.values())
        or result.get("outcome") != "passed"
        or result.get("run_id") != receipt.get("run_id")
        or result.get("snapshots") != snapshots
        or result.get("manifest_sha256") != receipt.get("manifest_sha256")
        or result.get("problem_count") != 0
        or result.get("governance_gaps") != []
        or type(result.get("expected_cases")) is not int
        or result["expected_cases"] < 1
        or conservation.get("outcome") != "passed"
        or conservation.get("snapshots") != snapshots
        or conservation.get("manifest_sha256") != receipt.get("manifest_sha256")
        or not isinstance(receipt.get("run_id"), str)
        or not receipt["run_id"]
        or not isinstance(receipt.get("source_root"), str)
        or not receipt["source_root"]
        or not re.fullmatch(r"[a-f0-9]{64}", str(receipt.get("manifest_sha256", "")))
    ):
        raise ValueError("Bronze receipt lacks matching accepted snapshots and conservation")
    receipt_hash = digest(receipt)
    receipt_key = f"dp2/bronze-receipts/{receipt_hash}.json"
    existing_receipt = silver_store.get(receipt_key)
    if existing_receipt is None:
        silver_store.put(receipt_key, receipt, create=True)
    elif existing_receipt != receipt:
        raise ValueError("immutable Bronze receipt identity changed")
    pinned = {
        "dataset_version": version,
        "manifest_sha256": receipt["manifest_sha256"],
        "run_id": receipt["run_id"],
        "snapshots": snapshots,
        "receipt_sha256": receipt_hash,
        "receipt_key": receipt_key,
        "source_root": receipt.get("source_root"),
        "input_mode": receipt.get("input_mode", "historical"),
    }
    if pinned["input_mode"] == "landing":
        epoch = receipt.get("source_epoch")
        if not isinstance(epoch, str):
            raise ValueError("accepted landing input lacks a source epoch")
        progress = bronze_store.get(state_key(epoch))
        if not progress:
            raise ValueError("accepted landing input lacks its progress ledger")
        batches = receipt.get("batches", [])
        if (
            not batches
            or progress.get("source_root") != receipt["source_root"]
            or any(
                progress.get("accepted", {}).get(batch["key"]) != batch["sha256"]
                or batch["range"] not in progress.get("ranges", [])
                for batch in batches
            )
        ):
            raise ValueError("accepted landing batches are absent from the progress ledger")
        binding_key = f"dp2/accepted-ledgers/{pinned['receipt_sha256']}.json"
        binding = silver_store.get(binding_key)
        if binding is None:
            ledger = {name: progress[name] for name in ("source_root", "accepted", "ranges")}
            ledger_hash = digest(ledger)
            key = f"dp2/ledgers/{ledger_hash}.json"
            existing = silver_store.get(key)
            if existing is None:
                silver_store.put(key, ledger, create=True)
            elif existing != ledger:
                raise ValueError("immutable landing ledger identity changed")
            binding = {"landing_ledger_sha256": ledger_hash, "landing_ledger_key": key}
            silver_store.put(binding_key, binding, create=True)
        ledger = silver_store.get(binding["landing_ledger_key"])
        if ledger is None or digest(ledger) != binding["landing_ledger_sha256"]:
            raise ValueError("pinned landing ledger is missing or altered")
        if any(ledger.get("accepted", {}).get(batch["key"]) != batch["sha256"] for batch in batches):
            raise ValueError("pinned landing ledger does not contain the accepted batches")
        pinned.update(source_epoch=epoch, batches=batches, **binding)
    elif pinned["input_mode"] != "historical":
        raise ValueError("unsupported accepted Bronze input mode")
    return pinned


def open_candidate(bronze_store, silver_store, versions, transformation_revision, completeness, boundaries=None):
    if not versions or len(set(versions)) != len(versions) or not transformation_revision:
        raise ValueError("candidate requires unique accepted inputs and a transformation revision")
    if completeness not in ("complete", "recurring"):
        raise ValueError("candidate requires an explicit completeness boundary")
    if boundaries is not None:
        if not isinstance(boundaries, dict) or set(boundaries) != {"start", "end"}:
            raise ValueError("backfill requires explicit start and end boundaries")
        try:
            start, end = (datetime.fromisoformat(boundaries[name].replace("Z", "+00:00")) for name in ("start", "end"))
            if start.utcoffset() is None or end.utcoffset() is None or start >= end:
                raise ValueError
        except (AttributeError, TypeError, ValueError):
            raise ValueError("backfill boundaries must be ordered timezone-aware timestamps") from None
    selected = [pin_input(bronze_store, silver_store, version) for version in sorted(versions)]
    identity = {
        "inputs": selected,
        "transformation_revision": transformation_revision,
        "contracts": hashes(),
        "product_sha256": product_hash(),
        "quality_sha256": quality_hash(),
        "completeness": completeness,
        "boundaries": boundaries,
    }
    candidate_id = digest(identity)
    key = f"dp2/candidates/{candidate_id}.json"
    candidate = silver_store.get(key)
    if candidate is None:
        candidate = {
            "candidate_id": candidate_id,
            **identity,
            "snapshots": {},
            "state": "candidate",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        silver_store.put(key, candidate, create=True)
    elif any(candidate.get(name) != value for name, value in identity.items()):
        raise ValueError("candidate identity changed")
    return candidate
