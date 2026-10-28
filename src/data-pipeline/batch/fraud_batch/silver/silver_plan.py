import os
import re
from pathlib import Path

from fraud_batch.common.storage import Store
from fraud_batch.silver.silver_runs import digest, open_candidate


def main():
    bronze_evidence = os.environ["BRONZE_EVIDENCE_ROOT"]
    silver_evidence = os.environ["SILVER_EVIDENCE_ROOT"]
    accepted_versions = os.environ.get("SILVER_ACCEPTED_VERSIONS", "")
    merge_current = os.environ.get("SILVER_MERGE_CURRENT", "false")
    if merge_current not in ("true", "false"):
        raise ValueError("SILVER_MERGE_CURRENT must be true or false")
    completeness = os.environ["SILVER_COMPLETENESS"]
    if completeness not in ("complete", "recurring"):
        raise ValueError("SILVER_COMPLETENESS must be complete or recurring")
    backfill_start = os.environ.get("SILVER_BACKFILL_START")
    backfill_end = os.environ.get("SILVER_BACKFILL_END")
    image_digest = os.environ["IMAGE_DIGEST"]
    out = os.environ.get("PIPELINE_OUTPUT", "/tmp/out")
    if not re.fullmatch(r".+@sha256:[a-f0-9]{64}", image_digest):
        raise ValueError("image digest must identify an immutable image")
    if bool(backfill_start) != bool(backfill_end):
        raise ValueError("backfill requires both start and end")
    boundaries = {"start": backfill_start, "end": backfill_end} if backfill_start else None
    bronze, silver = Store(bronze_evidence), Store(silver_evidence)
    versions = {value.strip() for value in accepted_versions.split(",") if value.strip()}
    pointer = silver.get("dp2/accepted.json")
    if pointer and (merge_current == "true" or completeness == "recurring"):
        receipt = silver.get(pointer["receipt_key"])
        if not receipt or digest(receipt) != pointer["receipt_sha256"] or receipt.get("outcome") != "passed":
            raise ValueError("current Silver acceptance receipt is missing or altered")
        previous = {item["dataset_version"] for item in receipt["inputs"]}
        if merge_current == "true":
            versions.update(previous)
        elif not previous.issubset(versions):
            raise ValueError("recurring Silver input cannot omit previously accepted Bronze receipts")
    if not versions:
        raise ValueError("direct Silver execution requires accepted Bronze input")
    candidate = open_candidate(bronze, silver, sorted(versions), image_digest, completeness, boundaries)
    should_run = not pointer or pointer.get("candidate_id") != candidate["candidate_id"]
    if candidate.get("state") != "candidate" and should_run:
        raise ValueError("selected Silver candidate is already accepted but not current")
    output = Path(out)
    output.mkdir(parents=True, exist_ok=True)
    (output / "candidate-id").write_text(candidate["candidate_id"])
    (output / "run").write_text("true" if should_run else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
