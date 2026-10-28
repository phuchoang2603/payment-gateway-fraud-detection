import os
from pathlib import Path

from fraud_batch.common.images import immutable_image
from fraud_batch.common.storage import Store
from fraud_batch.silver.silver_runs import open_candidate


def main():
    bronze_evidence = os.environ["BRONZE_EVIDENCE_ROOT"]
    silver_evidence = os.environ["SILVER_EVIDENCE_ROOT"]
    accepted_versions = os.environ.get("SILVER_ACCEPTED_VERSIONS", "")
    completeness = os.environ["SILVER_COMPLETENESS"]
    if completeness not in ("complete", "recurring"):
        raise ValueError("SILVER_COMPLETENESS must be complete or recurring")
    image = immutable_image(os.environ["IMAGE_REFERENCE"])
    out = os.environ.get("PIPELINE_OUTPUT", "/tmp/out")
    bronze, silver = Store(bronze_evidence), Store(silver_evidence)
    versions = {value.strip() for value in accepted_versions.split(",") if value.strip()}
    pointer = silver.get("dp2/accepted.json")
    if not versions:
        raise ValueError("direct Silver execution requires accepted Bronze input")
    candidate = open_candidate(bronze, silver, sorted(versions), image, completeness)
    should_run = not pointer or pointer.get("candidate_id") != candidate["candidate_id"]
    if candidate.get("state") != "candidate" and should_run:
        raise ValueError("selected Silver candidate is already accepted but not current")
    silver.put("spark-events/.keep", {})
    output = Path(out)
    output.mkdir(parents=True, exist_ok=True)
    (output / "candidate-id").write_text(candidate["candidate_id"])
    (output / "run").write_text("true" if should_run else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
