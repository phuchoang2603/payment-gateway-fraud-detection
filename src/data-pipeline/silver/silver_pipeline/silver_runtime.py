import json
import os
import re

from pipeline_common.storage import Store

from silver_pipeline.silver_runs import open_candidate, pin_input, verify_candidate


def main():
    bronze_evidence = os.environ["BRONZE_EVIDENCE_ROOT"]
    silver_evidence = os.environ["SILVER_EVIDENCE_ROOT"]
    accepted_version = [
        value.strip() for value in os.environ.get("SILVER_ACCEPTED_VERSIONS", "").split(",") if value.strip()
    ]
    candidate_id = os.environ.get("SILVER_CANDIDATE_ID")
    image_digest = os.environ["IMAGE_DIGEST"]
    completeness = os.environ["SILVER_COMPLETENESS"]
    if completeness not in ("complete", "recurring"):
        raise ValueError("SILVER_COMPLETENESS must be complete or recurring")
    relative_error = float(os.environ.get("SILVER_RELATIVE_ERROR", "0.05"))
    backfill_start = os.environ.get("SILVER_BACKFILL_START")
    backfill_end = os.environ.get("SILVER_BACKFILL_END")
    if not re.fullmatch(r".+@sha256:[a-f0-9]{64}", image_digest):
        raise ValueError("image digest must identify an immutable image")
    if bool(accepted_version) == bool(candidate_id):
        raise ValueError("select accepted versions or one prepared candidate")
    if bool(backfill_start) != bool(backfill_end):
        raise ValueError("backfill requires both start and end")
    boundaries = {"start": backfill_start, "end": backfill_end} if backfill_start else None
    from pipeline_common.spark import session

    from silver_pipeline.diagnostics import retain_diagnostics
    from silver_pipeline.silver_transform import persist_intermediates, transform
    from silver_pipeline.silver_write import write_candidate

    bronze = Store(bronze_evidence)
    silver = Store(silver_evidence)
    if candidate_id:
        if not re.fullmatch(r"[a-f0-9]{64}", candidate_id):
            raise ValueError("invalid candidate identity")
        candidate = silver.get(f"dp2/candidates/{candidate_id}.json")
        if (
            not candidate
            or candidate.get("candidate_id") != candidate_id
            or candidate.get("state") != "candidate"
            or candidate.get("transformation_revision") != image_digest
            or candidate.get("completeness") != completeness
            or (boundaries is not None and candidate.get("boundaries") != boundaries)
            or [pin_input(bronze, silver, item["dataset_version"]) for item in candidate["inputs"]]
            != candidate["inputs"]
        ):
            raise ValueError("prepared Silver candidate no longer matches accepted inputs and execution")
        verify_candidate(candidate)
    else:
        candidate = open_candidate(bronze, silver, accepted_version, image_digest, completeness, boundaries)
    spark = session()
    try:
        frames = transform(spark, bronze, silver, candidate, relative_error=relative_error)
        with persist_intermediates(frames):
            candidate = write_candidate(spark, silver, candidate, frames, completeness)
            diagnostics = retain_diagnostics(silver, candidate, frames)
        candidate["diagnostics"] = diagnostics
        silver.put(f"dp2/candidates/{candidate['candidate_id']}.json", candidate)
    finally:
        spark.stop()
    print(
        json.dumps(
            {
                "candidate_id": candidate["candidate_id"],
                "state": candidate["state"],
                "snapshots": candidate["snapshots"],
                "diagnostics": diagnostics,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
