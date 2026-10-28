import argparse
import json
import os
import re

from pipeline_common.storage import Store

from silver_pipeline.silver_runs import open_candidate, pin_input, verify_candidate


def main(argv=None):
    parser = argparse.ArgumentParser(prog="dp2")
    parser.add_argument(
        "--bronze-evidence",
        default=os.environ.get("BRONZE_EVIDENCE_ROOT"),
        required=not os.environ.get("BRONZE_EVIDENCE_ROOT"),
    )
    parser.add_argument(
        "--silver-evidence",
        default=os.environ.get("SILVER_EVIDENCE_ROOT"),
        required=not os.environ.get("SILVER_EVIDENCE_ROOT"),
    )
    parser.add_argument("--accepted-version", action="append")
    parser.add_argument("--candidate-id", default=os.environ.get("SILVER_CANDIDATE_ID"))
    parser.add_argument(
        "--image-digest", default=os.environ.get("IMAGE_DIGEST"), required=not os.environ.get("IMAGE_DIGEST")
    )
    parser.add_argument("--completeness", choices=("complete", "recurring"), required=True)
    parser.add_argument("--relative-error", type=float, default=0.05)
    parser.add_argument("--backfill-start")
    parser.add_argument("--backfill-end")
    args = parser.parse_args(argv)
    if not re.fullmatch(r".+@sha256:[a-f0-9]{64}", args.image_digest):
        parser.error("image digest must identify an immutable image")
    if bool(args.accepted_version) == bool(args.candidate_id):
        parser.error("select accepted versions or one prepared candidate")
    if bool(args.backfill_start) != bool(args.backfill_end):
        parser.error("backfill requires both start and end")
    boundaries = {"start": args.backfill_start, "end": args.backfill_end} if args.backfill_start else None
    from pipeline_common.spark import session

    from silver_pipeline.diagnostics import retain_diagnostics
    from silver_pipeline.silver_transform import persist_intermediates, transform
    from silver_pipeline.silver_write import write_candidate

    bronze = Store(args.bronze_evidence)
    silver = Store(args.silver_evidence)
    if args.candidate_id:
        if not re.fullmatch(r"[a-f0-9]{64}", args.candidate_id):
            parser.error("invalid candidate identity")
        candidate = silver.get(f"dp2/candidates/{args.candidate_id}.json")
        if (
            not candidate
            or candidate.get("candidate_id") != args.candidate_id
            or candidate.get("state") != "candidate"
            or candidate.get("transformation_revision") != args.image_digest
            or candidate.get("completeness") != args.completeness
            or (boundaries is not None and candidate.get("boundaries") != boundaries)
            or [pin_input(bronze, silver, item["dataset_version"]) for item in candidate["inputs"]]
            != candidate["inputs"]
        ):
            raise ValueError("prepared Silver candidate no longer matches accepted inputs and execution")
        verify_candidate(candidate)
    else:
        candidate = open_candidate(
            bronze, silver, args.accepted_version, args.image_digest, args.completeness, boundaries
        )
    spark = session()
    try:
        frames = transform(spark, bronze, silver, candidate, relative_error=args.relative_error)
        with persist_intermediates(frames):
            candidate = write_candidate(spark, silver, candidate, frames, args.completeness)
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
