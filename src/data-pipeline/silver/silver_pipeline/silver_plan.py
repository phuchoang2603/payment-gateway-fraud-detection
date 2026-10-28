import argparse
import os
import re
from pathlib import Path

from data_platform.storage import Store

from silver_pipeline.silver_runs import digest, open_candidate


def main(argv=None):
    parser = argparse.ArgumentParser(prog="dp2-plan")
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
    parser.add_argument("--accepted-versions", default="")
    parser.add_argument("--selected-version", default="none")
    parser.add_argument("--require-input", choices=("true", "false"), default="false")
    parser.add_argument("--merge-current", choices=("true", "false"), default="false")
    parser.add_argument("--completeness", choices=("complete", "recurring"), required=True)
    parser.add_argument("--backfill-start", default="")
    parser.add_argument("--backfill-end", default="")
    parser.add_argument(
        "--image-digest", default=os.environ.get("IMAGE_DIGEST"), required=not os.environ.get("IMAGE_DIGEST")
    )
    parser.add_argument("--out", default="/tmp/out")
    args = parser.parse_args(argv)
    if not re.fullmatch(r".+@sha256:[a-f0-9]{64}", args.image_digest):
        parser.error("image digest must identify an immutable image")
    if bool(args.backfill_start) != bool(args.backfill_end):
        parser.error("backfill requires both start and end")
    boundaries = {"start": args.backfill_start, "end": args.backfill_end} if args.backfill_start else None
    bronze, silver = Store(args.bronze_evidence), Store(args.silver_evidence)
    versions = {value.strip() for value in args.accepted_versions.split(",") if value.strip()}
    if args.selected_version != "none":
        versions.add(args.selected_version)
    pointer = silver.get("dp2/accepted.json")
    if pointer and (args.merge_current == "true" or args.completeness == "recurring"):
        receipt = silver.get(pointer["receipt_key"])
        if not receipt or digest(receipt) != pointer["receipt_sha256"] or receipt.get("outcome") != "passed":
            raise ValueError("current Silver acceptance receipt is missing or altered")
        previous = {item["dataset_version"] for item in receipt["inputs"]}
        if args.merge_current == "true":
            versions.update(previous)
        elif not previous.issubset(versions):
            raise ValueError("recurring Silver input cannot omit previously accepted Bronze receipts")
    if args.require_input == "true" and not versions:
        raise ValueError("direct Silver execution requires accepted Bronze input")
    candidate = (
        open_candidate(bronze, silver, sorted(versions), args.image_digest, args.completeness, boundaries)
        if versions
        else None
    )
    should_run = bool(candidate and (not pointer or pointer.get("candidate_id") != candidate["candidate_id"]))
    if candidate and candidate.get("state") != "candidate" and should_run:
        raise ValueError("selected Silver candidate is already accepted but not current")
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=True)
    (output / "candidate-id").write_text(candidate["candidate_id"] if candidate else "none")
    (output / "run").write_text("true" if should_run else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
