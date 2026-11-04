"""Decide which pipeline stages a run can skip.

A stage is skipped when every table it writes has, at the head of main, a commit for the same day range, from the same
input snapshots, by the same code. That output is exactly what the stage would write again, so re-running a range after
a later stage failed resumes there instead of rebuilding the layers before it. A commit without a code version never
matches, so output written before this rule existed is always rebuilt.
"""

import json
import os
import urllib.error
from pathlib import Path

from openmetadata.wap import Catalog, ref_snapshot

CODE_PROPERTY = "dp.code_version"
SILVER_TABLES = ("silver.payment_events", "silver.attempts", "silver.labels", "silver.rejected_events")
GOLD_TABLES = ("gold.dim_customer", "gold.feat_customer_daily", "gold.feat_payment_method_daily", "gold.label_attempt")
SILVER_INPUTS = ("silver.attempts", "silver.labels", "silver.payment_events")
LAYERS = {
    "silver": {
        "tables": SILVER_TABLES,
        "range": "dp2.range",
        "inputs": {
            "bronze.payment_events": "bronze.payment_events.snapshot_id",
            "bronze.fraud_labels": "bronze.fraud_labels.snapshot_id",
        },
    },
    "gold": {
        "tables": GOLD_TABLES,
        "range": "dp3.range",
        "inputs": {table: f"{table}.snapshot_id" for table in SILVER_INPUTS},
    },
}


def head_summary(metadata: dict) -> dict | None:
    """Read the commit summary at the published main snapshot."""
    head = ref_snapshot(metadata, "main")
    return next(
        (
            snapshot.get("summary") or {}
            for snapshot in metadata.get("snapshots", [])
            if snapshot["snapshot-id"] == head
        ),
        None,
    )


def main_snapshots(catalog, tables) -> dict:
    """Pin each input table to its current published snapshot."""
    return {table: ref_snapshot(catalog.metadata(table), "main") for table in tables}


def unchanged(catalog, layer: str, day_range: str, code: str) -> bool:
    """Skip only outputs matching the range, code and current input snapshots."""
    spec = LAYERS[layer]
    if not code or not day_range:
        return False
    inputs = main_snapshots(catalog, spec["inputs"])
    for table in spec["tables"]:
        try:
            summary = head_summary(catalog.metadata(table))
        except urllib.error.HTTPError:
            return False
        if not summary or summary.get(spec["range"]) != day_range or summary.get(CODE_PROPERTY) != code:
            return False
        if any(summary.get(prop) != str(inputs[source]) for source, prop in spec["inputs"].items()):
            return False
    return True


def main() -> int:
    """Write the run decision and pinned Silver inputs for downstream tasks."""
    layer = os.environ["PLAN_LAYER"]
    start, end = os.environ.get("DP_START", ""), os.environ.get("DP_END", "")
    day_range = f"{start}/{end}" if start and end else ""
    catalog = Catalog()
    output = Path(os.environ.get("PLAN_OUTPUT_DIR", "/tmp/plan"))
    output.mkdir(parents=True, exist_ok=True)
    decision = "skip" if unchanged(catalog, layer, day_range, os.environ.get("DP_CODE_VERSION", "")) else "run"
    (output / "decision").write_text(decision)
    if layer == "gold":
        # The marts read the same published Silver, so they get its snapshots here rather than from Gold.
        snapshots = main_snapshots(catalog, SILVER_INPUTS)
        (output / "silver_snapshots").write_text(json.dumps(snapshots, sort_keys=True))
    print(f"{layer} {day_range or 'daily'}: {decision}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
