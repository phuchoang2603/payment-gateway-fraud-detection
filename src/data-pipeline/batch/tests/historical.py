import json
import shutil

EVENTS_PART = "payment_events/date=2026-07-31/part-00000.v3.parquet"
LABELS_PART = "fraud_labels/date=2026-07-31/part-00000.parquet"


def write_part(spark, root, relative, rows, schema):
    staging = root / ".staging"
    spark.createDataFrame(rows, schema=schema).coalesce(1).write.mode("overwrite").parquet(str(staging))
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    next(staging.glob("*.parquet")).replace(target)
    shutil.rmtree(staging)
    return {
        "table": relative.split("/")[0],
        "path": relative,
        "rows": spark.read.parquet(str(target)).count(),
        "bytes": target.stat().st_size,
    }


def write_manifest(root, files, version="v-test"):
    rows = {
        table: sum(part["rows"] for part in files if part["table"] == table)
        for table in ("payment_events", "fraud_labels")
    }
    (root / "manifest.json").write_text(
        json.dumps({"dataset_version": version, "rows": rows, "files": files}, sort_keys=True)
    )
