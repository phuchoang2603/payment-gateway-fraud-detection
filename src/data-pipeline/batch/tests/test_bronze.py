import io
import json
import shutil
from datetime import datetime, timezone
from hashlib import sha256

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import functions as F

from batch import bronze
from batch.schema import EVENT_COLUMNS, LABEL_COLUMNS, business_schema, decode_events
from batch.store import parquet_rows

MOMENT = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)


def event(event_id, recorded_at):
    money = {"minor_units": 100, "currency": "EUR"}
    snapshot = {
        "external_order_id": "order-1",
        "total": money,
        "items": [{"product_id": "p-1", "category": "phone", "condition": "good", "quantity": 1, "unit_price": money}],
        "merchant_account_created_at": MOMENT,
        "customer_account_created_at": MOMENT,
        "shipping": {"country": "DE", "region": "BE", "postal_code": "10115", "address_fingerprint": "f"},
    }
    return {
        "event_id": event_id,
        "payment_id": "pay-1",
        "aggregate_version": 1,
        "integration_id": "int-1",
        "occurred_at": MOMENT,
        "recorded_at": recorded_at,
        "schema_version": 3,
        "correlation_id": "corr-1",
        "payment_created": {
            "merchant": {"merchant_id": "m-1", "external_seller_id": "s-1"},
            "customer": {"customer_id": "c-1", "external_buyer_id": "b-1"},
            "snapshot": snapshot,
        },
        "attempt_started": None,
        "risk_decision_recorded": None,
        "processor_operation_requested": None,
        "processor_outcome_unknown": None,
        "attempt_declined": None,
        "payment_succeeded": None,
        "payment_expired": None,
    }


def write_part(spark, root, relative, rows, schema):
    staging = root / ".staging"
    spark.createDataFrame(rows, schema=schema).coalesce(1).write.mode("overwrite").parquet(str(staging))
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    next(staging.glob("*.parquet")).replace(target)
    shutil.rmtree(staging)
    body = target.read_bytes()
    return {
        "table": relative.split("/")[0],
        "path": relative,
        "rows": parquet_rows(body),
        "bytes": len(body),
        "sha256": sha256(body).hexdigest(),
    }


def test_altered_source_file_fails_verification(tmp_path):
    (tmp_path / "payment_events").mkdir()
    buffer = io.BytesIO()
    pq.write_table(pa.table({"event_id": ["evt-1"]}), buffer)
    body = buffer.getvalue()
    (tmp_path / "payment_events/part.parquet").write_bytes(body)
    part = {
        "table": "payment_events",
        "path": "payment_events/part.parquet",
        "rows": 1,
        "bytes": len(body),
        "sha256": sha256(body).hexdigest(),
    }
    assert bronze.verify_part(str(tmp_path), part) is None
    part["sha256"] = sha256(b"other").hexdigest()
    assert bronze.verify_part(str(tmp_path), part) == "checksum mismatch: payment_events/part.parquet"


def test_historical_rerun_is_idempotent_and_keeps_duplicates(spark, tmp_path):
    schema = business_schema(spark, EVENT_COLUMNS)
    rows = [event("evt-1", MOMENT), event("evt-1", datetime(2026, 8, 1, tzinfo=timezone.utc)), event("evt-2", MOMENT)]
    files = [write_part(spark, tmp_path, "payment_events/date=2026-07-31/part-00000.v3.parquet", rows, schema)]
    label = {
        "integration_id": "int-1",
        "payment_id": "pay-1",
        "attempt_id": "att-1",
        "is_fraud": False,
        "occurred_at": MOMENT,
        "label_available_at": MOMENT,
        "source": "LABEL_SOURCE_SIMULATION_TRUTH",
        "scenario": "none",
    }
    files.append(
        write_part(
            spark,
            tmp_path,
            "fraud_labels/date=2026-07-31/part-00000.parquet",
            [label],
            business_schema(spark, LABEL_COLUMNS),
        )
    )
    (tmp_path / "manifest.json").write_text(
        json.dumps({"dataset_version": "v-test", "rows": {"payment_events": 3, "fraud_labels": 1}, "files": files})
    )

    first = bronze.run_historical(spark, str(tmp_path), "v-test", "run-1")
    second = bronze.run_historical(spark, str(tmp_path), "v-test", "run-2")

    assert first["tables"][bronze.EVENTS]["rows"] == 3
    assert second["tables"][bronze.EVENTS]["rows"] == 3
    assert spark.table(bronze.EVENTS).select("event_id").distinct().count() == 2
    assert spark.table(bronze.LABELS).count() == 1


def test_corrupt_protobuf_bytes_are_flagged_and_empty_message_decodes(spark):
    frame = spark.createDataFrame([(b"",), (b"\xff\xff\xff",)], "value binary")
    decoded = frame.select(decode_events(F.col("value")).alias("decoded")).collect()

    assert decoded[0]["decoded"]["event_id"] == ""
    assert decoded[1]["decoded"] is None or decoded[1]["decoded"]["event_id"] is None
