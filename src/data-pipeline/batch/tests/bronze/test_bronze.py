import json
import shutil
from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F

from batch.bronze import ingest as bronze
from batch.bronze.schema import EVENT_COLUMNS, LABEL_COLUMNS, business_schema, decode_events
from batch.silver import curate as silver
from tests.events import approved, created, label, requested, started, succeeded
from tests.historical import EVENTS_PART, LABELS_PART, write_manifest, write_part

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


def read_manifest(root):
    return json.loads((root / "manifest.json").read_text())


@pytest.fixture
def dataset(spark, tmp_path):
    later = datetime(2026, 8, 1, tzinfo=timezone.utc)
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
    files = [
        write_part(
            spark,
            tmp_path,
            EVENTS_PART,
            [event("evt-1", MOMENT), event("evt-1", later), event("evt-2", MOMENT)],
            business_schema(spark, EVENT_COLUMNS),
        ),
        write_part(spark, tmp_path, LABELS_PART, [label], business_schema(spark, LABEL_COLUMNS)),
    ]
    write_manifest(tmp_path, files)
    return tmp_path


def test_rerun_is_idempotent_and_keeps_duplicates(spark, dataset):
    first = bronze.run_historical(spark, str(dataset), "v-test", "run-1")
    second = bronze.run_historical(spark, str(dataset), "v-test", "run-2")

    assert first["tables"][bronze.EVENTS]["rows"] == 3
    assert second["tables"][bronze.EVENTS]["rows"] == 3
    assert spark.table(bronze.EVENTS).select("event_id").distinct().count() == 2
    assert spark.table(bronze.LABELS).count() == 1


def test_missing_source_file_fails_before_reading(spark, dataset):
    (dataset / LABELS_PART).unlink()

    with pytest.raises(ValueError, match=f"missing file: {LABELS_PART}"):
        bronze.run_historical(spark, str(dataset), "v-test", "run-missing")


def test_extra_source_file_fails_before_reading(spark, dataset):
    extra = "payment_events/date=2026-07-30/part-00009.v3.parquet"
    (dataset / extra).parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(dataset / EVENTS_PART, dataset / extra)

    with pytest.raises(ValueError, match=f"extra file: {extra}"):
        bronze.run_historical(spark, str(dataset), "v-test", "run-extra")


def test_size_mismatch_fails_before_reading(spark, dataset):
    with (dataset / LABELS_PART).open("ab") as stream:
        stream.write(b"\x00")

    with pytest.raises(ValueError, match=f"size mismatch: {LABELS_PART} is"):
        bronze.run_historical(spark, str(dataset), "v-test", "run-size")


def test_per_file_row_mismatch_names_the_file(spark, dataset):
    manifest = read_manifest(dataset)
    for part in manifest["files"]:
        if part["path"] == EVENTS_PART:
            part["rows"] = 99
    (dataset / "manifest.json").write_text(json.dumps(manifest, sort_keys=True))

    with pytest.raises(ValueError, match=f"row mismatch in {EVENTS_PART}: wrote 3 rows, manifest lists 99"):
        bronze.run_historical(spark, str(dataset), "v-test", "run-rows")


def test_corrupt_protobuf_bytes_are_flagged_and_empty_message_decodes(spark):
    frame = spark.createDataFrame([(b"",), (b"\xff\xff\xff",)], "value binary")
    decoded = frame.select(decode_events(F.col("value")).alias("decoded")).collect()

    assert decoded[0]["decoded"]["event_id"] == ""
    assert decoded[1]["decoded"] is None or decoded[1]["decoded"]["event_id"] is None


@pytest.fixture
def mixed_schema_tables(spark):
    tables = (bronze.EVENTS, bronze.LABELS, *silver.TABLES)
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    yield
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_mixed_physical_schemas_and_redelivery_survive_bronze_then_canonicalize_in_silver(
    spark, tmp_path, report_root, mixed_schema_tables
):
    schema = business_schema(spark, EVENT_COLUMNS)
    files = []
    for version in (2, 3):
        payment, attempt = f"pay-v{version}", f"att-v{version}"
        rows = [
            created(payment, f"v{version}-1"),
            started(payment, f"v{version}-2", 2, 1, attempt),
            approved(payment, f"v{version}-3", 3, 2, attempt),
            requested(payment, f"v{version}-4", 4, 3, attempt),
            succeeded(payment, f"v{version}-5", 5, 4, attempt),
        ]
        for row in rows:
            row["schema_version"] = version
        physical = schema
        if version == 2:
            physical = (
                spark.createDataFrame([], schema)
                .withColumn("attempt_started", F.col("attempt_started").dropFields("signals.user_agent_family"))
                .withColumn(
                    "risk_decision_recorded",
                    F.col("risk_decision_recorded").dropFields(
                        "assessment.assessment_id", "assessment.evidence_sha256"
                    ),
                )
                .schema
            )
        else:
            rows.append({**rows[2], "recorded_at": MOMENT.replace(minute=10)})
        path = f"payment_events/date=2026-07-31/part-00000.v{version}.parquet"
        files.append(write_part(spark, tmp_path, path, rows, physical))

    files.append(
        write_part(
            spark,
            tmp_path,
            LABELS_PART,
            [label(f"pay-v{version}", f"att-v{version}", available=10) for version in (2, 3)],
            business_schema(spark, LABEL_COLUMNS),
        )
    )
    write_manifest(tmp_path, files, version="mixed-schema")
    bronze.run_historical(spark, str(tmp_path), "mixed-schema", "mixed-bronze")

    decisions = spark.table(bronze.EVENTS).where(F.col("risk_decision_recorded").isNotNull())
    assert decisions.count() == 3
    field = "risk_decision_recorded.assessment.assessment_id"
    assert (
        decisions.where(F.col("schema_version") == 2).first()["risk_decision_recorded"]["assessment"]["assessment_id"]
        is None
    )
    assert decisions.where((F.col("schema_version") == 3) & F.col(field).isNotNull()).count() == 2

    silver.run(spark, "mixed-silver", MOMENT.date(), MOMENT.date())

    assert spark.table(bronze.EVENTS).where(F.col("_dataset_version") == "mixed-schema").count() == 11
    assert spark.table(silver.PAYMENT_EVENTS).count() == 10
    attempts = {row.attempt_id: row for row in spark.table(silver.ATTEMPTS).collect()}
    assert attempts["att-v2"].assessment_id is None
    assert attempts["att-v3"].assessment_id == "assessment-1"
    assert all(row.lifecycle_valid for row in attempts.values())
