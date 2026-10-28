from data_platform.ingest import source_frames, table_name
from data_platform.manifest import selected_manifest
from data_platform.products import PORTS, hashes
from data_platform.runs import open_run
from data_platform.storage import Store
from pyspark.sql import functions as F

from silver_pipeline.silver_source import selected_bronze


def test_prior_bronze_snapshots_retain_source_identity_and_normalize_references(
    spark, dataset, tmp_path, bounded_shuffle
):
    root, manifest = dataset
    _, manifest_hash = selected_manifest(str(root))
    bronze_store = Store(str(tmp_path / "evidence"))
    run = open_run(bronze_store, "smoke", manifest_hash, hashes(), "image", "workflow")
    frames = source_frames(spark, str(root), manifest, run)
    assessment = F.col("risk_decision_recorded.assessment").dropFields("assessment_id", "evidence_sha256")
    legacy_events = (
        frames["payment_events"]
        .withColumn("risk_decision_recorded", F.col("risk_decision_recorded").withField("assessment", assessment))
        .drop("_source_schema_version")
    )
    spark.sql("CREATE NAMESPACE lake.silver_old_input")
    snapshots = {}
    for table in PORTS:
        name = table_name(table, "silver_old_input")
        frame = legacy_events if table == "payment_events" else frames[table].drop("_source_schema_version")
        frame.writeTo(name).using("iceberg").create()
        snapshots[table] = spark.table(name + ".snapshots").first().snapshot_id
    candidate = {
        "inputs": [
            {
                "input_mode": "historical",
                "dataset_version": "smoke",
                "manifest_sha256": manifest_hash,
                "source_root": str(root),
                "snapshots": snapshots,
            }
        ]
    }
    selected = selected_bronze(spark, bronze_store, candidate, "silver_old_input")
    for table in PORTS:
        assert not selected[table].exceptAll(frames[table]).take(1)
        assert not frames[table].exceptAll(selected[table]).take(1)
