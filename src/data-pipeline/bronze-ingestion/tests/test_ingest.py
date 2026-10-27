from copy import deepcopy
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from data_platform.ingest import ingest, selected_snapshots, table_name, verify_content
from data_platform.manifest import selected_manifest
from data_platform.products import PORTS, hashes
from data_platform.runs import open_run
from data_platform.storage import Store


def test_raw_chunks_ingest_directly_into_date_partitioned_bronze(spark, dataset, tmp_path):
    from data_platform.ingest import source_frames

    root, manifest = dataset
    assert manifest["source_layout"] == "task-fragments-v1"
    store = Store(str(tmp_path / "evidence"))
    run = open_run(store, "smoke", "a" * 64, hashes(), "image", "workflow")
    source = source_frames(spark, str(root), manifest, run)
    assert source["payment_events"].where(F.col("_source_schema_version") != 3).count() == 0
    spark.sql("CREATE NAMESPACE lake.raw_chunks")
    ingest(spark, str(root), manifest, store, run, namespace="raw_chunks")
    snapshots = selected_snapshots(spark, run, namespace="raw_chunks")
    for table in PORTS:
        actual = snapshots[table]
        expected = source[table]
        assert run["conservation"]["snapshots"] == run["snapshots"]
        assert actual.count() == sum(manifest["partitions"][table].values())
        assert not actual.exceptAll(expected).take(1) and not expected.exceptAll(actual).take(1)
        partition_type = spark.table(table_name(table, "raw_chunks") + ".partitions").schema["partition"].dataType
        assert len(partition_type.fields) == 2
        assert partition_type.fields[1].name.endswith("_day")

    name = table_name("payment_events", "raw_chunks")
    snapshots["payment_events"].withColumn("correlation_id", F.lit("changed-content")).writeTo(name).overwrite(
        F.lit(True)
    )
    changed = deepcopy(run)
    changed["snapshots"]["payment_events"] = (
        spark.table(name + ".snapshots").orderBy(F.col("committed_at").desc()).first().snapshot_id
    )
    with pytest.raises(ValueError, match="persisted content differs"):
        verify_content(spark, source, changed, "raw_chunks")


def test_mixed_physical_schemas_normalize_without_relabeling_envelopes(spark, tmp_path, bounded_shuffle):
    import pyarrow.fs as fs
    import yaml
    from simulator.config import HistoricalConfig
    from simulator.storage import generate

    from data_platform.ingest import source_frames

    generator = Path(__file__).resolve().parents[4] / "src/data-pipeline/simulator"
    config = yaml.safe_load((generator / "tests/small-config.yaml").read_text())
    config.update(dataset_version="mixed", duration_days=2)
    config["counts"]["customers"] = 100
    config["schema_schedule"] = {
        "initial_version": 3,
        "changes": [{"effective_at": "2026-07-02T00:00:00Z", "version": 4}],
    }
    manifest = generate(HistoricalConfig.model_validate(config), fs.LocalFileSystem(), str(tmp_path))
    root = tmp_path / "historical/mixed"
    run = open_run(Store(str(tmp_path / "evidence")), "mixed", "a" * 64, hashes(), "image", "workflow")
    events = source_frames(spark, str(root), manifest, run)["payment_events"]
    assert {row[0] for row in events.select("_source_schema_version").distinct().collect()} == {3, 4}
    assert events.where(F.col("schema_version") != 3).count() == 0
    assessments = events.where(F.col("risk_decision_recorded").isNotNull())
    reference = F.col("risk_decision_recorded.assessment.assessment_id")
    digest = F.col("risk_decision_recorded.assessment.evidence_sha256")
    assert assessments.where((F.col("_source_schema_version") == 3) & reference.isNull() & digest.isNull()).count() > 0
    assert (
        assessments.where((F.col("_source_schema_version") == 4) & reference.isNotNull() & digest.isNotNull()).count()
        > 0
    )


def test_snapshot_recovery_isolation_and_replay(spark, dataset, tmp_path):
    root, manifest = dataset
    _, digest = selected_manifest(str(root))
    store = Store(str(tmp_path / "evidence"))
    contracts = hashes()
    run = open_run(store, "smoke", digest, contracts, "image@sha256:test", "workflow")
    run["source_root"] = str(root)
    stopped = []

    def stop_after_events(table):
        stopped.append(table)
        raise RuntimeError("driver stopped after atomic commit")

    with pytest.raises(RuntimeError, match="stopped"):
        ingest(spark, str(root), manifest, store, run, namespace="recovery", after_commit=stop_after_events)
    assert stopped == ["payment_events"] and run["snapshots"] == {}
    assert store.get("accepted/smoke.json") is None
    ingest(spark, str(root), manifest, store, run, namespace="recovery")
    original = deepcopy(run["snapshots"])
    frames = selected_snapshots(spark, run, "recovery")
    for table in PORTS:
        assert frames[table].count() == sum(manifest["partitions"][table].values())
    ingest(spark, str(root), manifest, store, run, namespace="recovery")
    assert run["snapshots"] == original
    events = frames["payment_events"]
    (
        events.withColumn("_source_dataset_version", F.lit("unrelated"))
        .writeTo(table_name("payment_events", "recovery"))
        .append()
    )
    assert selected_snapshots(spark, run, "recovery")["payment_events"].count() == events.count()
    assert spark.table(table_name("payment_events", "recovery")).count() == 2 * events.count()
    replacement = open_run(store, "second", digest, contracts, "image", "second-workflow")
    ingest(spark, str(root), manifest, store, replacement, namespace="recovery")
    before = spark.table(table_name("payment_events", "recovery")).where("_source_dataset_version != 'second'")
    replacement["run_id"] = "isolated-replacement"
    replacement["snapshots"] = {}
    ingest(spark, str(root), manifest, store, replacement, namespace="recovery")
    after = spark.table(table_name("payment_events", "recovery")).where("_source_dataset_version != 'second'")
    assert before.exceptAll(after).count() == after.exceptAll(before).count() == 0
    assert spark.table(table_name("payment_events", "recovery")).count() == 3 * events.count()
    assert open_run(store, "smoke", digest, {"payment_events": "changed"}, "other", "retry")["snapshots"] == original


def test_integrity_and_overflow_fail_before_table_creation(spark, dataset, tmp_path):
    import shutil
    from decimal import Decimal
    from hashlib import sha256

    import pyarrow as pa
    import pyarrow.parquet as pq

    root, manifest = dataset
    corrupted = deepcopy(manifest)
    corrupted["inventory"][0]["sha256"] = "0" * 64
    store = Store(str(tmp_path / "evidence"))
    run = open_run(store, "corrupt", "digest", hashes(), "image", "workflow")
    with pytest.raises(Exception, match="source integrity mismatch"):
        ingest(spark, str(root), corrupted, store, run, namespace="prewrite")
    assert not spark.catalog.tableExists(table_name("payment_events", "prewrite"))
    cloned = tmp_path / "overflow"
    shutil.copytree(root, cloned)
    candidate = deepcopy(manifest)
    for entry in candidate["inventory"]:
        if entry["table"] != "payment_events":
            continue
        path = cloned / entry["key"]
        frame = pq.ParquetFile(path).read()
        index = frame.schema.get_field_index("aggregate_version")
        frame = frame.set_column(
            index, "aggregate_version", pa.array([Decimal(2**64)] * frame.num_rows, type=pa.decimal128(38, 0))
        )
        pq.write_table(frame, path)
        entry.update(bytes=path.stat().st_size, sha256=sha256(path.read_bytes()).hexdigest())
    with pytest.raises(ValueError, match="schema type differs"):
        ingest(spark, str(cloned), candidate, store, run, namespace="prewrite")
    assert not spark.catalog.tableExists(table_name("payment_events", "prewrite"))
    assert run["snapshots"] == {}
