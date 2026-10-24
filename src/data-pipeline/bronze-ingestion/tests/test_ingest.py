from copy import deepcopy

import pytest
from pyspark.sql import functions as F

from data_platform.ingest import ingest, selected_snapshots, table_name
from data_platform.manifest import selected_manifest
from data_platform.products import PORTS, hashes
from data_platform.runs import open_run
from data_platform.storage import Store
from data_platform.validate import validate


def test_snapshot_recovery_isolation_validation_and_replay(spark, dataset, tmp_path):
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
    report = validate(spark, str(root), manifest, store, run, contracts, "image@sha256:test", "recovery")
    assert report["data_outcome"] == "passed", report
    assert all(t["conserved"] for t in report["tables"].values())
    ingest(spark, str(root), manifest, store, run, namespace="recovery")
    assert run["snapshots"] == original
    revised = {**contracts, "payment_events": "revised-contract"}
    revalidated = validate(spark, str(root), manifest, store, run, revised, "new-image", "recovery")
    assert revalidated["data_outcome"] == "passed"
    assert run["snapshots"] == original
    from data_platform.runs import report_key

    assert store.get(report_key(run, contracts))["contracts"] == contracts
    assert store.get(report_key(run, revised))["contracts"] == revised
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


def test_validation_failure_and_bounded_samples_are_persisted(spark, dataset, tmp_path, monkeypatch):
    from data_platform import validate as validation
    from data_platform.ingest import source_frames
    from data_platform.runs import report_key

    root, manifest = dataset
    store = Store(str(tmp_path))
    contracts = hashes()
    declaration = {"result": "skipped", "gate": "dp1-spark-gx", "reason": "deferred source validation"}
    run = open_run(store, "smoke", "a" * 64, contracts, "image", "workflow", source_validation=declaration)
    run["snapshots"] = {"payment_events": 1, "fraud_labels": 2}
    frames = source_frames(spark, str(root), manifest, run)
    frames["fraud_labels"] = frames["fraud_labels"].withColumn(
        "label_available_at", F.col("occurred_at") - F.expr("INTERVAL 1 DAY")
    )
    monkeypatch.setattr(validation, "selected_snapshots", lambda *args: frames)
    report = validation.validate(spark, str(root), manifest, store, run, contracts, "image")
    assert report["data_outcome"] == "failed"
    persisted = store.get(report_key(run, contracts))
    assert persisted["source_validation"] == declaration
    assert persisted["data_outcome"] == "failed" and not persisted["execution_errors"]
    results = persisted["tables"]["fraud_labels"]["gx"]["results"]
    availability = next(r for r in results if r["expectation_config"]["meta"].get("id") == "label_availability")
    assert availability["success"] is False
    assert len(availability["result"]["details"]["unexpected_rows"]) == 20
    monkeypatch.setattr(
        validation, "selected_snapshots", lambda *args: (_ for _ in ()).throw(ValueError("missing snapshot"))
    )
    error = validation.validate(spark, str(root), manifest, store, run, contracts, "image")
    assert error["data_outcome"] == "error" and error["execution_errors"]
    assert error["source_validation"] == declaration
    assert store.get("accepted/smoke.json") is None


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
        if not entry["key"].startswith("payment_events/"):
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
