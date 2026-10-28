from datetime import UTC, datetime

import pytest
from data_platform.ingest import source_frames
from data_platform.runs import open_run
from pipeline_common.products import hashes
from pipeline_common.storage import Store
from pyspark.sql import functions as F

import silver_pipeline.silver_incremental as incremental
from silver_pipeline.silver_incremental import accepted_base, affected_payments
from silver_pipeline.silver_runs import digest
from silver_pipeline.silver_transform import build
from silver_pipeline.silver_write import candidate_outputs


def test_affected_payments_include_late_labels_cross_payment_conflicts_and_backfill(spark, bounded_shuffle):
    events = spark.createDataFrame(
        [
            ("scope", "same", "first", datetime(2025, 1, 1, tzinfo=UTC)),
            ("scope", "same", "conflict", datetime(2025, 1, 1, tzinfo=UTC)),
            ("scope", "backfill", "bounded", datetime(2025, 2, 2, tzinfo=UTC)),
            ("scope", "unrelated", "untouched", datetime(2025, 3, 1, tzinfo=UTC)),
        ],
        ["integration_id", "event_id", "payment_id", "occurred_at"],
    )
    labels = spark.createDataFrame(
        [("scope", "late-label", datetime(2025, 1, 1, tzinfo=UTC))],
        ["integration_id", "payment_id", "label_available_at"],
    )
    source = {"payment_events": events, "fraud_labels": labels}
    delta = {"payment_events": events.where("payment_id = 'first'"), "fraud_labels": labels}
    affected = affected_payments(source, delta, {"start": "2025-02-01T00:00:00Z", "end": "2025-02-03T00:00:00Z"})
    assert {row.payment_id for row in affected.collect()} == {"first", "conflict", "bounded", "late-label"}


def test_recurring_candidate_cannot_drop_accepted_source_progress(tmp_path):
    store = Store(str(tmp_path))
    receipt = {
        "candidate_id": "previous",
        "outcome": "passed",
        "snapshots": {
            "payment_events": 1,
            "event_delivery_provenance": 2,
            "attempt_histories": 3,
            "label_observations": 4,
            "label_delivery_provenance": 5,
        },
        "inputs": [{"dataset_version": "older"}],
        "contracts": {},
        "transformation_revision": "revision",
        "completeness": "recurring",
    }
    store.put("dp2/receipts/previous.json", receipt)
    store.put(
        "dp2/accepted.json",
        {"candidate_id": "previous", "receipt_key": "dp2/receipts/previous.json", "receipt_sha256": digest(receipt)},
    )
    candidate = {
        "inputs": [{"dataset_version": "newer"}],
        "contracts": {},
        "transformation_revision": "revision",
        "completeness": "recurring",
    }
    with pytest.raises(ValueError, match="cannot omit previously accepted"):
        accepted_base(store, candidate)


def test_late_terminal_and_labels_recompute_only_affected_payment(
    spark, dataset, tmp_path, monkeypatch, bounded_shuffle
):
    root, manifest = dataset
    store = Store(str(tmp_path / "evidence"))
    run = open_run(store, "smoke", "a" * 64, hashes(), "image", "workflow")
    source = source_frames(spark, str(root), manifest, run)
    payment = source["fraud_labels"].select("payment_id").first().payment_id
    same_payment = F.col("payment_id") == payment
    terminal = F.col("payment_succeeded").isNotNull() | F.col("payment_expired").isNotNull()
    prior_source = {
        "payment_events": source["payment_events"].where(~(same_payment & terminal)),
        "fraud_labels": source["fraud_labels"].where(~same_payment),
    }
    delta = {
        "payment_events": source["payment_events"].where(same_payment & terminal),
        "fraud_labels": source["fraud_labels"].where(same_payment),
    }
    prior_frames = build(prior_source)
    assert prior_frames.attempts.pending.where(F.col("payment_id") == payment).take(1)
    prior_outputs = {
        name: frame.withColumn("_candidate_id", F.lit("previous"))
        for name, frame in candidate_outputs(prior_frames).items()
    }
    old_input, new_input = {"dataset_version": "old"}, {"dataset_version": "new"}
    metadata = {
        "contracts": {},
        "product_sha256": "product",
        "quality_sha256": "quality",
        "transformation_revision": "revision",
        "completeness": "recurring",
    }
    receipt = {
        **metadata,
        "candidate_id": "previous",
        "outcome": "passed",
        "inputs": [old_input],
        "snapshots": {name: index + 1 for index, name in enumerate(prior_outputs)},
    }
    store.put("dp2/receipts/previous.json", receipt)
    store.put(
        "dp2/accepted.json",
        {"candidate_id": "previous", "receipt_key": "dp2/receipts/previous.json", "receipt_sha256": digest(receipt)},
    )
    monkeypatch.setattr(incremental, "selected_bronze", lambda *args: delta)
    monkeypatch.setattr(
        incremental,
        "selected_snapshot",
        lambda spark, name, snapshot, candidate_id: prior_outputs[name.rsplit(".", 1)[-1]],
    )
    candidate = {**metadata, "inputs": [old_input, new_input], "boundaries": None}
    updated = incremental.recompute(spark, store, store, candidate, source, build)
    expected = candidate_outputs(build(source))
    actual = candidate_outputs(updated)
    for name in expected:
        assert not expected[name].exceptAll(actual[name]).take(1)
        assert not actual[name].exceptAll(expected[name]).take(1)
