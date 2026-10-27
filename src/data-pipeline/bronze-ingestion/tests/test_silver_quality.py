from pyspark.sql import functions as F

from data_platform.ingest import source_frames
from data_platform.products import hashes
from data_platform.runs import open_run
from data_platform.silver import canonical_events
from data_platform.silver_quality import (
    invalid_attempt_contexts,
    invalid_checkout_amounts,
    invalid_complete_lifecycles,
    invalid_events,
)
from data_platform.storage import Store


def test_complete_histories_reject_invalid_amounts_and_lifecycle_order(spark, dataset, tmp_path, bounded_shuffle):
    root, manifest = dataset
    run = open_run(Store(str(tmp_path / "evidence")), "smoke", "a" * 64, hashes(), "image", "workflow")
    raw = source_frames(spark, str(root), manifest, run)["payment_events"]
    events = canonical_events(raw).canonical
    assert not invalid_events(events).take(1)
    assert not invalid_checkout_amounts(events).take(1)
    assert not invalid_attempt_contexts(events, "complete").take(1)
    assert not invalid_complete_lifecycles(events).take(1)
    total = F.col("payment_created.snapshot.total").withField(
        "minor_units", F.col("payment_created.snapshot.total.minor_units") + 1
    )
    snapshot = F.col("payment_created.snapshot").withField("total", total)
    changed = events.withColumn("payment_created", F.col("payment_created").withField("snapshot", snapshot))
    assert invalid_checkout_amounts(changed).take(1)
    assert invalid_complete_lifecycles(events.withColumn("aggregate_version", F.col("aggregate_version") + 1)).take(1)
