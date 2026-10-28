from fraud_batch.bronze.ingest import source_frames
from fraud_batch.bronze.runs import open_run
from fraud_batch.common.products import hashes
from fraud_batch.common.storage import Store
from fraud_batch.silver.silver_contracts import hashes as silver_hashes
from fraud_batch.silver.silver_contracts import product_hash, quality_hash
from fraud_batch.silver.silver_runs import IDENTITY_FIELDS, digest
from fraud_batch.silver.silver_transform import build
from fraud_batch.silver.silver_write import OUTPUTS, selected_snapshot, table_name, write_candidate


def test_repeated_candidate_write_keeps_the_same_snapshots(spark, dataset, tmp_path, bounded_shuffle):
    root, manifest = dataset
    store = Store(str(tmp_path / "evidence"))
    run = open_run(store, "smoke", "a" * 64, hashes(), "image", "workflow")
    source = source_frames(spark, str(root), manifest, run)
    frames = build(source)
    candidate = {
        "candidate_id": "a" * 64,
        "state": "candidate",
        "snapshots": {},
        "completeness": "complete",
        "contracts": silver_hashes(),
        "product_sha256": product_hash(),
        "quality_sha256": quality_hash(),
        "inputs": [],
        "transformation_revision": "test-image",
    }
    candidate["candidate_id"] = digest({field: candidate[field] for field in IDENTITY_FIELDS})

    write_candidate(spark, store, candidate, frames, "complete", "silver_replay")
    snapshots = dict(candidate["snapshots"])
    assert set(snapshots) == set(OUTPUTS)
    write_candidate(spark, store, candidate, frames, "complete", "silver_replay")
    assert candidate["snapshots"] == snapshots
    events_output = selected_snapshot(
        spark, table_name("payment_events", "silver_replay"), snapshots["payment_events"]
    )
    assert events_output.count() == frames.events.canonical.count()
    assert store.get("dp2/accepted.json") is None
