import json
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_platform.ingest import ingest, selected_snapshots, table_name
from data_platform.landing import advance, select, selection, state_key
from data_platform.products import hashes
from data_platform.runs import open_run
from data_platform.storage import Store

EPOCH = "landing-fixture"


def publish(source, root, manifest, table, offset):
    from simulator.landing.parquet import TRANSPORT

    entry = next(e for e in manifest["inventory"] if e["table"] == table)
    rows = pq.ParquetFile(root / entry["key"]).read().slice(0, 1)
    topic = "payments.events.v1" if table == "payment_events" else "fraud.labels.v1"
    transport = {
        "topic": topic,
        "partition": 0,
        "offset": offset,
        "timestamp": None,
        "key": b"key",
        "value": b"original-wire-bytes",
        "headers": [{"key": "origin", "value": b"synthetic"}, {"key": "origin", "value": None}],
    }
    rows = rows.append_column("_transport", pa.array([transport], type=TRANSPORT))
    sink = pa.BufferOutputStream()
    pq.write_table(rows, sink)
    body = sink.getvalue().to_pybytes()
    checksum = sha256(body).hexdigest()
    version = 3 if table == "payment_events" else 1
    stem = f"epoch={EPOCH}/topic={topic}/partition=0/{offset:020d}-{offset:020d}"
    key = f"streaming/{table}/{stem}-v{version}-{checksum}.parquet"
    path = Path(source.root) / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    batch = {
        "format": "decoded-payments-parquet-v1",
        "source_epoch": EPOCH,
        "topic": topic,
        "partition": 0,
        "first_offset": offset,
        "last_offset": offset,
        "rows": 1,
        "files": [
            {"key": key, "sha256": checksum, "bytes": len(body), "rows": 1, "table": table, "schema_version": version}
        ],
    }
    body = json.dumps(batch, sort_keys=True).encode()
    key = f"streaming/_batches/{stem}-{sha256(body).hexdigest()}.json"
    path = Path(source.root) / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return key


def test_independent_late_batches_recover_exact_transport_and_skip_completed_input(spark, dataset, tmp_path):
    root, manifest = dataset
    source = Store(str(tmp_path / "landing"))
    store = Store(str(tmp_path / "evidence"))
    publish(source, root, manifest, "payment_events", 50)
    chosen = select(source, store, EPOCH)
    assert select(source, store, EPOCH) == chosen
    chosen, digest = selection(store, chosen["dataset_version"])
    run = open_run(store, chosen["dataset_version"], digest, hashes(), "image", "workflow")
    stopped = []

    def crash(table):
        stopped.append(table)
        raise RuntimeError("lost commit response")

    with pytest.raises(RuntimeError, match="lost commit"):
        ingest(spark, source.root, chosen, store, run, "landing_batches", after_commit=crash)
    ingest(spark, source.root, chosen, store, run, "landing_batches")
    original = deepcopy(run["snapshots"])
    frame = selected_snapshots(spark, run, "landing_batches")["payment_events"]
    row = frame.first()
    assert frame.count() == 1 and row._transport.value == b"original-wire-bytes"
    assert [h.key for h in row._transport.headers] == ["origin", "origin"]
    assert row._source_epoch == EPOCH and row._source_batch == chosen["batches"][0]["key"]
    advance(store, run)
    assert select(source, store, EPOCH) is None
    publish(source, root, manifest, "fraud_labels", 20)
    late = select(source, store, EPOCH)
    assert late["totals"] == {"payment_events": 0, "fraud_labels": 1}
    late, digest = selection(store, late["dataset_version"])
    other = open_run(store, late["dataset_version"], digest, hashes(), "image", "later")
    ingest(spark, source.root, late, store, other, "landing_batches")
    assert other["snapshots"]["payment_events"] == original["payment_events"]
    assert spark.table(table_name("payment_events", "landing_batches")).count() == 1
    advance(store, other)
    assert select(source, store, EPOCH) is None
    assert len(store.get(state_key(EPOCH))["accepted"]) == 2


def test_landing_rejects_mutated_pending_manifest_and_overlapping_positions(dataset, tmp_path):
    root, manifest = dataset
    source = Store(str(tmp_path / "landing"))
    store = Store(str(tmp_path / "receipts"))
    key = publish(source, root, manifest, "payment_events", 90)
    chosen = select(source, store, EPOCH)
    path = Path(source.root) / key
    original = path.read_bytes()
    path.write_bytes(original + b" ")
    with pytest.raises(ValueError, match="pending landing manifest changed"):
        select(source, store, EPOCH)
    path.write_bytes(original)
    batch = json.loads(original)
    batch["files"][0]["bytes"] += 1
    body = json.dumps(batch, sort_keys=True).encode()
    other = path.with_name(path.name.replace(sha256(original).hexdigest(), sha256(body).hexdigest()))
    other.write_bytes(body)
    run = {
        "dataset_version": chosen["dataset_version"],
        "manifest_sha256": selection(store, chosen["dataset_version"])[1],
        "snapshots": {"payment_events": 1, "fraud_labels": 2},
    }
    advance(store, run)
    with pytest.raises(ValueError, match="overlapping landing"):
        select(source, store, EPOCH)
