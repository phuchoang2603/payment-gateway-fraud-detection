import json

import duckdb
import pyarrow.fs as fs
import pytest

from simulator import storage
from simulator.object_store import ConditionalWriteFailed, ObjectStore
from simulator.resources import CapacityExceeded
from simulator.run_state import RunState
from tests.conftest import small_config


def configuration(**overrides):
    return small_config(
        duration_days=2, customer_block_size=50, **{"data_problems.schema_cutover_date": "2026-07-02", **overrides}
    )


def final_rows(root):
    return {
        table: duckdb.sql(
            f"SELECT to_json(t) FROM read_parquet('{root}/{table}/*/*.parquet', union_by_name=true) t ORDER BY 1"
        ).fetchall()
        for table in ("payment_events", "fraud_labels")
    }


class FailedStore(ObjectStore):
    def __init__(self, prefix, boundary):
        super().__init__(prefix)
        self.boundary = boundary
        self.failed = False
        self.uploads = 0

    def put_file(self, key, path):
        self.uploads += 1
        if self.boundary == "upload-before" and self.uploads == 3:
            self.failed = True
            raise OSError("injected upload failure")
        super().put_file(key, path)
        if self.boundary == "upload-after" and self.uploads == 3:
            self.failed = True
            raise OSError("injected lost upload acknowledgement")

    def put_json(self, key, value, **conditions):
        result = super().put_json(key, value, **conditions)
        if self.boundary == "receipt-after" and key.startswith("_run/tasks/") and not self.failed:
            self.failed = True
            raise OSError("injected lost receipt acknowledgement")
        return result


@pytest.mark.parametrize("boundary", ["upload-before", "upload-after", "receipt-after"])
def test_interrupted_run_resumes_with_different_workers_without_extra_duplicates(tmp_path, boundary):
    config = configuration()
    first, resumed = tmp_path / "first", tmp_path / "resumed"
    expected = storage.generate(config, fs.LocalFileSystem(), str(first))
    root = resumed / "historical" / "smoke"
    failing = FailedStore(str(root), boundary)
    with pytest.raises(OSError, match="injected"):
        storage.generate(config, fs.LocalFileSystem(), str(resumed), store=failing)
    assert failing.failed and not (root / "manifest.json").exists()
    assert (root / "_run/state.json").exists()
    changed = configuration(**{"execution.workers": 2, "execution.max_in_flight": 2, "chunk_purchases": 30})
    actual = storage.generate(changed, fs.LocalFileSystem(), str(resumed), resume=True)
    assert final_rows(first / "historical" / "smoke") == final_rows(root)
    assert actual["observed"] == expected["observed"]
    assert actual["partitions"] == expected["partitions"]
    assert actual["generation_identity"] == expected["generation_identity"]


def test_prepare_handoff_verifies_inventory_without_regeneration(tmp_path, monkeypatch):
    config = configuration()
    storage.generate(config, fs.LocalFileSystem(), str(tmp_path), prepare_only=True)
    root = tmp_path / "historical" / "smoke"
    assert not (root / "manifest.json").exists()
    prepared = json.loads((root / "_run/prepared.json").read_text())
    assert prepared["files"] and all(item["sha256"] for item in prepared["files"])

    def unexpected(*args):
        raise AssertionError("prepared final objects must not regenerate")

    monkeypatch.setattr(storage, "_generate", unexpected)
    changed = configuration(**{"execution.duckdb_memory_limit_mb": 768, "execution.duckdb_threads": 2})
    manifest = storage.generate(changed, fs.LocalFileSystem(), str(tmp_path), resume=True)
    assert len(manifest["execution_attempts"]) == 2
    assert manifest["inventory"] == prepared["files"]


def test_corrupt_prepared_file_is_rebuilt_and_incompatible_resume_is_refused(tmp_path):
    config = configuration()
    storage.generate(config, fs.LocalFileSystem(), str(tmp_path), prepare_only=True)
    root = tmp_path / "historical" / "smoke"
    with pytest.raises(ValueError, match="incompatible"):
        storage.generate(configuration(seed=8), fs.LocalFileSystem(), str(tmp_path), resume=True)
    item = json.loads((root / "_run/prepared.json").read_text())["files"][0]
    (root / item["key"]).write_bytes(b"truncated")
    manifest = storage.generate(config, fs.LocalFileSystem(), str(tmp_path), resume=True)
    assert ObjectStore(str(root)).verify(manifest["inventory"][0])
    assert (root / item["key"]).stat().st_size == item["bytes"]


def test_single_writer_crash_takeover_and_exclusive_publication(tmp_path):
    store = ObjectStore(str(tmp_path))
    first = RunState(store, "generation", "profiles")
    with pytest.raises(FileExistsError, match="another writer"):
        RunState(store, "generation", "profiles", resume=True)
    second = RunState(store, "generation", "profiles", resume=True, takeover=True)
    with pytest.raises(FileExistsError, match="ownership lost"):
        first.checkpoint()
    second.release()
    store.put_json("manifest.json", {"accepted": True}, absent=True)
    with pytest.raises(ConditionalWriteFailed):
        store.put_json("manifest.json", {"accepted": False}, absent=True)
    assert store.json("manifest.json") == {"accepted": True}


def test_size_gate_keeps_prepared_data(tmp_path):
    with pytest.raises(ValueError, match="physical source byte gate"):
        storage.generate(configuration(minimum_source_parquet_bytes=10**12), fs.LocalFileSystem(), str(tmp_path))
    root = tmp_path / "historical" / "smoke"
    assert not (root / "manifest.json").exists()
    assert (root / "_run/prepared.json").exists()


def test_post_check_file_change_refuses_completion(tmp_path, monkeypatch):
    observe = storage._observations

    def change_after_observations(connection, stats):
        result = observe(connection, stats)
        root = tmp_path / "historical" / "smoke"
        item = json.loads((root / "_run/prepared.json").read_text())["files"][0]
        (root / item["key"]).write_bytes(b"changed after checks")
        return result

    monkeypatch.setattr(storage, "_observations", change_after_observations)
    with pytest.raises(ValueError, match="integrity changed"):
        storage.generate(configuration(), fs.LocalFileSystem(), str(tmp_path))
    assert not (tmp_path / "historical/smoke/manifest.json").exists()


def test_task_capacity_failure_preserves_receipts_and_no_manifest(tmp_path):
    config = configuration(**{"execution.task_scratch_bytes": 50000})
    with pytest.raises(CapacityExceeded, match="scratch"):
        storage.generate(config, fs.LocalFileSystem(), str(tmp_path))
    root = tmp_path / "historical/smoke"
    state = json.loads((root / "_run/state.json").read_text())
    assert state["failure"]["phase"] == "generation"
    assert state["owner"] is None and not (root / "manifest.json").exists()
    storage.generate(configuration(), fs.LocalFileSystem(), str(tmp_path), resume=True, prepare_only=True)


def test_cumulative_remote_output_exceeds_active_scratch(tmp_path):
    config = configuration(
        **{
            "execution.scratch_capacity_bytes": 600000,
            "execution.task_scratch_bytes": 300000,
            "execution.duckdb_spill_bytes": 0,
            "chunk_purchases": 10,
        }
    )
    storage.generate(config, fs.LocalFileSystem(), str(tmp_path), prepare_only=True)
    root = tmp_path / "historical/smoke"
    state = json.loads((root / "_run/state.json").read_text())
    report = state["attempts"][-1]["resources"]
    assert report["sampled_peaks"]["scratch_bytes"] <= config.execution.scratch_capacity_bytes
    remote = sum(item.stat().st_size for item in root.rglob("*.parquet"))
    assert remote > config.execution.scratch_capacity_bytes


def test_lost_state_checkpoint_acknowledgement_releases_writer_for_normal_resume(tmp_path):
    class LostCheckpoint(ObjectStore):
        updates = 0
        failed = False

        def put_json(self, key, value, **conditions):
            result = super().put_json(key, value, **conditions)
            if key == "_run/state.json":
                self.updates += 1
                if self.updates == 4 and not self.failed:
                    self.failed = True
                    raise OSError("injected lost state checkpoint acknowledgement")
            return result

    root = tmp_path / "historical/smoke"
    store = LostCheckpoint(str(root))
    with pytest.raises(OSError, match="lost state checkpoint acknowledgement"):
        storage.generate(configuration(), fs.LocalFileSystem(), str(tmp_path), store=store)
    assert store.json("_run/state.json")["owner"] is None
    storage.generate(configuration(), fs.LocalFileSystem(), str(tmp_path), resume=True, prepare_only=True)
