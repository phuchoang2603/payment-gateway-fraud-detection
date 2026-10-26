from hashlib import sha256
from pathlib import Path

import pytest

from simulator.object_store import CleanupFailed, ObjectStore
from simulator.publication import complete_publication, finish_publication
from tests.conftest import small_config


def test_completed_publication_refuses_corrupt_or_incompatible_source(tmp_path):
    config = small_config()
    store = ObjectStore(str(tmp_path))
    data = b"source"
    (tmp_path / "source").write_bytes(data)
    manifest = {
        "configuration": config.model_dump(mode="json"),
        "inventory": [{"key": "source", "bytes": len(data), "sha256": sha256(data).hexdigest()}],
    }
    store.put_json("manifest.json", manifest)
    original = (tmp_path / "manifest.json").read_bytes()
    (tmp_path / "source").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="integrity mismatch"):
        finish_publication(config, store)
    (tmp_path / "source").write_bytes(data)
    with pytest.raises(ValueError, match="configuration differs"):
        finish_publication(small_config(seed=999), store)
    assert finish_publication(config, store, cleanup_intermediates=True) == manifest
    assert store.json("_run/publication.json")["cleanup"] == "removed"
    assert (tmp_path / "manifest.json").read_bytes() == original
    assert (tmp_path / "source").read_bytes() == data


def test_partial_s3_cleanup_counts_only_successful_deletes(tmp_path):
    class Client:
        def delete_objects(self, **kwargs):
            return {"Errors": [{"Key": "denied", "Code": "AccessDenied"}]}

    store = ObjectStore(str(tmp_path))
    store.client, store.bucket = Client(), "test"
    with pytest.raises(CleanupFailed, match="AccessDenied") as failure:
        store._delete_objects(
            [{"Key": "removed", "Size": 7}, {"Key": "denied", "Size": 19}], {"objects": 0, "bytes": 0}
        )
    assert failure.value.removed == {"objects": 1, "bytes": 7}


def test_cleanup_failure_retains_progress_and_can_be_retried(tmp_path, monkeypatch):
    config = small_config()
    store = ObjectStore(str(tmp_path))
    manifest = {"configuration": config.model_dump(mode="json"), "inventory": []}
    store.put_json("manifest.json", manifest)
    original = (tmp_path / "manifest.json").read_bytes()
    purge = store.purge

    def partial(*args, **kwargs):
        raise CleanupFailed("AccessDenied", {"objects": 1, "bytes": 7})

    monkeypatch.setattr(store, "purge", partial)
    with pytest.raises(OSError, match="cleanup is incomplete"):
        complete_publication(store, manifest, cleanup_intermediates=True)
    receipt = store.json("_run/publication.json")
    assert receipt["cleanup"] == "failed" and receipt["bytes"] == 7
    monkeypatch.setattr(store, "purge", purge)
    finish_publication(config, store, cleanup_intermediates=True)
    assert store.json("_run/publication.json")["cleanup"] == "removed"
    assert (tmp_path / "manifest.json").read_bytes() == original


def test_local_cleanup_failure_counts_only_removed_files(tmp_path, monkeypatch):
    store = ObjectStore(str(tmp_path))
    root = tmp_path / "_run"
    root.mkdir()
    (root / "first").write_bytes(b"first")
    (root / "second").write_bytes(b"second")
    unlink = Path.unlink
    deleted = []

    def fail_second(path, *args, **kwargs):
        if deleted:
            raise PermissionError("denied")
        size = path.stat().st_size
        unlink(path, *args, **kwargs)
        deleted.append(size)

    monkeypatch.setattr(Path, "unlink", fail_second)
    with pytest.raises(CleanupFailed) as failure:
        store.purge("_run/")
    assert failure.value.removed == {"objects": 1, "bytes": deleted[0]}


def test_manifest_without_integrity_inventory_is_not_modified(tmp_path):
    store = ObjectStore(str(tmp_path))
    store.put_json("manifest.json", {"dataset_version": "legacy"})
    original = (tmp_path / "manifest.json").read_bytes()
    with pytest.raises(ValueError, match="lacks an integrity inventory"):
        finish_publication(small_config(), store)
    assert (tmp_path / "manifest.json").read_bytes() == original
    assert not (tmp_path / "_run/publication.json").exists()
