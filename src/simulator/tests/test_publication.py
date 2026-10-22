from hashlib import sha256
from pathlib import Path

import pytest

from simulator.object_store import CleanupFailed, ObjectStore
from simulator.publication import complete_publication, finish_publication
from tests.conftest import small_config


class TagService:
    def __init__(self):
        self.tags = {}
        self.fail_key = "second"

    def get_object_tagging(self, *, Bucket, Key):
        return {"TagSet": [{"Key": key, "Value": value} for key, value in self.tags.get(Key, {}).items()]}

    def put_object_tagging(self, *, Bucket, Key, Tagging):
        if self.tags.get(Key, {}).get("accepted") == "true":
            raise AssertionError("protected objects cannot be tagged again")
        self.tags[Key] = {item["Key"]: item["Value"] for item in Tagging["TagSet"]}
        if Key == self.fail_key:
            raise OSError("lost tagging acknowledgement")


class RemoteStore:
    remote = True
    bucket = "test"

    def __init__(self, root):
        self.local = ObjectStore(str(root))
        self.client = TagService()

    def __getattr__(self, name):
        return getattr(self.local, name)

    def _key(self, key):
        return key

    tag = ObjectStore.tag


def test_failed_tagging_recovers_without_retagging_or_rewriting_completed_objects(tmp_path):
    config = small_config(accepted_version=True)
    store = RemoteStore(tmp_path)
    inventory = []
    for key in ("first", "second"):
        data = key.encode()
        (tmp_path / key).write_bytes(data)
        inventory.append({"key": key, "bytes": len(data), "sha256": sha256(data).hexdigest()})
    manifest = {"configuration": config.model_dump(mode="json"), "inventory": inventory}
    store.put_json("manifest.json", manifest)
    original = (tmp_path / "manifest.json").read_bytes()
    store.client.tags["first"] = {"owner": "source"}
    with pytest.raises(OSError, match="rerun the same historical command"):
        complete_publication(store, manifest, cleanup_intermediates=True)
    assert store.json("_run/publication.json")["tagging"] == "failed"
    assert store.client.tags["first"]["owner"] == "source"
    (tmp_path / "first").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="integrity mismatch"):
        finish_publication(config, store)
    (tmp_path / "first").write_bytes(b"first")
    with pytest.raises(ValueError, match="configuration differs"):
        finish_publication(small_config(seed=999, accepted_version=True), store)
    with pytest.raises(ValueError, match="configuration differs"):
        finish_publication(small_config(accepted_version=True, validation_gate="dp1-spark-gx"), store)
    assert finish_publication(config, store, cleanup_intermediates=True) == manifest
    receipt = store.json("_run/publication.json")
    assert receipt["tagging"] == "tagged" and receipt["cleanup"] == "removed"
    assert all(tags["accepted"] == "true" for tags in store.client.tags.values())
    assert (tmp_path / "manifest.json").read_bytes() == original


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


def test_legacy_publication_without_integrity_inventory_is_not_modified(tmp_path):
    store = ObjectStore(str(tmp_path))
    store.put_json("manifest.json", {"dataset_version": "legacy"})
    original = (tmp_path / "manifest.json").read_bytes()
    with pytest.raises(ValueError, match="lacks an integrity inventory"):
        finish_publication(small_config(), store)
    assert (tmp_path / "manifest.json").read_bytes() == original
    assert not (tmp_path / "_run/publication.json").exists()
