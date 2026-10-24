from data_platform import ingest
from data_platform.runs import open_run
from data_platform.storage import Store


def test_preprovisioned_namespace_does_not_request_namespace_creation(tmp_path, monkeypatch):
    class Catalog:
        def databaseExists(self, name):
            assert name == "lake.bronze"
            return True

        def tableExists(self, name):
            return True

    class Session:
        catalog = Catalog()

        def sql(self, query):
            raise AssertionError("preprovisioned namespace must not be mutated")

    monkeypatch.setattr(ingest, "verify_files", lambda *args: None)
    monkeypatch.setattr(ingest, "source_frames", lambda *args: {"payment_events": None, "fraud_labels": None})
    monkeypatch.setattr(ingest, "committed_snapshot", lambda *args: 1)
    store = Store(str(tmp_path))
    run = open_run(store, "candidate", "digest", {}, "image", "workflow")
    ingest.ingest(Session(), "source", {}, store, run)
    assert run["snapshots"] == {"payment_events": 1, "fraud_labels": 1}
