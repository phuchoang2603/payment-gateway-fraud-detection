import pytest
from pipeline_common.storage import Store

from data_platform import ingest
from data_platform.runs import open_run


def test_preprovisioned_namespace_does_not_request_namespace_creation(tmp_path, monkeypatch):
    class Catalog:
        def databaseExists(self, name):
            assert name == "lake.bronze"
            return True

        def tableExists(self, name):
            return True

    class Session:
        catalog = Catalog()

        def table(self, name):
            from pipeline_common.contracts import prepare

            class Table:
                schema = prepare("payment_events" if name.endswith("payment_events") else "fraud_labels").schema

            return Table()

        def sql(self, query):
            if query.startswith("SHOW CREATE TABLE lake.bronze.raw_"):
                column = "occurred_at" if query.endswith("payment_events") else "label_available_at"

                class Definition:
                    def first(self):
                        return [f"PARTITIONED BY (_source_dataset_version, days({column}))"]

                return Definition()
            raise AssertionError("preprovisioned namespace must not be mutated")

    monkeypatch.setattr(ingest, "verify_content", lambda *args: None)
    monkeypatch.setattr(ingest, "verify_files", lambda *args: None)
    from pipeline_common.contracts import prepare

    frames = {
        table: type("Frame", (), {"schema": prepare(table).schema})() for table in ("payment_events", "fraud_labels")
    }
    monkeypatch.setattr(ingest, "source_frames", lambda *args: frames)
    monkeypatch.setattr(ingest, "committed_snapshot", lambda *args: 1)
    store = Store(str(tmp_path))
    run = open_run(store, "candidate", "digest", {}, "image", "workflow")
    ingest.ingest(Session(), "source", {}, store, run)
    assert run["snapshots"] == {"payment_events": 1, "fraud_labels": 1}


def test_missing_native_secret_fails_before_starting_a_process(monkeypatch):
    from pipeline_common.catalog import native

    monkeypatch.delenv("REQUIRED_CONNECTOR_SECRET", raising=False)
    with pytest.raises(KeyError, match="REQUIRED_CONNECTOR_SECRET"):
        native("ingest", {"password": "${REQUIRED_CONNECTOR_SECRET}"})
