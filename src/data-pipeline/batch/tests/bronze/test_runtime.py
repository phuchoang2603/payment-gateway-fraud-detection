import pytest

from fraud_batch.bronze import ingest
from fraud_batch.bronze.runs import open_run
from fraud_batch.common.storage import Store


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
            from fraud_batch.common.contracts import prepare

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
    from fraud_batch.common.contracts import prepare

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
    from fraud_batch.common.catalog import native

    monkeypatch.delenv("REQUIRED_CONNECTOR_SECRET", raising=False)
    with pytest.raises(KeyError, match="REQUIRED_CONNECTOR_SECRET"):
        native("ingest", {"password": "${REQUIRED_CONNECTOR_SECRET}"})


def test_installed_contract_root_is_found_from_project_ancestors(tmp_path, monkeypatch):
    from fraud_batch.common import products

    root = tmp_path / "checkout"
    contracts = root / "contracts/data-products"
    contracts.mkdir(parents=True)
    installed = root / "src/data-pipeline/batch/.venv/lib/python3.12/site-packages/fraud_batch/common/products.py"
    installed.parent.mkdir(parents=True)
    monkeypatch.delenv("DATA_PRODUCTS_ROOT", raising=False)
    monkeypatch.setattr(products, "__file__", str(installed))
    monkeypatch.chdir(tmp_path)
    assert products.contracts_root() == contracts
