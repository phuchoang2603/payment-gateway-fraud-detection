import json
from urllib.parse import unquote

import pytest

from data_platform.governance import PIPELINE_FQN, publish
from data_platform.openmetadata import ApiError, Client
from data_platform.runs import open_run, report_key
from data_platform.storage import Store


class FakeCatalog:
    def __init__(self):
        self.entities = {}
        self.writes = []
        self.unavailable = False
        self.drop_results = False

    def put(self, path, body, query=None, content_type="application/json"):
        if self.unavailable:
            raise ConnectionError("metadata unavailable")
        self.writes.append(path)
        if path == "/v1/tables":
            fqn = body["databaseSchema"] + "." + body["name"]
            entity = self.entities.setdefault(("table", fqn), {"id": "id-" + fqn, "fullyQualifiedName": fqn})
            entity["columns"] = body["columns"]
            return entity
        if path == "/v1/dataProducts":
            self.entities[("product", body["name"])] = {"assets": body["assets"]}
        elif path == "/v1/dataContracts/odcs/yaml":
            assert query["createTestCases"] == "false" and body.startswith(b"apiVersion")
            self.entities[("contract", query["entityId"])] = {"entity": query["entityId"]}
        elif path == "/v1/pipelines":
            return self.entities.setdefault(("pipeline", body["name"]), {"id": "id-pipeline", **body})
        elif path.endswith("/status"):
            self.entities.setdefault(("status", unquote(path)), []).append(body)
        elif path == "/v1/lineage":
            edge = body["edge"]
            self.entities.setdefault(("lineage", edge["toEntity"]["id"]), set()).add(edge["fromEntity"]["id"])
        elif path == "/v1/dataQuality/testCases":
            fqn = body["entityLink"].split("::")[2].rstrip(">") + "." + body["name"]
            return {"fullyQualifiedName": fqn}
        elif path.startswith("/v1/dataQuality/testCases/testCaseResults/"):
            self.entities.setdefault(("result", unquote(path.rsplit("/", 1)[1])), []).append(body)
        return {"id": "id-" + path}

    def find(self, path, query=None):
        if self.unavailable:
            raise ConnectionError("metadata unavailable")
        path = unquote(path)
        if path.startswith("/v1/dataProducts/name/"):
            return self.entities.get(("product", path.rsplit("/", 1)[1]))
        if path.startswith("/v1/tables/name/"):
            return self.entities.get(("table", path.rsplit("/", 1)[1]))
        if path == "/v1/dataContracts/entity":
            return self.entities.get(("contract", query["entityId"]))
        if path.startswith("/v1/lineage/table/name/"):
            table = self.entities[("table", path.rsplit("/", 1)[1])]
            return {
                "upstreamEdges": [
                    {"fromEntity": source, "toEntity": table["id"]}
                    for source in self.entities.get(("lineage", table["id"]), ())
                ]
            }
        if path.startswith("/v1/dataQuality/testCases/testCaseResults/"):
            if self.drop_results:
                return {"data": []}
            return {"data": self.entities.get(("result", path.rsplit("/", 1)[1]), [])}
        if path.endswith("/status"):
            return {"data": self.entities.get(("status", path), [])}


def fixture_report(outcome="failed"):
    return {
        "run_id": "logical-run",
        "run_time": "2026-01-01T00:00:00+00:00",
        "manifest_sha256": "a" * 64,
        "data_outcome": outcome,
        "snapshots": {"payment_events": 1, "fraud_labels": 2},
        "contracts": {"payment_events": "b" * 64, "fraud_labels": "c" * 64},
        "execution_errors": [],
        "tables": {
            "payment_events": {
                "gx": {
                    "success": outcome == "passed",
                    "results": [
                        {
                            "success": outcome == "passed",
                            "expectation_config": {"meta": {"id": "payment_envelope"}},
                            "result": {"unexpected_count": 1},
                            "exception_info": {"raised_exception": outcome == "error"},
                        }
                    ],
                }
            }
        },
    }


def discovery_for(catalog):
    def discover(*_):
        identities = {}
        for t in ("payment_events", "fraud_labels"):
            entity = catalog.put("/v1/tables", {"databaseSchema": "lake.bronze", "name": f"raw_{t}", "columns": [{}]})
            identities[t] = {"id": entity["id"], "fqn": entity["fullyQualifiedName"]}
        return identities

    return discover


def prepared(tmp_path, monkeypatch, report=None):
    monkeypatch.setenv("OPENMETADATA_URL", "http://unused")
    monkeypatch.setenv("OPENMETADATA_TOKEN", "test-only")
    store = Store(str(tmp_path))
    contracts = {"payment_events": "b" * 64, "fraud_labels": "c" * 64}
    run = open_run(store, "v10", "a" * 64, contracts, "image", "workflow")
    run.update(source_root="s3://synthetic-source/historical/v10", snapshots={"payment_events": 1, "fraud_labels": 2})
    if report:
        report["run_id"] = run["run_id"]
        store.put(report_key(run, contracts), report)
    return store, run, contracts


def test_publication_covers_products_contracts_lineage_and_results(tmp_path, monkeypatch):
    report = fixture_report()
    store, run, contracts = prepared(tmp_path, monkeypatch, report)
    catalog = FakeCatalog()
    outcome = publish(store, run, contracts, catalog, discovery_for(catalog), sleep=lambda _: None)
    assert outcome["outcome"] == "passed" and outcome["readback_verified"]
    assert set(outcome["identities"]) == {"payment_events", "fraud_labels"}
    products = {k[1]: v for k, v in catalog.entities.items() if k[0] == "product"}
    assert set(products) == {"historical-payments", "bronze-payments"}
    assert len(products["bronze-payments"]["assets"]) == 2
    result = catalog.entities[("result", "lake.bronze.raw_payment_events.payment_envelope")][0]
    assert result["testCaseStatus"] == "Failed"
    assert f"run_id={run['run_id']}" in result["result"] and "contract_sha256=" + "b" * 64 in result["result"]
    assert catalog.entities[("status", f"/v1/pipelines/{PIPELINE_FQN}/status")][0]["executionStatus"] == "Failed"
    assert len([k for k in catalog.entities if k[0] == "contract"]) == 4
    assert store.get("accepted/v10.json") is None


def test_execution_errors_are_aborted_not_passed(tmp_path, monkeypatch):
    report = fixture_report("error")
    report["execution_errors"] = [{"stage": "validate", "type": "RuntimeError"}]
    store, run, contracts = prepared(tmp_path, monkeypatch, report)
    catalog = FakeCatalog()
    publish(store, run, contracts, catalog, discovery_for(catalog), sleep=lambda _: None)
    statuses = {k[1]: v[0]["testCaseStatus"] for k, v in catalog.entities.items() if k[0] == "result"}
    assert statuses["lake.bronze.raw_payment_events.payment_envelope"] == "Aborted"
    assert statuses["lake.bronze.raw_payment_events.dp1.execution"] == "Aborted"
    assert statuses["lake.bronze.raw_fraud_labels.dp1.execution"] == "Aborted"


def test_missing_report_is_durably_error_not_acceptance(tmp_path, monkeypatch):
    store, run, contracts = prepared(tmp_path, monkeypatch)
    catalog = FakeCatalog()
    outcome = publish(store, run, contracts, catalog, discovery_for(catalog), sleep=lambda _: None)
    assert outcome["outcome"] == "error" and not outcome["readback_verified"]
    assert store.get(report_key(run, contracts).replace("validation.json", "governance.json")) == outcome
    assert store.get("accepted/v10.json") is None


def test_outage_recovery_preserves_outcomes_and_reuses_identities(tmp_path, monkeypatch):
    report = fixture_report()
    store, run, contracts = prepared(tmp_path, monkeypatch, report)
    discoveries = []

    def discovery(*args):
        discoveries.append(True)
        return discovery_for(catalog)()

    catalog = FakeCatalog()
    catalog.unavailable = True
    failed = publish(store, run, contracts, catalog, discovery, sleep=lambda _: None)
    assert failed["outcome"] == "error" and failed["data_outcome"] == "failed"
    catalog.unavailable = False
    first = publish(store, run, contracts, catalog, discovery, sleep=lambda _: None)
    replay = publish(store, run, contracts, catalog, discovery, sleep=lambda _: None)
    assert first["outcome"] == replay["outcome"] == "passed" and first["data_outcome"] == "failed"
    assert len(discoveries) == 2
    assert store.get(report_key(run, contracts)) == report
    assert store.get("accepted/v10.json") is None
    results = [v for k, v in catalog.entities.items() if k[0] == "result"]
    assert all(len({r["timestamp"] for r in rs}) == 1 for rs in results)


def test_unresolved_readback_is_incomplete_governance(tmp_path, monkeypatch):
    store, run, contracts = prepared(tmp_path, monkeypatch, fixture_report())
    catalog = FakeCatalog()
    catalog.drop_results = True
    outcome = publish(store, run, contracts, catalog, discovery_for(catalog), sleep=lambda _: None)
    assert outcome["outcome"] == "error" and not outcome["readback_verified"]
    assert outcome["error_type"] == "RuntimeError"


class Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return self.payload


def test_client_sends_bearer_token_and_retries_server_errors():
    from urllib import error

    calls = []

    def opener(req, timeout):
        calls.append(req)
        if len(calls) == 1:
            raise error.HTTPError(req.full_url, 503, "unavailable", {}, None)
        return Response(json.dumps({"ok": True}).encode())

    client = Client("http://catalog/api/", "secret", opener, sleep=lambda _: None)
    assert client.put("/v1/tables", {"name": "x"}) == {"ok": True}
    assert len(calls) == 2 and calls[1].get_header("Authorization") == "Bearer secret"
    assert calls[1].full_url == "http://catalog/api/v1/tables"


def test_client_does_not_retry_authorization_failures_and_find_maps_404():
    from urllib import error

    def opener(req, timeout):
        raise error.HTTPError(req.full_url, 401 if "tables" in req.full_url else 404, "no", {}, None)

    client = Client("http://catalog/api", "secret", opener, sleep=lambda _: None)
    with pytest.raises(ApiError):
        client.get("/v1/tables")
    assert client.find("/v1/dataProducts/name/x") is None


def test_lake_columns_preserve_nested_iceberg_types():
    from pyiceberg.schema import Schema
    from pyiceberg.types import ListType, NestedField, StringType, StructType, TimestamptzType

    from data_platform.catalog import lake_columns

    schema = Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "occurred_at", TimestamptzType()),
        NestedField(3, "items", ListType(4, StructType(NestedField(5, "sku", StringType())), element_required=False)),
    )
    columns = {c["name"]: c for c in lake_columns(schema)}
    assert columns["occurred_at"]["dataType"] == "TIMESTAMP"
    assert columns["items"]["dataType"] == "ARRAY" and columns["items"]["arrayDataType"] == "STRUCT"
    assert columns["items"]["children"][0]["name"] == "sku"


def test_catalog_signing_and_file_io_use_metadata_identity(monkeypatch):
    from data_platform.catalog import iceberg_config

    for name, value in {
        "ICEBERG_REST_URI": "https://catalog/iceberg/v1",
        "CATALOG_ACCESS_KEY_ID": "metadata-key",
        "CATALOG_SECRET_ACCESS_KEY": "metadata-secret",
        "LAKE_ACCESS_KEY_ID": "writer-key",
        "LAKE_SECRET_ACCESS_KEY": "writer-secret",
    }.items():
        monkeypatch.setenv(name, value)
    properties = iceberg_config()
    assert properties["client.access-key-id"] == properties["s3.access-key-id"] == "metadata-key"
    assert properties["client.secret-access-key"] == properties["s3.secret-access-key"] == "metadata-secret"
