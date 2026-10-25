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
        self.drop_contract_field = None

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
            self.entities.setdefault(("product", body["name"]), {"assets": []})
        elif path.startswith("/v1/dataProducts/") and path.endswith("/assets/add"):
            self.entities[("product", path.split("/")[3])]["assets"] = body["assets"]
        elif path == "/v1/dataContracts":
            assert "qualityExpectations" not in body
            self.entities[("contract", body["entity"]["id"])] = body
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

    def get(self, path, query=None):
        if path.startswith("/v1/tables/"):
            return next(
                v for (kind, _), v in self.entities.items() if kind == "table" and v["id"] == path.rsplit("/", 1)[1]
            )
        return self.find(path, query)

    def post(self, path, body):
        existing = self.entities.get(("result", unquote(path.rsplit("/", 1)[1])), [])
        if any(r["timestamp"] == body["timestamp"] for r in existing):
            raise ApiError(409, "POST", path)
        return self.put(path, body)

    def patch(self, path, operations):
        entity = self.get(path)
        for operation in operations:
            entity[operation["path"].lstrip("/")] = operation["value"]
        return entity

    def find(self, path, query=None):
        if self.unavailable:
            raise ConnectionError("metadata unavailable")
        path = unquote(path)
        if path.startswith("/v1/dataProducts/name/"):
            product = self.entities.get(("product", path.split("/")[4]))
            return {"data": product["assets"]} if product else None
        if path.startswith("/v1/tables/name/"):
            return self.entities.get(("table", path.rsplit("/", 1)[1]))
        if path == "/v1/dataContracts/entity":
            contract = json.loads(json.dumps(self.entities.get(("contract", query["entityId"]))))
            if contract:
                for rule in contract["odcsQualityRules"]:
                    rule.setdefault("tags", [])
                if self.drop_contract_field:
                    contract.pop(self.drop_contract_field, None)
            return contract
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
            identities[t] = {"id": entity["id"], "fqn": entity["fullyQualifiedName"], "columns": entity["columns"]}
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
    contracts = [v for (kind, _), v in catalog.entities.items() if kind == "contract"]
    assert len(contracts) == 4
    assert all(
        "sha256=" in c["description"] and "artifact=contracts/data-products/" in c["description"] for c in contracts
    )
    assert all(c["name"].startswith("urn:datacontract:fraud:") for c in contracts)
    bronze = next(c for c in contracts if c["name"].endswith(":bronze-payment-events"))
    assert isinstance(bronze["odcsQualityRules"][0]["implementation"], str)
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
    assert all(len(rs) == 1 for rs in results)


def test_replay_refuses_conflicting_execution_evidence(tmp_path, monkeypatch):
    report = fixture_report()
    store, run, contracts = prepared(tmp_path, monkeypatch, report)
    catalog = FakeCatalog()
    assert publish(store, run, contracts, catalog, discovery_for(catalog), sleep=lambda _: None)["outcome"] == "passed"
    report["tables"]["payment_events"]["gx"]["results"][0]["result"]["unexpected_count"] = 2
    store.put(report_key(run, contracts), report)
    replay = publish(store, run, contracts, catalog, discovery_for(catalog), sleep=lambda _: None)
    assert replay["outcome"] == "error" and replay["error_type"] == "ValueError"
    results = catalog.entities[("result", "lake.bronze.raw_payment_events.payment_envelope")]
    assert len(results) == 1 and results[0]["testCaseStatus"] == "Failed"


@pytest.mark.parametrize("missing", ["results", "description", "odcsQualityRules", "schema"])
def test_unresolved_readback_is_incomplete_governance(tmp_path, monkeypatch, missing):
    store, run, contracts = prepared(tmp_path, monkeypatch, fixture_report())
    catalog = FakeCatalog()
    catalog.drop_results = missing == "results"
    catalog.drop_contract_field = None if missing == "results" else missing
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
