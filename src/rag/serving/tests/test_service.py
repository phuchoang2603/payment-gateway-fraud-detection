import asyncio

import pytest
from fastapi.testclient import TestClient
from fraud_rag_common import store
from pydantic import ValidationError

from fraud_rag_serving import app as service

ROWS = {
    "decline-codes#high-amount": {
        "chunk_id": "decline-codes#high-amount",
        "doc_id": "decline-codes",
        "title": "Decline and reason-code glossary",
        "section": "HIGH_AMOUNT",
        "audience": "both",
        "synthetic": True,
        "source_path": "x",
        "text": "Decline and reason-code glossary › HIGH_AMOUNT\n\nSource: risk.",
        "knowledge_version": "v",
        "content_sha256": "h",
    },
    "checkout-api#public-routes": {
        "chunk_id": "checkout-api#public-routes",
        "doc_id": "checkout-api",
        "title": "Merchant checkout API",
        "section": "Requirement: Public routes",
        "audience": "merchant",
        "synthetic": False,
        "source_path": "y",
        "text": "Merchant checkout API › Requirement: Public routes\n\nRoutes.",
        "knowledge_version": "v",
        "content_sha256": "h",
    },
}


class FakeEmbedder:
    def __init__(self):
        self.calls = 0

    async def query(self, client, text):
        self.calls += 1
        return [1.0]


@pytest.fixture
def client(monkeypatch):
    embedder = FakeEmbedder()
    monkeypatch.setattr(service.knowledge, "embedder", embedder)
    monkeypatch.setattr(service.knowledge, "feature_store", object())
    monkeypatch.setattr(store, "read", lambda fs, ids: [ROWS.get(chunk_id) for chunk_id in ids])
    monkeypatch.setattr(
        store,
        "search",
        lambda fs, vector, top_k: [("checkout-api#public-routes", 0.2), ("decline-codes#high-amount", 0.1)],
    )
    test_client = TestClient(service.app)
    test_client.embedder = embedder
    return test_client


def test_chunk_by_id(client):
    response = client.get("/v1/chunks/decline-codes%23high-amount")

    assert response.status_code == 200
    assert response.json()["section"] == "HIGH_AMOUNT"


def test_unknown_chunk_is_404(client):
    assert client.get("/v1/chunks/decline-codes%23missing").status_code == 404


def test_malformed_chunk_id_is_422(client):
    assert client.get("/v1/chunks/not-a-chunk-id").status_code == 422


def test_search_tool_ranks_by_distance_and_filters_audience(client):
    ranked = asyncio.run(service.search_knowledge("high amount")).hits
    analyst = asyncio.run(service.search_knowledge("high amount", audience="analyst")).hits

    assert [hit.chunk.chunk_id for hit in ranked] == ["decline-codes#high-amount", "checkout-api#public-routes"]
    assert [hit.chunk.chunk_id for hit in analyst] == ["decline-codes#high-amount"]


@pytest.mark.parametrize("arguments", [{"query": ""}, {"query": "x", "top_k": 0}, {"query": "x", "top_k": 21}])
def test_invalid_search_is_rejected_without_calling_the_gateway(client, arguments):
    with pytest.raises(ValidationError):
        asyncio.run(service.search_knowledge(**arguments))
    assert client.embedder.calls == 0


def test_mcp_exposes_two_tools_backed_by_the_same_functions(client):
    tools = asyncio.run(service.mcp.list_tools())

    assert {tool.name for tool in tools} == {"search_knowledge", "get_knowledge_chunk"}
    assert asyncio.run(service.get_knowledge_chunk("decline-codes#high-amount")).doc_id == "decline-codes"
    with pytest.raises(ValueError, match="unknown chunk_id"):
        asyncio.run(service.get_knowledge_chunk("decline-codes#missing"))


def test_liveness_needs_nothing(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_readiness_follows_the_database(client, monkeypatch):
    monkeypatch.setattr(service, "ping", lambda: None)
    assert client.get("/readyz").status_code == 200

    def down():
        raise OSError("connection refused")

    monkeypatch.setattr(service, "ping", down)
    assert client.get("/readyz").status_code == 503


def test_store_loads_lazily_and_answers_503_until_the_registry_exists(client, monkeypatch):
    monkeypatch.setattr(service.knowledge, "feature_store", None)

    def missing():
        raise RuntimeError("registry tables do not exist yet")

    monkeypatch.setattr(store, "store", missing)
    assert client.get("/v1/chunks/decline-codes%23high-amount").status_code == 503

    class Loaded:
        def get_feature_view(self, name):
            return name

    monkeypatch.setattr(store, "store", Loaded)
    assert client.get("/v1/chunks/decline-codes%23high-amount").status_code == 200
    assert isinstance(service.knowledge.feature_store, Loaded)


def test_metrics_count_rest_requests_and_tool_outcomes(client):
    client.get("/v1/chunks/decline-codes%23high-amount")
    asyncio.run(service.search_knowledge("high amount"))
    with pytest.raises(ValueError):
        asyncio.run(service.get_knowledge_chunk("decline-codes#missing"))

    metrics = client.get("/metrics/").text

    assert 'knowledge_api_requests_total{endpoint="/v1/chunks/{chunk_id}",status="200"}' in metrics
    assert 'knowledge_mcp_tool_calls_total{outcome="success",tool="search_knowledge"}' in metrics
    assert 'knowledge_mcp_tool_calls_total{outcome="failure",tool="get_knowledge_chunk"}' in metrics


def test_lifespan_builds_the_embedder_and_serves_mcp(monkeypatch):
    monkeypatch.setattr(service.Embedder, "from_env", classmethod(lambda cls: FakeEmbedder()))
    with TestClient(service.app) as running:
        assert isinstance(service.knowledge.embedder, FakeEmbedder)
        response = running.post(
            "/mcp",
            headers={"Accept": "application/json, text/event-stream", "Content-Type": "application/json"},
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0"},
                },
            },
        )
        assert response.status_code == 200
        assert response.json()["result"]["serverInfo"]["name"] == "knowledge"
