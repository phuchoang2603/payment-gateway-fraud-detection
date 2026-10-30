import asyncio

import pytest
from fastapi.testclient import TestClient
from fraud_rag_common import store

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


def test_search_ranks_by_distance_and_filters_audience(client):
    ranked = client.post("/v1/chunks/search", json={"query": "high amount"}).json()["hits"]
    analyst = client.post("/v1/chunks/search", json={"query": "high amount", "audience": "analyst"}).json()["hits"]

    assert [hit["chunk"]["chunk_id"] for hit in ranked] == ["decline-codes#high-amount", "checkout-api#public-routes"]
    assert [hit["chunk"]["chunk_id"] for hit in analyst] == ["decline-codes#high-amount"]


@pytest.mark.parametrize("body", [{"query": ""}, {"query": "x", "top_k": 0}, {"query": "x", "top_k": 21}])
def test_invalid_search_is_422_without_calling_the_gateway(client, body):
    assert client.post("/v1/chunks/search", json=body).status_code == 422
    assert client.embedder.calls == 0


def test_mcp_exposes_two_tools_backed_by_the_same_functions(client):
    tools = asyncio.run(service.mcp.list_tools())

    assert {tool.name for tool in tools} == {"search_knowledge", "get_knowledge_chunk"}
    assert asyncio.run(service.get_knowledge_chunk("decline-codes#high-amount")).doc_id == "decline-codes"
    with pytest.raises(ValueError, match="unknown chunk_id"):
        asyncio.run(service.get_knowledge_chunk("decline-codes#missing"))
