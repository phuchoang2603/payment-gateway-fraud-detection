import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fraud_rag_common import store
from hypothesis import given, settings
from hypothesis import strategies as st
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
        self.client = object()

    async def query(self, client, text):
        assert client is self.client
        assert isinstance(text, str) and text
        self.calls += 1
        return [1.0]


@pytest.fixture
def client(monkeypatch):
    embedder = FakeEmbedder()
    monkeypatch.setattr(service.knowledge, "embedder", embedder)
    registry = object()
    monkeypatch.setattr(service.knowledge, "feature_store", registry)
    monkeypatch.setattr(service.knowledge, "client", embedder.client)

    def read(fs, ids):
        assert fs is registry
        return [ROWS.get(chunk_id) for chunk_id in ids]

    def search(fs, vector, top_k):
        assert fs is registry
        assert vector == [1.0]
        assert isinstance(top_k, int) and top_k > 0
        return [("checkout-api#public-routes", 0.2), ("decline-codes#high-amount", 0.1)][:top_k]

    monkeypatch.setattr(store, "read", read)
    monkeypatch.setattr(store, "search", search)
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


@pytest.mark.parametrize(
    "arguments",
    [{"query": ""}, {"query": "x" * 2001}, {"query": "x", "top_k": 0}, {"query": "x", "top_k": 21}],
)
def test_invalid_search_is_rejected_without_calling_the_gateway(client, arguments):
    with pytest.raises(ValidationError):
        asyncio.run(service.search_knowledge(**arguments))
    assert client.embedder.calls == 0


@pytest.mark.parametrize("query,top_k", [("x", 1), ("x" * 2000, 20)])
def test_search_accepts_boundary_values_and_limits_results(client, query, top_k):
    hits = asyncio.run(service.search_knowledge(query, top_k=top_k)).hits

    assert len(hits) == min(top_k, len(ROWS))
    assert client.embedder.calls == 1


def test_repeated_chunk_reads_are_stable_for_arbitrary_existing_ids(client):
    @settings(max_examples=10, derandomize=True)
    @given(ids=st.lists(st.sampled_from(list(ROWS)), min_size=1, max_size=10))
    def repeated(ids):
        async def read():
            return [await service.get_knowledge_chunk(chunk_id) for chunk_id in ids]

        first, second = asyncio.run(read()), asyncio.run(read())
        assert [chunk.model_dump() for chunk in first] == [ROWS[chunk_id] for chunk_id in ids]
        assert first == second
        assert client.embedder.calls == 0

    repeated()


def test_concurrent_first_reads_initialize_the_registry_once(monkeypatch):
    loaded = []

    class Registry:
        def get_feature_view(self, name):
            assert name == store.VIEW

    def load():
        registry = Registry()
        loaded.append(registry)
        return registry

    monkeypatch.setattr(store, "store", load)
    knowledge = service.Knowledge()

    async def read():
        return await asyncio.gather(*(knowledge.store() for _ in range(10)))

    results = asyncio.run(read())
    assert len(loaded) == 1
    assert all(result is loaded[0] for result in results)


def test_embedding_failure_does_not_return_partial_search_results(client, monkeypatch):
    async def unavailable(*_):
        raise TimeoutError("embedding unavailable")

    failures = service.TOOL_CALLS.labels("search_knowledge", "failure")
    before = failures._value.get()
    monkeypatch.setattr(client.embedder, "query", unavailable)
    with pytest.raises(TimeoutError, match="embedding unavailable"):
        asyncio.run(service.search_knowledge("high amount"))

    assert failures._value.get() == before + 1


def test_mcp_exposes_two_tools_backed_by_the_same_functions(client):
    tools = asyncio.run(service.mcp.list_tools())

    assert {tool.name for tool in tools} == {"search_knowledge", "get_knowledge_chunk"}
    assert asyncio.run(service.get_knowledge_chunk("decline-codes#high-amount")).doc_id == "decline-codes"
    with pytest.raises(ValueError, match="unknown chunk_id"):
        asyncio.run(service.get_knowledge_chunk("decline-codes#missing"))


def test_liveness_needs_nothing(client):
    assert client.get("/healthz").json() == {"status": "ok"}


@pytest.mark.parametrize("query_fails", [False, True])
def test_readiness_probes_the_database_and_hides_dependency_errors(client, monkeypatch, query_fails):
    queries = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, query):
            queries.append(query)
            if query_fails:
                raise OSError("private database error")

    def connect(*, host, connect_timeout):
        assert host == "knowledge-db"
        assert 0 < connect_timeout <= 3
        return Connection()

    monkeypatch.setattr(store, "connection", lambda: {"host": "knowledge-db"})
    monkeypatch.setattr(service.psycopg, "connect", connect)
    response = client.get("/readyz")

    assert queries == ["SELECT 1"]
    assert response.status_code == (503 if query_fails else 200)
    assert response.json() == ({"detail": "knowledge store unavailable"} if query_fails else {"status": "ready"})


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
    monkeypatch.setattr(store, "read", lambda fs, ids: [ROWS.get(chunk_id) for chunk_id in ids])
    assert client.get("/v1/chunks/decline-codes%23high-amount").status_code == 200
    assert isinstance(service.knowledge.feature_store, Loaded)


def test_warm_loads_the_store_and_tolerates_a_missing_registry(monkeypatch):
    monkeypatch.setattr(service.knowledge, "feature_store", None)

    def missing():
        raise RuntimeError("registry tables do not exist yet")

    monkeypatch.setattr(store, "store", missing)
    asyncio.run(service.knowledge.warm())
    assert service.knowledge.feature_store is None

    class Loaded:
        def get_feature_view(self, name):
            return name

    monkeypatch.setattr(store, "store", Loaded)
    asyncio.run(service.knowledge.warm())
    assert isinstance(service.knowledge.feature_store, Loaded)


def test_metrics_count_rest_requests_and_tool_outcomes(client):
    counters = [
        service.REQUESTS.labels("/v1/chunks/{chunk_id}", "200"),
        service.TOOL_CALLS.labels("search_knowledge", "success"),
        service.TOOL_CALLS.labels("get_knowledge_chunk", "failure"),
    ]
    before = [counter._value.get() for counter in counters]
    client.get("/v1/chunks/decline-codes%23high-amount")
    asyncio.run(service.search_knowledge("high amount"))
    with pytest.raises(ValueError):
        asyncio.run(service.get_knowledge_chunk("decline-codes#missing"))

    assert [counter._value.get() for counter in counters] == [value + 1 for value in before]


def test_lifespan_builds_the_embedder_and_serves_mcp(monkeypatch):
    monkeypatch.setattr(service.Embedder, "from_env", classmethod(lambda cls: FakeEmbedder()))
    monkeypatch.setattr(service.knowledge, "feature_store", object())
    application = FastAPI(lifespan=service.lifespan)
    application.mount(
        "/",
        service.mcp.streamable_http_app(
            stateless_http=True,
            json_response=True,
            transport_security=service.TransportSecuritySettings(enable_dns_rebinding_protection=False),
        ),
    )
    with TestClient(application) as running:
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
