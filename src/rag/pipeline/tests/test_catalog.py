import json

import httpx
import pytest

from fraud_rag_pipeline.openmetadata import client as catalog


def recorder(tables_present=True):
    calls = []

    def handler(request):
        body = json.loads(request.content) if request.content else None
        calls.append((request.method, request.url.path, body))
        if request.method == "GET":
            if not tables_present:
                return httpx.Response(404, json={})
            return httpx.Response(200, json={"id": "table-id"})
        if request.url.path.endswith(("/lineage", "/status")):
            return httpx.Response(200)
        name = (body or {}).get("name", "")
        fqn = f"{body['service']}.{name}" if body and "service" in body else name
        return httpx.Response(200, json={"id": f"{name or 'x'}-id", "name": name, "fullyQualifiedName": fqn})

    client = httpx.Client(base_url="http://om/api/", transport=httpx.MockTransport(handler))
    return catalog.Catalog(client), calls


def test_success_records_status_corpus_and_one_lineage_edge():
    om, calls = recorder()

    summary = catalog.publish(om, "Succeeded", "run-1", "abc")

    paths = [(method, path) for method, path, _ in calls]
    assert ("PUT", "/api/v1/pipelines/argo.rag-knowledge/status") in paths
    corpus = next(body for _, path, body in calls if path == "/api/v1/containers")
    assert corpus["prefix"] == "datasets/payment-gateway/knowledge/abc/"
    edge = next(body for _, path, body in calls if path == "/api/v1/lineage")["edge"]
    assert edge["fromEntity"] == {"id": "knowledge-abc-id", "type": "container"}
    assert edge["toEntity"] == {"id": "table-id", "type": "table"}
    assert edge["lineageDetails"]["pipeline"]["id"] == "rag-knowledge-id"
    assert summary == {"pipeline": "rag-knowledge", "status": "Successful", "lineage_edges": 1}


def test_failure_records_status_only():
    om, calls = recorder()

    summary = catalog.publish(om, "Failed", "run-1", "")

    assert [path for _, path, _ in calls] == ["/api/v1/pipelines", "/api/v1/pipelines/argo.rag-knowledge/status"]
    assert summary["status"] == "Failed" and summary["lineage_edges"] == 0


def test_success_without_version_or_table_fails():
    with pytest.raises(ValueError):
        catalog.publish(recorder()[0], "Succeeded", "run-1", "")
    with pytest.raises(httpx.HTTPStatusError):
        catalog.publish(recorder(tables_present=False)[0], "Succeeded", "run-1", "abc")


ASSETS = {
    "llmService": {"name": "agent-gateway", "baseUrl": "http://gateway/v1"},
    "models": [
        {
            "name": "qwen3-8b",
            "baseModel": "Qwen/Qwen3-8B-FP8",
            "revision": "r1",
            "provider": "Qwen",
            "description": "chat",
        },
        {
            "name": "qwen3-embedding",
            "baseModel": "Qwen/Qwen3-Embedding-0.6B",
            "revision": "r2",
            "provider": "Qwen",
            "description": "emb",
        },
    ],
    "mcpServer": {
        "name": "knowledge",
        "service": "agent-gateway-mcp",
        "url": "http://gateway/mcp/knowledge",
        "description": "kb",
    },
    "agent": {"name": "evidence-agent", "version": "0.2.0", "primaryModel": "qwen3-8b", "description": "Explains."},
}


def test_success_registers_the_ai_assets_linked_to_model_tool_and_table():
    om, calls = recorder()

    summary = catalog.publish(
        om, "Succeeded", "run-1", "abc", ASSETS, [{"name": "search_knowledge", "description": "x"}]
    )

    server = next(body for _, path, body in calls if path == "/api/v1/mcpServers")
    assert [tool["name"] for tool in server["tools"]] == ["search_knowledge"]
    application = next(body for _, path, body in calls if path == "/api/v1/aiApplications")
    assert application["primaryModel"] == "agent-gateway.qwen3-8b"
    assert application["tools"] == [{"id": "knowledge-id", "type": "mcpServer"}]
    assert application["knowledgeBases"] == [{"id": "table-id", "type": "table"}]
    assert "0.2.0" in application["description"]
    assert summary["assets"] == {
        "models": ["qwen3-8b", "qwen3-embedding"],
        "mcpServer": "knowledge",
        "application": "evidence-agent",
    }


def test_failed_runs_register_no_assets():
    om, calls = recorder()

    catalog.publish(om, "Failed", "run-1", "", ASSETS)

    assert not [path for _, path, _ in calls if "llm" in path or "mcp" in path.lower() or "aiApplications" in path]


def test_tools_are_discovered_from_the_mcp_server_through_the_gateway():
    seen = []

    def handler(request):
        body = json.loads(request.content)
        seen.append((body["method"], request.headers.get("authorization"), request.headers.get("mcp-session-id")))
        if body["method"] == "initialize":
            return httpx.Response(200, json={"result": {}}, headers={"mcp-session-id": "s1"})
        if body["method"] == "tools/list":
            tools = [{"name": "search_knowledge", "description": "Search the knowledge base.\n"}]
            event = "event: message\ndata: " + json.dumps({"jsonrpc": "2.0", "id": 2, "result": {"tools": tools}})
            return httpx.Response(200, text=event, headers={"content-type": "text/event-stream"})
        return httpx.Response(202)

    client = httpx.Client(transport=httpx.MockTransport(handler))

    tools = catalog.discover_tools("http://gateway/mcp/knowledge", "k", client)

    assert tools == [{"name": "search_knowledge", "description": "Search the knowledge base."}]
    assert [method for method, _, _ in seen] == ["initialize", "notifications/initialized", "tools/list"]
    assert all(auth == "Bearer k" for _, auth, _ in seen)
    assert seen[-1][2] == "s1"
