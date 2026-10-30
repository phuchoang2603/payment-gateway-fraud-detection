import json
import os
import time
from urllib.parse import quote

import httpx

PIPELINE_SERVICE = "argo"
PIPELINE = "rag-knowledge"
STORAGE_SERVICE = "platform-object-storage"
KNOWLEDGE_PREFIX = "datasets/payment-gateway/knowledge"
# Same naming as the data pipelines' warehouse helper: ClickHouse service, default database, lakehouse schema.
CHUNKS_TABLE = 'clickhouse.default.lakehouse."knowledge.chunks"'


MCP_HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


def status_for(argo_status: str) -> str:
    return "Successful" if argo_status == "Succeeded" else "Failed"


def _mcp_result(response: httpx.Response) -> dict:
    response.raise_for_status()
    if response.headers.get("content-type", "").startswith("text/event-stream"):
        data = [line[len("data:") :].strip() for line in response.text.splitlines() if line.startswith("data:")]
        return json.loads(data[-1])["result"]
    return response.json()["result"]


def discover_tools(url: str, api_key: str, client: httpx.Client | None = None) -> list[dict]:
    """List an MCP server's tools through the gateway, so the catalog always matches the server."""
    http = client or httpx.Client(timeout=30.0)
    headers = {**MCP_HEADERS, "Authorization": f"Bearer {api_key}"}
    initialize = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "catalog", "version": "1"},
        },
    }
    response = http.post(url, headers=headers, json=initialize)
    _mcp_result(response)
    if session := response.headers.get("mcp-session-id"):
        headers["Mcp-Session-Id"] = session
    http.post(url, headers=headers, json={"jsonrpc": "2.0", "method": "notifications/initialized"}).raise_for_status()
    tools = _mcp_result(http.post(url, headers=headers, json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
    return [{"name": tool["name"], "description": (tool.get("description") or "").strip()} for tool in tools["tools"]]


class Catalog:
    def __init__(self, client: httpx.Client):
        self.client = client

    @classmethod
    def from_env(cls) -> "Catalog":
        return cls(
            httpx.Client(
                base_url=os.environ["OPENMETADATA_URL"].rstrip("/") + "/",
                headers={"Authorization": f"Bearer {os.environ['OPENMETADATA_TOKEN']}"},
                timeout=30.0,
            )
        )

    def _put(self, path: str, body: dict) -> dict:
        response = self.client.put(path, json=body)
        response.raise_for_status()
        # Lineage and pipeline-status writes succeed with an empty body.
        return response.json() if response.content else {}

    def _get(self, path: str) -> dict:
        response = self.client.get(path)
        response.raise_for_status()
        return response.json()

    def pipeline(self) -> dict:
        return self._put(
            "v1/pipelines",
            {
                "name": PIPELINE,
                "service": PIPELINE_SERVICE,
                "description": "RAG knowledge chunks embedded through the agent gateway",
            },
        )

    def status(self, argo_status: str, run_id: str) -> None:
        self._put(
            f"v1/pipelines/{quote(f'{PIPELINE_SERVICE}.{PIPELINE}', safe='')}/status",
            {"timestamp": int(time.time() * 1000), "executionStatus": status_for(argo_status), "executionId": run_id},
        )

    def corpus(self, version: str) -> dict:
        return self._put(
            "v1/containers",
            {
                "name": f"knowledge-{version}",
                "service": STORAGE_SERVICE,
                "prefix": f"{KNOWLEDGE_PREFIX}/{version}/",
                "description": f"RAG knowledge corpus {version}: rendered and synthetic documents with their manifest",
            },
        )

    def lineage(self, pipeline: dict, corpus: dict) -> None:
        table = self._get(f"v1/tables/name/{quote(CHUNKS_TABLE, safe='')}")
        self._put(
            "v1/lineage",
            {
                "edge": {
                    "fromEntity": {"id": corpus["id"], "type": "container"},
                    "toEntity": {"id": table["id"], "type": "table"},
                    "lineageDetails": {"pipeline": {"id": pipeline["id"], "type": "pipeline"}},
                }
            },
        )

    def assets(self, assets: dict, tools: list[dict]) -> dict:
        llm = assets["llmService"]
        self._put(
            "v1/services/llmServices",
            {
                "name": llm["name"],
                "serviceType": "CustomLLM",
                "description": f"Self-hosted models behind the agent gateway at {llm['baseUrl']}; clients use API keys.",
            },
        )
        models = {}
        for model in assets["models"]:
            models[model["name"]] = self._put(
                "v1/llmModels",
                {
                    "name": model["name"],
                    "service": llm["name"],
                    "baseModel": model["baseModel"],
                    "modelVersion": model["revision"],
                    "modelProvider": model["provider"],
                    "description": model["description"],
                    "deploymentInfo": {"deploymentType": "SelfHosted", "endpoint": llm["baseUrl"]},
                },
            )
        mcp = assets["mcpServer"]
        self._put(
            "v1/services/mcpServices",
            {"name": mcp["service"], "serviceType": "Mcp", "description": "MCP servers behind the agent gateway."},
        )
        server = self._put(
            "v1/mcpServers",
            {
                "name": mcp["name"],
                "service": mcp["service"],
                "serverType": "DataAccess",
                "transportType": "StreamableHTTP",
                "developmentStage": "Development",
                "description": mcp["description"],
                "connectionConfig": {"url": mcp["url"]},
                "capabilities": {"toolsSupported": True},
                "tools": [
                    {
                        "name": tool["name"],
                        "description": tool["description"],
                        "toolCategory": "SearchOperation",
                        "riskLevel": "Low",
                        "sideEffects": False,
                        "idempotent": True,
                    }
                    for tool in tools
                ],
            },
        )
        agent = assets["agent"]
        primary = models[agent["primaryModel"]]
        table = self._get(f"v1/tables/name/{quote(CHUNKS_TABLE, safe='')}")
        application = self._put(
            "v1/aiApplications",
            {
                "name": agent["name"],
                "applicationType": "Agent",
                "developmentStage": "Development",
                "description": f"{agent['description']} kagent agent, version {agent['version']}.",
                "primaryModel": primary["fullyQualifiedName"],
                "modelConfigurations": [{"model": {"id": primary["id"], "type": "llmModel"}, "purpose": "Primary"}],
                "tools": [{"id": server["id"], "type": "mcpServer"}],
                "knowledgeBases": [{"id": table["id"], "type": "table"}],
                "framework": {"name": "Custom", "language": "Python"},
            },
        )
        return {"models": sorted(models), "mcpServer": server["name"], "application": application["name"]}


def publish(
    catalog: Catalog,
    argo_status: str,
    run_id: str,
    version: str,
    assets: dict | None = None,
    tools: list[dict] | None = None,
) -> dict:
    pipeline = catalog.pipeline()
    catalog.status(argo_status, run_id)
    summary = {"pipeline": PIPELINE, "status": status_for(argo_status), "lineage_edges": 0}
    if argo_status == "Succeeded":
        if not version:
            raise ValueError("a successful run must have a knowledge version")
        catalog.lineage(pipeline, catalog.corpus(version))
        summary["lineage_edges"] = 1
        if assets:
            summary["assets"] = catalog.assets(assets, tools or [])
    return summary
