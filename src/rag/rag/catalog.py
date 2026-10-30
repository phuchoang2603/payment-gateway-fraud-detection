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


def status_for(argo_status: str) -> str:
    return "Successful" if argo_status == "Succeeded" else "Failed"


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
        return response.json()

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

    def assets(self, assets: dict) -> dict:
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
                    for tool in mcp["tools"]
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


def publish(catalog: Catalog, argo_status: str, run_id: str, version: str, assets: dict | None = None) -> dict:
    pipeline = catalog.pipeline()
    catalog.status(argo_status, run_id)
    summary = {"pipeline": PIPELINE, "status": status_for(argo_status), "lineage_edges": 0}
    if argo_status == "Succeeded":
        if not version:
            raise ValueError("a successful run must have a knowledge version")
        catalog.lineage(pipeline, catalog.corpus(version))
        summary["lineage_edges"] = 1
        if assets:
            summary["assets"] = catalog.assets(assets)
    return summary
