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


def publish(catalog: Catalog, argo_status: str, run_id: str, version: str) -> dict:
    pipeline = catalog.pipeline()
    catalog.status(argo_status, run_id)
    summary = {"pipeline": PIPELINE, "status": status_for(argo_status), "lineage_edges": 0}
    if argo_status == "Succeeded":
        if not version:
            raise ValueError("a successful run must have a knowledge version")
        catalog.lineage(pipeline, catalog.corpus(version))
        summary["lineage_edges"] = 1
    return summary
