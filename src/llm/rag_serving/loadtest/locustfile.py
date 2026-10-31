"""Load test for the knowledge Web API's lookup by id, run in-cluster by infra/charts/rag-pipeline/loadtest/job.yaml."""

import os
import random
from pathlib import Path
from urllib.parse import quote

from locust import HttpUser, between, task

CHUNK_IDS = Path(os.environ.get("CHUNK_IDS", "/loadtest/chunk-ids.txt")).read_text().split()


class KnowledgeClient(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(19)
    def chunk_by_id(self) -> None:
        self.client.get(f"/v1/chunks/{quote(random.choice(CHUNK_IDS))}", name="/v1/chunks/{chunk_id}")

    @task(1)
    def unknown_chunk(self) -> None:
        with self.client.get(
            "/v1/chunks/decline-codes%23missing", name="/v1/chunks/{unknown}", catch_response=True
        ) as response:
            if response.status_code == 404:
                response.success()
            else:
                response.failure(f"expected 404, got {response.status_code}")
