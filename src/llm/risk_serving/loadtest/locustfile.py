"""Load test for the customer features Web API, run in-cluster by infra/charts/llm-platform/benchmark/features-loadtest.yaml."""

import os
import random
from pathlib import Path
from urllib.parse import quote

from locust import HttpUser, between, task

INTEGRATION_ID = os.environ["INTEGRATION_ID"]
CUSTOMER_IDS = Path(os.environ.get("CUSTOMER_IDS", "/loadtest/customer-ids.txt")).read_text().split()


class FeaturesClient(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(19)
    def customer_features(self) -> None:
        self.client.get(
            f"/v1/customers/{quote(random.choice(CUSTOMER_IDS))}/features",
            params={"integration_id": INTEGRATION_ID},
            name="/v1/customers/{customer_id}/features",
        )

    @task(1)
    def invalid_request(self) -> None:
        with self.client.get(
            "/v1/customers/unknown/features",
            name="/v1/customers/{customer_id}/features (no integration_id)",
            catch_response=True,
        ) as response:
            if response.status_code == 422:
                response.success()
            else:
                response.failure(f"expected 422, got {response.status_code}")
