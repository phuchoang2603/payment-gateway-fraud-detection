"""Locust load test of the drift API: `locust -f src/ml/loadtest/drift_api.py --host <drift API URL>`."""

from locust import HttpUser, between, task


class DriftReader(HttpUser):
    """Reads the drift state the way the hourly check and a dashboard user would."""

    wait_time = between(0.2, 0.6)

    @task(4)
    def short_window(self):
        """Reads the 6 hour drift."""
        self.client.get("/v1/drift?window=6h", name="/v1/drift?window=6h")

    @task(2)
    def long_window(self):
        """Reads the 24 hour drift."""
        self.client.get("/v1/drift?window=24h", name="/v1/drift?window=24h")

    @task(1)
    def reference(self):
        """Reads the serving version and its reference profile."""
        self.client.get("/v1/reference")

    @task(1)
    def metrics(self):
        """Reads the exported gauges."""
        self.client.get("/metrics")
