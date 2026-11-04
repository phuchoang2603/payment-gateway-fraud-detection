"""The Kubeflow Pipelines REST calls that start retraining runs and list the ones that were started."""

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from risk_model.cluster import MODEL_NAME, SERVICE_ACCOUNT, namespace

PIPELINE = "payment-risk-training"
EXPERIMENT = MODEL_NAME


class Pipelines:
    """A small client for the Kubeflow Pipelines v2 REST API.

    `base` is the API server address, `KFP_URL` or the in-cluster service when omitted, and `sleep`
    is injectable so tests do not wait.
    """

    def __init__(self, base=None, sleep=time.sleep):
        default = f"http://ml-pipeline.{namespace()}.svc.cluster.local:8888"
        self.base = (base or os.environ.get("KFP_URL", default)).rstrip("/") + "/apis/v2beta1"
        self.sleep = sleep

    def call(self, method, path, body=None):
        """Sends one JSON request and returns the decoded answer, or an empty dict for no body."""
        request = urllib.request.Request(
            self.base + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            text = response.read()
        return json.loads(text) if text else {}

    def version(self, name, timeout, interval=15):
        """Waits for a pipeline version to appear and returns its pipeline and version IDs."""
        deadline = time.monotonic() + timeout
        while True:
            pipelines = self.call("GET", f"/pipelines?namespace={namespace()}&page_size=100").get(
                "pipelines", []
            )
            pipeline = next((p for p in pipelines if p["display_name"] == PIPELINE), None)
            if pipeline is not None:
                path = f"/pipelines/{pipeline['pipeline_id']}/versions?page_size=100"
                versions = self.call("GET", path).get("pipeline_versions", [])
                found = next((v for v in versions if v["display_name"] == name), None)
                if found is not None:
                    return pipeline["pipeline_id"], found["pipeline_version_id"]
            if time.monotonic() >= deadline:
                raise TimeoutError(f"pipeline version {name} did not appear in the API")
            self.sleep(interval)

    def experiment(self):
        """Returns the ID of the experiment the runs belong to, creating it if needed."""
        found = self.call("GET", f"/experiments?namespace={namespace()}&page_size=100").get(
            "experiments", []
        )
        match = next((e for e in found if e["display_name"] == EXPERIMENT), None)
        return (
            match
            or self.call(
                "POST", "/experiments", {"display_name": EXPERIMENT, "namespace": namespace()}
            )
        )["experiment_id"]

    def runs(self, prefix, days=7):
        """Returns the runs whose name starts with `prefix` from the last `days`, as dicts with their name, state and start."""
        since = datetime.now(timezone.utc) - timedelta(days=days)
        filter_ = {
            "predicates": [
                {"key": "display_name", "operation": "IS_SUBSTRING", "string_value": prefix},
                {
                    "key": "created_at",
                    "operation": "GREATER_THAN",
                    "timestamp_value": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            ]
        }
        query = urllib.parse.quote(json.dumps(filter_))
        listing = self.call("GET", f"/runs?namespace={namespace()}&page_size=100&filter={query}")
        return [
            {
                "name": run["display_name"],
                "state": run.get("state", ""),
                "created": datetime.fromisoformat(run["created_at"].replace("Z", "+00:00")),
            }
            for run in listing.get("runs", [])
            if run["display_name"].startswith(prefix)
        ]

    def start(self, name, description, parameters):
        """Starts the newest `payment-risk-training` version with `parameters` and returns the run's ID."""
        pipelines = self.call("GET", f"/pipelines?namespace={namespace()}&page_size=100")
        pipeline = next(p for p in pipelines["pipelines"] if p["display_name"] == PIPELINE)
        path = (
            f"/pipelines/{pipeline['pipeline_id']}/versions?page_size=1&sort_by=created_at%20desc"
        )
        version = self.call("GET", path)["pipeline_versions"][0]
        body = {
            "display_name": name,
            "description": description,
            "experiment_id": self.experiment(),
            "pipeline_version_reference": {
                "pipeline_id": pipeline["pipeline_id"],
                "pipeline_version_id": version["pipeline_version_id"],
            },
            "runtime_config": {"parameters": parameters},
            "service_account": SERVICE_ACCOUNT,
        }
        return self.call("POST", "/runs", body)["run_id"]
