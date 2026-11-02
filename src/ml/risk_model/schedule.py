import json
import os
import time
import urllib.request

PIPELINE = "payment-risk-training"
EXPERIMENT = "payment-risk"
SERVICE_ACCOUNT = "fraud-training"


class Pipelines:
    def __init__(self, base=None, sleep=time.sleep):
        self.base = (
            base or os.environ.get("KFP_URL", "http://ml-pipeline.ml.svc.cluster.local:8888")
        ).rstrip("/") + "/apis/v2beta1"
        self.sleep = sleep

    def call(self, method, path, body=None):
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
        deadline = time.monotonic() + timeout
        while True:
            pipelines = self.call("GET", "/pipelines?namespace=ml&page_size=100").get(
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
        found = self.call("GET", "/experiments?namespace=ml&page_size=100").get("experiments", [])
        match = next((e for e in found if e["display_name"] == EXPERIMENT), None)
        return (
            match
            or self.call("POST", "/experiments", {"display_name": EXPERIMENT, "namespace": "ml"})
        )["experiment_id"]

    def replace(self, body):
        listing = self.call("GET", "/recurringruns?namespace=ml&page_size=100")
        for run in listing.get("recurringRuns") or listing.get("recurring_runs") or []:
            if run["display_name"] == body["display_name"]:
                self.call("DELETE", f"/recurringruns/{run['recurring_run_id']}")
        return self.call("POST", "/recurringruns", body)["recurring_run_id"]


def recurring_body(*, pipeline_id, version_id, experiment_id, cron, enabled, parameters):
    return {
        "display_name": PIPELINE,
        "description": "Rolling-window retraining of payment_risk_v1",
        "experiment_id": experiment_id,
        "pipeline_version_reference": {
            "pipeline_id": pipeline_id,
            "pipeline_version_id": version_id,
        },
        "runtime_config": {"parameters": parameters},
        "service_account": SERVICE_ACCOUNT,
        "trigger": {"cron_schedule": {"cron": cron}},
        "max_concurrency": "1",
        "no_catchup": True,
        "mode": "ENABLE" if enabled else "DISABLE",
    }
