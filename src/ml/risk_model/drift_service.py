"""The drift API: the serving model's feature drift from the metrics store, with no labels and no state."""

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Response

from risk_model.drift import assess, load_policy
from risk_model.drift_schemas import DriftReport, Serving, Status

STATES = {"insufficient": 0, "ok": 1, "drifting": 2}
WINDOW = r"^[1-9][0-9]*[mhd]$"
REGISTRY_API = "/api/model_registry/v1alpha3"


def get(url):
    """Returns the decoded JSON answer of a GET request."""
    with urllib.request.urlopen(url, timeout=20) as response:
        return json.load(response)


def text(properties, name):
    """Returns a registry custom property's string value, or None."""
    return (properties.get(name) or {}).get("string_value")


class Source:
    """Reads the serving version from the Model Registry and its bin counts from the metrics store."""

    def __init__(self, registry, metrics, environment, namespace, model):
        self.registry, self.metrics = registry.rstrip("/"), metrics.rstrip("/")
        self.environment, self.namespace, self.model = environment, namespace, model

    def serving(self):
        """Returns the production version's name, registration time and reference profile, or None before one exists."""
        base = f"{self.registry}{REGISTRY_API}"
        found = get(f"{base}/registered_model?name={urllib.parse.quote(self.model)}")
        versions = get(f"{base}/registered_models/{found['id']}/versions?pageSize=100")
        for version in versions.get("items", []):
            properties = version.get("customProperties", {})
            if text(properties, "stage") == "production" and version.get("state") == "LIVE":
                reference = text(properties, "reference")
                return {
                    "version": version["name"],
                    "created": datetime.fromtimestamp(
                        int(version["createTimeSinceEpoch"]) / 1000, timezone.utc
                    ).isoformat(),
                    "reference": json.loads(reference) if reference else None,
                }
        return None

    def counts(self, version, reference, window):
        """Returns the scored requests' bin counts per feature over `window`, missing values last."""
        selector = (
            '{__name__="fraud.model.feature_bin",'
            f'"model_version"="{version}","k8s.namespace.name"="{self.namespace}",'
            f'"deployment.environment"="{self.environment}"}}'
        )
        query = f"sum by (feature, bin) (increase({selector}[{window}]))"
        answer = get(f"{self.metrics}/api/v1/query?query={urllib.parse.quote(query)}")
        result = {name: [0.0] * len(entry["share"]) for name, entry in reference.items()}
        for series in answer["data"]["result"]:
            name, label = series["metric"].get("feature"), series["metric"].get("bin")
            if name not in result:
                continue
            slot = len(result[name]) - 1 if label == "m" else int(label)
            if 0 <= slot < len(result[name]):
                result[name][slot] = float(series["value"][1])
        return result


def create(source, policy):
    """Returns the API over a source and a policy."""
    app = FastAPI(title="payment-risk drift")
    served = {"requests": 0}

    @app.middleware("http")
    async def count(request, call_next):
        """Counts the API calls, which the autoscaler reads."""
        if request.url.path.startswith("/v1/"):
            served["requests"] += 1
        return await call_next(request)

    def current(window):
        serving = source.serving()
        if serving is None or serving["reference"] is None:
            raise HTTPException(404, "no serving version with a reference profile")
        counts = source.counts(serving["version"], serving["reference"], window)
        return serving, {"window": window, **assess(serving["reference"], counts, policy)}

    @app.get("/healthz", response_model=Status)
    def healthz():
        """Reports that the process is up."""
        return {"status": "ok"}

    @app.get("/readyz", response_model=Status)
    def readyz():
        """Reports that the process can serve requests."""
        return {"status": "ready"}

    @app.get("/v1/reference", response_model=Serving)
    def reference():
        """Returns the serving version and the profile its requests are compared with."""
        serving = source.serving()
        if serving is None or serving["reference"] is None:
            raise HTTPException(404, "no serving version with a reference profile")
        return serving

    @app.get("/v1/drift", response_model=DriftReport)
    def drift(
        window: str = Query("24h", pattern=WINDOW, description="A duration such as 30m, 6h or 1d."),
    ):
        """Returns the drift of the serving version's scored features over a window."""
        serving, assessment = current(window)
        return {
            "version": serving["version"],
            "created": serving["created"],
            "assessment": assessment,
        }

    @app.get("/metrics")
    def metrics():
        """Returns the 24 hour drift as Prometheus gauges."""
        try:
            serving, assessment = current("24h")
        except HTTPException:
            return Response(
                f"ml_drift_requests_total {served['requests']}\n", media_type="text/plain"
            )
        version = serving["version"]
        lines = [
            f'ml_drift_state{{model_version="{version}"}} {STATES[assessment["state"]]}',
            f'ml_drift_samples{{model_version="{version}"}} {assessment["samples"]}',
            f'ml_drift_features_over{{model_version="{version}"}} {len(assessment["over"])}',
            f"ml_drift_requests_total {served['requests']}",
        ]
        lines += [
            f'ml_feature_drift_psi{{feature="{name}",model_version="{version}"}} {value}'
            for name, value in assessment["psi"].items()
        ]
        created = datetime.fromisoformat(serving["created"]).timestamp()
        lines.append(f'ml_model_created_seconds{{model_version="{version}"}} {created}')
        return Response("\n".join(lines) + "\n", media_type="text/plain")

    return app


def app():
    """Builds the API from the environment, for `uvicorn --factory`."""
    source = Source(
        os.environ.get(
            "MODEL_REGISTRY_URL", "http://model-registry-service.ml.svc.cluster.local:8080"
        ),
        os.environ.get("METRICS_URL", "http://vmsingle-vmks.observability.svc.cluster.local:8428"),
        os.environ.get("ENVIRONMENT", "dev"),
        os.environ.get("FRAUD_NAMESPACE", "payment-gateway"),
        os.environ.get("MODEL_NAME", "payment-risk"),
    )
    return create(source, load_policy(os.environ.get("DRIFT_POLICY", "")))
