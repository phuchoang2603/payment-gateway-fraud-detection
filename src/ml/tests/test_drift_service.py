"""Tests for the drift API: the registry and metrics reads, the endpoints and the gauges."""

from fastapi.testclient import TestClient

from risk_model import drift_service
from risk_model.drift import Policy
from risk_model.drift_service import Source, create

REFERENCE = {
    "a": {"edges": [0.0], "share": [0.5, 0.4, 0.1], "gain": 2.0},
    "b": {"edges": [1.0, 2.0], "share": [0.3, 0.3, 0.3, 0.1], "gain": 0.0},
}
SERVING = {"version": "v3", "created": "2026-11-01T00:00:00+00:00", "reference": REFERENCE}
SAME = {"a": [500.0, 400.0, 100.0], "b": [300.0, 300.0, 300.0, 100.0]}
MOVED = {"a": [50.0, 100.0, 850.0], "b": [300.0, 300.0, 300.0, 100.0]}


class Fixed:
    """A source with a fixed serving version and fixed bin counts."""

    def __init__(self, serving=SERVING, counts=SAME):
        self.serving_version, self.observed, self.windows = serving, counts, []

    def serving(self):
        """Returns the prepared serving version."""
        return self.serving_version

    def counts(self, version, reference, window):
        """Records the window and returns the prepared counts."""
        self.windows.append((version, window))
        return self.observed


def client(source, policy=Policy(samples=100)):
    """Returns a test client for the API over a source."""
    return TestClient(create(source, policy))


def test_the_probes_answer_without_touching_the_registry_or_the_metrics():
    """The health and readiness probes answer on their own."""
    api = client(Fixed(serving=None))
    assert api.get("/healthz").json() == {"status": "ok"}
    assert api.get("/readyz").json() == {"status": "ready"}


def test_the_drift_of_a_window_names_the_version_and_its_state():
    """The drift answer names the serving version, when it was registered and the assessment."""
    source = Fixed()
    answer = client(source).get("/v1/drift?window=6h").json()
    assert answer["version"] == "v3" and answer["created"] == SERVING["created"]
    assert answer["assessment"]["state"] == "ok" and answer["assessment"]["window"] == "6h"
    assert source.windows == [("v3", "6h")]
    assert client(Fixed(counts=MOVED)).get("/v1/drift").json()["assessment"]["state"] == "drifting"


def test_a_bad_window_or_a_missing_reference_is_refused():
    """A malformed window is a 422, and no serving version or no profile is a 404."""
    assert client(Fixed()).get("/v1/drift?window=forever").status_code == 422
    assert client(Fixed(serving=None)).get("/v1/drift").status_code == 404
    assert (
        client(Fixed(serving={**SERVING, "reference": None})).get("/v1/reference").status_code
        == 404
    )
    assert client(Fixed(serving=None)).get("/v1/reference").status_code == 404
    assert client(Fixed()).get("/v1/reference").json()["reference"] == REFERENCE


def test_the_gauges_carry_the_state_and_each_features_index():
    """The metrics are Prometheus gauges for the state, samples, features over and each index."""
    body = client(Fixed(counts=MOVED)).get("/metrics").text
    assert 'ml_drift_state{model_version="v3"} 2' in body
    assert 'ml_drift_samples{model_version="v3"} 1000.0' in body
    assert 'ml_drift_features_over{model_version="v3"} 1' in body
    assert 'ml_feature_drift_psi{feature="a",model_version="v3"}' in body
    assert 'ml_model_created_seconds{model_version="v3"}' in body


def test_the_gauges_still_report_the_request_count_without_a_serving_version():
    """Before any version is served the metrics answer with the request count only."""
    assert client(Fixed(serving=None)).get("/metrics").text == "ml_drift_requests_total 0\n"


def test_the_api_calls_are_counted_for_the_autoscaler_and_the_probes_are_not():
    """Only /v1 calls count towards the request gauge."""
    api = client(Fixed())
    api.get("/healthz")
    api.get("/v1/drift")
    api.get("/v1/reference")
    assert "ml_drift_requests_total 2" in api.get("/metrics").text


def test_the_serving_version_is_the_live_production_one_with_its_reference(monkeypatch):
    """The registry read returns the live production version, with its profile decoded."""
    import json

    def get(url):
        if "registered_model?" in url:
            return {"id": "1"}
        return {
            "items": [
                {
                    "name": "v2",
                    "state": "ARCHIVED",
                    "customProperties": {"stage": {"string_value": "archived"}},
                },
                {
                    "name": "v3",
                    "state": "LIVE",
                    "createTimeSinceEpoch": "1793750400000",
                    "customProperties": {
                        "stage": {"string_value": "production"},
                        "reference": {"string_value": json.dumps(REFERENCE)},
                    },
                },
            ]
        }

    monkeypatch.setattr(drift_service, "get", get)
    source = Source("http://registry/", "http://metrics/", "dev", "payment-gateway", "payment-risk")
    found = source.serving()
    assert found["version"] == "v3" and found["reference"] == REFERENCE
    assert found["created"] == "2026-11-03T21:20:00+00:00"
    monkeypatch.setattr(
        drift_service,
        "get",
        lambda url: {"id": "1"} if "registered_model?" in url else {"items": []},
    )
    assert source.serving() is None


def test_the_counts_come_from_the_metrics_store_by_feature_and_bin(monkeypatch):
    """Counts are read per feature and bin, the missing bin goes last, and unknown series are ignored."""
    seen = []

    def get(url):
        seen.append(url)
        series = [
            ({"feature": "a", "bin": "0"}, "5"),
            ({"feature": "a", "bin": "m"}, "2"),
            ({"feature": "b", "bin": "9"}, "7"),
            ({"feature": "zzz", "bin": "0"}, "1"),
        ]
        return {"data": {"result": [{"metric": m, "value": [0, v]} for m, v in series]}}

    monkeypatch.setattr(drift_service, "get", get)
    source = Source("http://registry", "http://metrics", "dev", "payment-gateway", "payment-risk")
    counts = source.counts("v3", REFERENCE, "6h")
    assert counts == {"a": [5.0, 0.0, 2.0], "b": [0.0, 0.0, 0.0, 0.0]}
    assert "model_version" in seen[0] and "6h" in seen[0] and "deployment.environment" in seen[0]


def test_the_api_is_built_from_the_environment(monkeypatch):
    """The factory reads the registry, metrics, environment and policy from the environment."""
    monkeypatch.setenv("DRIFT_POLICY", '{"samples": 10}')
    monkeypatch.setenv("ENVIRONMENT", "prod")
    assert drift_service.app().title == "payment-risk drift"
