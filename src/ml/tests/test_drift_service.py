"""Tests for the drift API: the registry and metrics reads, the endpoints and the gauges."""

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from risk_model import drift_service
from risk_model.drift import Policy
from risk_model.drift_schemas import Assessment, DriftReport, FeatureProfile, Serving
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


@pytest.mark.parametrize("window", ["1m", "30m", "6h", "24h", "7d", "10d"])
def test_a_window_of_a_positive_count_and_one_unit_is_accepted(window):
    """A window of a positive count of minutes, hours or days is answered and passed on unchanged."""
    source = Fixed()
    assert client(source).get(f"/v1/drift?window={window}").status_code == 200
    assert source.windows == [("v3", window)]


@pytest.mark.parametrize("window", ["", "0h", "00m", "01h", "6", "h", "6w", "-1h", "1.5h", "6h6h"])
def test_a_window_without_a_positive_count_or_a_known_unit_is_refused_before_any_read(window):
    """Zero, a leading zero, a missing count or unit, another unit and a fraction are all 422 and read nothing."""
    source = Fixed()
    assert client(source).get("/v1/drift", params={"window": window}).status_code == 422
    assert source.windows == []


def test_the_answers_validate_against_the_response_models():
    """The drift answer and the reference answer parse back into their models."""
    api = client(Fixed(counts=MOVED))
    report = DriftReport.model_validate(api.get("/v1/drift?window=6h").json())
    assert report.version == "v3" and report.assessment.state == "drifting"
    assert report.assessment.over and set(report.assessment.psi) == {"a", "b"}
    assert Serving.model_validate(api.get("/v1/reference").json()).reference["b"].edges == [
        1.0,
        2.0,
    ]


def test_a_malformed_profile_from_the_registry_is_a_server_error_and_not_a_wrong_answer():
    """A profile without a feature's bin shares cannot be answered, so the call fails instead of guessing."""
    broken = {**SERVING, "reference": {"a": {"edges": [0.0]}}}
    api = TestClient(
        create(Fixed(serving=broken), Policy(samples=100)), raise_server_exceptions=False
    )
    assert api.get("/v1/reference").status_code == 500


@pytest.mark.parametrize(
    "build",
    [
        lambda: Assessment(window="6h", samples=-1, state="ok", psi={}, missing={}, over=[]),
        lambda: Assessment(window="6h", samples=1, state="bogus", psi={}, missing={}, over=[]),
        lambda: FeatureProfile(edges=[0.0], share=[1.0]),
        lambda: Serving(version="", created="2026-11-01T00:00:00+00:00", reference={}),
    ],
)
def test_the_models_refuse_negative_counts_unknown_states_short_profiles_and_empty_versions(build):
    """Each model rejects the value just outside what it allows."""
    with pytest.raises(ValidationError):
        build()


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
    assert found["created"] == "2026-11-04T00:00:00+00:00"
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
