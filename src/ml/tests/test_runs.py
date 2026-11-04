"""Tests for the Kubeflow Pipelines client: finding versions, starting runs and listing automatic ones."""

import io
import json

import pytest

from risk_model import runs
from risk_model.cluster import SERVICE_ACCOUNT
from risk_model.runs import PIPELINE, Pipelines


def pipelines(responses):
    """Returns a client whose `call` answers from `responses(method, path, count)`, and its call log."""
    client = Pipelines(base="http://kfp:8888/", sleep=lambda _: None)
    calls = []

    def call(method, path, body=None):
        calls.append((method, path, body))
        return responses(method, path, len(calls))

    client.call = call
    return client, calls


def test_the_version_is_found_by_name_once_it_appears():
    """The version is found by name once it appears."""

    def responses(method, path, count):
        if path.startswith("/pipelines?"):
            return {"pipelines": [{"display_name": PIPELINE, "pipeline_id": "p1"}]}
        versions = [{"display_name": "old", "pipeline_version_id": "v0"}]
        if count >= 5:
            versions.append({"display_name": "wanted", "pipeline_version_id": "v9"})
        return {"pipeline_versions": versions}

    client, calls = pipelines(responses)
    assert client.version("wanted", timeout=60) == ("p1", "v9")
    assert len(calls) == 6


def test_a_version_that_never_appears_times_out():
    """A version that never appears times out."""
    client, _ = pipelines(lambda *_: {"pipelines": []})
    with pytest.raises(TimeoutError, match="did not appear"):
        client.version("wanted", timeout=0)


def test_starting_a_run_uses_the_newest_pipeline_version_and_the_training_account():
    """A started run points at the newest version, carries its parameters and runs as the training account."""

    def responses(method, path, count):
        if path.startswith("/pipelines?"):
            return {"pipelines": [{"display_name": PIPELINE, "pipeline_id": "p1"}]}
        if path.startswith("/pipelines/p1/versions"):
            assert "page_size=1&sort_by=created_at%20desc" in path
            return {"pipeline_versions": [{"pipeline_version_id": "v9"}]}
        if path.startswith("/experiments?"):
            return {"experiments": [{"display_name": "payment-risk", "experiment_id": "e1"}]}
        return {"run_id": "r1"}

    client, calls = pipelines(responses)
    assert client.start("drift-v3-x", "why", {"trigger": "drift"}) == "r1"
    assert calls[-1] == (
        "POST",
        "/runs",
        {
            "display_name": "drift-v3-x",
            "description": "why",
            "experiment_id": "e1",
            "pipeline_version_reference": {"pipeline_id": "p1", "pipeline_version_id": "v9"},
            "runtime_config": {"parameters": {"trigger": "drift"}},
            "service_account": SERVICE_ACCOUNT,
        },
    )


def test_listing_runs_keeps_those_with_the_prefix_and_reads_their_state_and_start():
    """Runs are listed by name prefix with their state and start time as a timezone-aware instant."""

    def responses(method, path, count):
        assert method == "GET" and "namespace=ml" in path and "display_name" in path
        assert "Z%22%7D" in path and "%2B" not in path
        return {
            "runs": [
                {
                    "display_name": "drift-v3-a",
                    "state": "RUNNING",
                    "created_at": "2026-11-10T08:00:00Z",
                },
                {
                    "display_name": "other",
                    "state": "SUCCEEDED",
                    "created_at": "2026-11-10T07:00:00Z",
                },
                {"display_name": "drift-age-v3-b", "created_at": "2026-11-09T07:00:00+00:00"},
            ]
        }

    client, _ = pipelines(responses)
    found = client.runs("drift-")
    assert [(run["name"], run["state"]) for run in found] == [
        ("drift-v3-a", "RUNNING"),
        ("drift-age-v3-b", ""),
    ]
    assert found[0]["created"].utcoffset().total_seconds() == 0 and found[0]["created"].hour == 8
    assert client.runs("nothing-") == []


def test_the_experiment_is_created_only_when_missing():
    """The experiment is created only when missing."""

    def responses(method, path, count):
        if method == "GET":
            return {"experiments": []}
        return {"experiment_id": "e1"}

    client, calls = pipelines(responses)
    assert client.experiment() == "e1"
    assert calls[-1] == (
        "POST",
        "/experiments",
        {"display_name": "payment-risk", "namespace": "ml"},
    )
    existing, made = pipelines(
        lambda *_: {"experiments": [{"display_name": "payment-risk", "experiment_id": "e7"}]}
    )
    assert existing.experiment() == "e7"
    assert [call[0] for call in made] == ["GET"]


class Response(io.BytesIO):
    """A urlopen result that works as a context manager."""


def test_a_call_sends_json_to_the_api_and_decodes_the_answer(monkeypatch):
    """call posts the JSON body to the API path under the base URL and returns the decoded answer."""
    sent = []

    def urlopen(request, timeout):
        sent.append((request.method, request.full_url, request.data, timeout))
        return Response(b'{"ok": true}')

    monkeypatch.setattr(runs.urllib.request, "urlopen", urlopen)
    client = Pipelines(base="http://kfp:8888/")
    assert client.call("POST", "/runs", {"a": 1}) == {"ok": True}
    assert sent == [("POST", "http://kfp:8888/apis/v2beta1/runs", b'{"a": 1}', 60)]


def test_an_empty_answer_is_an_empty_dict_and_the_base_defaults_to_the_namespace(monkeypatch):
    """A delete's empty body decodes to {}, and the default API address follows the namespace."""
    monkeypatch.setattr(runs.urllib.request, "urlopen", lambda request, timeout: Response(b""))
    monkeypatch.delenv("KFP_URL", raising=False)
    monkeypatch.setenv("ML_NAMESPACE", "team")
    client = Pipelines()
    assert client.base == "http://ml-pipeline.team.svc.cluster.local:8888/apis/v2beta1"
    assert client.call("DELETE", "/runs/1") == {}
    monkeypatch.setenv("KFP_URL", "http://other:1/")
    assert Pipelines().base == "http://other:1/apis/v2beta1"


def test_the_namespace_in_every_query_follows_the_configuration(monkeypatch):
    """Listings and the experiment request use the configured namespace, not a fixed one."""
    monkeypatch.setenv("ML_NAMESPACE", "team")
    seen = []

    def responses(method, path, count):
        seen.append((method, path))
        return {
            "pipelines": [],
            "experiments": [],
            "experiment_id": "e",
        }

    client, _ = pipelines(responses)
    client.experiment()
    with pytest.raises(TimeoutError):
        client.version("v", timeout=0)
    assert ("GET", "/experiments?namespace=team&page_size=100") in seen
    assert ("POST", "/experiments") in seen
    assert json.dumps(seen).count("namespace=ml") == 0
