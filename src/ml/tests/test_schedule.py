"""Tests for the Kubeflow Pipelines client and the recurring-run request."""

import io
import json

import pytest

from risk_model import schedule
from risk_model.cluster import SERVICE_ACCOUNT
from risk_model.schedule import PIPELINE, Pipelines, recurring_body


def pipelines(responses):
    """Returns a client whose `call` answers from `responses(method, path, count)`, and its call log."""
    client = Pipelines(base="http://kfp:8888/", sleep=lambda _: None)
    calls = []

    def call(method, path, body=None):
        calls.append((method, path, body))
        return responses(method, path, len(calls))

    client.call = call
    return client, calls


def test_the_recurring_run_follows_one_pipeline_version_and_never_catches_up():
    """The recurring run follows one pipeline version and never catches up."""
    body = recurring_body(
        pipeline_id="p1",
        version_id="v1",
        experiment_id="e1",
        cron="0 17 3 * * *",
        enabled=True,
        parameters={"train_days": 4},
    )
    assert body == {
        "display_name": PIPELINE,
        "description": "Rolling-window retraining of payment_risk_v1",
        "experiment_id": "e1",
        "pipeline_version_reference": {"pipeline_id": "p1", "pipeline_version_id": "v1"},
        "runtime_config": {"parameters": {"train_days": 4}},
        "service_account": SERVICE_ACCOUNT,
        "trigger": {"cron_schedule": {"cron": "0 17 3 * * *"}},
        "max_concurrency": "1",
        "no_catchup": True,
        "mode": "ENABLE",
    }


def test_a_paused_schedule_is_created_disabled():
    """A paused schedule is created disabled."""
    body = recurring_body(
        pipeline_id="p", version_id="v", experiment_id="e", cron="c", enabled=False, parameters={}
    )
    assert body["mode"] == "DISABLE"


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


@pytest.mark.parametrize("key", ["recurringRuns", "recurring_runs"])
def test_replacing_deletes_runs_with_the_same_name_and_creates_a_new_one(key):
    """Replacing deletes runs with the same name and creates a new one."""

    def responses(method, path, count):
        if method == "GET":
            return {
                key: [
                    {"display_name": PIPELINE, "recurring_run_id": "old"},
                    {"display_name": "other", "recurring_run_id": "keep"},
                ]
            }
        if method == "POST":
            return {"recurring_run_id": "new"}
        return {}

    client, calls = pipelines(responses)
    assert client.replace({"display_name": PIPELINE}) == "new"
    assert [(method, path) for method, path, _ in calls if method != "GET"] == [
        ("DELETE", "/recurringruns/old"),
        ("POST", "/recurringruns"),
    ]


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

    monkeypatch.setattr(schedule.urllib.request, "urlopen", urlopen)
    client = Pipelines(base="http://kfp:8888/")
    assert client.call("POST", "/runs", {"a": 1}) == {"ok": True}
    assert sent == [("POST", "http://kfp:8888/apis/v2beta1/runs", b'{"a": 1}', 60)]


def test_an_empty_answer_is_an_empty_dict_and_the_base_defaults_to_the_namespace(monkeypatch):
    """A delete's empty body decodes to {}, and the default API address follows the namespace."""
    monkeypatch.setattr(schedule.urllib.request, "urlopen", lambda request, timeout: Response(b""))
    monkeypatch.delenv("KFP_URL", raising=False)
    monkeypatch.setenv("ML_NAMESPACE", "team")
    client = Pipelines()
    assert client.base == "http://ml-pipeline.team.svc.cluster.local:8888/apis/v2beta1"
    assert client.call("DELETE", "/recurringruns/1") == {}
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
            "recurringRuns": [],
            "experiment_id": "e",
            "recurring_run_id": "r",
        }

    client, _ = pipelines(responses)
    client.experiment()
    client.replace({"display_name": PIPELINE})
    with pytest.raises(TimeoutError):
        client.version("v", timeout=0)
    assert ("GET", "/experiments?namespace=team&page_size=100") in seen
    assert ("GET", "/recurringruns?namespace=team&page_size=100") in seen
    assert ("POST", "/experiments") in seen
    assert json.dumps(seen).count("namespace=ml") == 0
