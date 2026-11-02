import pytest

from risk_model.schedule import PIPELINE, SERVICE_ACCOUNT, Pipelines, recurring_body


def pipelines(responses):
    client = Pipelines(base="http://kfp:8888/", sleep=lambda _: None)
    calls = []

    def call(method, path, body=None):
        calls.append((method, path, body))
        return responses(method, path, len(calls))

    client.call = call
    return client, calls


def test_the_recurring_run_follows_one_pipeline_version_and_never_catches_up():
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
    body = recurring_body(
        pipeline_id="p", version_id="v", experiment_id="e", cron="c", enabled=False, parameters={}
    )
    assert body["mode"] == "DISABLE"


def test_the_version_is_found_by_name_once_it_appears():
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
    client, _ = pipelines(lambda *_: {"pipelines": []})
    with pytest.raises(TimeoutError, match="did not appear"):
        client.version("wanted", timeout=0)


@pytest.mark.parametrize("key", ["recurringRuns", "recurring_runs"])
def test_replacing_deletes_runs_with_the_same_name_and_creates_a_new_one(key):
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
