import pytest

from openmetadata_jobs import publish


@pytest.fixture
def calls(monkeypatch):
    recorded = {"status": [], "lineage": []}
    monkeypatch.setattr(publish, "client", lambda: object())
    monkeypatch.setattr(publish, "ensure_pipeline", lambda sdk, name: name)
    monkeypatch.setattr(
        publish, "publish_status", lambda sdk, name, status, run_id: recorded["status"].append((name, status))
    )
    monkeypatch.setattr(
        publish, "publish_lineage", lambda sdk, name, ref, version: recorded["lineage"].append(name) or 1
    )
    monkeypatch.setenv("RUN_ID", "run-1")
    monkeypatch.setenv("DATASET_VERSION", "v18")
    return recorded


def publish_with(monkeypatch, pipelines, status, steps="", failures=""):
    monkeypatch.setenv("PIPELINES", pipelines)
    monkeypatch.setenv("STEPS", steps)
    monkeypatch.setenv("WORKFLOW_STATUS", status)
    monkeypatch.setenv("FAILED_STEPS", failures)
    return publish.main()


def failure(name):
    return f'[{{"displayName": "kafka[0].{name}", "phase": "Failed"}}]'


def test_a_failed_single_pipeline_publishes_status_without_lineage(monkeypatch, calls):
    publish_with(monkeypatch, "dp2-silver", "Failed")

    assert calls["status"] == [("dp2-silver", "Failed")]
    assert calls["lineage"] == []


def test_a_succeeded_single_pipeline_publishes_status_and_lineage(monkeypatch, calls):
    publish_with(monkeypatch, "dp2-silver", "Succeeded")

    assert calls["status"] == [("dp2-silver", "Succeeded")]
    assert calls["lineage"] == ["dp2-silver"]


def test_kafka_cron_failed_bronze_publishes_both_statuses_and_no_lineage(monkeypatch, calls):
    publish_with(monkeypatch, "dp1-bronze-kafka,dp2-silver", "Failed", "bronze,silver", failure("bronze"))

    assert calls["status"] == [("dp1-bronze-kafka", "Failed"), ("dp2-silver", "Failed")]
    assert calls["lineage"] == []


def test_kafka_cron_failed_silver_keeps_bronze_lineage(monkeypatch, calls):
    publish_with(monkeypatch, "dp1-bronze-kafka,dp2-silver", "Failed", "bronze,silver", failure("silver"))

    assert calls["lineage"] == ["dp1-bronze-kafka"]


def test_kafka_cron_success_publishes_lineage_for_both(monkeypatch, calls):
    publish_with(monkeypatch, "dp1-bronze-kafka,dp2-silver", "Succeeded", "bronze,silver")

    assert calls["lineage"] == ["dp1-bronze-kafka", "dp2-silver"]
