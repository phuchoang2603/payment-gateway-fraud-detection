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


def publish_with(monkeypatch, pipelines, status):
    monkeypatch.setenv("PIPELINES", pipelines)
    monkeypatch.setenv("WORKFLOW_STATUS", status)
    return publish.main()


def test_a_successful_run_publishes_status_and_lineage_for_each_pipeline(monkeypatch, calls):
    publish_with(monkeypatch, "dp1-bronze-kafka,dp2-silver", "Succeeded")

    assert calls["status"] == [("dp1-bronze-kafka", "Succeeded"), ("dp2-silver", "Succeeded")]
    assert calls["lineage"] == ["dp1-bronze-kafka", "dp2-silver"]


def test_a_failed_run_publishes_failed_status_and_no_lineage(monkeypatch, calls):
    publish_with(monkeypatch, "dp1-bronze-kafka,dp2-silver", "Failed")

    assert calls["status"] == [("dp1-bronze-kafka", "Failed"), ("dp2-silver", "Failed")]
    assert calls["lineage"] == []


def test_historical_pipeline_requires_a_dataset_version(monkeypatch, calls):
    monkeypatch.delenv("DATASET_VERSION", raising=False)

    with pytest.raises(ValueError, match="DATASET_VERSION"):
        publish_with(monkeypatch, "dp1-bronze-historical", "Succeeded")
