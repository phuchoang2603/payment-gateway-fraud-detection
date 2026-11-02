import json
from pathlib import Path

import yaml

from openmetadata import integrations, lineage, publish

ROOT = Path(__file__).resolve().parents[4]


def test_feature_lineage_traces_writer_and_history_tables():
    edges = lineage.edges("v19")
    selected = [
        edge
        for edge in edges
        if edge.pipeline == "feature-writer" or edge.pipeline in ("feast-materialize", "stream-history")
    ]
    assert len(selected) == 7
    writer = lineage.Node("pipeline", lineage.pipeline_fqn("feature-writer"))
    writes = [edge for edge in selected if edge.pipeline == "feature-writer" and edge.target.kind == "table"]
    assert {edge.source for edge in writes} == {writer}
    assert {edge.target for edge in writes} == {
        lineage.table(f"gold.feat_stream_{scope}") for scope in ("customer", "payment_method")
    }
    history = [edge for edge in selected if edge.pipeline == "stream-history"]
    assert {edge.source for edge in history} == {lineage.BRONZE_EVENTS}
    assert {edge.target for edge in history} == {
        lineage.table("gold.feat_stream_customer"),
        lineage.table("gold.feat_stream_payment_method"),
    }
    assert any(
        edge.source == lineage.topic("payments.events.v1") and edge.pipeline == "fraud-streaming" for edge in edges
    )
    assert all("redis" not in edge.target.fqn.lower() for edge in selected)


def test_history_and_materialization_reports_keep_governance_parameters(tmp_path):
    for report in (
        {"start": "2025-09-01", "end": "2025-10-01", "sampled": 12, "since": "2025-09-01", "checked": True},
        {
            "start": "2025-09-01",
            "end": "2025-10-01",
            "job_state": "FINISHED",
            "counts": {"rejects": 1},
            "seed": {"events_per_second": 12},
        },
    ):
        path = tmp_path / "report.json"
        path.write_text(json.dumps(report))
        parameters = publish.run_parameters(str(path))
        assert all(parameters[key] == value for key, value in report.items())
    assert publish.run_parameters(str(tmp_path / "missing.json")) is None


def test_flink_connector_native_workflow_and_journal_discovery(tmp_path, monkeypatch):
    values = yaml.safe_load((ROOT / "infra/charts/data-pipelines/values.yaml").read_text())
    connectors = values["catalog"]["connectors"]
    path = tmp_path / "connectors.json"
    path.write_text(json.dumps(connectors))
    monkeypatch.setenv("METADATA_CONNECTORS_FILE", str(path))
    monkeypatch.setenv("OPENMETADATA_URL", "http://metadata/api")
    monkeypatch.setenv("OPENMETADATA_TOKEN", "ci-placeholder")
    workflow = integrations.workflow("flink")
    assert workflow["source"]["serviceConnection"]["config"]["type"] == "Flink"
    patterns = connectors["kafka"]["source"]["sourceConfig"]["config"]["topicFilterPattern"]["includes"]
    assert any("feature-updates" in pattern for pattern in patterns)
    assert any("feature-rejects" in pattern for pattern in patterns)
