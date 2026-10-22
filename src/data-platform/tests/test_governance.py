from copy import deepcopy
from datetime import datetime, timezone

from datahub.metadata import schema_classes as S
from datahub_gx_plugin.common import build_assertions_with_results
from great_expectations.core import ExpectationSuiteValidationResult, RunIdentifier

from data_platform.catalog import action_for, publish_gx
from data_platform.governance import metadata_proposals, publish
from data_platform.runs import open_run, report_key
from data_platform.storage import Store


class Emitter:
    def __init__(self):
        self.proposals = []

    def emit(self, value):
        self.proposals.append(value)

    def emit_mcp(self, value):
        self.emit(value)

    def flush(self):
        pass


def fixture_report(outcome="failed"):
    return {
        "run_id": "logical-run",
        "run_time": "2026-01-01T00:00:00+00:00",
        "manifest_sha256": "a" * 64,
        "data_outcome": outcome,
        "snapshots": {"payment_events": 1, "fraud_labels": 2},
        "contracts": {"payment_events": "b" * 64, "fraud_labels": "c" * 64},
        "execution_errors": [],
        "tables": {
            "payment_events": {
                "gx": {
                    "suite_name": "payment_events.1.0.0",
                    "success": outcome == "passed",
                    "results": [
                        {
                            "success": outcome == "passed",
                            "expectation_config": {
                                "type": "unexpected_rows_expectation",
                                "kwargs": {"unexpected_rows_query": "SELECT * FROM {batch} WHERE event_id = ''"},
                                "meta": {"id": "payment_envelope"},
                            },
                            "result": {"unexpected_count": 1},
                            "exception_info": {"raised_exception": outcome == "error"},
                        }
                    ],
                    "meta": {"expectation_suite_name": "payment_events.1.0.0"},
                }
            }
        },
    }


def identities():
    return {
        t: f"urn:li:dataset:(urn:li:dataPlatform:iceberg,fraud-lakehouse-prod.bronze.raw_{t},PROD)"
        for t in ("payment_events", "fraud_labels")
    }


def test_plugin_identity_and_proven_execution_error_and_replay_gaps():
    report = fixture_report("error")
    action = action_for(identities()["payment_events"], "http://unused", None, "PROD", "fraud-lakehouse-prod")
    result = ExpectationSuiteValidationResult(**report["tables"]["payment_events"]["gx"])
    native = build_assertions_with_results(
        result,
        "suite",
        RunIdentifier(run_name="logical-run", run_time=report["run_time"]),
        action._datasets_from_validation_result(result),
    )[0]["assertionResults"][0]
    assert native.result.type == "FAILURE"
    assert native.timestampMillis != int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    emitter = Emitter()
    first = publish_gx(report, identities(), emitter, "PROD", "fraud-lakehouse-prod")
    replay = publish_gx(deepcopy(report), identities(), emitter, "PROD", "fraud-lakehouse-prod")
    assert first[0]["assertionResults"][0].to_obj() == replay[0]["assertionResults"][0].to_obj()
    assert first[0]["assertionResults"][0].result.type == "ERROR"
    assert first[0]["assertionInfo"].customProperties["semantic_rule_id"] == "payment_envelope"
    assert first[0]["assertionResults"][0].asserteeUrn == identities()["payment_events"]


def test_products_ports_and_jobs_are_linked_to_canonical_tables():
    run = {
        "dataset_version": "v7",
        "manifest_sha256": "a",
        "workflow": "workflow",
        "run_id": "run",
        "source_root": "s3://synthetic-source/historical/v7",
        "snapshots": {"payment_events": 1, "fraud_labels": 2},
    }
    proposals = list(metadata_proposals(run, fixture_report(), identities(), "s3://report", "https://contracts"))
    products = [p for p in proposals if isinstance(p.aspect, S.DataProductPropertiesClass)]
    assert len(products) == 2
    bronze = next(p for p in products if p.entityUrn.endswith("bronze-payments"))
    assert {a.destinationUrn for a in bronze.aspect.assets} == set(identities().values())
    labels = next(
        p
        for p in proposals
        if p.entityUrn == identities()["fraud_labels"] and isinstance(p.aspect, S.UpstreamLineageClass)
    )
    assert identities()["payment_events"] in {u.dataset for u in labels.aspect.upstreams}


def test_publication_missing_report_is_durably_error_not_acceptance(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://unavailable")
    monkeypatch.setenv("DATAHUB_TOKEN", "test-only")
    store = Store(str(tmp_path))
    contracts = {"payment_events": "one", "fraud_labels": "two"}
    run = open_run(store, "v7", "digest", contracts, "image", "workflow")
    outcome = publish(store, run, contracts)
    assert outcome["outcome"] == "error" and not outcome["readback_verified"]
    assert store.get(report_key(run, contracts).replace("validation.json", "governance.json")) == outcome
    assert store.get("accepted/v7.json") is None


def test_persisted_report_metadata_outage_and_recovery_preserve_outcomes(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://unused")
    monkeypatch.setenv("DATAHUB_TOKEN", "test-only")
    monkeypatch.setenv("ICEBERG_REST_URI", "http://unused/iceberg")
    monkeypatch.setenv("LAKE_ACCESS_KEY_ID", "test-only")
    monkeypatch.setenv("LAKE_SECRET_ACCESS_KEY", "test-only")
    store = Store(str(tmp_path))
    contracts = {"payment_events": "b" * 64, "fraud_labels": "c" * 64}
    run = open_run(store, "v7", "a" * 64, contracts, "image", "workflow")
    run.update(source_root="s3://synthetic-source/historical/v7", snapshots={"payment_events": 1, "fraud_labels": 2})
    report = fixture_report()
    report["run_id"] = run["run_id"]
    store.put(report_key(run, contracts), report)
    discoveries = []

    def discovery(*args):
        discoveries.append(True)
        return identities()

    class Catalog(Emitter):
        unavailable = True

        def emit(self, proposal):
            if self.unavailable:
                raise ConnectionError("metadata unavailable")
            super().emit(proposal)

        def get_aspect(self, entity_urn, aspect_type):
            if aspect_type == S.SchemaMetadataClass:
                return S.SchemaMetadataClass(
                    schemaName="fixture",
                    platform="urn:li:dataPlatform:iceberg",
                    version=0,
                    hash="",
                    fields=[],
                    platformSchema=S.OtherSchemaClass(rawSchema="fixture"),
                )
            return next(
                (
                    p.aspect
                    for p in reversed(self.proposals)
                    if p.entityUrn == entity_urn and isinstance(p.aspect, aspect_type)
                ),
                None,
            )

        def get_timeseries_values(self, entity_urn, aspect_type, filter, limit):
            return [
                p.aspect
                for p in reversed(self.proposals)
                if p.entityUrn == entity_urn and isinstance(p.aspect, aspect_type)
            ][:limit]

    catalog = Catalog()
    failed = publish(store, run, contracts, catalog, catalog, discovery, sleep=lambda _: None)
    assert failed["outcome"] == "error" and failed["data_outcome"] == "failed"
    catalog.unavailable = False
    recovered = publish(store, run, contracts, catalog, catalog, discovery, sleep=lambda _: None)
    assert recovered["outcome"] == "passed" and recovered["readback_verified"]
    assert recovered["data_outcome"] == "failed"
    assert len(discoveries) == 1
    assert store.get(report_key(run, contracts)) == report
    assert run["snapshots"] == report["snapshots"]
    assert store.get("accepted/v7.json") is None


def test_native_catalog_signing_and_file_io_use_metadata_identity(monkeypatch):
    from data_platform.catalog import iceberg_config

    for name, value in {
        "ICEBERG_REST_URI": "https://catalog/iceberg/v1",
        "CATALOG_ACCESS_KEY_ID": "metadata-key",
        "CATALOG_SECRET_ACCESS_KEY": "metadata-secret",
        "LAKE_ACCESS_KEY_ID": "writer-key",
        "LAKE_SECRET_ACCESS_KEY": "writer-secret",
    }.items():
        monkeypatch.setenv(name, value)
    properties = iceberg_config()["catalog"]["lake"]
    assert properties["client.access-key-id"] == properties["s3.access-key-id"] == "metadata-key"
    assert properties["client.secret-access-key"] == properties["s3.secret-access-key"] == "metadata-secret"
