import os
from datetime import datetime

from datahub.emitter import mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.iceberg.iceberg import IcebergSource
from datahub.metadata import schema_classes as S
from datahub_gx_plugin.action_v1 import DataHubValidationAction
from datahub_gx_plugin.common import build_assertions_with_results, emit_assertion_results
from great_expectations.core import ExpectationSuiteValidationResult, RunIdentifier

from data_platform.products import PORTS


def iceberg_config():
    return {
        "env": os.environ.get("DATAHUB_ENV", "PROD"),
        "platform_instance": os.environ.get("DATAHUB_PLATFORM_INSTANCE", "fraud-lakehouse-prod"),
        "catalog": {
            "lake": {
                "type": "rest",
                "uri": os.environ["ICEBERG_REST_URI"],
                "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "fraud-lakehouse-prod"),
                "client.access-key-id": os.environ.get("CATALOG_ACCESS_KEY_ID") or os.environ["LAKE_ACCESS_KEY_ID"],
                "client.secret-access-key": os.environ.get("CATALOG_SECRET_ACCESS_KEY")
                or os.environ["LAKE_SECRET_ACCESS_KEY"],
                "client.region": os.environ.get("AWS_REGION", "us-east-1"),
                "rest.sigv4-enabled": os.environ.get("ICEBERG_SIGV4_ENABLED", "true"),
                "rest.signing-name": os.environ.get("ICEBERG_SIGNING_NAME", "s3"),
                "rest.signing-region": os.environ.get("AWS_REGION", "us-east-1"),
                "s3.endpoint": os.environ.get("S3_ENDPOINT", "https://minio.home.phuchoang.sbs"),
                "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
                "s3.access-key-id": os.environ.get("CATALOG_ACCESS_KEY_ID") or os.environ["LAKE_ACCESS_KEY_ID"],
                "s3.secret-access-key": os.environ.get("CATALOG_SECRET_ACCESS_KEY")
                or os.environ["LAKE_SECRET_ACCESS_KEY"],
            }
        },
        "namespace_pattern": {"allow": ["^bronze$"]},
        "table_pattern": {"allow": ["^bronze\\.raw_(payment_events|fraud_labels)$"]},
        "profiling": {"enabled": False},
        "stateful_ingestion": {"enabled": False},
    }


def discover(emitter, config, run_id):
    source = IcebergSource.create(config, PipelineContext(run_id=run_id))
    identities = {}
    try:
        for workunit in source.get_workunits():
            proposal = workunit.metadata
            if getattr(proposal, "aspectName", None) == "schemaMetadata":
                urn = workunit.get_urn()
                for table in PORTS:
                    if f"bronze.raw_{table}," in urn:
                        identities[table] = urn
            emitter.emit(proposal)
        report = source.get_report()
        if getattr(report, "failures", None) or set(identities) != set(PORTS):
            raise RuntimeError("native Iceberg ingestion did not discover both canonical schemas")
    finally:
        source.close()
    return identities


def action_for(urn, endpoint, token, env, platform_instance):
    dataset = urn.split(",", 1)[1].rsplit(",", 1)[0]
    prefix = platform_instance + "."
    if not dataset.startswith(prefix):
        raise ValueError("canonical native Iceberg identity has a different platform instance")
    action = DataHubValidationAction(
        server_url=endpoint,
        token=token,
        platform="iceberg",
        dataset_name=dataset.removeprefix(prefix),
        platform_instance=platform_instance,
        env=env,
        graceful_exceptions=False,
        timeout_sec=15,
        retry_max_times=2,
        emit_mode="SYNC_WAIT",
    )
    resolved = action._resolve_dataset_urn({}, action.dataset_name, "")
    if resolved != urn:
        raise ValueError("GX identity differs from native Iceberg ingestion")
    return action


def assertion_evidence(report, identities, env, instance):
    evidence = []
    run_identifier = RunIdentifier(run_name=report["run_id"], run_time=report["run_time"])
    for table, data in report["tables"].items():
        result = ExpectationSuiteValidationResult(**data["gx"])
        action = action_for(identities[table], "http://unused", None, env, instance)
        datasets = action._datasets_from_validation_result(result)
        assertions = build_assertions_with_results(
            result,
            result.meta.get("expectation_suite_name"),
            run_identifier,
            datasets,
            context_properties={"dp1_run_id": report["run_id"]},
        )
        for raw_result, assertion in zip(result.results, assertions):
            meta = raw_result["expectation_config"].get("meta", {})
            props = assertion["assertionInfo"].customProperties or {}
            props.update(
                {
                    "semantic_rule_id": str(meta.get("id", meta.get("expectation_id", "schema"))),
                    "contract_sha256": report["contracts"][table],
                    "manifest_sha256": report["manifest_sha256"],
                    "snapshot_id": str(report["snapshots"][table]),
                }
            )
            assertion["assertionInfo"].customProperties = props
            evidence.append(assertion)
    return evidence


def publish_gx(report, identities, emitter, env, instance):
    assertions = assertion_evidence(report, identities, env, instance)
    timestamp = int(datetime.fromisoformat(report["run_time"]).timestamp() * 1000)
    for assertion in assertions:
        for event in assertion["assertionResults"]:
            event.timestampMillis = timestamp
            event.runId = (
                report["run_id"]
                + ":"
                + report["contracts"][next(t for t in identities if identities[t] == event.asserteeUrn)][:16]
            )
    for table, data in report["tables"].items():
        for raw, assertion in zip(
            data["gx"]["results"], [a for a in assertions if a["assertionResults"][0].asserteeUrn == identities[table]]
        ):
            if raw.get("exception_info", {}).get("raised_exception"):
                assertion["assertionResults"][0].result.type = "ERROR"
    if report.get("execution_errors"):
        for table, urn in identities.items():
            assertion_urn = builder.make_assertion_urn(builder.datahub_guid({"dataset": urn, "rule": "dp1.execution"}))
            assertions.append(
                {
                    "assertionUrn": assertion_urn,
                    "assertionInfo": S.AssertionInfoClass(
                        type="CUSTOM",
                        customAssertion=S.CustomAssertionInfoClass(type="dp1.execution", entity=urn),
                        customProperties={"dp1_run_id": report["run_id"]},
                    ),
                    "assertionPlatform": S.DataPlatformInstanceClass(
                        platform=builder.make_data_platform_urn("great-expectations")
                    ),
                    "assertionResults": [
                        S.AssertionRunEventClass(
                            timestampMillis=timestamp,
                            runId=report["run_id"],
                            asserteeUrn=urn,
                            assertionUrn=assertion_urn,
                            status="COMPLETE",
                            result=S.AssertionResultClass(type="ERROR", nativeResults={"stage": "validation"}),
                        )
                    ],
                }
            )
    emit_assertion_results(emitter, assertions)
    return assertions
