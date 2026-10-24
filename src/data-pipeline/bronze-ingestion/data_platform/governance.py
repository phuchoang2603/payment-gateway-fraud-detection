import os
import time
from datetime import datetime

import yaml
from datahub.emitter import mce_builder as B
from datahub.emitter.mcp import MetadataChangeProposalWrapper as MCP
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata import schema_classes as S

from data_platform.catalog import discover, iceberg_config, publish_gx
from data_platform.products import PORTS, PRODUCTS, contract_path
from data_platform.runs import report_key, save_run


def metadata_proposals(run, report, identities, report_uri, artifact_base):
    env = os.environ.get("DATAHUB_ENV", "PROD")
    source = {
        t: B.make_dataset_urn("s3", f"synthetic-source/historical/{run['dataset_version']}/{t}", env) for t in PORTS
    }
    owner = S.OwnershipClass(
        owners=[S.OwnerClass(owner=B.make_user_urn("phuchoang2603"), type=S.OwnershipTypeClass.DATAOWNER)]
    )
    tags = S.GlobalTagsClass(tags=[S.TagAssociationClass(tag=B.make_tag_urn("synthetic"))])
    expected = []

    def emit(urn, aspect):
        expected.append((urn, type(aspect)))
        return MCP(entityUrn=urn, aspect=aspect)

    for product, assets in [("historical-payments", source), ("bronze-payments", identities)]:
        doc = yaml.safe_load((PRODUCTS / product / "product.odps.yaml").read_text())
        product_urn = f"urn:li:dataProduct:{product}"
        yield emit(
            product_urn,
            S.DataProductPropertiesClass(
                name=doc["name"],
                description=doc["description"]["purpose"],
                customProperties={"odps_id": doc["id"], "version": doc["version"]},
                assets=[S.DataProductAssociationClass(destinationUrn=u) for u in assets.values()],
            ),
        )
        yield emit(product_urn, owner)
        for table, urn in assets.items():
            contract = yaml.safe_load(contract_path(table, product).read_text())
            yield emit(urn, owner)
            yield emit(urn, tags)
            yield emit(
                urn,
                S.DatasetPropertiesClass(
                    name=table,
                    description=contract["description"]["purpose"],
                    customProperties={
                        "odcs_id": contract["id"],
                        "odcs_version": contract["version"],
                        "contract_url": f"{artifact_base}/{product}/{PORTS[table]}.odcs.yaml",
                        "manifest_sha256": run["manifest_sha256"],
                        "manifest_uri": run["source_root"] + "/manifest.json",
                        "validation_report": report_uri,
                        "snapshot_id": str(run["snapshots"].get(table, ""))
                        if product == "bronze-payments"
                        else "source",
                    },
                ),
            )
    flow = B.make_data_flow_urn("argo", "historical-dp1-bronze", env)
    yield emit(flow, S.DataFlowInfoClass(name="Historical DP1 bronze", customProperties={"workflow": run["workflow"]}))
    for job in ("ingest", "validate"):
        urn = B.make_data_job_urn("argo", "historical-dp1-bronze", job, env)
        yield emit(
            urn,
            S.DataJobInfoClass(
                name=job,
                type="BATCH",
                customProperties={
                    "run_id": run["run_id"],
                    "workflow": run["workflow"],
                    "data_outcome": report["data_outcome"],
                    "manifest_sha256": run["manifest_sha256"],
                    "snapshots": str(run["snapshots"]),
                    "report": report_uri,
                },
            ),
        )
        yield emit(
            urn,
            S.DataJobInputOutputClass(
                inputDatasets=list(source.values()) if job == "ingest" else list(identities.values()),
                outputDatasets=list(identities.values()),
            ),
        )
        timestamp = int(datetime.fromisoformat(report["run_time"]).timestamp() * 1000)
        instance = "urn:li:dataProcessInstance:" + run["run_id"] + ":" + job
        actor = B.make_user_urn("phuchoang2603")
        yield emit(
            instance,
            S.DataProcessInstancePropertiesClass(
                name=job,
                created=S.AuditStampClass(time=timestamp, actor=actor),
                externalUrl=run["workflow"] if run["workflow"].startswith("https://") else None,
                customProperties={"workflow": run["workflow"], "report": report_uri},
            ),
        )
        yield emit(instance, S.DataProcessInstanceRelationshipsClass(upstreamInstances=[], parentTemplate=urn))
        yield emit(
            instance,
            S.DataProcessInstanceInputClass(
                inputs=list(source.values()) if job == "ingest" else list(identities.values())
            ),
        )
        yield emit(instance, S.DataProcessInstanceOutputClass(outputs=list(identities.values())))
        complete = set(run["snapshots"]) == set(PORTS)
        outcome = ("passed" if complete else "error") if job == "ingest" else report["data_outcome"]
        yield emit(
            instance,
            S.DataProcessInstanceRunEventClass(
                timestampMillis=timestamp,
                status="COMPLETE",
                result=S.DataProcessInstanceRunResultClass(
                    type="SUCCESS" if outcome == "passed" else "FAILURE", nativeResultType=outcome
                ),
            ),
        )
    for table, urn in identities.items():
        upstreams = [S.UpstreamClass(dataset=source[table], type=S.DatasetLineageTypeClass.TRANSFORMED)]
        if table == "fraud_labels":
            upstreams.append(
                S.UpstreamClass(dataset=identities["payment_events"], type=S.DatasetLineageTypeClass.TRANSFORMED)
            )
        contract = yaml.safe_load(contract_path(table).read_text())
        fields = [p["name"] for p in contract["schema"][0]["properties"] if not p["name"].startswith("_")]
        yield emit(
            urn,
            S.UpstreamLineageClass(
                upstreams=upstreams,
                fineGrainedLineages=[
                    S.FineGrainedLineageClass(
                        upstreamType="FIELD_SET",
                        downstreamType="FIELD",
                        upstreams=[B.make_schema_field_urn(source[table], field)],
                        downstreams=[B.make_schema_field_urn(urn, field)],
                    )
                    for field in fields
                ],
            ),
        )


def publish(store, run, contracts, emitter=None, graph=None, discover_fn=discover, sleep=time.sleep):
    key = report_key(run, contracts)
    report = store.get(key)
    outcome = {
        "outcome": "error",
        "readback_verified": False,
        "data_outcome": "error" if not report else report["data_outcome"],
    }
    endpoint = os.environ["DATAHUB_GMS_URL"]
    token = os.environ["DATAHUB_TOKEN"]
    try:
        if not report:
            raise ValueError("missing validation report is an execution error")
        emitter = emitter or DatahubRestEmitter(
            gms_server=endpoint,
            token=token,
            default_emit_mode="SYNC_WAIT",
            read_timeout_sec=15,
            connect_timeout_sec=10,
            retry_max_times=2,
        )
        graph = graph or DataHubGraph(DatahubClientConfig(server=endpoint, token=token, timeout_sec=15))
        config = iceberg_config()
        identities = store.get(f"runs/{run['run_id']}/catalog-identities.json")
        if identities is None:
            identities = discover_fn(emitter, config, run["run_id"])
            store.put(f"runs/{run['run_id']}/catalog-identities.json", identities, create=True)
        if set(identities) != set(PORTS):
            raise ValueError("missing canonical identities")
        proposals = list(
            metadata_proposals(
                run,
                report,
                identities,
                store.root + "/" + key,
                os.environ.get(
                    "CONTRACT_ARTIFACT_BASE",
                    "https://github.com/phuchoang2603/payment-gateway-fraud-detection/blob/main/contracts/data-products",
                ),
            )
        )
        required = [
            (p.entityUrn, type(p.aspect), p.aspect)
            for p in proposals
            if not isinstance(p.aspect, S.DataProcessInstanceRunEventClass)
        ]
        execution_events = [p for p in proposals if isinstance(p.aspect, S.DataProcessInstanceRunEventClass)]
        required += [(urn, S.SchemaMetadataClass, None) for urn in identities.values()]
        for attempt in range(3):
            try:
                for proposal in proposals:
                    emitter.emit(proposal)
                evidence = publish_gx(report, identities, emitter, config["env"], config["platform_instance"])
                for item in evidence:
                    emitter.emit(MCP(entityUrn=item["assertionUrn"], aspect=item["assertionInfo"]))
                    required.append((item["assertionUrn"], S.AssertionInfoClass, item["assertionInfo"]))
                emitter.flush()
                for read_attempt in range(4):
                    missing = []
                    for urn, aspect_type, expected in required:
                        actual = graph.get_aspect(entity_urn=urn, aspect_type=aspect_type)
                        if actual is None or (expected is not None and actual.to_obj() != expected.to_obj()):
                            missing.append((urn, aspect_type.__name__))
                    for proposal in execution_events:
                        values = graph.get_timeseries_values(
                            entity_urn=proposal.entityUrn,
                            aspect_type=S.DataProcessInstanceRunEventClass,
                            filter={},
                            limit=1,
                        )
                        if not values or values[0].result.nativeResultType != proposal.aspect.result.nativeResultType:
                            missing.append((proposal.entityUrn, "dataProcessInstanceRunEvent"))
                    for assertion in evidence:
                        for event in assertion["assertionResults"]:
                            values = graph.get_timeseries_values(
                                entity_urn=assertion["assertionUrn"],
                                aspect_type=S.AssertionRunEventClass,
                                filter={
                                    "or": [
                                        {"and": [{"field": "runId", "condition": "EQUAL", "values": [event.runId]}]}
                                    ]
                                },
                                limit=1,
                            )
                            if (
                                not values
                                or values[0].result.type != event.result.type
                                or values[0].asserteeUrn != event.asserteeUrn
                            ):
                                missing.append((assertion["assertionUrn"], "assertionRunEvent"))
                    if not missing:
                        break
                    sleep(min(2**read_attempt, 8))
                if missing:
                    raise RuntimeError("catalog readback is incomplete")
                outcome.update(outcome="passed", readback_verified=True, identities=identities)
                break
            except Exception:
                if attempt == 2:
                    raise
                sleep(2**attempt)
    except Exception as error:
        outcome["error_type"] = type(error).__name__
    finally:
        store.put(key.replace("validation.json", "governance.json"), outcome)
        run["governance"] = outcome["outcome"]
        save_run(store, run)
    return outcome
