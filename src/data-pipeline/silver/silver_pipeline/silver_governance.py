import json
import os
import re
import time
import urllib.parse
from datetime import datetime
from hashlib import sha256
from uuid import uuid4

import yaml
from pipeline_common import governance, readback
from pipeline_common.catalog import client, ref
from pipeline_common.governance import resolve_owners
from pipeline_common.products import PRODUCTS, SILVER_PORTS

from silver_pipeline.silver_contracts import contract_path, hashes, product_hash, quality_hash
from silver_pipeline.silver_runs import digest, publication_lock, verify_candidate

PRODUCT = "silver-payments"
PIPELINE = "argo.historical-dp1-dp2"
TASKS = {
    "dp2-transform": ["dp2-discover"],
    "dp2-discover": ["dp2-validate"],
    "dp2-validate": ["dp2-publish"],
    "dp2-publish": [],
}
UPSTREAM = {
    "payment_events": ("payment_events",),
    "event_delivery_provenance": ("payment_events",),
    "attempt_histories": ("payment_events",),
    "label_observations": ("payment_events", "fraud_labels"),
    "label_delivery_provenance": ("fraud_labels",),
}


def entity(table):
    if table not in SILVER_PORTS:
        raise ValueError("unknown Silver output")
    return f"starrocks.lakehouse.silver.{table}"


def contract_body(table, table_id, owners, tests, revision):
    artifact = contract_path(table)
    original = artifact.read_bytes()
    document = yaml.safe_load(original)
    product = yaml.safe_load((PRODUCTS / PRODUCT / "product.odps.yaml").read_text())
    if document["team"] != product["team"] or not owners or not tests:
        raise ValueError("Silver contract requires its declared owners and native tests")
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("Silver contract provenance requires a full Git revision")
    path = f"contracts/data-products/{PRODUCT}/{artifact.name}"
    url = f"{governance.REPOSITORY}/blob/{revision}/{path}"
    return {
        "name": document["id"],
        "displayName": document["name"],
        "description": f"ODCS id={document['id']} version={document['version']} sha256={sha256(original).hexdigest()}\n\nSource revision: [{revision}]({url})\n\n```yaml\n{original.decode()}\n```",
        "entity": {"id": table_id, "type": "table"},
        "schema": governance.columns(document["schema"][0]["properties"]),
        "qualityExpectations": tests,
        "owners": owners,
        "sourceUrl": url,
        "termsOfUse": "\n\n".join(document["description"].values()),
    }


def execution(candidate, stages, outcome):
    started = int(datetime.fromisoformat(candidate["created_at"]).timestamp() * 1000)
    inputs = [
        {"datasetFQN": f"starrocks.lakehouse.bronze.raw_{table}", "version": str(snapshot)}
        for selected in candidate["inputs"]
        for table, snapshot in selected["snapshots"].items()
    ]
    outputs = [
        {"datasetFQN": entity(table), "version": str(snapshot), "mode": "overwrite"}
        for table, snapshot in candidate["snapshots"].items()
    ]
    statuses = []
    for name in TASKS:
        state = stages[name]
        statuses.append(
            {
                "name": name,
                "executionStatus": {"passed": "Successful", "pending": "Pending"}.get(state["outcome"], "Failed"),
                **({"startTime": state["started_at"]} if "started_at" in state else {}),
                **({"endTime": state["ended_at"]} if "ended_at" in state else {}),
                **({"inputs": inputs, "outputs": outputs} if name == "dp2-transform" else {}),
            }
        )
    return {
        "timestamp": started,
        "executionId": candidate["candidate_id"],
        "executionStatus": {"passed": "Successful", "pending": "Pending"}.get(outcome, "Failed"),
        "taskStatus": statuses,
        "endTime": max(state.get("ended_at", started) for state in stages.values()),
        "inputs": inputs,
        "outputs": outputs,
        "parameters": {
            "candidate_id": candidate["candidate_id"],
            "inputs": candidate["inputs"],
            "snapshots": candidate["snapshots"],
            "contracts": candidate["contracts"],
            "product_sha256": candidate["product_sha256"],
            "quality_sha256": candidate["quality_sha256"],
            "transformation_revision": candidate["transformation_revision"],
            "completeness": candidate["completeness"],
            "transform_workflow": candidate["transform"].get("workflow_id"),
            "publication_workflow": os.environ["WORKFLOW_ID"],
        },
    }


def synchronize(sdk, candidate, expected, stages, outcome, report_uri):
    from metadata.generated.schema.api.data.createDataContract import CreateDataContractRequest
    from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
    from metadata.generated.schema.api.domains.createDataProduct import CreateDataProductRequest
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.entity.data.pipeline import PipelineStatus
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.entity.domains.dataProduct import DataProduct
    from metadata.generated.schema.entity.domains.domain import Domain
    from metadata.generated.schema.entity.services.pipelineService import PipelineService
    from metadata.generated.schema.tests.testCase import TestCase
    from metadata.generated.schema.type.entityReference import EntityReference

    def merge_references(existing, additions):
        references = list(existing or [])
        present = {str(reference.id.root) for reference in references}
        references.extend(EntityReference.model_validate(item) for item in additions if item["id"] not in present)
        return references

    product = yaml.safe_load((PRODUCTS / PRODUCT / "product.odps.yaml").read_text())
    if product["team"] != governance.product_document()["team"]:
        raise ValueError("Silver product requires separately resolved owners")
    owners = resolve_owners(sdk)
    domain = sdk.get_by_name(Domain, governance.DOMAIN, nullable=False)
    sdk.create_or_update(
        CreateDataProductRequest(
            name=PRODUCT, description=product["description"]["purpose"], domains=[governance.DOMAIN], owners=owners
        )
    )
    native_product = sdk.get_by_name(DataProduct, PRODUCT, nullable=False)
    sources = {
        table: sdk.get_by_name(Table, f"starrocks.lakehouse.bronze.raw_{table}", nullable=False)
        for table in ("payment_events", "fraud_labels")
    }
    outputs, contracts = {}, {}
    revision = os.environ["GIT_REVISION"]
    for table in SILVER_PORTS:
        found = sdk.get_by_name(Table, entity(table), fields=["owners", "dataProducts", "domains"], nullable=False)
        destination = found.model_copy(deep=True)
        destination.owners = merge_references(found.owners, owners)
        destination.dataProducts = merge_references(found.dataProducts, [ref(native_product, "dataProduct")])
        destination.domains = merge_references(found.domains, [ref(domain, "domain")])
        sdk.patch(
            Table,
            found,
            destination,
            allowed_fields={"owners": True, "dataProducts": True, "domains": True},
            override_metadata=True,
            skip_on_failure=False,
        )
        outputs[table] = found
        tests = [
            ref(sdk.get_by_name(TestCase, fqn, fields=["testDefinition", "testSuite"], nullable=False), "testCase")
            for fqn, case in expected["cases"].items()
            if case["table"] == table
        ]
        body = contract_body(table, str(found.id.root), owners, tests, revision)
        request = CreateDataContractRequest.model_validate(body)
        if not readback.contains(request.model_dump(mode="json", by_alias=True, exclude_none=True), body):
            raise ValueError("Silver contract has an unsupported native mapping")
        contract = sdk.create_or_update(request)
        sdk.patch_description(type(contract), contract, body["description"], force=True, skip_on_failure=False)
        contracts[table] = {"id": str(contract.id.root), "body": body}
    sdk.get_by_name(PipelineService, "argo", nullable=False)
    transform_workflow = candidate["transform"].get("workflow_id")
    if not transform_workflow:
        raise ValueError("Silver lineage requires the actual transform workflow identity")
    url = (
        os.environ["ARGO_WORKFLOWS_URL"].rstrip("/")
        + f"/workflows/{os.environ.get('WORKFLOW_NAMESPACE', 'fraud-data-prod')}/{transform_workflow}"
    )
    publication_url = (
        os.environ["ARGO_WORKFLOWS_URL"].rstrip("/")
        + f"/workflows/{os.environ.get('WORKFLOW_NAMESPACE', 'fraud-data-prod')}/{os.environ['WORKFLOW_ID']}"
    )
    pipeline = sdk.create_or_update(
        CreatePipelineRequest(
            name="historical-dp1-dp2",
            service="argo",
            description="Accepted Bronze inputs and separately validated Silver candidates.",
            owners=owners,
            dataProducts=[PRODUCT],
            domains=[governance.DOMAIN],
            sourceUrl=url,
            tasks=[
                {
                    "name": name,
                    "downstreamTasks": children,
                    "sourceUrl": publication_url if name == "dp2-publish" else url,
                }
                for name, children in TASKS.items()
            ],
        )
    )
    source_refs = [EntityReference.model_validate(ref(value, "table")) for value in sources.values()]
    output_refs = [EntityReference.model_validate(ref(value, "table")) for value in outputs.values()]
    sdk.add_assets_to_data_product(PRODUCT, output_refs + [EntityReference.model_validate(ref(pipeline, "pipeline"))])
    sdk.add_input_ports_to_data_product(PRODUCT, source_refs)
    sdk.add_output_ports_to_data_product(PRODUCT, output_refs)
    lineage_description = f"candidate={candidate['candidate_id']} inputs_sha256={digest(candidate['inputs'])} outputs={json.dumps(candidate['snapshots'], sort_keys=True)} evidence={report_uri}"
    for table, upstream in UPSTREAM.items():
        for source in upstream:
            sdk.add_lineage(
                AddLineageRequest.model_validate(
                    {
                        "edge": {
                            "fromEntity": ref(sources[source], "table"),
                            "toEntity": ref(outputs[table], "table"),
                            "lineageDetails": {
                                "pipeline": ref(pipeline, "pipeline"),
                                "description": lineage_description,
                            },
                        }
                    }
                )
            )
    status = execution(candidate, stages, outcome)
    sdk.add_pipeline_status(PIPELINE, PipelineStatus.model_validate(status))
    return {
        "owners": owners,
        "sources": {table: ref(value, "table") for table, value in sources.items()},
        "outputs": {table: ref(value, "table") for table, value in outputs.items()},
        "contracts": contracts,
        "pipeline": ref(pipeline, "pipeline"),
        "status": status,
        "url": url,
        "publication_url": publication_url,
        "lineage_description": lineage_description,
    }


def gaps(published, candidate, fetch=readback.get):
    problems = []
    owners = readback.ids(published["owners"])
    domain = fetch(f"domains/name/{governance.DOMAIN}")
    product = fetch(f"dataProducts/name/{PRODUCT}", {"fields": "owners,domains"})
    bronze = fetch("dataProducts/name/bronze-payments", {"fields": "owners,domains"})
    if owners != readback.ids(product.get("owners")) or domain["id"] not in readback.ids(product.get("domains")):
        problems.append("product:ownership-domain")
    for relation, expected_ids in (
        ("inputPorts", readback.ids(list(published["sources"].values()))),
        ("outputPorts", readback.ids(list(published["outputs"].values()))),
        ("assets", readback.ids(list(published["outputs"].values())) | {published["pipeline"]["id"]}),
    ):
        found = fetch(f"dataProducts/name/{PRODUCT}/{relation}", {"limit": readback.PAGE})
        if not expected_ids <= readback.ids(found.get("data")):
            problems.append(f"product:{relation}")
    for table, identity in published["sources"].items():
        fqn = urllib.parse.quote(f"starrocks.lakehouse.bronze.raw_{table}", safe="")
        source = fetch(f"tables/name/{fqn}", {"fields": "owners,dataProducts,domains"})
        if (
            source["id"] != identity["id"]
            or bronze["id"] not in readback.ids(source.get("dataProducts"))
            or domain["id"] not in readback.ids(source.get("domains"))
        ):
            problems.append(f"source:{table}:membership")
    for table, identity in published["outputs"].items():
        fqn = urllib.parse.quote(entity(table), safe="")
        found = fetch(f"tables/name/{fqn}", {"fields": "owners,dataProducts,domains"})
        if (
            found["id"] != identity["id"]
            or not owners <= readback.ids(found.get("owners"))
            or product["id"] not in readback.ids(found.get("dataProducts"))
            or domain["id"] not in readback.ids(found.get("domains"))
        ):
            problems.append(f"table:{table}:membership")
        contract = fetch(
            "dataContracts/entity", {"entityId": identity["id"], "entityType": "table", "fields": "owners"}
        )
        expected = published["contracts"][table]
        body = expected["body"]
        if (
            candidate["contracts"][table] not in contract.get("description", "")
            or contract["id"] != expected["id"]
            or not readback.contains(
                contract,
                {
                    **{key: value for key, value in body.items() if key != "qualityExpectations"},
                    "termsOfUse": {"content": body["termsOfUse"]},
                },
            )
            or readback.ids(contract.get("qualityExpectations")) != readback.ids(body["qualityExpectations"])
        ):
            problems.append(f"contract:{table}")
        edges = fetch(f"lineage/table/name/{fqn}", {"upstreamDepth": 1, "downstreamDepth": 0}).get("upstreamEdges", [])
        for source in UPSTREAM[table]:
            if not any(
                edge["fromEntity"] == published["sources"][source]["id"]
                and edge["toEntity"] == identity["id"]
                and (edge.get("lineageDetails") or {}).get("pipeline", {}).get("id") == published["pipeline"]["id"]
                and (edge.get("lineageDetails") or {}).get("description") == published["lineage_description"]
                for edge in edges
            ):
                problems.append(f"lineage:{source}:{table}")
    pipeline = fetch(f"pipelines/name/{PIPELINE}", {"fields": "owners,dataProducts,domains,tasks,pipelineStatus"})
    tasks = {task["name"]: task for task in pipeline.get("tasks", [])}
    if (
        pipeline["id"] != published["pipeline"]["id"]
        or pipeline.get("sourceUrl") != published["url"]
        or readback.ids(pipeline.get("owners")) != owners
        or product["id"] not in readback.ids(pipeline.get("dataProducts"))
        or domain["id"] not in readback.ids(pipeline.get("domains"))
        or set(tasks) != set(TASKS)
        or any(
            set(tasks[name].get("downstreamTasks") or []) != set(children)
            or tasks[name].get("sourceUrl")
            != (published["publication_url"] if name == "dp2-publish" else published["url"])
            for name, children in TASKS.items()
            if name in tasks
        )
    ):
        problems.append("pipeline:identity-or-tasks")
    status = pipeline.get("pipelineStatus") or {}
    if not readback.contains(
        {**status, "taskStatus": sorted(status.get("taskStatus", []), key=lambda item: item.get("name", ""))},
        {
            **published["status"],
            "taskStatus": sorted(published["status"]["taskStatus"], key=lambda item: item["name"]),
        },
    ):
        problems.append("pipeline:execution")
    return problems


def _publish_locked(store, candidate):
    from silver_pipeline.silver_native import cases, expected, observed

    candidate_id = candidate["candidate_id"]
    prefix = f"dp2/candidates/{candidate_id}"
    previous = store.get("dp2/accepted.json")
    if previous and previous.get("candidate_id") == candidate_id:
        receipt = store.get(previous["receipt_key"])
        if (
            receipt is None
            or digest(receipt) != previous["receipt_sha256"]
            or receipt.get("candidate_id") != candidate_id
            or receipt.get("snapshots") != candidate.get("snapshots")
            or receipt.get("inputs") != candidate.get("inputs")
            or receipt.get("contracts") != candidate.get("contracts")
            or receipt.get("product_sha256") != candidate.get("product_sha256")
            or receipt.get("quality_sha256") != candidate.get("quality_sha256")
        ):
            raise ValueError("accepted Silver receipt is missing or altered")
        candidate["state"] = "accepted"
        store.put(f"{prefix}.json", candidate)
        return previous
    if (
        candidate.get("state") != "candidate"
        or candidate.get("contracts") != hashes()
        or candidate.get("product_sha256") != product_hash()
        or candidate.get("quality_sha256") != quality_hash()
    ):
        raise ValueError("Silver acceptance requires matching candidate contracts")
    rules = cases(candidate)
    prepared = store.get(f"{prefix}/native-expected.json")
    discovery = store.get(f"{prefix}/native-discovery.json")
    validation = store.get(f"{prefix}/native-validation.json")
    transformation = candidate.get("transform", {})
    if not prepared or any(prepared.get(key) != value for key, value in expected(candidate, rules).items()):
        raise ValueError("Silver native rule identities do not match the candidate")
    for name, stage in (("transform", transformation), ("discovery", discovery), ("validation", validation)):
        if (
            not stage
            or stage.get("outcome") != "passed"
            or stage.get("candidate_id") != candidate_id
            or stage.get("snapshots") != candidate["snapshots"]
        ):
            raise ValueError(f"Silver {name} stage is missing, failed or stale")
    if validation.get("problem_count") != 0 or validation.get("problems") != {}:
        raise ValueError("Silver native checks are not a complete passing set")
    found = {key: value for table in SILVER_PORTS for key, value in observed(table).items()}
    if readback.verify(prepared, found, validation["started_at"]):
        raise ValueError("Silver native results changed since validation")
    diagnostic_ref = candidate.get("diagnostics") or {}
    diagnostic = store.get(diagnostic_ref.get("key", "")) if diagnostic_ref.get("key") else None
    if (
        not diagnostic
        or digest(diagnostic) != diagnostic_ref.get("sha256")
        or diagnostic.get("candidate_id") != candidate_id
        or diagnostic.get("candidate_snapshots") != candidate["snapshots"]
        or diagnostic.get("inputs") != candidate["inputs"]
        or diagnostic.get("transformation_revision") != candidate["transformation_revision"]
    ):
        raise ValueError("Silver diagnostics are missing or do not match the candidate")
    from metadata.generated.schema.entity.data.pipeline import PipelineStatus

    attempt = {
        "attempt_id": str(uuid4()),
        "candidate_id": candidate_id,
        "started_at": int(time.time() * 1000),
        "outcome": "error",
    }
    stages = {
        "dp2-transform": transformation,
        "dp2-discover": discovery,
        "dp2-validate": validation,
        "dp2-publish": {**attempt, "outcome": "pending"},
    }
    published = None
    try:
        sdk = client()
        published = synchronize(sdk, candidate, prepared, stages, "pending", f"{store.root}/{prefix}.json")
        problems = gaps(published, candidate)
        if problems:
            raise ValueError("Silver governance readback has gaps: " + ", ".join(problems[:10]))
        store.put(f"{prefix}/governance-pending/{attempt['attempt_id']}.json", published, create=True)
        stages["dp2-publish"] = {**attempt, "outcome": "passed", "ended_at": int(time.time() * 1000)}
        published["status"] = execution(candidate, stages, "passed")
        sdk.add_pipeline_status(PIPELINE, PipelineStatus.model_validate(published["status"]))
        problems = gaps(published, candidate)
        if problems:
            raise ValueError("Silver governance readback has gaps: " + ", ".join(problems[:10]))
        found = {key: value for table in SILVER_PORTS for key, value in observed(table).items()}
        if readback.verify(prepared, found, validation["started_at"]):
            raise ValueError("Silver native results changed during governance publication")
        receipt = {
            "candidate_id": candidate_id,
            "inputs": candidate["inputs"],
            "snapshots": candidate["snapshots"],
            "contracts": candidate["contracts"],
            "product_sha256": candidate["product_sha256"],
            "quality_sha256": candidate["quality_sha256"],
            "transformation_revision": candidate["transformation_revision"],
            "completeness": candidate["completeness"],
            "diagnostics": diagnostic_ref,
            "native_validation": validation,
            "governance": published,
            "outcome": "passed",
        }
        receipt_hash = digest(receipt)
        receipt_key = f"dp2/receipts/{receipt_hash}.json"
        existing = store.get(receipt_key)
        if existing is None:
            store.put(receipt_key, receipt, create=True)
        elif existing != receipt:
            raise ValueError("immutable Silver receipt identity changed")
        pointer = {"candidate_id": candidate_id, "receipt_key": receipt_key, "receipt_sha256": receipt_hash}
        if not store.compare_and_swap("dp2/accepted.json", previous, pointer):
            raise ValueError("Silver accepted pointer changed during validation")
        candidate["state"] = "accepted"
        candidate["accepted_receipt"] = pointer
        store.put(f"{prefix}.json", candidate)
        attempt["outcome"] = "passed"
        return pointer
    except Exception as error:
        attempt["error_type"] = type(error).__name__
        if published:
            try:
                stages["dp2-publish"] = {**attempt, "outcome": "failed", "ended_at": int(time.time() * 1000)}
                client().add_pipeline_status(
                    PIPELINE, PipelineStatus.model_validate(execution(candidate, stages, "failed"))
                )
            except Exception as status_error:
                attempt["status_error_type"] = type(status_error).__name__
        raise
    finally:
        attempt["ended_at"] = int(time.time() * 1000)
        store.put(f"{prefix}/governance/{attempt['attempt_id']}.json", attempt, create=True)


def publish(store, candidate, takeover_stopped=False):
    verify_candidate(candidate)
    with publication_lock(store, candidate["candidate_id"], os.environ["WORKFLOW_ID"], takeover_stopped):
        return _publish_locked(store, candidate)
