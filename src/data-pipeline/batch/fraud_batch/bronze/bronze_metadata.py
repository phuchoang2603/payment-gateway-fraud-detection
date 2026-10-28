import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import yaml

from fraud_batch.bronze import governance, quality, readback
from fraud_batch.bronze.runs import accept, evidence_key
from fraud_batch.common.catalog import client, expand, native, ref
from fraud_batch.common.governance import resolve_owners
from fraud_batch.common.products import PORTS, hashes
from fraud_batch.common.storage import Store


def configure_profiles(sdk, run):
    from metadata.generated.schema.entity.data.table import Table, TableProfilerConfig

    plans = {}
    for table in PORTS:
        doc = {
            "schema": governance.columns(
                yaml.safe_load(governance.contract_path(table).read_text())["schema"][0]["properties"]
            )
        }
        names = [c["name"] for c in doc["schema"] if c["dataType"] not in ("STRUCT", "ARRAY", "BINARY")]
        sample = int(os.environ.get("PROFILE_MAX_ROWS", "100000"))
        if sample < 1:
            raise ValueError("profile row bound must be positive")
        discovered = sdk.get_by_name(Table, quality.entity(table), nullable=False)
        projection = [str(column.name.root) for column in discovered.columns]
        query = (
            f"SELECT {quality.marker(run)}"
            + ", ".join("`" + name.replace("`", "``") + "`" for name in projection)
            + f" FROM {quality.CATALOG}.{quality.SCHEMA}.{quality.physical_table(table)} LIMIT {sample}"
        )
        config = TableProfilerConfig(
            profileQuery=query,
            includeColumns=[{"columnName": name} for name in names],
            computeColumnMetrics=True,
            computeTableMetrics=True,
        )
        if sdk.create_or_update_table_profiler_config(quality.entity(table), config) is None:
            raise ValueError("native profiler configuration was not saved")
        plans[table] = {
            "columns": names,
            "scope": "latest-state-bounded-query",
            "max_rows": sample,
            "sampling": "first-rows",
            "query": query,
        }
    return plans


def synchronize(sdk, run, manifest, expected, stages, report_uri, outcome):
    from metadata.generated.schema.api.data.createContainer import CreateContainerRequest
    from metadata.generated.schema.api.data.createDataContract import CreateDataContractRequest
    from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
    from metadata.generated.schema.api.domains.createDataProduct import CreateDataProductRequest
    from metadata.generated.schema.api.domains.createDomain import CreateDomainRequest
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.entity.data.pipeline import PipelineStatus
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.entity.domains.dataProduct import DataProduct
    from metadata.generated.schema.entity.domains.domain import Domain
    from metadata.generated.schema.entity.services.pipelineService import PipelineService
    from metadata.generated.schema.entity.services.storageService import StorageService
    from metadata.generated.schema.tests.testCase import TestCase
    from metadata.generated.schema.type.entityReference import EntityReference

    owners = resolve_owners(sdk)
    sdk.create_or_update(
        CreateDomainRequest(
            name=governance.DOMAIN, domainType="Aggregate", description="Fraud detection data platform"
        )
    )
    source_product = "payment-event-landing" if run.get("input_mode") == "landing" else "historical-payments"
    product = governance.product_document()
    for name, description in (
        (source_product, f"Immutable source ports from {run['source_root']}"),
        (governance.PRODUCT, f"{product['description']['purpose']} ODPS {product['id']} version {product['version']}"),
    ):
        sdk.create_or_update(
            CreateDataProductRequest(name=name, description=description, domains=[governance.DOMAIN], owners=owners)
        )
    sdk.get_by_name(StorageService, governance.STORAGE, nullable=False)
    paths = governance.source_paths(run, manifest)
    bronze_product = sdk.get_by_name(DataProduct, governance.PRODUCT, nullable=False)
    domain = sdk.get_by_name(Domain, governance.DOMAIN, nullable=False)
    sources, tables, contracts = {}, {}, {}
    for table, path in paths.items():
        source = sdk.create_or_update(
            CreateContainerRequest(
                name=path["name"],
                service=governance.STORAGE,
                fullPath=path["fullPath"],
                prefix=path["prefix"],
                displayName=f"{source_product}: {PORTS[table]}",
                fileFormats=["parquet"],
                owners=owners,
                dataProducts=[source_product],
                domains=[governance.DOMAIN],
                description=f"Selected port source; immutable execution inventory: {report_uri}",
            )
        )
        sources[table] = source
    for table in PORTS:
        found = sdk.get_by_name(
            Table, quality.entity(table), fields=["owners", "dataProducts", "domains"], nullable=False
        )
        destination = found.model_copy(deep=True)
        destination.owners = [EntityReference.model_validate(o) for o in owners]
        destination.dataProducts = [EntityReference(id=bronze_product.id, type="dataProduct")]
        destination.domains = [EntityReference(id=domain.id, type="domain")]
        sdk.patch(
            Table,
            found,
            destination,
            allowed_fields={"owners": True, "dataProducts": True, "domains": True},
            override_metadata=True,
            skip_on_failure=False,
        )
        tables[table] = found
        tests = [
            ref(
                sdk.get_by_name(TestCase, fqn, fields=["testDefinition", "testSuite"], nullable=False),
                "testCase",
            )
            for fqn, case in expected["cases"].items()
            if case["table"] == table
        ]
        body = governance.contract_body(table, str(found.id.root), owners, tests, os.environ["GIT_REVISION"])
        request = CreateDataContractRequest.model_validate(body)
        if not readback.contains(request.model_dump(mode="json", by_alias=True, exclude_none=True), body):
            raise ValueError("contract contains an unsupported required native mapping")
        contract = sdk.create_or_update(request)
        sdk.patch_description(type(contract), contract, body["description"], force=True, skip_on_failure=False)
        contracts[table] = {"id": str(contract.id.root), "body": body}
    sdk.get_by_name(PipelineService, "argo", nullable=False)
    link = (
        os.environ["ARGO_WORKFLOWS_URL"].rstrip("/")
        + f"/workflows/{os.environ.get('WORKFLOW_NAMESPACE', 'data')}/{os.environ.get('WORKFLOW_ID', run['workflow'])}"
    )
    pipeline = sdk.create_or_update(
        CreatePipelineRequest(
            name="historical-dp1-bronze",
            service="argo",
            description="Raw ingestion; independent event/label outputs",
            owners=owners,
            dataProducts=[governance.PRODUCT],
            domains=[governance.DOMAIN],
            sourceUrl=link,
            tasks=[
                {"name": name, "downstreamTasks": downstream, "sourceUrl": link}
                for name, downstream in governance.TASKS.items()
            ],
        )
    )
    source_refs = [EntityReference.model_validate(ref(s, "container")) for s in sources.values()]
    table_refs = [EntityReference.model_validate(ref(t, "table")) for t in tables.values()]
    sdk.add_assets_to_data_product(source_product, source_refs)
    sdk.add_output_ports_to_data_product(source_product, source_refs)
    sdk.add_assets_to_data_product(
        governance.PRODUCT, table_refs + [EntityReference.model_validate(ref(pipeline, "pipeline"))]
    )
    sdk.add_input_ports_to_data_product(governance.PRODUCT, source_refs)
    sdk.add_output_ports_to_data_product(governance.PRODUCT, table_refs)
    for table, source in sources.items():
        sdk.add_lineage(
            AddLineageRequest.model_validate(
                {
                    "edge": {
                        "fromEntity": ref(source, "container"),
                        "toEntity": ref(tables[table], "table"),
                        "lineageDetails": {
                            "pipeline": ref(pipeline, "pipeline"),
                            "description": f"run={run['run_id']} manifest={run['manifest_sha256']} evidence={report_uri}",
                        },
                    }
                }
            )
        )
    status = governance.execution(run, manifest, paths, stages, outcome, os.environ["GIT_REVISION"])
    sdk.add_pipeline_status(governance.PIPELINE, PipelineStatus.model_validate(status))
    return {
        "owners": owners,
        "contracts": contracts,
        "sources": {
            t: {**ref(s, "container"), "fqn": str(s.fullyQualifiedName.root), "path": paths[t]["fullPath"]}
            for t, s in sources.items()
        },
        "tables": {t: ref(value, "table") for t, value in tables.items()},
        "pipeline": ref(pipeline, "pipeline"),
        "status": status,
        "source_product": source_product,
        "url": link,
    }


def cancel_owned_queries(cursor, username, own_connection):
    cursor.execute("SHOW FULL PROCESSLIST")
    active = [
        row
        for row in cursor.fetchall()
        if row["User"] == username and int(row["Id"]) != own_connection and row["Command"] == "Query"
    ]
    for query in active:
        if "/* dp1 run " in (query.get("Info") or ""):
            cursor.execute(f"KILL QUERY {int(query['Id'])}")
    return bool(active)


def quiesce():
    import pymysql

    config = expand(quality.connection())
    host, port = config["hostPort"].rsplit(":", 1)
    with pymysql.connect(
        host=host,
        port=int(port),
        user=config["username"],
        password=config["password"],
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
        read_timeout=30,
    ) as conn:
        deadline = time.monotonic() + 120
        while True:
            with conn.cursor() as cursor:
                active = cancel_owned_queries(cursor, config["username"], conn.thread_id())
            if not active:
                return
            if time.monotonic() >= deadline:
                raise TimeoutError("native queries are not quiescent")
            time.sleep(2)


def stage_matches(receipt, run):
    return bool(
        receipt
        and receipt.get("run_id") == run["run_id"]
        and receipt.get("manifest_sha256") == run["manifest_sha256"]
        and receipt.get("snapshots") == run["snapshots"]
        and receipt.get("outcome") in ("passed", "skipped")
    )


def publish(store, run, contracts, manifest):
    from metadata.generated.schema.entity.data.pipeline import PipelineStatus

    expected = store.get(evidence_key(run, contracts, "expected"))
    if (
        not expected
        or not all(expected.get(k) == run[k] for k in ("run_id", "manifest_sha256", "snapshots"))
        or expected.get("contracts") != contracts
    ):
        raise ValueError("expected native checks do not match this run")
    stages = {
        name: store.get(evidence_key(run, contracts, name)) or {"outcome": "missing"}
        for name in ("discover", "validate", "profile")
    }
    if run.get("input_mode") == "landing" and stages["profile"]["outcome"] == "missing":
        stages["profile"] = {
            "run_id": run["run_id"],
            "manifest_sha256": run["manifest_sha256"],
            "snapshots": run["snapshots"],
            "outcome": "skipped",
            "reason": "recurring-disabled",
        }
        store.put(evidence_key(run, contracts, "profile"), stages["profile"])
    receipt = store.get(evidence_key(run, contracts, "publish")) or {
        "run_id": run["run_id"],
        "started_at": int(time.time() * 1000),
    }
    problems, gaps, published = {}, [], None
    try:
        found = {fqn: case for table in PORTS for fqn, case in readback.observed(table).items()}
        problems = readback.verify(expected, found, expected["prepared_at"])
        problems.update(
            {f"stage:{name}": state["outcome"] for name, state in stages.items() if not stage_matches(state, run)}
        )
        receipt["outcome"] = "pending"
        stages["publish"] = receipt
        published = synchronize(
            client(),
            run,
            manifest,
            expected,
            stages,
            f"{store.root}/{evidence_key(run, contracts, 'result')}",
            "failed" if problems else "pending",
        )
        published["profile"] = stages["profile"]
        published["profile_required"] = run.get("input_mode") != "landing"
        for attempt in range(4):
            gaps = readback.governance_gaps(published, contracts)
            if not gaps:
                break
            time.sleep(2**attempt)
        receipt.update(
            outcome="failed" if problems or gaps else "passed",
            ended_at=int(time.time() * 1000),
        )
        published["status"] = governance.execution(
            run,
            manifest,
            governance.source_paths(run, manifest),
            stages,
            receipt["outcome"],
            os.environ["GIT_REVISION"],
        )
        client().add_pipeline_status(governance.PIPELINE, PipelineStatus.model_validate(published["status"]))
        if not problems and not gaps:
            gaps = readback.governance_gaps(published, contracts)
        store.put(evidence_key(run, contracts, "governance"), published)
        accept(store, run, contracts, problems, gaps)
    except Exception as error:
        receipt.update(outcome="failed", ended_at=int(time.time() * 1000), error_type=type(error).__name__)
        store.put(evidence_key(run, contracts, "governance-error"), {**receipt, "problems": problems, "gaps": gaps})
        if published:
            try:
                client().add_pipeline_status(
                    governance.PIPELINE,
                    PipelineStatus.model_validate(
                        governance.execution(
                            run,
                            manifest,
                            governance.source_paths(run, manifest),
                            stages,
                            "failed",
                            os.environ["GIT_REVISION"],
                        )
                    ),
                )
            except Exception as status_error:
                logging.error("failed execution status publication: %s", type(status_error).__name__)
        raise
    finally:
        store.put(evidence_key(run, contracts, "publish"), receipt)


def main():
    logging.disable(logging.INFO)
    stage = os.environ["PIPELINE_STAGE"]
    if stage not in ("discover", "validate", "profile", "publish", "quiesce"):
        raise ValueError("unsupported Bronze metadata stage")
    refresh = os.environ.get("PROFILE_REFRESH", "false")
    if refresh not in ("true", "false"):
        raise ValueError("PROFILE_REFRESH must be true or false")
    if refresh == "true" and stage != "profile":
        raise ValueError("only on-demand profiling supports refresh")
    if stage == "quiesce":
        quiesce()
        return 0
    store, contracts = Store(os.environ["EVIDENCE_ROOT"]), hashes()
    run = store.get(f"versions/{os.environ['DATASET_VERSION']}/run.json")
    if not run or set(run["snapshots"]) != set(PORTS):
        raise ValueError("metadata stage requires recorded output snapshots")
    if run.get("input_mode") == "landing":
        from fraud_batch.bronze.landing import selection

        manifest, digest = selection(store, run["dataset_version"])
    else:
        from fraud_batch.common.manifest import selected_manifest

        manifest, digest = selected_manifest(run["source_root"])
    if digest != run["manifest_sha256"]:
        raise ValueError("metadata source identity changed")
    stage_key = evidence_key(run, contracts, "profile-on-demand" if refresh == "true" else stage)
    if stage == "publish":
        publish(store, run, contracts, manifest)
        return 0
    if stage_matches(store.get(stage_key), run) and refresh != "true":
        return 0
    receipt = {
        "run_id": run["run_id"],
        "manifest_sha256": digest,
        "snapshots": run["snapshots"],
        "started_at": int(time.time() * 1000),
        "outcome": "error",
        "mode": "on-demand" if refresh == "true" else "acceptance",
    }
    try:
        if stage == "discover":
            quality.prepare_run(store, run, contracts, manifest)
            native("ingest", quality.metadata_workflow())
        elif stage == "validate":
            cases = quality.cases(run, manifest)
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(
                    pool.map(
                        lambda table: native("test", quality.test_workflow(table, cases, quality.entity(table))), PORTS
                    )
                )
        elif stage == "profile":
            receipt["coverage"] = configure_profiles(client(), run)
            native("profile", quality.profiler_workflow())
            for table, coverage in receipt["coverage"].items():
                value = client().get_latest_table_profile(quality.entity(table))
                if not value or not value.profile or value.profile.timestamp.root < receipt["started_at"]:
                    raise ValueError("native column profile is missing or stale")
                profiles = {str(c.name.root): c.profile for c in value.columns if c.profile}
                if not set(coverage["columns"]) <= profiles.keys() or any(
                    profiles[name].nullCount is None or profiles[name].distinctCount is None
                    for name in coverage["columns"]
                ):
                    raise ValueError("native scalar-column statistics are incomplete")
        receipt["outcome"] = "passed"
        return 0
    finally:
        receipt["ended_at"] = int(time.time() * 1000)
        store.put(stage_key, receipt)


if __name__ == "__main__":
    raise SystemExit(main())
