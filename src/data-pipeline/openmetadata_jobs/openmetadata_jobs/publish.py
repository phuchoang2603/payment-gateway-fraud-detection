import json
import os
import time

from openmetadata_jobs.catalog import client
from openmetadata_jobs.lineage import (
    HISTORICAL_PREFIX,
    PIPELINES,
    SERVICE,
    STORAGE_SERVICE,
    dataset_container,
    edges,
    pipeline_fqn,
)

STATUS = {"Succeeded": "Successful"}


def status_for(argo_status: str) -> str:
    return STATUS.get(argo_status, "Failed")


def entity_class(kind: str):
    from metadata.generated.schema.entity.data.container import Container
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.entity.data.topic import Topic

    return {"table": Table, "topic": Topic, "container": Container}[kind]


def ensure_pipeline(sdk, name: str):
    from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
    from metadata.generated.schema.type.entityReference import EntityReference

    pipeline = sdk.create_or_update(CreatePipelineRequest(name=name, service=SERVICE, description=PIPELINES[name]))
    return EntityReference(id=pipeline.id, type="pipeline")


def publish_status(sdk, name: str, argo_status: str, run_id: str) -> None:
    from metadata.generated.schema.entity.data.pipeline import PipelineStatus, StatusType

    sdk.add_pipeline_status(
        pipeline_fqn(name),
        PipelineStatus(
            timestamp=int(time.time() * 1000),
            executionStatus=StatusType(status_for(argo_status)),
            executionId=run_id,
        ),
    )


def ensure_dataset(sdk, version: str) -> None:
    from metadata.generated.schema.api.data.createContainer import CreateContainerRequest

    sdk.create_or_update(
        CreateContainerRequest(
            name=dataset_container(version),
            service=STORAGE_SERVICE,
            prefix=f"{HISTORICAL_PREFIX}/{version}/",
            fileFormats=["parquet"],
            description=f"Historical payments dataset {version}",
        )
    )


def publish_lineage(sdk, name: str, pipeline_ref, dataset_version: str) -> int:
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
    from metadata.generated.schema.type.entityReference import EntityReference

    if name == "dp1-bronze-historical":
        ensure_dataset(sdk, dataset_version)
    published = 0
    for edge in edges(dataset_version):
        if edge.pipeline != name:
            continue
        source = sdk.get_by_name(entity=entity_class(edge.source.kind), fqn=edge.source.fqn)
        target = sdk.get_by_name(entity=entity_class(edge.target.kind), fqn=edge.target.fqn)
        if source is None or target is None:
            missing = edge.source.fqn if source is None else edge.target.fqn
            raise ValueError(f"lineage endpoint is not in OpenMetadata: {missing}")
        sdk.add_lineage(
            AddLineageRequest(
                edge=EntitiesEdge(
                    fromEntity=EntityReference(id=source.id, type=edge.source.kind),
                    toEntity=EntityReference(id=target.id, type=edge.target.kind),
                    lineageDetails=LineageDetails(pipeline=pipeline_ref),
                )
            )
        )
        published += 1
    return published


def failed_steps(failures: str) -> set[str]:
    names = set()
    for node in json.loads(failures or "[]"):
        names.update(node.get("displayName", "").split("."))
    return names


def lineage_allowed(index: int, steps: list[str], workflow_status: str, failed: set[str]) -> bool:
    if not steps:
        return workflow_status == "Succeeded"
    return not failed.intersection(steps[: index + 1])


def main() -> int:
    names = [name for name in os.environ["PIPELINES"].split(",") if name]
    steps = [step for step in os.environ.get("STEPS", "").split(",") if step]
    unknown = [name for name in names if name not in PIPELINES]
    if unknown or (steps and len(steps) != len(names)):
        raise ValueError(f"invalid publish request for {names or unknown}")
    workflow_status = os.environ["WORKFLOW_STATUS"]
    failed = failed_steps(os.environ.get("FAILED_STEPS", ""))
    run_id = os.environ["RUN_ID"]
    dataset_version = os.environ.get("DATASET_VERSION", "v18")
    sdk = client()
    summary = {}
    for index, name in enumerate(names):
        pipeline_ref = ensure_pipeline(sdk, name)
        publish_status(sdk, name, workflow_status, run_id)
        edges = 0
        if lineage_allowed(index, steps, workflow_status, failed):
            edges = publish_lineage(sdk, name, pipeline_ref, dataset_version)
        summary[name] = {"status": status_for(workflow_status), "lineage_edges": edges}
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
