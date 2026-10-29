import os
import time

from openmetadata_jobs.catalog import client
from openmetadata_jobs.lineage import PIPELINES, SERVICE, edges, pipeline_fqn

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


def publish_lineage(sdk, name: str, pipeline_ref, dataset_version: str) -> int:
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
    from metadata.generated.schema.type.entityReference import EntityReference

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


def main() -> int:
    name = os.environ["PIPELINE"]
    if name not in PIPELINES:
        raise ValueError(f"unknown pipeline {name}")
    argo_status = os.environ["WORKFLOW_STATUS"]
    sdk = client()
    pipeline_ref = ensure_pipeline(sdk, name)
    publish_status(sdk, name, argo_status, os.environ["RUN_ID"])
    published = publish_lineage(sdk, name, pipeline_ref, os.environ.get("DATASET_VERSION", "v18"))
    print(f"published status {status_for(argo_status)} and {published} lineage edges for {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
