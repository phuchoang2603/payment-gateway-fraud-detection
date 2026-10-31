import json
import os
import time
from pathlib import Path
from urllib.parse import urlsplit

from openmetadata.catalog import client
from openmetadata.lineage import (
    DP3,
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


def publish_status(sdk, name: str, argo_status: str, run_id: str, parameters=None) -> None:
    from metadata.generated.schema.entity.data.pipeline import PipelineStatus, StatusType

    sdk.add_pipeline_status(
        pipeline_fqn(name),
        PipelineStatus(
            timestamp=int(time.time() * 1000),
            executionStatus=StatusType(status_for(argo_status)),
            executionId=run_id,
            parameters=parameters,
        ),
    )


def run_parameters(uri: str):
    """The Silver snapshots and mode a DP3 run read, from its report; None when the run wrote no report."""
    if not uri:
        return None
    parsed = urlsplit(uri)
    if parsed.scheme in ("s3", "s3a"):
        import boto3
        from botocore.config import Config

        path_style = os.environ.get("S3_PATH_STYLE", "false") == "true"
        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT") or None,
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(s3={"addressing_style": "path" if path_style else "virtual"}),
        )
        try:
            body = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))["Body"].read()
        except client.exceptions.NoSuchKey:
            print(f"no DP3 report at {uri}")
            return None
    else:
        path = Path(uri)
        if not path.exists():
            print(f"no DP3 report at {uri}")
            return None
        body = path.read_bytes()
    report = json.loads(body)
    return {"mode": report.get("mode"), "silver_snapshots": report.get("silver_snapshots")}


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


def main() -> int:
    names = [name for name in os.environ["PIPELINES"].split(",") if name]
    unknown = [name for name in names if name not in PIPELINES]
    if unknown:
        raise ValueError(f"invalid publish request for {unknown}")
    dataset_version = os.environ.get("DATASET_VERSION", "")
    if "dp1-bronze-historical" in names and not dataset_version:
        raise ValueError("DATASET_VERSION is required to publish the historical pipeline")
    workflow_status = os.environ["WORKFLOW_STATUS"]
    run_id = os.environ["RUN_ID"]
    sdk = client()
    parameters = run_parameters(os.environ.get("RUN_REPORT", ""))
    summary = {}
    for name in names:
        pipeline_ref = ensure_pipeline(sdk, name)
        publish_status(sdk, name, workflow_status, run_id, parameters if name in DP3 else None)
        edges = 0
        if workflow_status == "Succeeded":
            edges = publish_lineage(sdk, name, pipeline_ref, dataset_version)
        summary[name] = {"status": status_for(workflow_status), "lineage_edges": edges}
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
