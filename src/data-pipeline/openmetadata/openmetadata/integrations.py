"""Register shared service connections and their ingestion workflows in OpenMetadata."""

import json
import os
from pathlib import Path

from openmetadata.catalog import client, expand, native, server


def connectors():
    """Load the configured native service-connection templates."""
    return json.loads(Path(os.environ["METADATA_CONNECTORS_FILE"]).read_text())


def connection(name):
    """Select one connector connection definition."""
    return connectors()[name]["source"]["serviceConnection"]["config"]


def workflow(name):
    """Require discovery to be enabled before building an ingestion workflow."""
    config = connectors()[name]
    if not config["discover"]:
        raise ValueError("connector does not enable native discovery")
    return {
        "source": config["source"],
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "INFO", "openMetadataServerConfig": server()},
    }


def register(sdk, configured):
    """Create or update the configured service entities before discovery."""
    from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
    from metadata.generated.schema.api.services.createMessagingService import CreateMessagingServiceRequest
    from metadata.generated.schema.api.services.createPipelineService import CreatePipelineServiceRequest
    from metadata.generated.schema.api.services.createStorageService import CreateStorageServiceRequest

    requests = {
        "database": CreateDatabaseServiceRequest,
        "messaging": CreateMessagingServiceRequest,
        "pipeline": CreatePipelineServiceRequest,
        "storage": CreateStorageServiceRequest,
    }
    for config in configured.values():
        source = config["source"]
        service_connection = expand(source["serviceConnection"])
        sdk.create_or_update(
            requests[config["kind"]].model_validate(
                {
                    "name": source["serviceName"],
                    "serviceType": service_connection["config"]["type"],
                    "connection": service_connection,
                }
            )
        )


def main():
    """Register services and execute the requested native discovery workflows."""
    stage = os.environ["METADATA_STAGE"]
    if stage not in ("register", "discover"):
        raise ValueError("METADATA_STAGE must be register or discover")
    connector = os.environ.get("METADATA_CONNECTOR")
    if stage == "register":
        configured = connectors()
        register(client(), {connector: configured[connector]} if connector else configured)
    else:
        if not connector:
            raise ValueError("discovery requires METADATA_CONNECTOR")
        native("ingest", workflow(connector))
        if connector == "kafka" and os.environ.get("DATASET_VERSION"):
            from openmetadata.publish import ensure_pipeline, publish_lineage

            sdk = client()
            for name in ("fraud-streaming", "feature-writer"):
                reference = ensure_pipeline(sdk, name)
                publish_lineage(sdk, name, reference, os.environ["DATASET_VERSION"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
