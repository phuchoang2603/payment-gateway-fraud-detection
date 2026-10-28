import json
import os
from pathlib import Path

from pipeline_common.catalog import client, expand, native, server


def connectors():
    return json.loads(Path(os.environ["METADATA_CONNECTORS_FILE"]).read_text())


def connection(name):
    return connectors()[name]["source"]["serviceConnection"]["config"]


def workflow(name):
    config = connectors()[name]
    if not config["discover"]:
        raise ValueError("connector does not enable native discovery")
    return {
        "source": config["source"],
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "INFO", "openMetadataServerConfig": server()},
    }


def register(sdk, configured):
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
