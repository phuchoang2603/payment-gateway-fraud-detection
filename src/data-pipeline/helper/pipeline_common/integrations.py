import argparse
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


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["register", "discover"])
    parser.add_argument("--connector")
    args = parser.parse_args(argv)
    if args.stage == "register":
        configured = connectors()
        register(client(), {args.connector: configured[args.connector]} if args.connector else configured)
    else:
        if not args.connector:
            parser.error("discover requires --connector")
        native("ingest", workflow(args.connector))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
