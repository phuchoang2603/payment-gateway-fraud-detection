## Why

Shared OpenMetadata connections and discovery currently live inside DP1. Registering Kafka there would couple unrelated service catalogs to ingestion acceptance and duplicate connection ownership.

## What Changes

- Extract shared connections, credentials, declarative registrations and scheduled native discovery into a metadata-integrations chart under Argo CD.
- Keep DP1 snapshot-specific validation, profiling and governance in data-pipelines, using the same configured connection.
- Include shared Kafka metadata and registered schemas through Karapace without sampling or automatic data-product creation; operational databases and Kubeflow are outside this change.
- Preserve canonical existing lake identities and accepted DP1 evidence. No pipeline rerun or source reset.

## Capabilities

### New Capabilities
- `metadata-integrations`: Shared Git-managed connections and native catalog discovery.

### Modified Capabilities
- `data-pipeline-orchestration`: Pipeline discovery uses the shared connection configuration.

## Impact

Metadata and Kafka charts, Argo root, thin metadata image and existing operational docs. Configuration moves intentionally without legacy duplicate connection definitions. Kafka discovery does not establish business quality, complete lineage or a data product.
