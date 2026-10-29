## MODIFIED Requirements

### Requirement: One shared current flow

The platform SHALL use one current environment, codebase, common runtime configuration and reusable topic-to-Bronze flow. Live payment events and labels SHALL reach Bronze only through their Kafka topics, read by DP1 in Kafka mode into the same tables as historical data. Historical and live synthetic truth SHALL be valid analytics and training inputs with source provenance. Actual-production isolation SHALL remain future scope.

#### Scenario: Historical and live inputs

- **WHEN** DP1 ingests a historical dataset and Kafka records
- **THEN** both land in the same Bronze tables with identifiable origins, without an archive stage or source-specific deployments

## REMOVED Requirements

### Requirement: Immutable raw landing
**Reason**: The Python landing archiver is removed; DP1 reads Kafka directly and Bronze is the durable raw record.
**Migration**: See `historical-bronze-ingestion` "Kafka source ingestion".

### Requirement: Completed-file batches and retry-safe DP1
**Reason**: There are no landing files to discover; offsets committed with Bronze rows provide retry safety.
**Migration**: See `historical-bronze-ingestion` "Kafka source ingestion".

### Requirement: Contextual validation and generator diagnostics
**Reason**: History-dependent validation belongs to Silver.
**Migration**: See `curated-payments`.

### Requirement: Schema cutover without shared-data deletion
**Reason**: Landing epochs are removed; Kafka-mode DP1 starts from configured offsets and then from committed offsets.
**Migration**: Configure the first Kafka-mode run's starting offsets to exclude incompatible retained records.

### Requirement: Live transport and recovery evidence
**Reason**: The landing archiver is removed, so its restart and replay evidence no longer applies.
**Migration**: Kafka-mode replay and offset continuity are covered by `historical-bronze-ingestion` "Kafka source ingestion".
