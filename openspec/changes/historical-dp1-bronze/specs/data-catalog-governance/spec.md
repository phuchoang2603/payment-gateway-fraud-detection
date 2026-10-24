## Purpose

Make payment data products discoverable and auditable through a persistent shared catalog with consistent asset identity, pipeline lineage and reproducible validation evidence.

## ADDED Requirements

### Requirement: Verified shared catalog deployment

OpenMetadata with persistent PostgreSQL and OpenSearch SHALL replace DataHub under platform ownership. Existing services SHALL be reused only after compatibility, state isolation and readiness verification. Governance publication SHALL operate through the existing Argo workflows without requiring Kafka or Airflow. Deployment SHALL include authenticated access, persistence, resource/storage budgets and capacity for concurrent pipeline execution.

#### Scenario: Catalog is not deployed

- **WHEN** no compatible live OpenMetadata instance is verified
- **THEN** rollout provisions the catalog and required backing services, completes setup/migrations and verifies authenticated publication/readback before claiming governance readiness

#### Scenario: Catalog restarts

- **WHEN** deployed catalog services restart
- **THEN** previously published product, table, contract, lineage and validation metadata remains available from persistent state

### Requirement: Product and contract discovery

The catalog SHALL expose source and bronze products, owners, classifications and output assets with the corresponding ODPS/ODCS identities, versions and artifact references. Source assets SHALL identify the immutable dataset manifest without creating a catalog entity for every file.

#### Scenario: Discover a bronze output

- **WHEN** a user opens a bronze product output
- **THEN** they can identify its owner, table schema, contract version, source product dependency and selected source-manifest reference

### Requirement: Authoritative contract publication

Git-versioned ODCS contracts SHALL remain authoritative. OpenMetadata SHALL expose imported contracts with their original identities, versions, hashes and artifact references. Catalog publication SHALL NOT create an independent validation rule source or rerun quality checks through a second engine.

#### Scenario: Inspect a published contract

- **WHEN** a user opens a bronze table's contract in OpenMetadata
- **THEN** its identity and version match the selected Git artifact and its validation evidence traces to the corresponding contract hash

#### Scenario: Unsupported contract or validation metadata

- **WHEN** the selected catalog integration cannot represent a required contract or validation outcome
- **THEN** publication reports incomplete governance rather than silently omitting it or claiming acceptance

### Requirement: Canonical table identities

Iceberg metadata ingestion, lineage and validation publication SHALL address the same catalog dataset identities. Metadata ingestion SHALL use the existing REST catalog and separately configured warehouse access. Publication SHALL NOT create duplicate assets under execution-engine names.

#### Scenario: Publish Spark validation results

- **WHEN** GX checks an Iceberg table already discovered by catalog ingestion
- **THEN** its test definitions and results appear on that existing OpenMetadata table entity under the expected service/database/schema identity

### Requirement: Source pipeline and table lineage

The catalog SHALL expose lineage from each historical source port through the DP1 ingest job to its bronze output. It SHALL identify the Argo workflow, validation job, cross-table label dependency and execution outcome, with run links and pinned source/snapshot references.

#### Scenario: Trace a bronze label table

- **WHEN** a user inspects its lineage and associated DP1 execution
- **THEN** they can trace the label source, corresponding ingestion job and event-table dependency used for validation, including the recorded source version and snapshots

### Requirement: Reproducible assertion evidence

The catalog SHALL retain quality-test definitions and successful, failed or errored execution evidence linked to contract rules, data assets and run identity. Local reports SHALL retain exact source, contract and snapshot references. Publication retries SHALL preserve logical rule/run identity and original outcomes.

#### Scenario: Deliberate contract violation

- **WHEN** an isolated candidate fails a contract rule
- **THEN** OpenMetadata shows that failed rule on the corresponding table with a reference to the durable report, and the candidate is not reported as accepted

#### Scenario: Metadata-only recovery

- **WHEN** publishing a stored validation result is retried
- **THEN** the same logical test and execution are addressed without creating a second table or changing the recorded data outcome

### Requirement: Catalog acceptance readback

DP1 governance acceptance SHALL verify product membership, schemas, imported contracts, job/table lineage and validation results by catalog readback with bounded indexing retries. A successful emit or healthy deployment alone SHALL NOT count as complete governance acceptance.

#### Scenario: Search index is delayed

- **WHEN** metadata submission succeeds but required assets or results are not yet visible
- **THEN** bounded readback retries occur, and unresolved absence is reported as incomplete governance rather than a passing run

### Requirement: Complete DataHub retirement

After fresh installation, OpenMetadata SHALL be the sole governance catalog and publication target. DataHub workloads, dedicated credentials and application configuration SHALL be retired while preserving shared infrastructure, source data, Iceberg snapshots and durable reports. Existing DataHub verification SHALL NOT count as OpenMetadata acceptance.

#### Scenario: Retire the previous catalog

- **WHEN** the platform replaces DataHub with a fresh OpenMetadata installation
- **THEN** DataHub and only its dedicated dependencies are removed from active deployment, with shared services and source/lake/evidence data unaffected

### Requirement: Private platform access and administrator identity

OpenMetadata SHALL be reachable at its HTTPS Cloudflare Tunnel hostname, authenticate the existing platform user through Dex OAuth2/OIDC and grant that identity administrator permissions. It SHALL NOT require Kubeflow's OAuth2 Proxy session or expose an unauthenticated metadata API. Existing Kubeflow and Grafana authentication SHALL remain functional.

#### Scenario: Platform administrator signs in

- **WHEN** the configured platform user completes Dex authentication for OpenMetadata
- **THEN** OpenMetadata identifies that user as an administrator without a separate local password

#### Scenario: Anonymous API request

- **WHEN** a caller requests protected metadata without authentication
- **THEN** the API rejects the request and does not disclose catalog data
