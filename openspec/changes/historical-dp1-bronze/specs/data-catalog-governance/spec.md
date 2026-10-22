## Purpose

Make payment data products discoverable and auditable through a persistent shared catalog with consistent asset identity, pipeline lineage and reproducible validation evidence.

## ADDED Requirements

### Requirement: Verified shared catalog deployment

DataHub and its required persistent backing services SHALL be provisioned under platform ownership or reused only after compatibility and readiness verification. Deployment SHALL include authenticated access, persistence, resource/storage budgets and capacity for concurrent pipeline execution.

#### Scenario: Catalog is not deployed

- **WHEN** no compatible live DataHub instance is verified
- **THEN** rollout provisions the catalog and required backing services, completes setup/migrations and verifies authenticated publication/readback before claiming governance readiness

#### Scenario: Catalog restarts

- **WHEN** deployed catalog services restart
- **THEN** previously published product, table, lineage and assertion metadata remains available from persistent state

### Requirement: Product and contract discovery

The catalog SHALL expose source and bronze products, owners, classifications and output assets with the corresponding ODPS/ODCS identities, versions and artifact references. Source assets SHALL identify the immutable dataset manifest without creating a catalog entity for every file.

#### Scenario: Discover a bronze output

- **WHEN** a user opens a bronze product output
- **THEN** they can identify its owner, table schema, contract version, source product dependency and selected source-manifest reference

### Requirement: Canonical table identities

Iceberg metadata ingestion, lineage and validation publication SHALL address the same catalog dataset identities. Metadata ingestion SHALL use the existing REST catalog and separately configured warehouse access. Publication SHALL NOT create duplicate assets under execution-engine names.

#### Scenario: Publish Spark validation results

- **WHEN** GX checks an Iceberg table already discovered by catalog ingestion
- **THEN** its assertions appear on that existing table entity with the expected environment and platform instance

### Requirement: Source pipeline and table lineage

The catalog SHALL expose lineage from each historical source port through the DP1 ingest job to its bronze output. It SHALL identify the Argo workflow, validation job, cross-table label dependency and execution outcome, with run links and pinned source/snapshot references.

#### Scenario: Trace a bronze label table

- **WHEN** a user inspects its lineage and associated DP1 execution
- **THEN** they can trace the label source, corresponding ingestion job and event-table dependency used for validation, including the recorded source version and snapshots

### Requirement: Reproducible assertion evidence

The catalog SHALL retain assertion definitions and successful, failed or errored execution evidence linked to contract rules, data assets and run identity. Local reports SHALL retain exact source, contract and snapshot references. Publication retries SHALL preserve logical rule/run identity and original outcomes.

#### Scenario: Deliberate contract violation

- **WHEN** an isolated candidate fails a contract rule
- **THEN** DataHub shows that failed rule on the corresponding table with a reference to the durable report, and the candidate is not reported as accepted

#### Scenario: Metadata-only recovery

- **WHEN** publishing a stored validation result is retried
- **THEN** the same logical assertion and execution are addressed without creating a second table or changing the recorded data outcome

### Requirement: Catalog acceptance readback

DP1 governance acceptance SHALL verify product membership, schemas, job/table lineage and validation results by catalog readback with bounded indexing retries. A successful emit or healthy deployment alone SHALL NOT count as complete governance acceptance.

#### Scenario: Search index is delayed

- **WHEN** metadata submission succeeds but required assets or results are not yet visible
- **THEN** bounded readback retries occur, and unresolved absence is reported as incomplete governance rather than a passing run
