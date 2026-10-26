## Purpose

Make payment data products discoverable and auditable through a persistent shared catalog with consistent asset identity, pipeline lineage and reproducible validation evidence.

## ADDED Requirements

### Requirement: Verified shared catalog deployment

OpenMetadata with persistent PostgreSQL and OpenSearch SHALL be the catalog under platform ownership. Existing services SHALL be reused only after compatibility, state isolation and readiness verification. Native discovery, profiling and SQL quality execution SHALL use the official version-matched OpenMetadata ingestion image through existing Argo workflows without requiring Kafka or Airflow. Deployment SHALL include authenticated access, persistence, resource/storage budgets and capacity for concurrent pipeline execution.

#### Scenario: Catalog is not deployed

- **WHEN** no compatible live OpenMetadata instance is verified
- **THEN** rollout provisions the catalog and required backing services, completes setup/migrations and verifies authenticated publication/readback before claiming governance readiness

#### Scenario: Catalog restarts

- **WHEN** deployed catalog services restart
- **THEN** previously published product, table, contract, lineage and validation metadata remains available from persistent state

### Requirement: Product and contract discovery

The catalog SHALL expose source and bronze products, owners, classifications and output assets with the corresponding ODPS/ODCS identities, versions and artifact references. Historical source assets SHALL identify the immutable dataset manifest without creating a catalog entity for every file.

#### Scenario: Discover a bronze output

- **WHEN** a user opens a bronze product output
- **THEN** they can identify its owner, table schema, contract version, source product dependency and selected source-manifest reference

### Requirement: Authoritative contract publication

Git-versioned ODCS contracts SHALL remain authoritative. OpenMetadata SHALL expose native contracts with the complete original ODCS YAML, identities, versions, hashes and artifact references. Native schemas and quality-rule metadata SHALL preserve the declared rules without modifying Git contracts or immutable source manifests. Native OpenMetadata quality workflows SHALL execute Git-derived StarRocks SQL as the sole bronze quality path. Catalog configuration SHALL NOT become an independent rule source or retain a custom GX result projection.

#### Scenario: Inspect a published contract

- **WHEN** a user opens a bronze table's contract in OpenMetadata
- **THEN** its identity and version match the selected Git artifact and its validation evidence traces to the corresponding contract hash

#### Scenario: Unsupported contract or validation metadata

- **WHEN** the selected catalog integration cannot represent a required contract or validation outcome
- **THEN** publication reports incomplete governance rather than silently omitting it or claiming acceptance

### Requirement: Canonical table identities

Native StarRocks discovery, profiling, quality execution and lineage SHALL address the same OpenMetadata table entities. StarRocks SHALL use the existing RustFS REST catalog and separately configured warehouse access. External-catalog selection and nested schema fidelity SHALL be verified for each native workflow connection; no custom discovery, duplicate Spark/custom registrations or mandatory internal-table copies SHALL substitute for native discovery.

#### Scenario: Execute native SQL validation

- **WHEN** a native OpenMetadata SQL workflow checks an Iceberg table discovered through StarRocks
- **THEN** its native test definitions and results appear on the discovered entity with matching run, contract and snapshot references

### Requirement: Source pipeline and table lineage

The catalog SHALL expose lineage from each historical source port through the DP1 ingest job to its bronze output. It SHALL identify the Argo workflow, validation job, cross-table label dependency and execution outcome, with run links and pinned source/snapshot references.

#### Scenario: Trace a bronze label table

- **WHEN** a user inspects its lineage and associated DP1 execution
- **THEN** they can trace the label source, corresponding ingestion job and event-table dependency used for validation, including the recorded source version and snapshots

### Requirement: Reproducible assertion evidence

The catalog SHALL retain quality-test definitions and successful, failed or errored execution evidence linked to contract rules, data assets and run identity. Durable bounded reports SHALL retain exact source, contract, feature-definition, policy, run and snapshot references and duplicate-aware conservation evidence. V16 source/live checks SHALL NOT substitute for persisted-bronze or governance acceptance. Failed assertions and execution errors SHALL remain distinct. Publication retries SHALL preserve logical rule/run identity and original outcomes.

#### Scenario: Deliberate contract violation

- **WHEN** an isolated candidate fails a contract rule
- **THEN** OpenMetadata shows that failed rule on the corresponding table with a reference to the durable report, and the candidate is not reported as accepted

#### Scenario: Metadata-only recovery

- **WHEN** publishing a stored validation result is retried
- **THEN** the same logical test and execution are addressed without creating a second table or changing the recorded data outcome

### Requirement: Catalog acceptance readback

DP1 governance acceptance SHALL verify product membership, schemas, native contracts, job/table lineage and the complete expected native quality result set by catalog readback with bounded indexing retries. Every required result SHALL be Success and match the recorded run/source/contract/snapshots. Failed, Aborted, missing, stale or incomplete results SHALL prevent acceptance even if the native workflow container exits zero. A successful emit or healthy deployment alone SHALL NOT count as complete governance acceptance.

#### Scenario: Search index is delayed

- **WHEN** metadata submission succeeds but required assets or results are not yet visible
- **THEN** bounded readback retries occur, and unresolved absence is reported as incomplete governance rather than a passing run

### Requirement: Single governance catalog

OpenMetadata SHALL be the sole governance catalog and publication target, with a single publisher path and no dual publication. Shared infrastructure, source data, Iceberg snapshots and durable reports SHALL be unaffected by catalog changes.

#### Scenario: Publish governance metadata

- **WHEN** DP1 publishes governance metadata
- **THEN** every product, contract, lineage and validation result is written to OpenMetadata only, with shared services and source/lake/evidence data unaffected

### Requirement: Private platform access and administrator identity

OpenMetadata SHALL be reachable at its HTTPS Cloudflare Tunnel hostname, authenticate the existing platform user through Dex OAuth2/OIDC and grant that identity administrator permissions. It SHALL NOT require Kubeflow's OAuth2 Proxy session or expose an unauthenticated metadata API. Existing Kubeflow and Grafana authentication SHALL remain functional.

#### Scenario: Platform administrator signs in

- **WHEN** the configured platform user completes Dex authentication for OpenMetadata
- **THEN** OpenMetadata identifies that user as an administrator without a separate local password

#### Scenario: Anonymous API request

- **WHEN** a caller requests protected metadata without authentication
- **THEN** the API rejects the request and does not disclose catalog data

### Requirement: Recurring batch governance identity

Recurring runs SHALL publish source/batch lineage, validation outcomes and exact changed/unchanged output snapshot references on canonical assets scoped to their source environment. Publication retries SHALL use persisted evidence without re-ingesting data. Missing synthetic manifest identities SHALL NOT prevent representation of real landing inputs or cause fabricated historical provenance.

#### Scenario: Publish a recurring batch

- **WHEN** recurring data checks pass and catalog readback completes
- **THEN** its accepted evidence identifies immutable landing inputs, source origin, validated snapshot set and applicable contracts without requiring separate real/simulator catalog infrastructure in the current environment

### Requirement: Clean native catalog artifacts

Implementation SHALL delete obsolete DP1-owned custom services/table registrations, products/contracts, pipelines, GX definitions/cases/results and compatibility probes including `dp1-compatibility`, then create native artifacts cleanly. It SHALL NOT introduce migration, alias or dual-publisher paths. Unrelated catalog assets, shared infrastructure, source/lake data and durable execution evidence SHALL remain unaffected. Argo HTTP templates SHALL handle only explicit product/contract/provenance/lineage operations and acceptance readback; native workflows SHALL own discovery, profiling and quality execution.

#### Scenario: Replace old DP1 registrations

- **WHEN** obsolete DP1-owned catalog artifacts are removed
- **THEN** native discovery becomes the sole table identity path, unrelated assets remain intact and old durable outcomes are not relabeled
