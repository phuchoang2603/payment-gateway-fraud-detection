# data-catalog-governance Specification

## Purpose
Make payment data products discoverable and auditable through a persistent shared catalog with consistent asset identity, pipeline lineage and reproducible validation evidence.

## Requirements

### Requirement: Verified shared catalog deployment
OpenMetadata with persistent PostgreSQL and OpenSearch SHALL be the catalog under platform ownership. Existing services SHALL be reused only after compatibility, state isolation and readiness verification. Native discovery, profiling and SQL quality execution SHALL use the server-version-matched metadata runtime based on the official OpenMetadata ingestion image through existing Argo workflows without requiring Kafka or Airflow. Deployment SHALL include authenticated access, persistence, resource/storage budgets and capacity for concurrent pipeline execution.

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
Git-versioned ODCS contracts SHALL remain authoritative. OpenMetadata SHALL expose native contracts with the complete original ODCS YAML, identities, versions, hashes and artifact references. Native contracts SHALL populate inspectable normalized nested schema, active raw quality-test references, declared owners and supported declared semantics/security/SLA fields, without inventing absent guarantees. Native contract source URLs SHALL use stable canonical repository paths. Contract provenance SHALL retain the full Git revision, immutable artifact URL and content hash separately; execution parameters SHALL retain the publication Git revision. An unrelated deployment revision SHALL NOT change the canonical contract link or require a shared server fork. Original YAML alone SHALL NOT satisfy native mapping. Native schemas and quality-rule metadata SHALL preserve the declared rules without modifying Git contracts or immutable source manifests. Native OpenMetadata quality workflows SHALL execute Git-derived ClickHouse SQL as the sole bronze quality path. Catalog configuration SHALL NOT become an independent rule source or retain a custom GX result projection.

#### Scenario: Inspect a published contract

- **WHEN** a user opens a bronze table's contract in OpenMetadata
- **THEN** its identity/version/hash match Git, its nested schema and active native quality references are inspectable, its declared owners resolve and its validation evidence traces to the selected contract

#### Scenario: Unsupported contract or validation metadata

- **WHEN** the selected catalog integration cannot represent a required contract or validation outcome
- **THEN** publication reports incomplete governance rather than silently omitting it or claiming acceptance

### Requirement: Canonical table identities
Native ClickHouse discovery, profiling, quality execution and lineage SHALL address the same OpenMetadata table entities. ClickHouse SHALL use the existing RustFS REST catalog and separately configured warehouse access. External-catalog selection and nested schema fidelity SHALL be verified for each native workflow connection; no custom discovery, duplicate Spark/custom registrations or mandatory internal-table copies SHALL substitute for native discovery.

#### Scenario: Execute native SQL validation

- **WHEN** a native OpenMetadata SQL workflow checks an Iceberg table discovered through ClickHouse
- **THEN** its native test definitions and results appear on the discovered entity with matching run, contract and snapshot references

### Requirement: Source pipeline and table lineage
The catalog SHALL expose each actual historical or clean-landing source port through the DP1 pipeline to its corresponding bronze output using supported source assets and canonical table identities. Events and labels SHALL be independent ingestion outputs; the catalog SHALL NOT fabricate an events-to-labels ingestion edge. Declared label/event dependencies SHALL remain contract semantics for silver, without implying raw business validation. Exact manifests/batches and snapshot references SHALL be available from the associated execution.

#### Scenario: Trace a bronze label table

- **WHEN** a user inspects its lineage and associated DP1 execution
- **THEN** they can trace the actual label source and ingestion pipeline to the label output, including its source/batch and snapshot identity, without treating the event table as the label ingestion source

### Requirement: Reproducible assertion evidence
The catalog SHALL retain quality-test definitions and successful, failed or errored execution evidence linked to contract rules, data assets and run identity. Durable bounded reports SHALL retain exact source, contract, feature-definition, policy, run and snapshot references and duplicate-aware conservation evidence. V16 source/live checks SHALL NOT substitute for persisted-bronze or governance acceptance. Failed assertions and execution errors SHALL remain distinct. Publication retries SHALL preserve logical rule/run identity and original outcomes.

#### Scenario: Deliberate contract violation

- **WHEN** an isolated candidate fails a contract rule
- **THEN** OpenMetadata shows that failed rule on the corresponding table with a reference to the durable report, and the candidate is not reported as accepted

#### Scenario: Metadata-only recovery

- **WHEN** publishing a stored validation result is retried
- **THEN** the same logical test and execution are addressed without creating a second table or changing the recorded data outcome

### Requirement: Catalog acceptance readback
DP1 governance acceptance SHALL verify product asset lists and entity-side membership, declared owners, nested schemas, native contract fields and active quality references, actual source/output lineage, current pipeline tasks and execution inputs/outputs/status, declared profile scope and the complete expected native quality result set by catalog readback with bounded indexing retries. A stale execution, empty required task/I/O mapping or hash-only contract SHALL NOT satisfy acceptance. Every required result SHALL be Success and match the recorded run/source/contract/snapshots. Failed, Aborted, missing, stale or incomplete results SHALL prevent acceptance even if the native workflow container exits zero. A successful emit or healthy deployment alone SHALL NOT count as complete governance acceptance.

#### Scenario: Search index is delayed

- **WHEN** metadata submission succeeds but required assets or results are not yet visible
- **THEN** bounded readback retries occur, and unresolved absence is reported as incomplete governance rather than a passing run

### Requirement: Single governance catalog
OpenMetadata SHALL be the sole governance catalog and publication target, with a single publisher path and no dual publication. Ordinary catalog publication SHALL NOT mutate source data or lake snapshots. A separately scoped fresh-start reset SHALL follow the ownership and quiescence requirements below.

#### Scenario: Publish governance metadata

- **WHEN** DP1 publishes governance metadata
- **THEN** every product, contract, lineage and validation result is written to OpenMetadata only, with shared services and immutable sources unaffected

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
Implementation SHALL remove obsolete DP1-owned catalog registrations, contracts/products/pipelines, old test definitions/cases/results, incorrect lineage and stale executions, including compatibility artifacts. A fresh-start reset SHALL inventory and delete only explicitly owned DP1 bronze data, run/acceptance/progress state and superseded repository receipts after suspending schedules and verifying Spark/query quiescence. It SHALL NOT remove the RustFS catalog service, shared backing services, unrelated metadata/lake assets, simulator historical/streaming sources or producer offsets. Unresolved ownership SHALL prevent deletion of the affected resource. Current artifacts SHALL be recreated without migration, aliases, imported acceptance or dual publication. The single SDK synchronization path SHALL own explicit governance metadata; native workflows SHALL own discovery, profiling and quality results.

#### Scenario: Replace old DP1 registrations

- **WHEN** obsolete DP1-owned catalog artifacts are removed
- **THEN** native discovery becomes the sole table identity path, unrelated assets and immutable sources remain intact and fresh acceptance cannot reuse deleted or superseded DP1 records

### Requirement: Current pipeline execution metadata
The catalog SHALL expose pipeline tasks/dependencies, owners, product membership, source links and current execution status. Execution and supported task inputs/outputs SHALL identify actual source assets and canonical outputs; linked immutable receipts SHALL identify selected manifests/batches, counts, run/contract identities and independent snapshots. A new run SHALL NOT display an unrelated old execution as its own. Metadata-only recovery SHALL preserve original native outcomes and execution identity.

#### Scenario: Inspect the current ingestion execution

- **WHEN** a user opens the pipeline after a fresh accepted run
- **THEN** its tasks, status, Argo links, actual inputs and independent outputs match that run rather than a historical execution

### Requirement: Meaningful scoped native profiling
Native profiling SHALL expose supported useful scalar-column statistics beyond table row/column counts, with explicit eligible columns, sample/row bounds, execution policy, timeout, collection time and sampled/full coverage. V16 development acceptance SHALL exercise meaningful column profiling. Daily recurring ingestion SHALL skip profiling explicitly; a separate on-demand Argo entrypoint SHALL collect bounded profiles when requested. Unsupported nested/binary metrics SHALL be excluded explicitly. Latest-state profiles SHALL NOT be reported as snapshot-specific acceptance or business/distribution validity.

#### Scenario: Inspect a development profile

- **WHEN** the v16 native profiling stage completes
- **THEN** supported scalar-column statistics and their collection/sampling scope are visible on the canonical assets and linked to the recorded run without inventing snapshot coverage
