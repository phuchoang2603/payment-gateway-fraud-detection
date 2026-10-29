## MODIFIED Requirements

### Requirement: Clean native catalog artifacts
Implementation SHALL remove obsolete DP1-owned catalog registrations, contracts, products, pipelines, test definitions, results, lineage and executions from the previous pipeline, including archive assets. Deletion SHALL be limited to explicitly owned DP1 assets and SHALL NOT remove the catalog service, shared backing services, unrelated assets, historical source objects or Kafka topics. Current artifacts SHALL be recreated without migration or imported results. The publication SDK path SHALL own explicit governance metadata; native workflows SHALL own discovery and quality results.

#### Scenario: Replace old DP1 registrations

- **WHEN** obsolete DP1 catalog artifacts are removed
- **THEN** only the rebuilt pipeline's assets remain for Bronze, and unrelated assets and source objects are intact

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

## ADDED Requirements

### Requirement: Product discovery
The catalog SHALL expose the historical source, Bronze and Silver products, owners, classifications and output assets with their ODPS/ODCS identities, versions and artifact references. Historical source assets SHALL identify the dataset manifest without creating a catalog entity for every file.

#### Scenario: Discover a Silver output

- **WHEN** a user opens a Silver product output
- **THEN** they can identify its owner, table schema, contract version and Bronze input products

### Requirement: Bronze and Silver lineage
The catalog SHALL expose lineage from the historical source and from the `payments.events.v1` and `fraud.labels.v1` topics through the DP1 pipeline to the corresponding Bronze tables, and from the Bronze tables through the DP2 pipeline to the Silver tables. Events and labels SHALL be independent Bronze outputs; the catalog SHALL NOT fabricate an events-to-labels ingestion edge. The run's dataset version, Kafka offsets or Bronze snapshot IDs SHALL be available from the associated pipeline execution.

#### Scenario: Trace a Silver label table

- **WHEN** a user inspects `silver.labels` lineage
- **THEN** they can trace it through DP2 to both Bronze tables and through DP1 to the label topic and the historical source

### Requirement: Native assertion evidence
The catalog SHALL retain native quality-test definitions and Success, Failed or Aborted results linked to contract rules, tables and run identity. Run reports SHALL retain the exact input, contract and snapshot references. Failed assertions and execution errors SHALL remain distinct.

#### Scenario: Contract violation

- **WHEN** a committed table fails a contract rule
- **THEN** OpenMetadata shows that failed test on the table with the run identity, and the run is reported as failed

### Requirement: Run acceptance from native results
A run SHALL succeed only when every expected native test for that run's tables reports Success. Publication SHALL write products, contracts, lineage and the pipeline execution status; a publication failure SHALL fail the publish stage without changing the recorded data or check outcome. Full catalog asset readback SHALL NOT be a run stage.

#### Scenario: Native test result missing

- **WHEN** the checks stage cannot find a Success result for an expected test of the run
- **THEN** the run fails and does not report passing checks

### Requirement: Kafka run governance identity
Kafka-mode runs SHALL publish their topic offset ranges, committed snapshots and check outcomes on the same canonical assets as historical runs. No fabricated dataset version or manifest SHALL be attributed to Kafka input.

#### Scenario: Publish a Kafka-mode run

- **WHEN** a Kafka-mode DP1 run completes
- **THEN** its pipeline execution identifies the topics, partitions, start and end offsets and the Bronze snapshots it committed

## REMOVED Requirements

### Requirement: Product and contract discovery
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Product discovery".

### Requirement: Source pipeline and table lineage
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Bronze and Silver lineage".

### Requirement: Reproducible assertion evidence
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Native assertion evidence".

### Requirement: Catalog acceptance readback
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Run acceptance from native results".

### Requirement: Recurring batch governance identity
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Kafka run governance identity".

### Requirement: Meaningful scoped native profiling
**Reason**: The rebuilt pipelines run no native profiling; the requirement has no implementation in this change.
**Migration**: None.
