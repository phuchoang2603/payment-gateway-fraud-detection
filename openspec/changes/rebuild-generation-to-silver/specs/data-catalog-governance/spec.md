## MODIFIED Requirements

### Requirement: Clean native catalog artifacts
Implementation SHALL remove obsolete DP1-owned catalog registrations, contracts, products, pipelines, test definitions, results, lineage and executions from the previous pipeline, including archive assets. Deletion SHALL be limited to explicitly owned DP1 assets and SHALL NOT remove the catalog service, shared backing services, unrelated assets, historical source objects or Kafka topics. Current artifacts SHALL be recreated without migration or imported results. The publication SDK path SHALL own explicit governance metadata; native workflows SHALL own discovery and quality results.

#### Scenario: Replace old DP1 registrations

- **WHEN** obsolete DP1 catalog artifacts are removed
- **THEN** only the rebuilt pipeline's assets remain for Bronze, and unrelated assets and source objects are intact

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

### Requirement: On-demand native profiling
Native profiling SHALL run only through an on-demand workflow and SHALL expose supported scalar-column statistics with explicit columns, sample bounds and collection time. Unsupported nested or binary metrics SHALL be excluded explicitly. Profiles SHALL NOT be reported as quality acceptance.

#### Scenario: Request a profile

- **WHEN** an operator runs the profiling workflow for a Bronze or Silver table
- **THEN** scalar-column statistics and their sampling scope appear on the canonical asset without affecting pipeline runs

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
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "On-demand native profiling".
