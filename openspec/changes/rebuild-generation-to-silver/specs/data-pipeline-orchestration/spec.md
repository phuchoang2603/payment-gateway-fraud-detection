## MODIFIED Requirements

### Requirement: Native metadata workflow execution
The checks stage SHALL run native OpenMetadata discovery for the run's tables followed by the test suite derived from the contracts, from the server-matched runtime. Process success SHALL NOT imply passing data quality: the stage SHALL succeed only when every expected test for the run reports Success. Credentials and permissions SHALL remain scoped to actual operations.

#### Scenario: Native job succeeds with a failed test

- **WHEN** the native test container exits successfully but a required result is Failed, Aborted or absent
- **THEN** the checks stage fails with that result

### Requirement: Shared metadata connections
Pipeline-specific discovery and native check execution SHALL consume centrally managed connector configuration while retaining their own run context and ordering. Scheduled service discovery SHALL NOT substitute for candidate-specific pipeline checks or mutate their native quality definitions or results.

#### Scenario: Discover a newly created pipeline table
- **WHEN** ingestion creates candidate tables before scheduled discovery runs
- **THEN** the pipeline explicitly discovers them through the shared connection before executing its checks

## ADDED Requirements

### Requirement: Three-stage pipeline workflows
DP1 Bronze and DP2 Silver SHALL each run as an Argo workflow with exactly three stages: a Spark job, contract SQL checks, and OpenMetadata publication. Checks SHALL start only after the Spark stage has committed. Publication SHALL run whether the earlier stages succeed or fail, recording the run outcome. Stage results SHALL distinguish failed data checks from execution errors.

#### Scenario: Inspect a workflow

- **WHEN** a user opens a DP1 or DP2 run
- **THEN** they see its three stages, its input (dataset version, Kafka offsets or Bronze snapshots), its committed snapshots, its check outcome and its publication outcome

#### Scenario: Checks fail

- **WHEN** a contract SQL check fails
- **THEN** the workflow fails, and publication still records the failed run

### Requirement: Single-level retry and writer serialization
Each stage SHALL have one bounded Argo retry policy for execution errors; nested retries SHALL NOT be used, and failed data checks SHALL NOT be retried. Spark stage retries SHALL be safe because historical writes are idempotent and failed Kafka appends commit nothing. Each workflow SHALL hold a mutex so that only one writer per pipeline runs at a time. Cancelling a workflow SHALL remove its SparkApplications. A platform-owned native Kubernetes admission policy SHALL reject Workflow deletion in `data` unless its recorded phase is Succeeded, Failed or Error, and SHALL reject setting `spec.shutdown: Terminate` unless the existing phase is Pending or terminal. Workflows in other namespaces SHALL remain unaffected.

#### Scenario: Retry after transient driver failure

- **WHEN** Argo retries a Spark stage after an execution error
- **THEN** the retry produces the same Bronze or Silver content as a single successful attempt

#### Scenario: Delete or hard-terminate an active workflow

- **WHEN** a user requests deletion or hard termination of a running Workflow in `data`
- **THEN** Kubernetes rejects the request with guidance to use graceful stop

### Requirement: Readable workflow graph
The Bronze and Silver workflows SHALL each show the Spark, checks and publish stages with direct dependencies and readable names. The benchmark SHALL be a separate on-demand workflow template with a single Spark stage. Metadata API call details SHALL remain in stage logs.

#### Scenario: Inspect the stage graph

- **WHEN** a user opens a normal DP1 or DP2 run
- **THEN** the graph shows three stages without select, discover, profile, readback or accept nodes

### Requirement: Pipeline inputs
A DP1 submission SHALL select either a historical dataset version, by source root and dataset version parameters with chart defaults selecting v18, or Kafka mode. A DP2 submission SHALL take no input selection and read current Bronze. A scheduled workflow SHALL run DP1 in Kafka mode followed by DP2, and SHALL be disabled by default in every environment.

#### Scenario: Ingest a dataset version

- **WHEN** a DP1 historical submission names a dataset version
- **THEN** all stages use that version's manifest

#### Scenario: Default schedule

- **WHEN** the charts are rendered with default values
- **THEN** the scheduled workflow exists but is suspended

### Requirement: Catalog registration and discovery
The data-pipelines application SHALL own the OpenMetadata connector configuration, its ExternalSecrets and the service registration. A PostSync register job SHALL create the OpenMetadata services on each deploy. Every checks run SHALL ingest the ClickHouse tables before its native tests. Kafka topics SHALL be ingested by the on-demand `metadata-discovery` WorkflowTemplate, not by a schedule. Registration SHALL NOT import contracts or data products.

#### Scenario: Deploy registers services
- **WHEN** data-pipelines syncs
- **THEN** the register job creates or updates the OpenMetadata services, and no discovery CronWorkflow exists

## REMOVED Requirements

### Requirement: Observable ingest and validate stages
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Three-stage pipeline workflows".

### Requirement: Readable stage dependencies
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Readable workflow graph".

### Requirement: Versioned-export batch selection
**Reason**: Covered by explicit pipeline inputs without accepted-version reuse.
**Migration**: See "Pipeline inputs".

### Requirement: Publication despite upstream failure
**Reason**: Metadata-only replay is removed; publication runs on every outcome as the third stage.
**Migration**: See "Three-stage pipeline workflows".

### Requirement: Direct batch execution
**Reason**: The disposable smoke stage it removed no longer exists; the rule is implied by the three-stage workflow.
**Migration**: None.

### Requirement: Recurring bounded ingestion
**Reason**: Landing inputs are removed; recurring ingestion reads Kafka offsets.
**Migration**: See "Pipeline inputs" and `historical-bronze-ingestion` "Kafka source ingestion".
