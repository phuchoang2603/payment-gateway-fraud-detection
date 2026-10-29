## MODIFIED Requirements

### Requirement: Operator-managed Spark stages
Argo SHALL create and watch an Apache `spark.apache.org/v1` SparkApplication for each Spark stage attempt, owned by the workflow so that it is removed with it. Native discovery and StarRocks SQL checks SHALL run as one finite container stage using the server-matched official ingestion runtime. Publication SHALL use the thin metadata image. The platform-owned Apache Spark Operator SHALL manage driver and executor lifecycle with operator restarts disabled. Stage success SHALL require a successful terminal application status; failed applications and deadlines SHALL fail the stage with diagnostics.

#### Scenario: Stage fails or times out

- **WHEN** a SparkApplication fails or its stage exceeds its deadline
- **THEN** its Argo stage fails with application diagnostics and its driver and executors are removed

### Requirement: Native metadata workflow execution
The checks stage SHALL run native OpenMetadata discovery for the run's tables followed by the test suite derived from the contracts, from the server-matched runtime. Process success SHALL NOT imply passing data quality: the stage SHALL succeed only when every expected test for the run reports Success. Credentials and permissions SHALL remain scoped to actual operations.

#### Scenario: Native job succeeds with a failed test

- **WHEN** the native test container exits successfully but a required result is Failed, Aborted or absent
- **THEN** the checks stage fails with that result

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
The Bronze and Silver workflows SHALL each show the Spark, checks and publish stages with direct dependencies and readable names. Diagnostics and benchmark SHALL be separate on-demand workflow templates with a single Spark stage. Metadata API call details SHALL remain in stage logs.

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

## REMOVED Requirements

### Requirement: Observable ingest and validate stages
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Three-stage pipeline workflows".

### Requirement: Bounded retry and cancellation
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Single-level retry and writer serialization".

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
