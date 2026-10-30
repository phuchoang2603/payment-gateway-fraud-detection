# data-pipeline-orchestration Specification

## Purpose
Coordinate finite data-pipeline stages through the shared cluster execution services with observable dependencies, bounded failures and independently recoverable evidence publication.

## Requirements

### Requirement: Observable ingest and validate stages
DP1 SHALL expose distinct ingest and validate stages with input, output and run references. Validation SHALL begin only after the candidate snapshot set is recorded, including both outputs for historical exports and explicit unchanged-port references for recurring batches. Stage results SHALL distinguish failed data checks, execution errors and governance publication errors.

#### Scenario: Inspect a historical workflow

- **WHEN** a user inspects a DP1 run
- **THEN** they can identify its source manifest, ingest result, validated snapshots, validation outcome and governance outcome

### Requirement: Versioned-export batch selection
Each historical-mode workflow submission SHALL select one completed, immutable source export through `source-root` and `dataset-version` parameters, with chart-configured defaults selecting a freshly generated and validated v17 source. Scale generation SHALL NOT be a prerequisite for development ingestion; later full-volume acceptance SHALL remain separate. Every stage SHALL use the selected parameters consistently. Subsequent historical batches SHALL select newly completed export versions. Recurring landing mode SHALL select completed input units through durable input tracking; a schedule alone SHALL NOT constitute discovery or retry safety.

#### Scenario: Process the next completed export

- **WHEN** a submission overrides the source root and dataset version for a newly completed export
- **THEN** all stages use that export's manifest and embedded inventory, and writes remain isolated from other source versions

#### Scenario: Resubmit an accepted export

- **WHEN** the same completed export is submitted after its source version is accepted
- **THEN** the run reuses its recorded snapshots without adding deliveries or treating the submission as new source data

### Requirement: Operator-managed Spark stages
Argo SHALL create and watch an Apache `spark.apache.org/v1` SparkApplication for each Spark stage attempt, owned by the workflow so that it is removed with it. Native discovery and ClickHouse SQL checks SHALL run as one finite container stage using the server-matched official ingestion runtime. Publication SHALL use the thin metadata image. The platform-owned Apache Spark Operator SHALL manage driver and executor lifecycle with operator restarts disabled. Stage success SHALL require a successful terminal application status; failed applications and deadlines SHALL fail the stage with diagnostics.

#### Scenario: Stage fails or times out

- **WHEN** a SparkApplication fails or its stage exceeds its deadline
- **THEN** its Argo stage fails with application diagnostics and its driver and executors are removed

### Requirement: Publication despite upstream failure
The workflow SHALL persist validation evidence before signaling failed quality checks and attempt governance publication on success and failure paths. Missing reports SHALL be recorded as execution errors. Publication failure SHALL preserve the original data outcome and support metadata-only replay.

#### Scenario: A quality check fails

- **WHEN** native SQL validation computes a failed assertion
- **THEN** the workflow retains the native failed result and durable report before signaling failure; governance readback preserves that failure rather than projecting a custom result

#### Scenario: OpenMetadata recovers after an outage

- **WHEN** a persisted run is replayed for metadata publication only
- **THEN** publication uses the original reports, snapshots and run identity without starting ingest or validation again

### Requirement: Scoped workload configuration
Pipeline stages SHALL use centrally supplied endpoints, explicit resource budgets and separate scoped workflow, data and metadata identities. Secrets SHALL be supplied externally and excluded from manifests, reports and logs. Consumer resources SHALL NOT install shared controllers.

#### Scenario: Missing workload prerequisite

- **WHEN** a required permission, credential or service endpoint is unavailable
- **THEN** preflight identifies the missing prerequisite and the run does not claim successful ingestion or platform readiness

### Requirement: Direct batch execution
The workflow SHALL proceed from input/run-context preflight to ingestion without a disposable-table compatibility smoke stage. The CLI SHALL NOT expose a rollout-only smoke command. Source integrity, persisted-data validation, governance acceptance and lifecycle cleanup SHALL remain enforced.

#### Scenario: Submit a historical batch

- **WHEN** manifest/run-context preflight succeeds
- **THEN** ingestion starts without creating a disposable compatibility table

### Requirement: Recurring bounded ingestion
DP1 SHALL support Argo-scheduled finite runs over newly completed immutable landing inputs, reusing its ingest, validate, publish and accept stages. Input progress SHALL identify files or completed batches independently of event dates. Historical exports SHALL remain an explicit input mode. Overlapping runs SHALL preserve writer serialization and cleanup guarantees; a run with no new input SHALL be a no-op without rewriting accepted data.

#### Scenario: Late file in an older date partition

- **WHEN** a new completed landing file contains events older than the latest processed event date
- **THEN** the next eligible run selects it once as input without rescanning accepted files as new deliveries

#### Scenario: Retry or metadata outage

- **WHEN** a recurring run is retried after an uncertain commit or governance failure
- **THEN** durable batch identity and recorded snapshots support recovery without adding ingestion-generated copies, and metadata-only replay does not start Spark

### Requirement: Native metadata workflow execution
Argo SHALL schedule native OpenMetadata metadata/profiler/test-suite workflows from the server-matched runtime. Explicit governance synchronization and readback SHALL use one idempotent SDK path in the thin metadata image rather than one graph node per API operation. Credentials and permissions SHALL remain scoped to actual operations. Native ingestion process success SHALL NOT imply passing data quality. Result acceptance SHALL require the complete expected successful assertion set correlated with immutable source, contract, policy and snapshot references. Deterministic quality failures SHALL NOT be retried for a passing outcome.

#### Scenario: Native job succeeds with a failed test

- **WHEN** a native quality job exits successfully but a required result is Failed, Aborted, stale or absent
- **THEN** DP1 refuses acceptance and persists the actual quality or execution outcome

Recurring execution SHALL use immutable pending selections and accepted progress, a daily UTC CronWorkflow, shared writer serialization and bounded selection resources. No-input runs SHALL skip Spark and native checks; daily ingestion SHALL skip profiling explicitly, with bounded profiling available through a separate on-demand Argo entrypoint.

### Requirement: Readable stage dependencies
The Argo graph SHALL expose select, ingest, discover, validate, profile and publish/readback/accept stages with direct dependencies and readable names. Validate and profile SHALL be eligible to run concurrently after discovery with isolated per-run configuration and bounded resources. Acceptance SHALL join their declared outcomes. API call details SHALL remain in stage logs, not expand into a request-per-node graph. Metadata replay and no-input paths SHALL retain their existing short-circuit behavior, writer serialization and exit cleanup.

#### Scenario: Inspect the stage graph

- **WHEN** a user opens a normal DP1 run
- **THEN** the graph shows the stage dependencies and parallel validate/profile for historical runs (recurring runs skip profiling) branch without expanding individual metadata API requests

#### Scenario: Profile and validation overlap

- **WHEN** native validation and profiling execute concurrently
- **THEN** each uses isolated configuration and the recorded candidate context, and acceptance cannot hide a required stage failure or change the snapshot selection

### Requirement: Shared metadata connections
Pipeline-specific discovery, profiling and snapshot validation SHALL consume centrally managed connector configuration while retaining their own run context, ordering and acceptance readback. Scheduled service discovery SHALL NOT substitute for candidate-specific pipeline acceptance or mutate its native quality definitions/results.

#### Scenario: Discover a newly created pipeline table
- **WHEN** ingestion creates candidate tables before scheduled discovery runs
- **THEN** the pipeline explicitly discovers them through the shared connection before executing its snapshot-specific checks
