## Purpose

Coordinate finite data-pipeline stages through the shared cluster execution services with observable dependencies, bounded failures and independently recoverable evidence publication.

## ADDED Requirements

### Requirement: Observable ingest and validate stages

DP1 SHALL expose distinct ingest and validate stages with input, output and run references. Validation SHALL begin only after the candidate snapshot set is recorded, including both outputs for historical exports and explicit unchanged-port references for recurring batches. Stage results SHALL distinguish failed data checks, execution errors and governance publication errors.

#### Scenario: Inspect a historical workflow

- **WHEN** a user inspects a DP1 run
- **THEN** they can identify its source manifest, ingest result, validated snapshots, validation outcome and governance outcome

### Requirement: Versioned-export batch selection

Each historical-mode workflow submission SHALL select one completed, immutable source export through `source-root` and `dataset-version` parameters, with chart-configured defaults. Every stage SHALL use the selected parameters consistently. Subsequent historical batches SHALL select newly completed export versions. Recurring landing mode SHALL select completed input units through durable input tracking; a schedule alone SHALL NOT constitute discovery or retry safety.

#### Scenario: Process the next completed export

- **WHEN** a submission overrides the source root and dataset version for a newly completed export
- **THEN** all stages use that export's manifest and embedded inventory, and writes remain isolated from other source versions

#### Scenario: Resubmit an accepted export

- **WHEN** the same completed export is submitted after its source version is accepted
- **THEN** the run reuses its recorded snapshots without adding deliveries or treating the submission as new source data

### Requirement: Operator-managed Spark stages

Argo SHALL create and watch an Apache `spark.apache.org/v1` SparkApplication for each ingestion attempt. Native discovery, profiling and StarRocks SQL validation SHALL run as finite official OpenMetadata ingestion-container steps, not SparkApplications. No second Spark validation application or dedicated custom metadata image SHALL remain. The platform-owned Apache Spark Operator SHALL manage driver and executor lifecycle. Stage success SHALL require a successful terminal application status; failed applications and deadlines SHALL fail the stage with diagnostics. Operator restarts SHALL be disabled so Argo remains the bounded retry authority.

#### Scenario: Stage fails or times out

- **WHEN** a SparkApplication fails or its stage exceeds its deadline
- **THEN** its Argo stage fails with application diagnostics and its driver and executors are removed

### Requirement: Bounded retry and cancellation

Argo SHALL own bounded stage retries with stable logical run identity and distinct execution attempts. Deterministic quality failures SHALL NOT be retried for acceptance. Graceful cancellation and deadlines SHALL delete active SparkApplications, verify driver/executor cleanup, and only then release writer serialization without deleting durable evidence. Read-only StarRocks validation queries SHALL be bounded by a per-user query timeout; unconfirmed Spark cleanup SHALL block writer handoff. A platform-owned native Kubernetes admission policy SHALL reject Workflow deletion in `fraud-data-prod` unless its recorded phase is Succeeded, Failed or Error. It SHALL reject setting `spec.shutdown: Terminate` unless the existing phase is Pending or terminal. Operators SHALL stop active Workflows gracefully and verify completed cleanup before deletion. Missing phase SHALL NOT permit deletion or hard termination. Workflows in other namespaces SHALL remain unaffected. Terminal status alone SHALL NOT be considered proof of successful Spark cleanup.

#### Scenario: Retry after transient driver failure

- **WHEN** Argo retries an ingest stage after a transient execution error
- **THEN** the attempt reuses durable ingestion context and safe recovery semantics rather than appending another delivery set

#### Scenario: Cancel an active run

- **WHEN** a workflow is cancelled while Spark is running
- **THEN** active applications are terminated, cleanup is confirmed before writer serialization is released and persisted reports/run context remain available

#### Scenario: Delete or hard-terminate an active workflow

- **WHEN** a user requests deletion or hard termination of a running Workflow in `fraud-data-prod`
- **THEN** Kubernetes rejects the request with guidance to use graceful stop and wait for completion
- **AND** graceful stop remains permitted, completed Workflows can be deleted, and other workflow namespaces are unaffected

#### Scenario: Cleanup fails

- **WHEN** exit cleanup fails and Spark resources remain
- **THEN** the next workflow removes previous workflow pods, SparkApplications and Spark pods under the writer mutex before ingestion
- **AND** a failed recovery prevents ingestion, and a Spark retry clears its prior stage resources before creating another application

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

Argo SHALL schedule version-matched official OpenMetadata metadata/profiler/test-suite workflows and use scoped HTTP templates for remaining explicit governance API operations and readback. Agent/WorkflowTaskSet permissions and secrets SHALL be scoped to those operations. Native ingestion process success SHALL NOT imply passing data quality. Result acceptance SHALL require the complete expected successful assertion set correlated with immutable source, contract, policy and snapshot references. Deterministic quality failures SHALL NOT be retried for a passing outcome.

#### Scenario: Native job succeeds with a failed test

- **WHEN** a native quality job exits successfully but a required result is Failed, Aborted, stale or absent
- **THEN** DP1 refuses acceptance and persists the actual quality or execution outcome
