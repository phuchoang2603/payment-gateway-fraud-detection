## Purpose

Coordinate finite data-pipeline stages through the shared cluster execution services with observable dependencies, bounded failures and independently recoverable evidence publication.

## ADDED Requirements

### Requirement: Observable ingest and validate stages

DP1 SHALL expose distinct ingest and validate stages with input, output and run references. Validation SHALL begin only after both output snapshots are recorded. Stage results SHALL distinguish failed data checks, execution errors and governance publication errors.

#### Scenario: Inspect a historical workflow

- **WHEN** a user inspects a DP1 run
- **THEN** they can identify its source manifest, ingest result, validated snapshots, validation outcome and governance outcome

### Requirement: Shared Spark application orchestration

Argo SHALL create and observe SparkApplication resources through reusable resource templates using the shared Spark Operator. It SHALL wait for application completion, recognize terminal execution/submission failures and enforce a deadline when status does not resolve.

#### Scenario: Submission is rejected

- **WHEN** a SparkApplication reaches terminal submission failure
- **THEN** its Argo stage fails with diagnostics rather than succeeding on resource creation

#### Scenario: Application stops reporting progress

- **WHEN** the application remains unknown, pending or without usable status beyond its deadline
- **THEN** Argo fails the stage and initiates cleanup without waiting indefinitely

### Requirement: Bounded retry and cancellation

Argo SHALL own bounded stage retries with stable logical run identity and distinct execution attempts. Deterministic quality failures SHALL NOT be retried for acceptance. Cancellation and deadlines SHALL stop active Spark applications and release writer serialization without deleting durable evidence.

#### Scenario: Retry after transient driver failure

- **WHEN** Argo retries an ingest stage after a transient execution error
- **THEN** the attempt reuses durable ingestion context and safe recovery semantics rather than appending another delivery set

#### Scenario: Cancel an active run

- **WHEN** a workflow is cancelled while Spark is running
- **THEN** active applications are terminated, writer serialization is released and persisted reports/run context remain available

### Requirement: Publication despite upstream failure

The workflow SHALL persist validation evidence before signaling failed quality checks and attempt governance publication on success and failure paths. Missing reports SHALL be recorded as execution errors. Publication failure SHALL preserve the original data outcome and support metadata-only replay.

#### Scenario: A quality check fails

- **WHEN** validation computes a failed expectation
- **THEN** the workflow remains failed, retains its report and attempts to publish the failed assertion to DataHub

#### Scenario: DataHub recovers after an outage

- **WHEN** a persisted run is replayed for metadata publication only
- **THEN** publication uses the original reports, snapshots and run identity without starting ingest or validation again

### Requirement: Scoped workload configuration

Pipeline stages SHALL use centrally supplied endpoints, explicit resource budgets and separate scoped workflow, data and metadata identities. Secrets SHALL be supplied externally and excluded from manifests, reports and logs. Consumer resources SHALL NOT install shared controllers.

#### Scenario: Missing workload prerequisite

- **WHEN** a watched namespace, required permission, credential or service endpoint is unavailable
- **THEN** preflight identifies the missing prerequisite and the run does not claim successful ingestion or platform readiness

### Requirement: Pinned legacy inventory configuration

For any accepted source version without an embedded inventory, stage configuration SHALL supply the companion URI and SHA-256 pin together. Preflight SHALL verify the companion's integrity and source bindings before ingestion; workflows SHALL NOT generate or silently discover replacement inventory evidence.

#### Scenario: Incomplete companion configuration

- **WHEN** a v7 workflow omits either the companion URI or its digest pin
- **THEN** preflight fails before ingestion and identifies the missing evidence configuration
