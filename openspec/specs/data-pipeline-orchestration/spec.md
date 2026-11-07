# data-pipeline-orchestration Specification

## Purpose
Coordinate finite data-pipeline stages through the shared cluster execution services with observable dependencies, bounded failures and independently recoverable evidence publication.

## Requirements

### Requirement: Operator-managed Spark stages
Argo SHALL create and watch an Apache `spark.apache.org/v1` SparkApplication for each Spark stage attempt, owned by the workflow so that it is removed with it. Native discovery and ClickHouse SQL checks SHALL run as one finite container stage using the server-matched official ingestion runtime. Publication SHALL use the thin metadata image. The platform-owned Apache Spark Operator SHALL manage driver and executor lifecycle with operator restarts disabled. Stage success SHALL require a successful terminal application status; failed applications and deadlines SHALL fail the stage with diagnostics.

#### Scenario: Stage fails or times out

- **WHEN** a SparkApplication fails or its stage exceeds its deadline
- **THEN** its Argo stage fails with application diagnostics and its driver and executors are removed

### Requirement: Scoped workload configuration
Pipeline stages SHALL use centrally supplied endpoints, explicit resource budgets and separate scoped workflow, data and metadata identities. Secrets SHALL be supplied externally and excluded from manifests, reports and logs. Consumer resources SHALL NOT install shared controllers.

#### Scenario: Missing workload prerequisite

- **WHEN** a required permission, credential or service endpoint is unavailable
- **THEN** preflight identifies the missing prerequisite and the run does not claim successful ingestion or platform readiness

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

### Requirement: Write-audit-publish pipeline workflows
DP1 Bronze (`dp1-bronze`) and the Silver, Gold and marts pipeline (`dp-pipeline`) SHALL write through write-audit-publish. Spark writes to an `audit-<workflow>` Iceberg branch, the contract SQL checks run against that branch's heads, and a publish stage fast-forwards each table's main branch only after every check of its layer succeeds. The marts use `__staging` tables in the same role. OpenMetadata publication SHALL be the exit handler. Readers of another pipeline's tables SHALL read main. Publication SHALL run whether the earlier stages succeed or fail, recording the run outcome. Stage results SHALL distinguish failed data checks from execution errors.

#### Scenario: Inspect a workflow

- **WHEN** a user opens a DP1 or `dp-pipeline` run
- **THEN** they see its stages, its input (dataset version, Kafka offsets or day range), its committed snapshots, its check outcome and its publication outcome

#### Scenario: Checks fail before publication

- **WHEN** a check fails on the audit branch
- **THEN** main is unchanged, the branch is kept for diagnosis, downstream pipelines keep reading the previous published data, and the workflow fails while publication still records the failed run

### Requirement: Single-level retry and writer serialization
Each stage SHALL have one bounded Argo retry policy for execution errors; nested retries SHALL NOT be used, and failed data checks SHALL NOT be retried. Spark stage retries SHALL be safe: historical and day-range writes are idempotent overwrites, failed Kafka appends commit nothing, and a table a run rewrites in several commits (`gold.dim_customer`) has its audit branch reset to main before a retried attempt reads it. Each workflow SHALL hold a mutex so that only one writer per pipeline runs at a time. Cancelling a workflow SHALL remove its SparkApplications. A platform-owned native Kubernetes admission policy SHALL reject Workflow deletion in `data` unless its recorded phase is Succeeded, Failed or Error, and SHALL reject setting `spec.shutdown: Terminate` unless the existing phase is Pending or terminal. Workflows in other namespaces SHALL remain unaffected.

#### Scenario: Retry after transient driver failure

- **WHEN** Argo retries a Spark stage after an execution error
- **THEN** the retry produces the same Bronze or Silver content as a single successful attempt

#### Scenario: Delete or hard-terminate an active workflow

- **WHEN** a user requests deletion or hard termination of a running Workflow in `data`
- **THEN** Kubernetes rejects the request with guidance to use graceful stop

### Requirement: Readable workflow graph
The DP1 workflow SHALL show its Spark, checks, publish-tables and publish stages, and `dp-pipeline` SHALL show its silver, check-silver, publish-silver, gold, marts, check-gold, publish-gold, publish-marts and profile stages, each with direct dependencies and readable names. The benchmarks and lakehouse maintenance SHALL be separate on-demand workflow templates. Metadata API call details SHALL remain in stage logs.

#### Scenario: Inspect the stage graph

- **WHEN** a user opens a normal DP1 or `dp-pipeline` run
- **THEN** the graph shows those stages without select, discover, readback or accept nodes

### Requirement: Pipeline inputs
A DP1 submission SHALL select either a historical dataset version, by source root and dataset version parameters with chart defaults selecting v18, or the Kafka source. Silver, Gold and the marts SHALL run as one `dp-pipeline` workflow taking `start` and `end` UTC days. A daily schedule SHALL run DP1 from Kafka and then `dp-pipeline` without parameters, and SHALL be suspended by default in every environment. A `dp-backfill` workflow SHALL run `dp-pipeline` month by month over a range, in date order, one month at a time.

#### Scenario: Ingest a dataset version

- **WHEN** a DP1 historical submission names a dataset version
- **THEN** all stages use that version's manifest

#### Scenario: Manual pipeline submission

- **WHEN** a user submits `dp-pipeline` without parameters
- **THEN** it covers yesterday and the lookback days before it

#### Scenario: Backfill stops at a failed month

- **WHEN** one month of a backfill fails
- **THEN** the later months do not run, and a new backfill from that month resumes

#### Scenario: Default schedule

- **WHEN** the charts are rendered with default values
- **THEN** the daily schedule exists but is suspended

### Requirement: Catalog registration and discovery
The data-pipelines application SHALL own the OpenMetadata connector configuration, its ExternalSecrets and the service registration. A PostSync register job SHALL create the OpenMetadata services on each deploy. Every checks run SHALL ingest the ClickHouse tables before its native tests. Kafka topics SHALL be ingested by the on-demand `metadata-discovery` WorkflowTemplate, not by a schedule. Registration SHALL NOT import contracts or data products.

#### Scenario: Deploy registers services
- **WHEN** data-pipelines syncs
- **THEN** the register job creates or updates the OpenMetadata services, and no discovery CronWorkflow exists

### Requirement: One pipeline workflow for Silver, Gold and the marts
Silver, Gold and the marts SHALL run as one Argo workflow template, `dp-pipeline`, over a `start` and `end` UTC day range, using write-audit-publish like DP1. Its stages SHALL be:

1. **Silver:** Spark writes the Silver tables for the range to the run's audit branch; the Silver contract tests run against the branch heads; only after they pass is Silver's `main` fast-forwarded.
2. **Gold, after Silver is published:** Spark writes the gold tables for the range from published Silver to the branch; the Gold contract tests run against the branch heads; only after they pass is Gold's `main` fast-forwarded.
3. **Marts, after Silver is published and alongside Gold:** a ClickHouse stage builds the range's mart days from published Silver into staging tables; the mart contract tests run against staging; only after they pass are the staged partitions published and the marts profiled.

The Gold and mart branches SHALL NOT depend on each other. The mart build, the Gold and mart checks and the mart profiling SHALL hold one ClickHouse mutex, so they never run on the server at the same time. No reader of `main` or the live marts SHALL see a run's output before its checks pass. An exit handler SHALL publish the run outcome on success and failure. Every stage SHALL retry only on execution errors, never on failed checks.

#### Scenario: Inspect a pipeline run

- **WHEN** a user opens a `dp-pipeline` run
- **THEN** they see the Silver stages, then the Gold and mart branches side by side, with the range and the publication outcome

#### Scenario: Mart build fails

- **WHEN** the mart build fails
- **THEN** the mart checks do not run, the live marts keep their previous days, Gold still publishes if its checks pass, and the workflow fails

#### Scenario: Gold check fails

- **WHEN** a gold contract rule fails on the audit branch
- **THEN** Gold `main` is unchanged, the branch remains for diagnosis, the marts still publish if their checks pass, and the workflow fails

### Requirement: Pipeline writer serialization
The `pipeline` template SHALL hold one writer mutex, so the daily run, manual runs and every backfill month serialize wherever the template is referenced. Gold's Spark and the marts stage SHALL read published Silver: the snapshots the run itself published, or `main` when its Silver stage was skipped. A backfill SHALL run its months in date order, one at a time, because `dim_customer` continues each range from the versions stored before it.

#### Scenario: Daily run during a backfill

- **WHEN** the daily schedule fires while a backfill month runs
- **THEN** the daily pipeline waits for that month to finish

### Requirement: On-demand warehouse benchmark
The warehouse indexing benchmark SHALL be a separate on-demand workflow template with a single stage. It SHALL hold the pipeline writer mutex, so it does not measure the marts while they are rebuilt.

#### Scenario: Benchmark during a DP3 run

- **WHEN** the benchmark is submitted while `dp-pipeline` runs
- **THEN** it waits for the pipeline run to finish

### Requirement: Skip unchanged stages
Every Silver and Gold commit SHALL record its day range, its input snapshot IDs and the code version that wrote it, and `marts.build_info` SHALL record the marts' range, Silver snapshot IDs and code version. Before each of Silver, Gold and the marts, a plan step SHALL compare the head of `main` (or the live `build_info`) with the range, the current input snapshots and the current code version; when every table matches, the stage's build, checks and publish SHALL be skipped. Output without a recorded code version SHALL never match, and a run without an explicit range SHALL never skip Silver or Gold.

#### Scenario: Re-run after the marts failed

- **WHEN** a range is re-run with the same image after its Silver and Gold were published and its mart build failed
- **THEN** Silver and Gold are skipped and only the marts are built, checked and published

#### Scenario: New code

- **WHEN** a range is re-run after a new batch image is released
- **THEN** no stage is skipped

### Requirement: Feature materialization workflow
Materialization SHALL run as an Argo workflow, `feast-materialize`, with two stages: an ingest stage that materializes the daily views with overlap in one local Spark container on data-platform-spark using the Feast virtualenv and a large spill emptyDir, and a verify stage that checks them online. It SHALL take an optional since day; end SHALL always be now. An exit handler SHALL publish the outcome on success and on failure. The workflow SHALL hold the pipeline writer mutex, so gold does not change while it reads. A suspended daily schedule SHALL run it after DP3's daily schedule. It SHALL also be submittable on its own after a backfill or manual pipeline run. It SHALL be rendered only where the feature store is enabled. Stages SHALL retry only on execution errors, never on a failed check.

#### Scenario: Daily run after DP3

- **WHEN** the daily schedule starts `feast-materialize` after `dp-daily`
- **THEN** its graph shows the materialize and verify stages and the publication

#### Scenario: Submitted during a DP3 run

- **WHEN** `feast-materialize` starts while `dp-pipeline` runs
- **THEN** it waits for DP3 to finish

### Requirement: Stream history workflow
The stream history SHALL run as an on-demand Argo workflow, `stream-history`, with optional start and end days. For each month in order, it SHALL seed isolated topics, await FINISHED on a bounded run-scoped streaming deployment, then replace each table’s history-epoch unit with an initial short range delete followed by separate batch commits. Each commit SHALL use the shared refresh/retry helper. Training SHALL NOT run during a replay or before all units pass their gates. The history writer SHALL retry failed units from offset zero through committed partition EOF, without a prepare step or offset resume. An exit handler SHALL delete the deployment and topics, and SHALL publish each unit's outcome on success and on failure. Runs SHALL be serialized by their own mutex. It SHALL NOT be scheduled.

#### Scenario: Replay stage fails

- **WHEN** the replay of one month fails
- **THEN** the exit handler deletes its deployment and topics, later months do not start, and a new run from that month resumes
