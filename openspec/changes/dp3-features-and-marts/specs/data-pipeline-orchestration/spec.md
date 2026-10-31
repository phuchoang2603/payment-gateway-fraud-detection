## ADDED Requirements

### Requirement: One pipeline workflow for Silver, Gold and the marts
Silver, Gold and the marts SHALL run as one Argo workflow template, `dp-pipeline`, over a `start` and `end` UTC day range, using write-audit-publish like DP1. Its stages SHALL be:

1. **Ingest:** Spark writes the Silver tables for the range to the run's audit branch.
2. **Ingest:** Spark writes the gold tables for the range to the same branch.
3. **Ingest:** a ClickHouse stage builds the range's mart days into staging tables.
4. **Validate:** the contract tests for Silver, Gold and the marts run against the branch heads and the staging tables.
5. **Publish:** only after every test succeeds, Silver's and Gold's `main` is fast-forwarded, and the staged mart partitions replace the live ones.
6. **Profile:** the published marts are profiled.

No reader of `main` or the live marts SHALL see a run's output before its checks pass. An exit handler SHALL publish the run outcome on success and failure. Every stage SHALL retry only on execution errors, never on failed checks.

#### Scenario: Inspect a pipeline run

- **WHEN** a user opens a `dp-pipeline` run
- **THEN** they see the Silver, Gold and marts ingest stages, the validate stage after them, the range and Bronze and Silver snapshot IDs the run read, and the publication outcome

#### Scenario: Mart build fails

- **WHEN** the marts stage fails
- **THEN** validation does not run, the workflow fails, publication records the failure, and the live marts keep their previous days

#### Scenario: Gold check fails

- **WHEN** a gold contract rule fails on the audit branch
- **THEN** Silver and Gold `main` and the live marts are unchanged, the branch remains for diagnosis, and the workflow fails

### Requirement: Pipeline writer serialization
The `pipeline` template SHALL hold one writer mutex, so the daily run, manual runs and every backfill month serialize wherever the template is referenced. Gold's Spark and the marts stage SHALL read the Silver snapshots the run itself wrote. A backfill SHALL run its months in date order, one at a time, because `dim_customer` continues each range from the versions stored before it.

#### Scenario: Daily run during a backfill

- **WHEN** the daily schedule fires while a backfill month runs
- **THEN** the daily pipeline waits for that month to finish

### Requirement: On-demand warehouse benchmark
The warehouse indexing benchmark SHALL be a separate on-demand workflow template with a single stage. It SHALL hold the pipeline writer mutex, so it does not measure the marts while they are rebuilt.

#### Scenario: Benchmark during a DP3 run

- **WHEN** the benchmark is submitted while `dp-pipeline` runs
- **THEN** it waits for the pipeline run to finish
