## ADDED Requirements

### Requirement: DP3 gold workflow
DP3 SHALL run as one Argo workflow, `dp3-gold`, using write-audit-publish like DP1 and DP2. It SHALL have these stages:

1. **Ingest:** a Spark stage writes the gold Iceberg tables to the run's audit branch.
2. **Ingest:** a ClickHouse stage builds the marts into staging tables.
3. **Validate:** the contract tests for both gold products run against the audit branch heads and the staging tables.
4. **Publish:** only after every test succeeds, the gold tables' `main` is fast-forwarded and the staging marts are exchanged into the live marts.
5. **Profile:** the published marts are profiled.

No reader of gold `main` or the live marts SHALL see a run's output before its checks pass.

An exit handler SHALL publish the run outcome on success and failure. DP3 SHALL take no input selection and SHALL read current Silver. Every stage SHALL retry only on execution errors, never on failed checks.

#### Scenario: Inspect a DP3 run

- **WHEN** a user opens a `dp3-gold` run
- **THEN** they see the features and marts ingest stages, the validate stage after them, the Silver snapshot IDs the run read, and the publication outcome

#### Scenario: Mart build fails

- **WHEN** the marts stage fails
- **THEN** validation does not run, the workflow fails, publication records the failure, and the live marts keep their previous build

#### Scenario: Gold check fails

- **WHEN** a gold contract rule fails on the audit branch
- **THEN** gold `main` and the live marts are unchanged, the branch remains for diagnosis, and the workflow fails

### Requirement: DP3 writer serialization
A DP3 run SHALL hold its own writer mutex and the DP2 Silver writer mutex for its whole duration. Silver therefore cannot be overwritten while DP3's Spark and ClickHouse stages read it, and both stages read the same Silver snapshots. DP3 SHALL have its own suspended daily schedule, running in incremental mode, and SHALL NOT run in the six-hourly Kafka schedule.

#### Scenario: Silver submitted during DP3

- **WHEN** a `dp2-silver` run is submitted while `dp3-gold` is running
- **THEN** the Silver run waits until DP3 finishes

### Requirement: On-demand warehouse benchmark
The warehouse indexing benchmark SHALL be a separate on-demand workflow template with a single stage. It SHALL hold the DP3 writer mutex, so it does not measure the marts while they are rebuilt.

#### Scenario: Benchmark during a DP3 run

- **WHEN** the benchmark is submitted while `dp3-gold` runs
- **THEN** it waits for the DP3 run to finish
