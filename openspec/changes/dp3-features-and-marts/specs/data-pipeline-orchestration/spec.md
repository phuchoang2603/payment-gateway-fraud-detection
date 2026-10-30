## ADDED Requirements

### Requirement: DP3 gold workflow
DP3 SHALL run as one Argo workflow, `dp3-gold`, with three stages:

1. Ingest: a Spark stage that writes the gold Iceberg tables.
2. Ingest: a ClickHouse stage that rebuilds the marts. It may run alongside the Spark stage.
3. Validate: runs only after both ingest stages commit. It runs the contract tests for both gold products and profiles the marts.

An exit handler SHALL publish the run outcome on success and failure. DP3 SHALL take no input selection and SHALL read current Silver. Every stage SHALL retry only on execution errors, never on failed checks.

#### Scenario: Inspect a DP3 run

- **WHEN** a user opens a `dp3-gold` run
- **THEN** they see the features and marts ingest stages, the validate stage after them, the Silver snapshot IDs the run read, and the publication outcome

#### Scenario: Mart build fails

- **WHEN** the marts stage fails
- **THEN** validation does not run, the workflow fails, publication records the failure, and the live marts keep their previous build

### Requirement: DP3 writer serialization
A DP3 run SHALL hold its own writer mutex and the DP2 Silver writer mutex for its whole duration. Silver therefore cannot be overwritten while DP3's Spark and ClickHouse stages read it, and both stages read the same Silver snapshots. The suspended Kafka schedule SHALL run DP3 after DP1 and DP2 and publish all three pipelines.

#### Scenario: Silver submitted during DP3

- **WHEN** a `dp2-silver` run is submitted while `dp3-gold` is running
- **THEN** the Silver run waits until DP3 finishes

### Requirement: On-demand warehouse benchmark
The warehouse indexing benchmark SHALL be a separate on-demand workflow template with a single stage. It SHALL hold the DP3 writer mutex, so it does not measure the marts while they are rebuilt.

#### Scenario: Benchmark during a DP3 run

- **WHEN** the benchmark is submitted while `dp3-gold` runs
- **THEN** it waits for the DP3 run to finish
