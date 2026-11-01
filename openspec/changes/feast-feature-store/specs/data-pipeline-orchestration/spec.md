## ADDED Requirements

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
