## ADDED Requirements

### Requirement: Feature materialization workflow
Materialization SHALL run as an Argo workflow, `feast-materialize`, with two stages: an ingest stage that materializes the daily views and refreshes rewritten ranges, and a validate stage that checks them online. It SHALL take optional start and end day parameters that override the watermarks. An exit handler SHALL publish the outcome on success and on failure. The workflow SHALL hold the pipeline writer mutex, so gold does not change while it reads. A suspended daily schedule SHALL run it after DP3's daily schedule. It SHALL also be submittable on its own after a backfill or manual pipeline run. It SHALL be rendered only where the feature store is enabled. Stages SHALL retry only on execution errors, never on a failed check.

#### Scenario: Daily run after DP3

- **WHEN** the daily schedule starts `feast-materialize` after `dp-daily`
- **THEN** its graph shows the materialize and check stages and the publication

#### Scenario: Submitted during a DP3 run

- **WHEN** `feast-materialize` starts while `dp-pipeline` runs
- **THEN** it waits for DP3 to finish

### Requirement: Push replay workflow
A bounded replay of either push job SHALL run as an on-demand Argo workflow, `feast-push-replay`, with the target store and the journal range as parameters and an exit-handler publication. Replays of the same store SHALL be serialized by a mutex. It SHALL NOT be scheduled.

#### Scenario: Two replays submitted together

- **WHEN** two replays are submitted at once
- **THEN** the second waits for the first to finish

### Requirement: Stream history workflow
The stream history SHALL run as an on-demand Argo workflow, `stream-history`, with optional start and end days. For each month in order, it SHALL seed the isolated topics, replay through a run-scoped streaming deployment, and write the offline rows. An exit handler SHALL delete the deployment and topics, and SHALL publish each unit's outcome on success and on failure. Runs SHALL be serialized by their own mutex. It SHALL NOT be scheduled.

#### Scenario: Replay stage fails

- **WHEN** the replay of one month fails
- **THEN** the exit handler deletes its deployment and topics, later months do not start, and a new run from that month resumes
