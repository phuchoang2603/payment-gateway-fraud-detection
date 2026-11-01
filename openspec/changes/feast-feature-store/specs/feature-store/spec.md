## Purpose

Serve the fraud features from one Feast project. It covers the online store for Go Fraud's decision-time reads and the offline store over the gold Iceberg tables for point-in-time training data, with natural keys, a TTL per view and versioned feature services, so training and serving request the same features with the same meaning.

## ADDED Requirements

### Requirement: Persistent feature registry
The feature store SHALL have one Feast project, with a registry that persists in object storage across restarts. A deploy-time apply step SHALL first create any missing stream feature tables, then write the registry from the repository definitions. The push jobs and the feature server SHALL only read the registry. Materialization SHALL record its intervals in the same registry.

#### Scenario: Server restart after materialization

- **WHEN** the feature server restarts after a materialization run
- **THEN** it serves the same feature services, and the next incremental materialization starts where the previous one ended

### Requirement: Natural-key entities
Feature views SHALL join on natural, integration-scoped keys:

- `integration_id` with `customer_id` for customer views;
- `integration_id` with `payment_method_fingerprint` for payment-method views.

These are modelled as separate single-key entities. No view or store SHALL use a synthetic, concatenated or epoch-scoped entity key.

#### Scenario: Same customer ID in two integrations

- **WHEN** integrations A and B both have a customer `c` with different histories
- **THEN** online and offline lookups for (A, `c`) and (B, `c`) return each integration's own values

### Requirement: Feature views and TTLs
The project SHALL define four views:

- a customer and a payment-method daily view, over the DP3 daily feature tables, with every feature column of their definition;
- a customer and a payment-method stream view, whose offline history is the stream feature tables, filtered to the configured epochs, and whose online values come from the streaming job.

Each view SHALL declare a TTL equal to its longest window plus the documented anchor and delivery delay: 31 days 12 hours for the daily views, 24 hours 5 minutes for the stream views. The feature-store documentation SHALL record each TTL's reason. A row older than `t − ttl` SHALL be unknown at time `t`.

#### Scenario: Dormant customer

- **WHEN** a customer's latest daily row is the all-zero row, written 40 days before `t`
- **THEN** the daily customer features are null at `t`

#### Scenario: Active customer with a stable 30-day count

- **WHEN** a customer's newest attempt entered the 30-day window 25 days before `t`
- **THEN** a row within the TTL exists, and its values are returned

#### Scenario: Stream history and live epochs

- **WHEN** an entity has history-epoch rows up to the end of the dataset and live-epoch rows afterwards, and both epochs are configured
- **THEN** retrieval at any `t` returns the latest row at or before `t` from either epoch, and rows of an unconfigured epoch are never returned

### Requirement: Versioned feature service
One versioned feature service, `fraud_model_v1`, SHALL name exactly the model inputs: the customer and payment-method daily and stream features. Training and Go Fraud SHALL both request it. The stream views SHALL be served only where the live streaming job and the online push job run. Changing the model inputs SHALL create a new service version rather than editing the existing one.

#### Scenario: A model needs another feature

- **WHEN** a later model uses an additional feature
- **THEN** `fraud_model_v2` is added, and `fraud_model_v1` keeps its feature list

### Requirement: Point-in-time offline retrieval
The offline store SHALL read only the gold Iceberg tables, through the distributed engine on the Iceberg catalog, never by loading a whole table into one process. For each entity row at time `t`, retrieval SHALL return the latest row of each requested view with `event_timestamp <= t` within its TTL. Ties SHALL resolve to the greatest `created`. Rows SHALL NOT be excluded because `created` is later than `t`. Simulation-truth rows of the label table SHALL be usable as the entity rows, one per attempt, with `event_timestamp` one microsecond before the attempt's start, so no stream snapshot that counts the scored attempt is returned. A positive training label SHALL require a chargeback row with `label = true` and `label_available_at <= cutoff`. Training retrieval SHALL request `fraud_model_v1` with `full_feature_names=True`, producing `<view>__<feature>` columns without collisions between views. Retrieval into Python SHALL use Arrow columnar transfer.

#### Scenario: Daily row on the scored attempt's day

- **WHEN** an entity row has time `t` on day D, and the daily table has rows at D 00:00 and D+1 00:00
- **THEN** retrieval returns the D 00:00 row

#### Scenario: Historical example after a later DP3 run

- **WHEN** a DP3 run committed after `t` recomputed the row, so `created` is later than `t`
- **THEN** retrieval still returns the latest row with `event_timestamp <= t`

#### Scenario: Snapshot at the scored attempt's start

- **WHEN** a stream snapshot at exactly the scored attempt's `started_at` already counts that attempt
- **THEN** retrieval for that attempt returns the previous snapshot

#### Scenario: Label not yet available

- **WHEN** a chargeback's `label_available_at` is after the cutoff
- **THEN** that attempt's simulation-truth entity row remains included, but that chargeback does not make its training label positive

### Requirement: Streaming push jobs
Two continuously running jobs SHALL consume the v2 feature journal of the configured epoch with read-committed isolation, each with its own consumer group:

- **Online job:** pushes each snapshot to the stream views' online store, keyed by natural keys, with the snapshot's online write time as its event time.
- **Offline job:** appends each snapshot to the stream feature tables, with the same event time, the record's commit time as `created`, and its epoch, revision and Kafka partition and offset.

Each job SHALL commit Kafka offsets only after its write is accepted. The offline job SHALL record its offsets in the same table commit as its rows and resume from them, so a restart neither repeats nor skips rows. An older or repeated revision SHALL NOT replace a newer online value. A record that fails v2 identity validation SHALL stop the job without committing.

#### Scenario: Offline job crashes after appending

- **WHEN** the offline job appends a batch and stops before committing to Kafka, then restarts
- **THEN** it resumes after the appended offsets, and each snapshot appears once in the tables

#### Scenario: Redelivered older revision

- **WHEN** the online job receives an older revision of a snapshot it already pushed
- **THEN** the online store keeps the newer values

### Requirement: Push job replay over an explicit range
Each push job SHALL also run as a bounded replay over an explicit journal range, given as offsets per partition or as start and end times, overriding its consumer watermark. Replays SHALL be idempotent:

- the online replay SHALL never move a key to an older snapshot;
- the offline replay SHALL replace the rows of the replayed epoch, partitions and offsets in one table commit, so the tables hold each journal record at most once.

An offline replay SHALL end at or before the offline job's recorded watermark, and SHALL leave that watermark unchanged. A replay SHALL fail when its range is no longer retained by the journal.

#### Scenario: Rebuild after an online store loss

- **WHEN** the online store is emptied and a replay covers the retained journal
- **THEN** every key with a retained snapshot holds its newest one, and a second replay changes nothing

#### Scenario: Offline replay run twice

- **WHEN** the offline replay runs twice over the same committed range
- **THEN** each journal record in the range appears exactly once in the stream tables, and the continuous job resumes from its unchanged watermark

### Requirement: Stream history by replay through the streaming job
Short-window history SHALL be produced only by the streaming job itself, replaying recorded payment events in logical-clock mode under a history epoch distinct from every live epoch. No other implementation of the short windows SHALL feed training.

- **Input:** the replay SHALL read events from Bronze, encoded exactly as the live topic carries them, into isolated topics. Kafka SHALL never hold more than a bounded slice of the range.
- **Ranges:** the replay SHALL take explicit start and end days, run in month-sized units in order, and stop at the first failed unit. Each unit SHALL start from empty state with one day of lead-in, so a unit's rows equal those of a replay over the whole history.
- **Ordering:** input and clock topics SHALL each have one partition. Clock controls SHALL carry an ordered sequence and exclusive input-offset barriers. Checkpointed buffering SHALL release the named input before advancing the clock; an offset-zero bootstrap SHALL precede input. Missing, regressing or oversized barriers SHALL fail the replay.
- **Writes:** each unit SHALL atomically replace the history epoch's rows of its days, so rerunning a range leaves the same rows with no duplicates.
- **Gate:** a unit SHALL fail on any decode rejection. Its other rejects SHALL match the duplicates and too-late events in its input, and every attempt with a customer or payment method SHALL appear in a snapshot.
- **Cleanup:** a unit's streaming deployment and topics SHALL be removed after it, whether it succeeds or fails.

#### Scenario: Month rerun

- **WHEN** one month of history is replayed again on the same Bronze
- **THEN** that month's stream rows are equal to before, and no other month changes

#### Scenario: Encoding fault

- **WHEN** the seed writes an event that the streaming job cannot decode
- **THEN** the unit fails, and its days keep their previous rows

### Requirement: Incremental materialization with rerun refreshes and an online check
After DP3, materialization SHALL load the daily views into the online store incrementally: from the previous run's end time to now, or from `now − ttl` on the first run, writing only the latest row per entity. It SHALL then refresh the day ranges that DP3 commits rewrote since the snapshots the last successful run consumed. These ranges are read from the commits themselves. Every entity with a row in those ranges, before or after the rewrite, SHALL be set to its latest row at or before now, or to unknown values when no row remains, even when that row's event time is not newer than the stored one. When the consumed snapshots are unavailable, it SHALL instead reload every entity with a row within the TTL.

An explicit start and end day SHALL override the watermarks. The run SHALL then refresh every entity with a row in that range to its latest row at or before now, and SHALL leave the registry interval and the consumed snapshots unchanged. Every run SHALL be idempotent: rerunning over the same gold snapshots SHALL write the same values, and no run SHALL set an entity to a row older than its latest row at or before now.

A validation stage SHALL read back, through `fraud_model_v1`, a sample of the entities with the newest rows and of the refreshed entities. It SHALL fail when any of them is missing, or differs in event time or values from the offline latest row at or before now. The consumed snapshots SHALL advance only after the validation passes. Stream views SHALL NOT be materialized from the offline store.

#### Scenario: Two DP3 runs a day apart

- **WHEN** materialization runs after each of two consecutive daily DP3 runs
- **THEN** the second run loads the new anchor incrementally, and the online check passes for the newest rows

#### Scenario: Lookback day rewritten

- **WHEN** a daily DP3 run's lookback changes an entity's row at an anchor before the previous materialization's end
- **THEN** the next materialization writes the changed row online, and the check reads it back

#### Scenario: DP3 removes an entity's last row

- **WHEN** a DP3 rerun deletes every row of an entity, for example after a conflicting redelivery
- **THEN** the online store returns unknown values for that entity, not the deleted ones

#### Scenario: Explicit range over old anchors

- **WHEN** materialization runs with a range from two months ago, for an entity whose latest row is from yesterday
- **THEN** the online store keeps yesterday's row, and the next default run starts from the same watermark as before

#### Scenario: Check fails

- **WHEN** the online check fails
- **THEN** the consumed snapshots do not advance, and the next run repeats the refresh

### Requirement: Online feature serving
A feature server SHALL serve `get-online-features` for the feature services, in-cluster, to Go Fraud. One request carrying the integration, customer and payment-method keys SHALL return every view of the service, with each view's event timestamp.

#### Scenario: One lookup for all views

- **WHEN** Go Fraud requests `fraud_model_v1` for one (integration, customer, payment method)
- **THEN** one response carries the daily and stream values and their event timestamps for both entities

#### Scenario: Clock overtakes input

- **WHEN** the clock topic delivers a control before the input offsets it names, including across a checkpoint restore
- **THEN** the replay buffers that control and produces the same lateness decisions and expiration snapshots as input-first delivery
