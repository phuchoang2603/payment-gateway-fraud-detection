## Purpose

Serve the fraud features from one Feast project. It covers the online store for Go Fraud's decision-time reads and the offline store over the gold Iceberg tables for point-in-time training data, with natural keys, a TTL per view and versioned feature services, so training and serving request the same features with the same meaning.

## ADDED Requirements

### Requirement: Persistent feature registry
The feature store SHALL have one Feast project, with a registry that persists in object storage across restarts. A deploy-time apply step SHALL first create any missing stream feature tables, then write the registry from the repository definitions. The stream writers and the feature server SHALL only read the registry. Materialization SHALL record its intervals in the same registry.

#### Scenario: Server restart after materialization

- **WHEN** the feature server restarts after a materialization run
- **THEN** it serves the same feature services, and the next materialization overlaps the previous end by the configured lookback plus one day

### Requirement: Natural-key entities
Feature views SHALL join on natural, integration-scoped keys:

- `integration_id` with `customer_id` for customer views;
- `integration_id` with `payment_method_fingerprint` for payment-method views.

These are modelled as separate single-key entities. No view or store SHALL use a synthetic, concatenated or epoch-scoped entity key.

#### Scenario: Same customer ID in two integrations

- **WHEN** integrations A and B both have a customer `c` with different histories
- **THEN** online and offline lookups for (A, `c`) and (B, `c`) return each integration's own values

### Requirement: Feature views and TTLs
The project SHALL define four stored views:

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
One versioned feature service, `payment_risk_v1`, SHALL name exactly the model inputs: the customer and payment-method daily and stream features, plus request_features. Training and Go Fraud SHALL both request it. The stream views SHALL be served only where the live streaming job and the online push job run. Changing the model inputs SHALL create a new service version rather than editing the existing one.

#### Scenario: A model needs another feature

- **WHEN** a later model uses an additional feature
- **THEN** `payment_risk_v2` is added, and `payment_risk_v1` keeps its feature list

### Requirement: Point-in-time offline retrieval
The offline store SHALL read stored features from Gold Iceberg through Spark SQL in local mode on the Iceberg catalog, with disk spill. For each entity row at time `t`, retrieval SHALL return the latest row of each requested view with `event_timestamp <= t` within its TTL. Ties SHALL resolve to the greatest `created`. Rows SHALL NOT be excluded because `created` is later than `t`. Simulation-truth rows of the label table with `label_available_at <= cutoff` SHALL be usable directly through `fraud_feast.training.entity_rows(spark, start, end, cutoff)`, joined to lifecycle-valid Silver attempts and their attempt_started request context, one per attempt, with `event_timestamp` equal to the attempt's start, the scoring time, and their `label` as the training label. Stream rows SHALL become visible one microsecond after their online write time, so no stream snapshot that counts the scored attempt is returned at its start. Training retrieval SHALL request `payment_risk_v1` with `full_feature_names=True`, producing `<view>__<feature>` columns without collisions between views. Retrieval into Python SHALL use Arrow columnar transfer.

#### Scenario: Daily row on the scored attempt's day

- **WHEN** an entity row has time `t` on day D, and the daily table has rows at D 00:00 and D+1 00:00
- **THEN** retrieval returns the D 00:00 row

#### Scenario: Historical example after a later DP3 run

- **WHEN** a DP3 run committed after `t` recomputed the row, so `created` is later than `t`
- **THEN** retrieval still returns the latest row with `event_timestamp <= t`

#### Scenario: Snapshot at the scored attempt's start

- **WHEN** a stream snapshot written at exactly the scored attempt's `started_at` already counts that attempt
- **THEN** retrieval at that `started_at` returns the previous snapshot, and a daily row anchored at exactly that instant is returned

#### Scenario: Label not yet available

- **WHEN** a simulation-truth label's `label_available_at` is after the cutoff
- **THEN** that attempt has no entity row

### Requirement: Streaming feature writers
Two continuously running jobs SHALL consume the v2 feature journal of the configured epoch with read-committed isolation, each with its own consumer group:

- **Online job:** pushes each snapshot to the stream views' online store, keyed by natural keys, with its online write time plus one microsecond as its event time.
- **Offline job:** appends each snapshot to the stream feature tables, with the same event time, the record's commit time as `created`, and its epoch, revision and Kafka partition and offset.

Each job SHALL commit Kafka offsets only after its write is accepted. The offline job SHALL record its offsets in the same table commit as its rows and resume from them, so a restart neither repeats nor skips rows. An older or repeated revision SHALL NOT replace a newer online value. A record that fails v2 identity validation SHALL stop the job without committing.

#### Scenario: Offline job crashes after appending

- **WHEN** the offline job appends a batch and stops before committing to Kafka, then restarts
- **THEN** it resumes after the appended offsets, and each snapshot appears once in the tables

#### Scenario: Redelivered older revision

- **WHEN** the online job receives an older revision of a snapshot it already pushed
- **THEN** the online store keeps the newer values

### Requirement: Online push replay over an explicit range
Only online online writer SHALL accept a bounded journal replay with from/until offsets per partition or timestamps. Replay SHALL override the consumer watermark without committing it, never move a key backwards, and fail when its range is no longer retained. The offline writer SHALL use only its normal exactly-once append path.

#### Scenario: Rebuild after an online store loss

- **WHEN** the online store is emptied and a replay covers the retained journal
- **THEN** every retained key holds its newest snapshot and a second replay changes nothing

### Requirement: Stream history by replay through the streaming job
Short-window history SHALL be produced only by the streaming job itself, replaying recorded payment events in logical-clock mode under a history epoch distinct from every live epoch. No other implementation of the short windows SHALL feed training.

- **Input:** the replay SHALL read events from Bronze, encoded exactly as the live topic carries them, into isolated topics. Kafka SHALL never hold more than a bounded slice of the range.
- **Ranges:** the replay SHALL take explicit start and end days, run in month-sized units in order, and stop at the first failed unit. Each unit SHALL start from empty state with one day of lead-in, so a unit's rows equal those of a replay over the whole history.
- **Ordering:** input and clock topics SHALL each have one partition. Clock controls SHALL carry an ordered sequence and exclusive input-offset barriers. Checkpointed buffering SHALL release the named input before advancing the clock; an offset-zero bootstrap SHALL precede input. Missing, regressing or oversized barriers SHALL fail the replay.
- **Writes:** each unit SHALL first DELETE its history-epoch rows, then use the offline writer's normal append path, dropping lead-in rows. Rerunning a month SHALL rebuild the same rows without duplicates; a failed month may be partial and SHALL be rerun before training.
- **Gate:** a unit SHALL require zero decode rejects and consumed input offsets reaching every seeded record's end, including the final clock control. Other rejects SHALL be reported without gating. No attempt-to-snapshot membership check SHALL be required.
- **Cleanup:** a unit's streaming deployment and topics SHALL be removed after it, whether it succeeds or fails.

#### Scenario: Month rerun

- **WHEN** one month of history is replayed again on the same Bronze
- **THEN** that month's stream rows are equal to before, and no other month changes

#### Scenario: Encoding fault

- **WHEN** the seed writes an event that the streaming job cannot decode
- **THEN** the unit fails and its range must be rerun before training

### Requirement: Materialization with overlap and an online check
Materialization SHALL call store.materialize(start, now) for each daily view with skip_dedup and latest-row selection. Default start SHALL be the registry last end minus (pipeline.lookbackDays + 1) days, or now minus TTL for the first run. An optional since day SHALL override start; end SHALL always be now. There SHALL be no state file, commit-range traversal, both-snapshot entity sets, null tombstones or full-refresh fallback. Stream views SHALL NOT be materialized offline.

A validation stage SHALL sample newest keys only and fail when online values or event timestamps differ from their offline rows. A deleted entity's prior online value SHALL remain until TTL; this known gap SHALL be documented.

#### Scenario: Lookback day rewritten

- **WHEN** DP3 rewrites an anchor within its lookback before the previous end
- **THEN** the overlap loads the changed row and the online check reads it back

#### Scenario: Explicit since after backfill

- **WHEN** materialization runs with since equal to an old backfill start
- **THEN** it reads through now and writes each entity's latest row, preserving newer values

#### Scenario: DP3 removes an entity's last row

- **WHEN** DP3 deletes every row of an entity
- **THEN** its previous online value may persist until TTL

### Requirement: Online feature serving
A feature server SHALL serve `get-online-features` for the feature services, in-cluster, to Go Fraud. One request carrying the integration, customer and payment-method keys and request-context fields SHALL return every view of the service, with each view's event timestamp.

#### Scenario: One lookup for all views

- **WHEN** Go Fraud requests `payment_risk_v1` for one (integration, customer, payment method)
- **THEN** one response carries the daily and stream values and their event timestamps for both entities

#### Scenario: Clock overtakes input

- **WHEN** the clock topic delivers a control before the input offsets it names, including across a checkpoint restore
- **THEN** the replay buffers that control and produces the same lateness decisions and expiration snapshots as input-first delivery

### Requirement: Shared request feature transformation
The project SHALL define request_context as a RequestSource with scored_at, amount_minor, customer_account_created_at, merchant_account_created_at, shipping_country, nullable ip_country, item_count and has_device. A pure Python on-demand request_features view SHALL output amount_minor, customer_account_age_days, merchant_account_age_days, ip_shipping_country_mismatch, item_count and has_device. Ages SHALL be elapsed whole days; mismatch SHALL be null for absent IP country. Item count SHALL count checkout line items and has_device SHALL reflect fingerprint presence. payment_risk_v1 SHALL include this view. Go SHALL send raw request values and SHALL NOT compute model request features. Rules SHALL retain their own logic.

#### Scenario: Same request online and offline

- **WHEN** identical request values are passed through historical retrieval and the feature server
- **THEN** request_features outputs are identical, including nullable mismatch and boolean false
