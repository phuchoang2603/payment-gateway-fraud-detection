## Purpose

Curate Bronze payment events and fraud labels into deduplicated events, one row per payment attempt and label observations with enforced linkage and availability, as the single trusted input for analytics and offline features.

## ADDED Requirements

### Requirement: Full recompute from pinned Bronze snapshots
DP2 SHALL read both Bronze tables at snapshot IDs fixed when the run starts and replace `silver.payment_events`, `silver.attempts` and `silver.labels` entirely in each run. Each Silver commit SHALL record the Bronze snapshot IDs it was built from. Rerunning on the same Bronze snapshots SHALL produce the same content. Historical and Kafka rows SHALL be curated by the same rules.

#### Scenario: Bronze advances during a run

- **WHEN** Bronze receives a new commit while DP2 is running
- **THEN** DP2 output reflects only the snapshots recorded at its start

#### Scenario: Rerun on unchanged Bronze

- **WHEN** DP2 runs twice without Bronze changes
- **THEN** all three Silver tables have equal content after each run

### Requirement: Event deduplication and conflicts
`silver.payment_events` SHALL contain one row per `event_id`. Deliveries of the same `event_id` SHALL collapse when they agree on every business field except `recorded_at`, which republished exports advance. The hash that decides this SHALL include `schema_version` and ignore `recorded_at` and provenance. Of the collapsing copies, the one with the earliest `recorded_at` SHALL be kept. Deliveries of the same `event_id` that differ in any other business field SHALL be conflicts: the run SHALL succeed, every row of that payment SHALL be written to `silver.rejected_events` with its conflict reason, and the payment SHALL be excluded from `silver.payment_events`, `silver.attempts` and `silver.labels`.

#### Scenario: Republished export

- **WHEN** Bronze holds an event and a copy with the same business content and a later `recorded_at`
- **THEN** Silver holds one row with the earliest `recorded_at`, and the report counts one republished export

#### Scenario: Conflicting content

- **WHEN** two deliveries of one `event_id` differ in any business field other than `recorded_at`
- **THEN** the run succeeds, every row of that payment is in `silver.rejected_events` with the conflict reason, the accepted Silver tables exclude that payment, and the run report counts the conflict and the rejected rows

#### Scenario: Historical conflict

- **WHEN** a conflict occurs among rows from the historical source
- **THEN** the rejected rows are reported and the Silver rejection check for the historical source fails

### Requirement: Attempt histories
`silver.attempts` SHALL contain one row per integration, payment and attempt, carrying top-level `merchant_id`, `customer_id`, `payment_method_fingerprint` and `amount_minor` from the AttemptStarted context and signals, with the server start time, the client `attempted_at`, the risk decision, the attempt outcome (`succeeded`, `declined`, `unknown` or `pending`) and its time, and the payment status (`open`, `succeeded` or `expired`). Attempts of open payments SHALL be included. An attempt with more than one start or terminal event, events out of aggregate-version order, or a terminal event before its start SHALL be marked lifecycle-invalid, and any lifecycle-invalid attempt SHALL fail the Silver checks.

#### Scenario: Retried payment

- **WHEN** a payment has a declined attempt followed by a succeeded attempt
- **THEN** Silver has two attempt rows with outcomes `declined` and `succeeded` and payment status `succeeded`

#### Scenario: Attempt filter keys

- **WHEN** an attempt has started
- **THEN** its row carries the merchant, customer, payment-method fingerprint and checkout amount from that AttemptStarted

#### Scenario: In-flight live attempt

- **WHEN** an attempt has started but no outcome has arrived and the payment has no terminal event
- **THEN** the attempt row has outcome `pending`, payment status `open` and is lifecycle-valid

### Requirement: Label observations and linkage
`silver.labels` SHALL contain one row per distinct label observation; identical copies SHALL collapse, and distinct reports for the same attempt SHALL remain separate. Each label SHALL be classified as `linked` when its attempt exists (and, for a chargeback, succeeded); `orphan` when it is a historical label whose payment has no events; `pending` when it is a live label whose payment or attempt has not arrived, or whose attempt has started with no outcome while the payment is open; and `violation` otherwise. Any `violation` or `orphan` SHALL fail the Silver checks. Simulation truth, chargebacks and risk decisions SHALL remain separate, and the absence of a chargeback SHALL NOT be recorded as a negative label.

#### Scenario: Chargeback on a declined attempt of a finished payment

- **WHEN** a chargeback references a declined attempt and the payment has succeeded on another attempt
- **THEN** the label is a `violation` and the Silver checks fail

#### Scenario: Label for a missing attempt on a finished payment

- **WHEN** a label references an attempt that does not exist and the payment is `succeeded` or `expired`
- **THEN** the label is a `violation`

#### Scenario: Historical label whose payment has no events

- **WHEN** a historical label references a payment that has no events in the complete source
- **THEN** the label is an `orphan` and the Silver checks fail

#### Scenario: Live label awaiting its payment

- **WHEN** a Kafka label references a payment whose events have not arrived yet
- **THEN** the label is `pending` and the Silver checks pass

### Requirement: Label availability
A label SHALL NOT be available before its occurrence. A label for an attempt that has started SHALL NOT be available before that attempt's start, whether or not the payment is complete, and a chargeback SHALL NOT be available before its attempt succeeded. Violations SHALL be classified as `violation`. Scenario and oracle fields SHALL be documented as excluded from features, and consumers SHALL join labels only at or after `label_available_at`.

#### Scenario: Truth label earlier than an in-flight attempt start

- **WHEN** a truth label's availability precedes the start of its open payment's attempt
- **THEN** the label is a `violation` and the Silver checks fail

### Requirement: Run report
Each DP2 run SHALL persist a report with its Bronze snapshot IDs, input rows per table, distinct events, identical duplicates, conflicts, rejected rows by table and source, attempts by outcome, labels by linkage class, the Silver snapshot IDs written with their added-record counts, and the tasks, stages and wall time of each phase. The report SHALL be written whether the run succeeds or fails after reading input.

#### Scenario: Inspect a run

- **WHEN** an operator opens a DP2 run report
- **THEN** input rows equal distinct events plus identical duplicates plus the rows of conflicting payments

### Requirement: Silver contract checks
After each commit, the Silver tables SHALL pass contract-derived StarRocks SQL checks executed by native OpenMetadata tests: unique keys, zero lifecycle-invalid attempts, zero label violations, non-negative amounts, truth labels with a scenario, chargebacks without one, and availability at or after occurrence. A failed check SHALL fail the run.

#### Scenario: Linkage violation reaches checks

- **WHEN** a committed Silver label is classified as `violation`
- **THEN** the corresponding native test reports a positive violation count and the run fails

#### Scenario: Historical rejections

- **WHEN** rejected rows from the historical source exist
- **THEN** the Silver rejection check fails

### Requirement: Lakehouse storage layout and maintenance
Bronze and Silver Iceberg tables SHALL be partitioned as the design declares and created with hash distribution and a 256 MB target file size. The `maintain-lakehouse` workflow SHALL compact each table with a bin-pack rewrite, rewrite its manifests, and expire snapshots older than seven days while keeping the last ten. Maintenance SHALL NOT change row content, and SHALL NOT change Kafka progress, which lives in the Bronze rows. The on-demand `storage-layout` benchmark SHALL measure compaction, partition pruning and Z-order clustering on fixed inputs, reporting files and bytes planned by each Iceberg scan, medians of three timed runs after one warm-up, and SHALL assert equal output in both directions before reporting any timing.

#### Scenario: Compaction between Kafka runs

- **WHEN** a Bronze table is rewritten by a bin-pack compaction between two Kafka-mode runs
- **THEN** the second run starts from the same offsets the first run ended at and replays no record

#### Scenario: Snapshot expiry

- **WHEN** maintenance expires snapshots
- **THEN** the last ten snapshots remain, every snapshot not older than seven days remains, and the table content is unchanged

#### Scenario: Layout output differs

- **WHEN** a layout's output differs from its baseline in either direction
- **THEN** the storage benchmark fails without reporting timings for that case

### Requirement: Optimization benchmark
The repository SHALL provide an on-demand Spark benchmark that measures two aggregations over the same pinned Silver snapshot. The first is the skewed merchant aggregation: attempts joined with per-merchant aggregates, grouped by merchant and day. The second is a high-cardinality aggregation: distinct devices and attempt counts per customer and per payment method over 24-hour windows, across about three million customers and 3.9 million payment methods. Each aggregation SHALL run as a baseline with adaptive skew handling off and without salting, and as an optimized variant on fixed resources. Each variant SHALL perform one warm-up and three timed runs, report median durations, and assert equal output in both directions with `exceptAll`. The benchmark SHALL NOT run in the pipeline workflows.

#### Scenario: Optimized result differs

- **WHEN** an optimized variant's output differs from its baseline for either aggregation
- **THEN** the benchmark fails without reporting a speedup for that aggregation

#### Scenario: High-cardinality measurement

- **WHEN** the benchmark runs against a snapshot with the full customer and payment-method population
- **THEN** it reports both aggregations' baseline and optimized medians and their output row counts
