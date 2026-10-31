## Purpose

Curate Bronze payment events and fraud labels into deduplicated events, one row per payment attempt and label observations with enforced linkage and availability, as the single trusted input for analytics and offline features.

## ADDED Requirements

### Requirement: Day-range curation from pinned Bronze
DP2 SHALL take a `start` and `end` UTC day, and without them run yesterday and the configured lookback days before it. It SHALL read both Bronze tables at snapshot IDs fixed when the run starts. Each payment SHALL belong to the UTC day of its first event, its `payment_day`. A run SHALL read Bronze events whose `occurred_at` falls between the day before `start` and the day after `end`, curate the payments whose `payment_day` is in the range, and overwrite only those `payment_day` partitions of `silver.payment_events`, `silver.attempts` and `silver.rejected_events`. It SHALL overwrite `silver.labels` by availability day, linking each label to the attempts of the 45 days before. Each Silver commit SHALL record the Bronze snapshot IDs and the range. A run SHALL be a pure function of its Bronze snapshots: re-running a range changes nothing, and daily runs SHALL build the same tables as one run over their days. Historical and Kafka rows SHALL be curated by the same rules.

#### Scenario: Bronze advances during a run

- **WHEN** Bronze receives a new commit while DP2 is running
- **THEN** DP2 output reflects only the snapshots recorded at its start

#### Scenario: Payment across midnight

- **WHEN** a payment's first event occurs at 23:58 UTC and its last at 00:01
- **THEN** all its rows carry the first day as `payment_day`, and only a run covering that day writes them

#### Scenario: Daily runs equal a backfill

- **WHEN** DP2 runs once per day over a range, and separately once over the whole range, on the same Bronze
- **THEN** the four Silver tables hold the same rows apart from `_run_id`

### Requirement: Event deduplication and conflicts
`silver.payment_events` SHALL contain one row per `event_id`. Deliveries of the same `event_id` SHALL collapse when they agree on every business field except `recorded_at`, which republished exports advance. The hash that decides this SHALL include `schema_version` and ignore `recorded_at` and provenance. Of the collapsing copies, the one with the earliest `recorded_at` SHALL be kept. Deliveries of the same `event_id` that differ in any other business field SHALL be conflicts: the run SHALL succeed, every row of that payment SHALL be written to `silver.rejected_events` with its conflict reason, and the payment SHALL be excluded from `silver.payment_events`, `silver.attempts` and `silver.labels`. Deduplication SHALL join the duplicated event IDs back to Bronze as a broadcast join when their estimated size is under `SILVER_DEDUP_BROADCAST_BYTES` (default 256 MB) and as a shuffled join otherwise, with identical output; the run report SHALL record the chosen strategy and the number of duplicated event IDs.

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

#### Scenario: Historical label whose payment has no attempts

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
After each commit, the Silver tables SHALL pass contract-derived ClickHouse SQL checks executed by native OpenMetadata tests: unique keys, zero lifecycle-invalid attempts, zero label violations, non-negative amounts, truth labels with a scenario, chargebacks without one, and availability at or after occurrence. A failed check SHALL fail the run.

#### Scenario: Linkage violation reaches checks

- **WHEN** a committed Silver label is classified as `violation`
- **THEN** the corresponding native test reports a positive violation count and the run fails

#### Scenario: Historical rejections

- **WHEN** rejected rows from the historical source exist
- **THEN** the Silver rejection check fails

### Requirement: Lakehouse storage layout and maintenance
Bronze and Silver Iceberg tables SHALL be partitioned as the design declares and created with hash distribution and a 256 MB target file size. The `maintain-lakehouse` workflow SHALL compact each table with a bin-pack rewrite, except `silver.attempts`, which SHALL use a sort rewrite by `zorder(customer_id,payment_method_fingerprint)`. It SHALL rewrite each table's manifests and expire snapshots older than seven days while keeping the last ten. Maintenance SHALL NOT change row content, and SHALL NOT change Kafka progress, which lives in the Bronze rows. The report SHALL give each table's files and bytes before and after from its `files` metadata table. It SHALL also give, for one customer's 30-day window on `silver.attempts` before and after the rewrite, and for partitioned Bronze a one-day filter against no filter, the files and bytes planned by each Iceberg scan and the planning time. These are plan-only measurements and copy no data.

#### Scenario: Compaction between Kafka runs

- **WHEN** a Bronze table is rewritten by a bin-pack compaction between two Kafka-mode runs
- **THEN** the second run starts from the same offsets the first run ended at and replays no record

#### Scenario: Snapshot expiry

- **WHEN** maintenance expires snapshots
- **THEN** the last ten snapshots remain, every snapshot not older than seven days remains, and the table content is unchanged

#### Scenario: Probe before and after the rewrite

- **WHEN** maintenance rewrites `silver.attempts`
- **THEN** the report holds the planned files and bytes for the customer window before and after the rewrite, and the table content is unchanged

### Requirement: Skewed merchant join benchmark
The repository SHALL provide an on-demand Spark benchmark that runs one skewed merchant aggregation over a pinned Silver snapshot: attempts joined with per-merchant totals as a sort-merge join, grouped by merchant and day. It SHALL run three variants, each once: AQE skew join off, AQE skew join on, and salting into 16 buckets. Each variant's output SHALL be reduced to its row count and an order-independent checksum, and every variant SHALL equal the baseline on both before any duration is reported. The report SHALL give each variant's duration, its Spark job, stage and task IDs, and the hot merchant's share with its denominator. The benchmark SHALL NOT run in the pipeline workflows.

#### Scenario: Variant output differs

- **WHEN** a variant's row count or checksum differs from the baseline
- **THEN** the benchmark fails without reporting a duration for that variant

#### Scenario: Report content

- **WHEN** the benchmark completes
- **THEN** the report lists each variant's duration, stage IDs and task count, and the hot merchant share with its denominator
