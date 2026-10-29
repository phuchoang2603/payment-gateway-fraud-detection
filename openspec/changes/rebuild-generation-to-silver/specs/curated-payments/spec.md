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
`silver.payment_events` SHALL contain one row per `event_id`. Deliveries of the same `event_id` SHALL collapse when their business content is equal, ignoring provenance and physical schema version. Deliveries of the same `event_id` with different business content SHALL fail the run before any Silver table is written, after the run report is persisted.

#### Scenario: Identical duplicate

- **WHEN** Bronze holds two deliveries of an event with equal business content from different files or offsets
- **THEN** Silver holds one row for that event and the report counts one identical duplicate

#### Scenario: Conflicting content

- **WHEN** two deliveries of one `event_id` differ in any business field
- **THEN** the run fails, Silver tables are unchanged and the report lists the conflict count

### Requirement: Attempt histories
`silver.attempts` SHALL contain one row per integration, payment and attempt, with the server start time, the client `attempted_at`, the risk decision, the attempt outcome (`succeeded`, `declined`, `unknown` or `pending`) and its time, and the payment status (`open`, `succeeded` or `expired`). Attempts of open payments SHALL be included. An attempt with more than one start or terminal event, events out of aggregate-version order, or a terminal event before its start SHALL be marked lifecycle-invalid, and any lifecycle-invalid attempt SHALL fail the Silver checks.

#### Scenario: Retried payment

- **WHEN** a payment has a declined attempt followed by a succeeded attempt
- **THEN** Silver has two attempt rows with outcomes `declined` and `succeeded` and payment status `succeeded`

#### Scenario: In-flight live attempt

- **WHEN** an attempt has started but no outcome has arrived and the payment has no terminal event
- **THEN** the attempt row has outcome `pending`, payment status `open` and is lifecycle-valid

### Requirement: Label observations and linkage
`silver.labels` SHALL contain one row per distinct label observation; identical copies SHALL collapse, and distinct reports for the same attempt SHALL remain separate. Each label SHALL be classified as `linked` when its attempt exists (and, for a chargeback, succeeded), `pending` when the attempt or its outcome has not arrived and the payment is not terminal, or `violation` otherwise. Any `violation` SHALL fail the Silver checks. Simulation truth, chargebacks and risk decisions SHALL remain separate, and the absence of a chargeback SHALL NOT be recorded as a negative label.

#### Scenario: Chargeback on a declined attempt of a finished payment

- **WHEN** a chargeback references a declined attempt and the payment has succeeded on another attempt
- **THEN** the label is a `violation` and the Silver checks fail

#### Scenario: Label for a missing attempt on a finished payment

- **WHEN** a label references an attempt that does not exist and the payment is `succeeded` or `expired`
- **THEN** the label is a `violation`

#### Scenario: Label before its attempt arrives

- **WHEN** a label references an attempt whose events are not yet in Bronze and the payment is unknown or open
- **THEN** the label is `pending` and the run succeeds

### Requirement: Label availability
A label SHALL NOT be available before its occurrence. A label for an attempt that has started SHALL NOT be available before that attempt's start, whether or not the payment is complete, and a chargeback SHALL NOT be available before its attempt succeeded. Violations SHALL be classified as `violation`. Scenario and oracle fields SHALL be documented as excluded from features, and consumers SHALL join labels only at or after `label_available_at`.

#### Scenario: Truth label earlier than an in-flight attempt start

- **WHEN** a truth label's availability precedes the start of its open payment's attempt
- **THEN** the label is a `violation` and the Silver checks fail

### Requirement: Run report
Each DP2 run SHALL persist a report with its Bronze snapshot IDs, input rows per table, distinct events, identical duplicates, conflicts, attempts by outcome, labels by linkage class and the Silver snapshot IDs written. The report SHALL be written whether the run succeeds or fails after reading input.

#### Scenario: Inspect a run

- **WHEN** an operator opens a DP2 run report
- **THEN** input rows equal distinct events plus identical duplicates plus conflicting deliveries for events

### Requirement: Silver contract checks
After each commit, the Silver tables SHALL pass contract-derived StarRocks SQL checks executed by native OpenMetadata tests: unique keys, zero lifecycle-invalid attempts, zero label violations, non-negative amounts, truth labels with a scenario, chargebacks without one, and availability at or after occurrence. A failed check SHALL fail the run.

#### Scenario: Linkage violation reaches checks

- **WHEN** a committed Silver label is classified as `violation`
- **THEN** the corresponding native test reports a positive violation count and the run fails

### Requirement: Optimization benchmark
The repository SHALL provide an on-demand Spark benchmark that runs one skewed aggregation over a pinned Silver snapshot as a baseline and as an optimized variant with fixed resources, performs one warm-up and three timed runs of each, reports median durations and asserts that both variants produce equal output in both directions. The benchmark SHALL NOT run in the pipeline workflows.

#### Scenario: Optimized result differs

- **WHEN** the optimized variant's output differs from the baseline
- **THEN** the benchmark fails without reporting a speedup
