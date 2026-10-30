## Purpose

Produce governed DP3 gold tables in the Iceberg lakehouse. They are an SCD2 customer dimension, long-window customer and payment-method history features, and a point-in-time label table. Feast's offline store, training and ClickHouse all read them with one meaning each.

## ADDED Requirements

### Requirement: Recompute from pinned Silver snapshots
DP3 SHALL read `silver.attempts`, `silver.payment_events` and `silver.labels` at snapshot IDs fixed when the run starts. It SHALL replace every gold table in full on each run, writing to the run's audit branch, and SHALL publish to `main` only after every gold check passes. Each gold commit SHALL record the published Silver snapshot IDs it was built from. Rerunning on the same Silver snapshots SHALL produce equal content in every column, `created` included. Lifecycle-invalid attempts and labels whose linkage is not `linked` SHALL NOT contribute to any gold table.

#### Scenario: Rerun on unchanged Silver

- **WHEN** DP3 runs twice without a Silver change
- **THEN** every gold table has equal content after each run, and both commits name the same Silver snapshot IDs

#### Scenario: Silver advances during a run

- **WHEN** Silver commits a new snapshot while DP3 is running
- **THEN** the DP3 output reflects only the snapshots recorded at its start

### Requirement: Gold naming and relationships
Gold Iceberg tables SHALL live in the `gold` namespace and use role prefixes:

- `dim_` for dimensions;
- `feat_` for feature tables;
- `label_` for label tables;
- `fact_` for facts.

Bronze and Silver SHALL keep their `bronze` and `silver` namespaces. Timestamp columns SHALL end in `_at` or `_ts`, surrogate keys in `_sk`, and booleans SHALL start with `is_`. Entity keys SHALL be integration-scoped: customers by (`integration_id`, `customer_id`), payment methods by (`integration_id`, `payment_method_fingerprint`), attempts by (`integration_id`, `payment_id`, `attempt_id`). The documented relationship diagram SHALL show every gold table and mart, with these keys as join columns and the point-in-time join conditions on the dimension and feature tables.

#### Scenario: Join a mart row to its customer version

- **WHEN** an analyst joins a `fact_payment_attempt` row to `dim_customer`
- **THEN** exactly one dimension version matches, on the integration-scoped customer key with `valid_from_ts <= started_at < coalesce(valid_to_ts, +infinity)`

### Requirement: SCD2 customer dimension
`gold.dim_customer` SHALL hold one row per version of each customer's observed profile:

- external buyer ID;
- customer account creation time;
- shipping country, region, postal code and address fingerprint, taken from the AttemptStarted checkout context;
- the familiar-device set: the sorted devices the customer used on at least two attempts within the 90 days up to and including the attempt, computed from that attempt and earlier ones only.

A new version SHALL start at the first attempt, ordered by start time and then attempt identity, whose profile differs from the previous version's in any tracked attribute, compared null-safely. `valid_from_ts` SHALL be that attempt's start time. `valid_to_ts` SHALL be the next version's `valid_from_ts`, and SHALL be null for the current version. `is_current` SHALL be true exactly when `valid_to_ts` is null. Each version SHALL carry `customer_sk`, a deterministic hash of the customer key and `valid_from_ts`, and the attempt that introduced it. Versions of one customer SHALL NOT overlap or leave gaps.

#### Scenario: One-off shipping address

- **WHEN** a customer's attempts use address A, then address B once, then A again
- **THEN** the dimension has three versions (A, B, A), the first two closed by the next version's start, and only the last current

#### Scenario: Device rotation

- **WHEN** a customer who used devices X and Y starts using Z twice and stops using X for 90 days
- **THEN** a version adds Z to the familiar set at Z's second use, and a later version drops X at the first attempt after X's last use is more than 90 days old

#### Scenario: One-off device

- **WHEN** a customer uses a new device once
- **THEN** no new version starts for it

#### Scenario: Unchanged profile

- **WHEN** a customer's attempts all carry the same tracked attributes
- **THEN** the dimension has one current version starting at the customer's first attempt

### Requirement: Daily-anchored history features
`gold.feat_customer_daily` and `gold.feat_payment_method_daily` SHALL hold the `fraud-history-batch-v1` quantities for 7- and 30-day windows. The anchor `s` is a UTC midnight, and each window covers attempts with attempt time in `[s − window, s)`. Each row SHALL carry the entity key, `event_timestamp` = `s`, and `created`, which is the commit time of the pinned `silver.attempts` snapshot. A row SHALL exist for an entity at an anchor exactly when any of its quantities differs from its previous row, including the first anchor after its last attempt leaves the 30-day window. The latest row at or before any time therefore equals the definition at the latest anchor at or before that time. Anchors SHALL NOT be later than the last midnight at or before the pinned Silver commit. A window not fully covered by Silver history SHALL make its quantities null, not zero.

#### Scenario: Attempt leaves the 7-day window

- **WHEN** a customer's only attempt has attempt time on day D
- **THEN** rows exist at anchors D+1 (count 1 in both windows), D+8 (7-day count 0, 30-day count 1) and D+31 (both counts 0), and no other rows exist for that customer

#### Scenario: Point-in-time lookup excludes the scored attempt's day

- **WHEN** a training example has attempt time t on day D
- **THEN** the latest feature row at or before t has `event_timestamp` at or before D 00:00 and includes no attempt from day D

#### Scenario: Early history

- **WHEN** an anchor is less than 30 days after the first Silver day
- **THEN** its 30-day quantities are null and its 7-day quantities are filled once 7 days are covered

### Requirement: Point-in-time label table
`gold.label_attempt` SHALL hold one row per attempt and label source (simulation truth or chargeback), using the earliest available linked observation for that pair. Each row SHALL carry:

- the attempt key, customer key and payment-method key;
- `event_timestamp`, equal to the attempt time used by the features;
- `label` (fraud or not), `label_source` and `label_available_at`.

Consumers SHALL be able to join it to the feature tables on entity key and `event_timestamp`, and to filter it by `label_available_at` at a training cutoff, without reading Silver. The absence of a chargeback row SHALL NOT be represented as a negative label.

#### Scenario: Chargeback not yet available at the cutoff

- **WHEN** a training cutoff precedes a chargeback's `label_available_at`
- **THEN** filtering `label_available_at <= cutoff` excludes that row, and the attempt has no chargeback label at that cutoff

### Requirement: Incremental gold mode
DP3 SHALL accept `mode=historical` or `mode=incremental`, with `historical` as the default for manual submits. In incremental mode it SHALL read DP2's change feed since the last published DP3 run, recompute only:

- the affected customers and payment methods;
- the entities whose windows change between the previous and new anchor caps;
- the affected payments' labels;

and replace their rows on the audit branch. The result SHALL equal a historical run over the same Silver in every column except `created`, which keeps the commit time of the Silver snapshot that last recomputed the row. Incremental mode SHALL refuse to run, naming historical mode as the remedy, when there is no prior DP3 run, when the feed contains a historical DP2 run since the last DP3 run, when an affected time precedes the coverage start, or when the logic version changed.

#### Scenario: Incremental equals historical

- **WHEN** DP3 runs incrementally after each of several DP2 incremental runs
- **THEN** every gold table equals a historical DP3 run over the final Silver, apart from `created`

#### Scenario: Window expiry without new data

- **WHEN** an entity has no changed payments but its only attempt leaves the 7-day window between two DP3 runs
- **THEN** the incremental run writes its expiry row at the same anchor a historical run would

#### Scenario: Historical DP2 run in the feed

- **WHEN** the change feed contains a historical DP2 run since the last DP3 run
- **THEN** incremental DP3 fails and asks for a historical DP3 run

### Requirement: Gold table layout
Feature and label tables SHALL be partitioned by day of `event_timestamp`, and `dim_customer` by day of `valid_from_ts`. Every gold table SHALL be created with hash write distribution and the lakehouse target file size, and SHALL be covered by the `maintain-lakehouse` compaction and snapshot expiry.

#### Scenario: One-month feature read

- **WHEN** a consumer reads one month of `feat_customer_daily`
- **THEN** Iceberg plans only that month's partitions
