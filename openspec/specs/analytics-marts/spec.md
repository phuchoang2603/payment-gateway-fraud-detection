# analytics-marts Specification

## Purpose
Serve analytical queries from native ClickHouse MergeTree marts built from the lakehouse. The marts carry sort keys and skip indexes chosen for fixed queries, are rebuilt atomically, sit behind least-privilege users, and come with measured before/after indexing evidence.

## Requirements

### Requirement: Attempt and merchant-day marts
ClickHouse SHALL hold two native MergeTree tables in the `marts` database.

- **`marts.fact_payment_attempt`:** one row per lifecycle-valid Silver attempt, with the attempt, customer and payment-method keys, the merchant, the amount and currency, start and client attempt times, the risk decision, the outcome and payment status, the earliest available simulation-truth label and scenario, and the earliest available chargeback time.
- **`marts.fact_merchant_day`:** one row per integration, merchant and UTC start day. It holds attempt, decided, succeeded, risk-declined and processor-declined counts, amounts, distinct customers, chargeback and truth-fraud counts, and their rates. Each rate uses the documented denominator and is null when the denominator is zero.

The marts SHALL keep a rolling retention window of the configured number of days, 90, measured from the current date. Expiry SHALL be enforced by a table `TTL` that drops whole expired days. Each build SHALL insert only attempts inside that window. A one-row `marts.build_info` table SHALL record each build's window start and end, the days it rebuilt, the Silver snapshot IDs, and the build time.

The marts SHALL be built by ClickHouse from the `lakehouse` database with `INSERT … SELECT`. No Spark job and no export SHALL write them. Column names SHALL be stable and documented, so the dashboards and the later analytics tool can depend on them.

#### Scenario: Reconcile the fact with Silver

- **WHEN** a mart build completes
- **THEN** `fact_payment_attempt` has exactly as many rows as there are lifecycle-valid `silver.attempts` rows inside the recorded window of the Silver snapshot the build read, with no duplicate attempt key

#### Scenario: Retention window

- **WHEN** a build runs on day T over Silver that holds 365 days
- **THEN** the marts hold only attempts with start days from T − 89 onward, and `build_info` records that window

#### Scenario: Day ages out between builds

- **WHEN** a day's attempts become older than the retention window
- **THEN** ClickHouse drops that day's partition without a rebuild

#### Scenario: Rate with an empty denominator

- **WHEN** a merchant has attempts on a day but none reached a decided outcome
- **THEN** its approval rate for that day is null, not zero

### Requirement: Staged, idempotent mart builds
Each run SHALL build its mart days into `__staging` tables, run every mart contract rule against the staging tables, and only after all of them pass replace each rebuilt live partition atomically with `REPLACE PARTITION`. Readers SHALL see each day either before or after a run, never partly built. A failed build or failed check SHALL leave the live tables unchanged. Rebuilding from the same Silver snapshot SHALL produce equal content. Staging tables left by a failed build SHALL be dropped before the next build starts. The fact SHALL be built by staging the rebuilt days' attempts and the per-attempt label summary once, each ordered by a payment-hash bucket, and joining one bucket of each at a time with the attempts as the hash join's build side, so each label is read once per build and a query's memory follows one bucket.

#### Scenario: Build fails midway

- **WHEN** a mart insert fails after writing part of a staging table
- **THEN** the live marts still hold their previous days and the stage fails

#### Scenario: Rerun

- **WHEN** the mart stage runs twice on unchanged Silver
- **THEN** both marts have equal content after each run

### Requirement: Day-range mart builds
The mart stage SHALL read published Silver only, and SHALL rebuild, within the retention window, the run's days and the 45-day label lookback before them, which covers every attempt a label available in the range can belong to and every day whose 21-day chargeback window completes in the range. It SHALL rebuild the months containing them for `fact_merchant_day`, audit those staging partitions, and then replace each live partition atomically. The live marts SHALL then equal a build over every day of the window on the same Silver.

#### Scenario: Late chargeback

- **WHEN** a chargeback arrives for an attempt started 20 days ago and the pipeline runs the arrival day
- **THEN** that start day of `fact_payment_attempt`, and its month of `fact_merchant_day`, are rebuilt and replaced

### Requirement: Query-driven sort keys and skip indexes
Each mart SHALL declare a partition key, an `ORDER BY` key and data-skipping indexes, each justified in the design by a named fixed query:

- time ranges, network-wide and per merchant, served by a time-first primary key that plain filters on the attempt start time can prune;
- one customer's or payment method's attempts, served by bloom-filter indexes;
- high-risk-score and outcome filters, served by minmax and set indexes.

#### Scenario: Time-range filter prunes by key

- **WHEN** `EXPLAIN indexes = 1` runs for a 24-hour filter on `started_at` alone
- **THEN** the plan shows partition and primary-key pruning selecting only the granules of those days

#### Scenario: Customer lookup skips granules

- **WHEN** `EXPLAIN indexes = 1` runs for one customer's 30-day attempts on `fact_payment_attempt`
- **THEN** the plan shows the customer bloom-filter index selecting fewer granules than the partition pruning alone

### Requirement: Warehouse indexing evidence
The indexing evidence SHALL be `EXPLAIN indexes = 1` plans and `system.query_log` read rows, bytes and selected marks for the fixed queries on the published prod marts, recorded with the `build_info` window they read. An on-demand `warehouse-benchmark` MAY additionally run each fixed query against raw Iceberg, an unsorted MergeTree copy and the indexed mart; when it runs, every variant of a query SHALL return the same result, it SHALL write its report to object storage, drop its scratch copies and never run in the pipeline workflow.

#### Scenario: Variants disagree

- **WHEN** one benchmark variant of a query returns a different result from the others
- **THEN** the benchmark fails and names the query and variant

### Requirement: Least-privilege warehouse users
ClickHouse SHALL define two new users, each with an externally managed password and a cluster-network restriction:

- **`marts` (writer):** may create, insert into, exchange and drop tables only in `marts` and the benchmark scratch database. It may read `lakehouse.*` and its own query log entries, and drop caches for the benchmark. It has no access management.
- **`grafana` (reader):** has a read-only profile with execution-time, memory and result-size limits, and `SELECT` on `marts.*` only.

The `openmetadata` user SHALL be able to read `marts.*` for discovery, tests and profiling.

#### Scenario: Dashboard user reads raw Silver

- **WHEN** the `grafana` user queries a `lakehouse` table
- **THEN** ClickHouse refuses the query for lack of privilege

#### Scenario: Dashboard user writes

- **WHEN** the `grafana` user issues an `INSERT` or DDL statement
- **THEN** ClickHouse refuses it as read-only
