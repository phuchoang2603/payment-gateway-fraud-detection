## Purpose

Serve analytical queries from native ClickHouse MergeTree marts built from the lakehouse. The marts carry sort keys and skip indexes chosen for fixed queries, are rebuilt atomically, sit behind least-privilege users, and come with measured before/after indexing evidence.

## ADDED Requirements

### Requirement: Attempt and merchant-day marts
ClickHouse SHALL hold two native MergeTree tables in the `marts` database.

- **`marts.fact_payment_attempt`:** one row per lifecycle-valid Silver attempt, with the attempt, customer and payment-method keys, the merchant, the amount and currency, start and client attempt times, the risk decision, the outcome and payment status, the earliest available simulation-truth label and scenario, and the earliest available chargeback time.
- **`marts.fact_merchant_day`:** one row per integration, merchant and UTC start day. It holds attempt, decided, succeeded, risk-declined and processor-declined counts, amounts, distinct customers, chargeback and truth-fraud counts, and their rates. Each rate uses the documented denominator and is null when the denominator is zero.

The marts SHALL keep a rolling retention window of the configured number of days, 180, measured from the current date. Expiry SHALL be enforced by a table `TTL` that drops whole expired days. Each build SHALL insert only attempts inside that window. A one-row `marts.build_info` table SHALL record each build's window start and end, the Silver snapshot IDs, and the build time.

The marts SHALL be built by ClickHouse from the `lakehouse` database with `INSERT … SELECT`. No Spark job and no export SHALL write them. Column names SHALL be stable and documented, so the dashboards and the later analytics tool can depend on them.

#### Scenario: Reconcile the fact with Silver

- **WHEN** a mart build completes
- **THEN** `fact_payment_attempt` has exactly as many rows as there are lifecycle-valid `silver.attempts` rows inside the recorded window of the Silver snapshot the build read, with no duplicate attempt key

#### Scenario: Retention window

- **WHEN** a build runs on day T over Silver that holds 365 days
- **THEN** the marts hold only attempts with start days from T − 179 onward, and `build_info` records that window

#### Scenario: Day ages out between builds

- **WHEN** a day's attempts become older than the retention window
- **THEN** ClickHouse drops that day's partition without a rebuild

#### Scenario: Rate with an empty denominator

- **WHEN** a merchant has attempts on a day but none reached a decided outcome
- **THEN** its approval rate for that day is null, not zero

### Requirement: Atomic idempotent rebuild
Each DP3 run SHALL rebuild both marts in full into staging tables, run every mart contract rule against the staging tables, and only after all of them pass atomically exchange the staging tables with the live tables. Readers SHALL see either the previous build or the new one, never a partial build. A failed build SHALL leave the live tables unchanged. Rebuilding from the same Silver snapshot SHALL produce equal content. Staging tables left by a failed build SHALL be dropped before the next build starts.

#### Scenario: Build fails midway

- **WHEN** a mart insert fails after writing part of a staging table
- **THEN** the live marts still hold the previous build and the stage fails

#### Scenario: Rerun

- **WHEN** the mart stage runs twice on unchanged Silver
- **THEN** both marts have equal content after each run

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
An on-demand benchmark SHALL run each fixed query against the same data three ways: raw Iceberg through `lakehouse`, an unsorted MergeTree copy with no skip indexes, and the indexed mart. For each query and variant it SHALL report:

- rows and bytes read, and selected versus total marks;
- peak memory;
- the cold duration after cache drops, and the median of five warm durations;
- the `EXPLAIN indexes = 1` plan.

It SHALL check that every variant of a query returns the same result, restricting the raw Iceberg variant to the marts' recorded window. It SHALL write its report to object storage and drop its scratch copies. It SHALL NOT run in the DP3 workflow.

#### Scenario: Variants disagree

- **WHEN** one variant of a query returns a different result from the others
- **THEN** the benchmark fails and names the query and variant

#### Scenario: Evidence report

- **WHEN** the benchmark completes on the prod v19 marts
- **THEN** its report holds the per-variant measurements for every fixed query, with the data snapshot it read

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
