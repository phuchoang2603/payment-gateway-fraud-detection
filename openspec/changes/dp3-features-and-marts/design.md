## Context

PR #102 (`rebuild-generation-to-silver`, not merged) produces four Silver Iceberg tables: `silver.payment_events`, `silver.attempts`, `silver.labels` and `silver.rejected_events`. They are partitioned by day and fully recomputed from pinned Bronze snapshots. That PR also gives this change the pieces it builds on:

- **Contracts:** ODCS contracts whose ClickHouse SQL rules `openmetadata.checks` turns into native OpenMetadata test suites.
- **Workflows:** the Argo templates `dp1-bronze` and `dp2-silver`, each a Spark `resource` step plus checks, with an exit-handler publish.
- **Spark sizing:** the `dev` and `large` profiles.
- **ClickHouse:** the app-owned 26.10.2.103 instance, with users `admin`, `langfuse` and `openmetadata` and the `lakehouse` DataLakeCatalog database.
- **Catalog:** OpenMetadata ingestion 2.0.6.

The roadmap's feature-store build order puts DP3 first.

Constraints:

- **Data:** synthetic. Dev runs v18: 90 days, 10k customers, about 0.8M events. Prod runs v19: 365 days, 3M customers, about 190M purchases and 984M events.
- **Customer behaviour in the generator:**
  - each customer's home address and account age are fixed;
  - 2% of purchases ship to a random address, or 35% under compromise;
  - devices rotate every 120 days;
  - each customer makes about 0.17 purchases a day.
- **Feature definitions:** `fraud-snapshot-v2` already fixes the meaning of "attempt time", `[s − window, s)` windows, a snapshot anchor `s` independent of the scored time `t`, and null-not-zero coverage.
- **ClickHouse sizing:** prod has a 4 GiB memory limit (about 3.4 GiB for the server) and a 30 Gi PVC. Dev has a 12 GiB limit and must run on dev-server1 or dev-server2. On v19, a full-history Silver check join exhausted the prod server with hash joins and passed only with external sort-merge (`docs/data/storage.md`, "Full-load check optimization on v19"). `openmetadata.checks` now appends those spill settings to every rule.
- **Platform ownership:** Grafana (VictoriaMetrics k8s stack, `observability`), Doppler and OpenMetadata belong to the platform. This repository only requests changes to them.
- **Process:** no local tests, and no cluster actions while planning.
- **Coursework examples** (`../fsds/example-proj`, read only): `card-fraud-platform` uses `feat_` tables with `event_timestamp = day + 1` and `created`, a fully rebuilt hash-diff SCD2 `dim_customer` (`valid_to` exclusive, one-current and no-overlap tests), a `label_` table, and a naming table with an ERD. None of the examples use ClickHouse, OpenMetadata, Argo or a Grafana warehouse datasource, so those parts are designed here from the platform's own docs.

## Goals / Non-Goals

**Goals:**

- Iceberg gold tables that Feast's offline store, training and ClickHouse can all read, with exact point-in-time semantics.
- Native MergeTree marts whose sort keys and skip indexes are justified by fixed queries, with measured before/after evidence.
- A DP3 workflow, contracts, native tests, profiling and lineage at the same level as DP1 and DP2.
- Dashboards as code, and mart names and schemas stable enough for the later read-only analytics MCP tool.

**Non-Goals:**

- Feast repository objects, materialization, TTLs and feature services (roadmap steps 2–7).
- Streaming features into the offline store; training; drift.
- The LLM platform (#103), and the MCP tool itself.
- DP2's incremental mode. It is on `main` (`a753980`, decision 17 of `rebuild-generation-to-silver`) and publishes its changes through Iceberg change data, with no feed table. DP3's incremental mode, which consumes that change data, is in scope here (decision 9b).
- Making the platform changes: they are listed, not made.

## Decisions

### 1. Two engines, two namespaces, role prefixes

| Layer | Engine and location | Tables | Built by |
|---|---|---|---|
| Bronze | Iceberg `bronze.` | `payment_events`, `fraud_labels`, `rejected_records` | DP1 Spark |
| Silver | Iceberg `silver.` | `payment_events`, `attempts`, `labels`, `rejected_events` | DP2 Spark |
| Gold, ML | Iceberg `gold.` | `dim_customer`, `feat_customer_daily`, `feat_payment_method_daily`, `label_attempt` | DP3 Spark |
| Gold, analytics | ClickHouse native database `marts` | `fact_payment_attempt`, `fact_merchant_day` | DP3 ClickHouse `INSERT … SELECT` |

- **Why the ML tables are in Iceberg.** Feast, training and ClickHouse all read the ML tables, so they live in Iceberg; ClickHouse sees them as ``lakehouse.`gold.feat_customer_daily` ``.
- **Why the facts are in ClickHouse.** They exist only for SQL analytics and indexing, so they live in ClickHouse.
- **Naming conventions.** The namespace or database states the layer and engine; the prefix states the role (`dim_`, `fact_`, `feat_`, `label_`). Bronze and Silver keep schema-based layer names rather than `raw_` and `stg_` prefixes. The rubric accepts "prefix or similar", and renaming #102's tables is out of scope.
- **Column naming.** Keys are integration-scoped. Timestamps end in `_at` (business time) or `_ts` (SCD validity). Surrogate keys end in `_sk`, and booleans start with `is_`. The exceptions are Feast's own `event_timestamp` and `created`.
- **Why the database is not called `gold`.** `marts` avoids having both an Iceberg `gold` namespace and a ClickHouse `gold` database. Confirmed by the user.

**Relationships (documented as a Mermaid ER diagram in `docs/data/offline.md`):**

- `fact_payment_attempt` (attempt grain) → `dim_customer` on (`integration_id`, `customer_id`), point-in-time on `started_at`.
- `fact_payment_attempt` → `fact_merchant_day` on (`integration_id`, `merchant_id`, `toDate(started_at)`).
- `label_attempt` ↔ `fact_payment_attempt` on the attempt key.
- `label_attempt` → `feat_customer_daily` and `feat_payment_method_daily` on the entity key, with `feat.event_timestamp <= label.event_timestamp`, taking the latest row.
- `dim_customer` → `feat_customer_daily` on the customer key.

A DBeaver ER export of the ClickHouse `marts` and `lakehouse.gold` tables is captured as evidence after the prod v19 DP3 run.

*Alternative:* gold facts in Iceberg, built by Spark. Rejected by the agreed scope: ClickHouse already reads the lake, and a Spark mart would add a write path without adding evidence.

### 2. DP3 reads pinned Silver and recomputes gold in full

- **Pinned inputs.** The `gold` Spark job (`batch/gold/features.py`) reads `silver.attempts`, `silver.payment_events` and `silver.labels` at their current snapshot IDs, which it records, with `versionAsOf`, as DP2 does.
- **Writes: write-audit-publish, as Bronze and Silver do since `78765df`.**
  - **Write.** The job runs with `spark.wap.branch = audit-<workflow>` and calls `batch.lakehouse.stage()` on each gold table before writing, so the branch starts from `main`. Each write uses `overwrite(lit(True))`, with `create` on first use. Each commit carries the Silver snapshot IDs through the Iceberg 1.12 write option `snapshot-property.silver.<table>.snapshot_id`.
  - **Audit.** The checks stage runs every `gold-features` rule against the branch heads, pinned with `SETTINGS iceberg_snapshot_id`.
  - **Publish.** The shared `publish-tables` step (`openmetadata.wap`) fast-forwards `main` for all four tables in one catalog transaction, only after every check passes.
  - **Readers.** Feast, training and ClickHouse read `main` only, so they never see unaudited gold.
  - **Failure.** A failed run leaves its branch behind for diagnosis.
  - **Inputs.** DP3 reads Silver at its published `main` refs, through `ref_snapshot`, never an audit branch.
- **Why full recompute.** Silver itself is fully recomputed, so any day can change and there is nothing incremental to follow. Full recompute is idempotent by construction.
- **`created`.** `created` is the commit timestamp of the pinned `silver.attempts` snapshot, not wall-clock time. It is deterministic for one input, increases across Silver runs, and is at or after every anchor in the table, because anchors stop at the last midnight before that commit. The rerun-equality test therefore compares every column.
- **Contributing rows.** Only lifecycle-valid attempts contribute. Their count is zero anyway, by Silver's checks.
- **Run report.** A report at `runs/payment-gateway/dp3/<run>/gold.json` records:
  - the Silver snapshot IDs;
  - per-table rows and added files;
  - the coverage start date;
  - per-phase tasks and time, through `batch.metrics.phase`.

**Where Apache Arrow helps, and where it does not.** Arrow speeds up a pipeline where data crosses between the JVM and Python, or between processes, in columnar form. DP3 avoids that crossing on purpose:

- **Spark:** the job is DataFrame and SQL only, with no Python UDFs, `toPandas` or `collect` of data. Rows stay in the JVM and the vectorized Parquet reader, so `spark.sql.execution.arrow.pyspark.enabled` would change nothing. A review rule for the implementation: any future Python UDF in `batch` must be an Arrow-based `pandas_udf` or `mapInArrow`, never a row-at-a-time `udf`.
- **ClickHouse marts:** `INSERT … SELECT` runs entirely inside ClickHouse, which reads Iceberg Parquet with its own native reader. No data passes through Python.
- **Benchmark and report code:** results are small aggregates, so Arrow transfer would save nothing measurable.
- **Where it does matter (follow-ups):** Feast `get_historical_features` and training read large point-in-time joins into Python. That change should enable Arrow transfer (`spark.sql.execution.arrow.pyspark.enabled`, `to_arrow()`), and it can measure the gain against the non-Arrow path. The generator already writes Parquet through `pyarrow`.

*Alternative:* `MERGE INTO` SCD2 and incremental feature days. Rejected: Silver can rewrite history, so a merge-based dimension would drift from it.

### 3. Feature grain: daily anchors, change-point rows

- **Definition file.** A new feature definition, `contracts/features/fraud-history-batch-v1.yaml` with `.examples.json`, uses the v2 format and vocabulary.
- **Anchor `s`.** `s` is each UTC midnight. Flink keeps the short windows (5 minutes, 1 hour, 24 hours); this definition covers 7 and 30 days.
- **Attempt time.** `attempt_time = coalesce(attempted_at, started_at)`, as in v2.
- **Eligibility.** A lifecycle-valid attempt is eligible at anchor `s` for window `w` when `s − w <= attempt_time < s` and `started_at < s`. Outcome-based quantities also need `outcome_at < s`.

**Why daily anchors, not one row per attempt time.** Online serving receives the long windows from a daily batch, so Go Fraud sees values as of the last DP3 anchor. Per-attempt-time training rows would be more current than serving can ever be, which is training/serving skew. With `event_timestamp = s`, Feast's "latest row at or before t" gives a training example exactly the anchor serving would have had, assuming DP3 finished. The residual skew is the DP3 delay after midnight, recorded in the definition's `availability` block. This corrects the sentence on the feature-store page that says "as of each attempt time".

**Why rows only at change points.** A row per entity per day would be about 1.1B rows a year for each entity type in prod. Instead, a row is written at an anchor only when a value changes:

- an attempt enters a window;
- an attempt leaves the 7- or 30-day window;
- coverage switches from null to filled.

The latest row at or before any time is then exact. Each entity's last row is all zeros, written at the anchor where its last attempt leaves the 30-day window.

**Shared engine.** The interval-delta and change-point computation lives in one reusable `batch/` function. Its anchor rule is a parameter: a daily grid for these tables, or exact change instants for the Feast change's `feature-backfill` job, which writes `gold.feat_stream_*_backfill` (user decision, Feast change `25bab4e`). One implementation serves both. The import path and signature go in the DP3 handback.

**Computation (all quantities are exact):**

- **Contribution intervals.** Each eligible attempt contributes to the inclusive anchor-day interval `[enter, exit)` for each window, where:
  - `enter = max(day(attempt_time), day(started_at)) + 1`, also bounded by `day(outcome_at) + 1` for outcome-based counts;
  - `exit = day(attempt_time) + w + 1`.

  An attempt whose `enter >= exit` contributes nothing.
- **Counts and sums.** These are cumulative sums of +value at `enter` and −value at `exit`, ordered by anchor day within each entity.
- **Distinct counts.** First merge the intervals of each (entity, value), then apply the same +1/−1 deltas. No collect-set windows are needed.
- **Rows.** The change points are the distinct delta days, plus the coverage-start days `H + 7` and `H + 30`, where `H` is the first Silver attempt day. The H + 7 and H + 30 rows exist only for entities whose window holds something on that day.
- **Coverage.** At anchor `s`, window `w` is complete when `s − w >= H`; otherwise every column for that window is null.
- **Cutoff.** Anchors stop at `floor_day(commit time of pinned silver.attempts)`.

**Columns.** Each window `w` in {`7d`, `30d`} has these columns; rates are null when their denominator is 0.

| Column | Definition | Customer | Payment method |
|---|---|---|---|
| `attempt_count_w` | eligible attempts | ✓ | ✓ |
| `amount_sum_minor_w` | sum of `amount_minor` over eligible attempts (single-currency synthetic data; a contract rule checks `currency` is one value) | ✓ | ✓ |
| `decided_count_w` | eligible attempts with outcome `succeeded` or `declined` and `outcome_at < s` | ✓ | ✓ |
| `declined_count_w` | the subset of decided attempts that were declined | ✓ | ✓ |
| `decline_rate_w` | `declined_count_w / decided_count_w`; the denominator is decided attempts, never all attempts, because pending and unknown outcomes were not known at `s` | ✓ | ✓ |
| `risk_declined_count_w` | declined with `decline_source = RISK`, the policy's own past decisions on earlier attempts | ✓ | ✓ |
| `distinct_merchant_count_w` | distinct merchants | ✓ | ✓ |
| `distinct_device_count_w` | distinct non-null device fingerprints | ✓ | – |
| `distinct_payment_method_count_w` | distinct payment-method fingerprints | ✓ | – |
| `distinct_customer_count_w` | distinct customers on the card; a card-testing and stolen-card signal | – | ✓ |

Keys are (`integration_id`, `customer_id`) and (`integration_id`, `payment_method_fingerprint`), plus `event_timestamp` and `created`.

**Keys and Feast.** Feast (v0.67) allows only one join key per `Entity`. The follow-up Feast change therefore declares `integration` and `customer` (or `payment_method`) as two entities on each feature view, which Feast supports. The tables keep separate key columns and need no concatenated key.

- **Never inputs:** labels, scenarios, risk scores and anything about the scored attempt itself. Daily anchors exclude the scored attempt's own day.
- **Unknown entities:** an entity seen for the first time has no row, so a lookup returns null ("unknown"), not zero. Feast TTLs (step 6) decide staleness later.
- **Conformance.** The examples cover boundaries, expiry without input, client and server time skew, outcomes after the anchor, an empty denominator, coverage and integration separation. A CI test runs the Spark job over them on a local Iceberg warehouse, as the Silver tests do.

**Size estimate.** One customer has about 60 active days a year, so at most about 180 change points. Prod is therefore at most about 500M rows per feature table, roughly 15 GB of Parquet. Dev is under 1M rows. The run report records the actual counts.

*Alternatives:*

- One row per entity per active day, as the coursework example does. Rejected: values go stale after expiry, so point-in-time lookups would be wrong.
- One row per attempt at `t`. Rejected because of the training/serving skew above.

### 4. `dim_customer` SCD2 over the observed profile

**Source.** The `attempt_started` context in `silver.payment_events`, joined to lifecycle-valid attempts.

**Tracked attributes:**

- `external_buyer_id` and `customer_account_created_at`;
- `shipping_country`, `shipping_region`, `shipping_postal_code` and `shipping_address_fingerprint`;
- `familiar_device_fingerprints`: the sorted set of devices the customer used on at least two attempts within the 90 days up to and including this attempt. The set is computed only from that attempt and earlier ones, so a version never depends on later data.

**Why the familiar set, not the attempt's device.** Each customer alternates at random between two devices, so a per-attempt `device_fingerprint` attribute would open a version on about every other attempt, making the dimension one row per attempt. The familiar set changes only on real device events:

- a new device used twice joins it (the account-takeover and stolen-card patterns);
- the 120-day device rotation in the generator swaps the pair;
- a device unused for 90 days drops out.

One-off devices (the 5% unfamiliar contexts) never enter the set. They stay visible per attempt in `fact_payment_attempt.device_fingerprint` and in the `distinct_device_count` features.

**Build steps:**

1. Order each customer's attempts by (`started_at`, `payment_id`, `attempt_id`).
2. Compute `familiar_device_fingerprints` per attempt: count each device's uses over the customer's attempts in `(started_at − 90 days, started_at]`, keep devices with two or more uses, and sort them. Then compute `profile_hash = sha2(to_json(named_struct(...)))` over all tracked attributes. JSON keeps nulls and array order, so the comparison is null-safe and deterministic.
3. Keep the rows where the hash differs from `lag(profile_hash)`.
4. Set `valid_from_ts = started_at` and `valid_to_ts = lead(valid_from_ts)`, null for the current version. `is_current = valid_to_ts IS NULL`.
5. Set `customer_sk = sha2(integration_id|customer_id|valid_from_ts)` and `version_number = row_number()`. Attempts at the same instant open at most one version, the last in key order, so `customer_sk` is unique and no version is empty. The table is partitioned by `months(valid_from_ts)`, since about 30M versions would make day partitions tiny.
6. Record `source_payment_id` and `source_attempt_id`, the attempt that introduced the version.

**Why null and not `9999-12-31` for the current version.** ClickHouse `DateTime64` stops at 2299, so a sentinel date would clip or fail when ClickHouse reads it through `lakehouse`.

**Expected versions.**

- **Addresses:** one-off shipping addresses (2% normally, 35% under compromise) and account-takeover address changes add about 0.25 versions per customer per year.
- **Device warm-up:** the familiar set starts empty and grows to one device, then two. That gives two to three warm-up versions per customer at the start of history.
- **Device rotation:** in v19, each 120-day rotation adds about two versions, as the new pair enters and the old pair leaves after 90 days. That is about six per customer per year. v18 covers 90 days, so dev shows only warm-up and compromise changes.
- **Totals:** about 25–30M rows in prod (3M customers) and about 40k in dev.

These versions are genuine fraud signals ("new familiar device", "address changed recently"), and a later feature version could read them.

**Checks:**

- exactly one current version per customer;
- no overlap and no gap: each `valid_to_ts` equals the next version's `valid_from_ts`;
- `valid_from_ts < valid_to_ts`;
- `customer_sk` is unique.

*Alternatives:*

- Track the attempt's raw device. Rejected: it versions on almost every attempt.
- Track the modal device or address per day. Rejected: it hides the one-off address changes that matter for fraud.

### 5. `label_attempt`

One row per (attempt key, `label_source`), taken from `silver.labels` where `linkage = 'linked'`. The earliest `label_available_at` wins, with ties broken by `observation_id`.

**Columns:**

- the attempt, customer and payment-method keys (from `silver.attempts`);
- `event_timestamp`, the same attempt time as the features;
- `label` (`is_fraud`), `label_source`, `label_available_at` and `occurred_at`.

**Point-in-time use.** Training takes `label_attempt` filtered by `label_available_at <= cutoff` as the Feast entity dataframe, and joins features on entity key and `event_timestamp`.

**Label sources.** Simulation truth and chargebacks stay separate rows. A missing chargeback is never a negative, as in #102's labels contract.

**Checks:** the key is unique; `label_available_at >= occurred_at`; the source is a known value; every attempt key exists in `silver.attempts`.

### 6. Marts: DDL, keys and indexes

**Database.** The ClickHouse setup job creates `marts` and `marts_benchmark` as admin, next to `langfuse`. The DDL lives in `src/data-pipeline/batch/batch/gold/sql/marts/*.sql`. The build job owns it and runs `CREATE TABLE IF NOT EXISTS` for the live tables, so a schema change is a `DROP` plus rebuild during the run (no legacy compatibility).

```sql
CREATE TABLE marts.fact_payment_attempt (
  integration_id LowCardinality(String),
  payment_id String, attempt_id String,
  merchant_id String, customer_id String, payment_method_fingerprint String,
  payment_method_type LowCardinality(String), device_fingerprint Nullable(String),
  amount_minor Int64, currency LowCardinality(String),
  started_at DateTime64(6, 'UTC'),
  attempted_at Nullable(DateTime64(6, 'UTC')),
  risk_outcome LowCardinality(Nullable(String)), risk_score Nullable(Float64),
  policy_version LowCardinality(Nullable(String)), model_version LowCardinality(Nullable(String)),
  outcome LowCardinality(String), decline_source LowCardinality(Nullable(String)),
  outcome_at Nullable(DateTime64(6, 'UTC')), payment_status LowCardinality(String),
  truth_is_fraud Nullable(Bool), truth_scenario LowCardinality(Nullable(String)),
  chargeback_available_at Nullable(DateTime64(6, 'UTC')),
  INDEX ix_customer customer_id TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX ix_method payment_method_fingerprint TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX ix_score risk_score TYPE minmax GRANULARITY 4,
  INDEX ix_outcome outcome TYPE set(8) GRANULARITY 4
) ENGINE = MergeTree
PARTITION BY toDate(started_at)
ORDER BY (integration_id, toDate(started_at), merchant_id, started_at)
TTL toDateTime(started_at) + INTERVAL {retention_days} DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE marts.fact_merchant_day (
  integration_id LowCardinality(String), merchant_id String, day Date,
  attempts UInt64, decided UInt64, succeeded UInt64, risk_declined UInt64, processor_declined UInt64,
  amount_attempted_minor Int64, amount_succeeded_minor Int64, distinct_customers UInt64,
  truth_labelled UInt64, truth_fraud UInt64, chargebacks UInt64,
  approval_rate Nullable(Float64),       -- succeeded / decided
  risk_decline_rate Nullable(Float64),   -- risk_declined / decided
  truth_fraud_rate Nullable(Float64),    -- truth_fraud / truth_labelled
  chargeback_rate Nullable(Float64),     -- chargebacks / succeeded
  chargeback_window_complete Bool        -- day + 21 days <= build cutoff (max chargeback delay)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (integration_id, day, merchant_id)
TTL day + INTERVAL {retention_days} DAY DELETE;
```

**Fixed queries and the structure that serves each one:**

| Query | Used by | Served by |
|---|---|---|
| Q0: all attempts in a time range (for example the last 24 hours, by hour and outcome) | overview, MCP | partition pruning, then the primary key's time prefix (`integration_id`, `toDate(started_at)`) |
| Q1: one merchant's attempts and outcomes over a date range | Merchant drill-down, MCP | time prefix, then `merchant_id` within each day |
| Q2: one customer's attempts over 30 days | investigation, MCP | `ix_customer` bloom filter plus partition pruning |
| Q3: one card's attempts across customers | card-testing investigation | `ix_method` bloom filter |
| Q4: approved attempts with `risk_score > 0.9`, and declines by outcome | Fraud outcomes | `ix_score` minmax and `ix_outcome` set |
| Q5: daily KPIs over the whole network | overview | `fact_merchant_day` (pre-aggregated, about 20k × 365 rows in prod) |

**Notes on the key choice:**

- **Time first, as ClickHouse recommends.** Key columns go in ascending cardinality and by how often queries filter on them. Almost every dashboard and tool query has a time range. A day has 365 values in v19 and a merchant has 20,000, so the key is `(integration_id, toDate(started_at), merchant_id, started_at)`.
  - The key uses the expression `toDate(started_at)` rather than a separate date column. ClickHouse analyses monotonic functions, so a plain `started_at >= … AND started_at < …` filter, which is what Grafana's `$__timeFilter(started_at)` produces, prunes through the primary key with no rewriting.
  - Inside each day, rows are ordered by merchant and then by time. One merchant's range is a contiguous slice per day, and "latest attempts" reads stay ordered.
  - Daily partitions line up with the day-granular TTL (decision 7), so expiry drops whole partitions instead of rewriting parts. That gives about 180 partitions at the 180-day retention, within ClickHouse's guidance of keeping the partition count in the hundreds to low thousands. Each partition holds about 0.5M rows in prod and about 9k in dev. `fact_merchant_day` is small, so it keeps monthly partitions and row-level TTL.
- **Integration first.** `integration_id` has one value today, but it leads the key so per-integration queries stay correct.
- **Hot merchant.** One merchant takes about 56% of rows, so Q1 on the hot merchant still reads about half of each day. The evidence reports Q1 for both the hot merchant and a median merchant, so the skew is visible instead of hidden.
- **Key choice considered:** `(merchant_id, day)` first. It is slightly faster for one merchant over a long range, but every network-wide time query would scan whole partitions. A time-first key serves both.
- **Granularity.** Bloom filters use `GRANULARITY 4`, one index block per 4 × 8192 rows, to balance index size against skipping. Customer IDs are near-unique per granule block, which is why a bloom filter is chosen over a set index.
- **Projections.** A customer-ordered projection would beat the bloom filter on Q2 but double storage. Prod's 30 Gi PVC is shared with Langfuse, so projections are not used. Revisit them if the evidence shows a weak bloom result.

### 7. Mart build: monthly batches, staging, exchange

**Job.** `batch/gold/marts.py` runs as a plain container on the Spark image, which already carries the `batch` package. It talks to ClickHouse over HTTP as user `marts` through a small standard-library client, as the chart's setup job and the tests do, so the image gains no dependency.

**Steps:**

1. Drop any `marts.*__staging` tables left by a failed run, then create empty staging tables from the DDL.
2. Compute the window from the wall clock, matching the tables' `TTL`. `window_start = today − (marts.retentionDays − 1)`, with 180 days in dev and prod. `window_end` is the latest Silver start day. The build inserts only attempts inside the window, so it never writes rows that the `TTL` would expire straight away.

   **Why a wall-clock `TTL`.** This is the standard ClickHouse retention for event data: `TTL … + INTERVAL N DAY DELETE`, with `ttl_only_drop_parts = 1` on day partitions, so expired days leave as whole partitions with no part rewrites. It is the right shape once the live simulator feeds Bronze, then Silver and DP3, continuously. Rebuilds and the background `TTL` then agree on one rolling 180-day window. The `{retention_days}` placeholder in the DDL comes from `marts.retentionDays`.
3. For each month in the window, run one `INSERT INTO marts.fact_payment_attempt__staging SELECT … FROM lakehouse.`silver.attempts` LEFT JOIN (labels aggregated per attempt) …`:
   - Attempts are filtered to the month and the window, so Iceberg partition pruning applies.
   - Labels are filtered to `occurred_at` within the month and `label_available_at >= month start`, which prunes earlier partitions and uses file statistics to skip later ones.
   - The join runs with `join_algorithm = 'full_sorting_merge'`, external sort and group-by thresholds, and the profile's memory limit: the settings that let the v19 Silver checks finish on the prod instance. Hash and grace-hash joins are not used.
   Labels are first aggregated once per attempt into an internal `marts.attempt_labels__staging` MergeTree table (earliest simulation truth and scenario, earliest chargeback), so each monthly insert joins a sorted local table instead of rereading Iceberg labels; it is dropped after the inserts.
4. Build `fact_merchant_day__staging` from the staged fact with one `INSERT … SELECT … GROUP BY`, and write `build_info__staging`: one row with the run and mode, `window_start`, `window_end`, `rebuilt_days` (every day of the window for a historical build), `anchor_cap`, the Silver snapshot IDs and `built_at`. The mart rules scope their reconciliation to `rebuilt_days`, so one rule set audits both a full build and an incremental one.
5. Reconcile: the staged fact's row count must equal the lifecycle-valid count of `lakehouse.`silver.attempts`` inside the window. A mismatch fails before the swap.

   **Write-audit-publish for the marts.** The `__staging` tables are the audit copy, the ClickHouse counterpart of the Iceberg audit branch. The marts stage ends after step 5 and leaves staging in place. The checks stage then runs every `payment-marts` rule against the staging tables: when `WAP_BRANCH` is set, `{object}` resolves to `marts.<table>__staging`. Results attach to the live table's OpenMetadata entity, the same way Iceberg rules audit a branch but report on the table. Only a separate `publish-marts` step, after all checks pass, runs step 6.
6. Run `EXCHANGE TABLES marts.fact_payment_attempt AND marts.fact_payment_attempt__staging`, then the same for the merchant and `build_info` tables, one statement each. Each exchange is atomic in an Atomic database, but a multi-pair exchange is not atomic as a batch, so there is no benefit to combining them. Then drop the staging tables, which now hold the old data. `EXCHANGE` has no privilege of its own: it needs `SELECT`, `INSERT`, `CREATE TABLE` and `DROP TABLE` on both tables.

**Consistency.** Spark pins Silver by snapshot ID. The marts stage reads the Spark stage's report and passes the same IDs to every lakehouse read with `SETTINGS iceberg_snapshot_id = <id>`. ClickHouse documents that setting (25.4+) only for the `iceberg()` table function and the Iceberg engine. The 26.x source reads it from the query context regardless of table kind, but no test covers DataLakeCatalog tables, so a dev spike confirms it on ``lakehouse.`silver.attempts` `` before implementation relies on it. Either way, the DP2 writer mutex (decision 9) keeps Silver unchanged while DP3 runs. If the spike fails, the stage compares `system.iceberg_history`'s current snapshot with the pinned ID and fails on a mismatch.

**Idempotency.** The same Silver snapshot gives the same marts. A failed check or a crash before publication leaves the live tables untouched. A crash between exchanges in `publish-marts` is the only partially published state: the step retries on error, and the next run rebuilds and audits all tables again.

**Storage.** With the 180-day prod window, `fact_payment_attempt` holds about 100M rows, estimated at 3–4 GB compressed, or 6–8 GB while a build holds both copies. That fits the existing 30 Gi PVC beside Langfuse, so the PVC is unchanged.

**Dashboards and tools.** Dashboards and the later analytics tool read `build_info` to show the window, so a 180-day history is not mistaken for missing data.

**Consequence while live data is off.** v19 and v18 end on 2026-10-28. Until the live simulator feeds new days, each rebuild and each `TTL` merge drops the oldest days. The marts empty out about 180 days after the last generated day, around 2027-04-26. The prod evidence (the benchmark, dashboards and profiles) must therefore be captured soon after the v19 DP3 run, and each report records `build_info`'s window. The Iceberg gold tables are unaffected: they keep all of Silver.

*Alternative:* incremental `REPLACE PARTITION` by month, after comparing per-month fingerprints between Iceberg and the mart. Deferred: a full rebuild is simpler, and Silver itself is not yet incremental.

### 8. Warehouse indexing evidence

`warehouse-benchmark` is an on-demand workflow template with one container step that runs `batch/gold/warehouse_benchmark.py`.

**Variants, built from the same pinned data:**

- **A, raw Iceberg:** ``lakehouse.`silver.attempts` `` joined to labels and restricted to `build_info`'s window, the logical equivalent of each query.
- **B, unsorted:** `marts_benchmark.fact_payment_attempt_unsorted`, built `AS SELECT * FROM marts.fact_payment_attempt` with `ORDER BY tuple()` and no skip indexes. It has the same columns and compression and no indexing.
- **C, indexed:** the live mart.
- **C′, primary key only:** the live mart with `use_skip_indexes = 0`.

A against C shows the full benefit of the warehouse. B against C isolates the effect of the sort order and indexes. C′ against C isolates the skip indexes. ClickHouse 26.2+ also has `use_primary_key = 0`, but the unsorted copy is the stronger control because it also loses the data locality that the sort gives.

**Measurements for Q0, Q1 (hot and median merchant), Q2, Q3 and Q4:**

- `EXPLAIN indexes = 1`: the primary key and each skip index, with selected over total parts and granules;
- one cold run after `SYSTEM DROP MARK CACHE`, `SYSTEM DROP UNCOMPRESSED CACHE`, `SYSTEM DROP INDEX UNCOMPRESSED CACHE` and `SYSTEM CLEAR FILESYSTEM CACHE`, plus 5 warm runs with `use_query_cache = 0`;
- from `system.query_log`, matched by `query_id`: `read_rows`, `read_bytes`, `memory_usage`, `query_duration_ms` and `ProfileEvents['SelectedMarks']`.

**Equality guard.** Every variant of a query must return the same row count and the same order-independent checksum (`sum(cityHash64(*))`), or the benchmark fails.

**Report and cleanup.** The report goes to `runs/payment-gateway/warehouse-benchmark/<run>/report.json`, and the scratch table is dropped. The equality guard compares every variant with the indexed mart and names the variant that differs.

**Page.** `docs/data/storage.md` § Warehouse gets one table per query and variant, with the plan excerpts.

### 9. DP3 workflow

```text
dp3-gold (WorkflowTemplate, mutexes: dp3-gold-writer, dp2-silver-writer, onExit: publish)
├── features        SparkApplication batch/gold/features.py, writes audit-<workflow> branch     (ingest)
├── marts           container batch/gold/marts.py build, writes marts.*__staging          (ingest, after features)
├── checks          openmetadata.checks, gold-features on branch heads, payment-marts on staging   (validate)
├── publish-tables  openmetadata.wap, fast-forwards gold main                  (after checks)
├── publish-marts   container batch/gold/marts.py publish, exchanges staging into live   (after publish-tables)
└── profile         OpenMetadata profiler on the published marts                    (after publish-marts)
publish (exit): pipelines dp3-features,dp3-marts; status, Silver snapshot IDs, lineage on success
```

- **Publication order.** Iceberg gold is published first because it is one atomic catalog transaction. If `publish-marts` then fails, gold is current and the marts keep the previous build until the next run; the stage retries on error. Profiling runs on the published tables, because OpenMetadata profiles entities, not staging tables.

- **Why `marts` runs after `features`.** It does not need `features`' output. It runs second only so that the run report can compare the Silver snapshot IDs (decision 7), and so that both stages do not compete on the prod spot pool and the small ClickHouse at once. The graph still shows the ingest stages, one validate stage and the publish steps. Parallel stages are possible later.
- **Mutexes.** Holding `dp2-silver-writer` serializes DP3 with DP2. Argo 3.6+ supports several mutexes on one workflow (to be verified in implementation).
- **Retries.** As in #102: Spark retries on error (prod: limit 10, `Always`, for spot loss), containers retry on error with limit 2, and checks are never retried.
- **Schedule: daily, not every six hours.** DP3 is not added to the six-hourly `dp1-bronze-kafka` CronWorkflow. It gets its own suspended daily CronWorkflow `dp3-gold-daily` at 04:17 UTC, after the 03:43 maintenance run, which references the template with `workflowTemplateRef` so the template's mutexes, placement and exit handler apply.
  - The features are daily-anchored, so a daily run is their natural cadence.
  - The marts are rebuilt in full, so a six-hourly rebuild would cost four times as much for no gain until incremental mode exists.
  - The workflow-level mutexes still apply, because `workflowTemplateRef` runs the whole template rather than a `templateRef` step.
- **RBAC.** No new RBAC is needed. The marts and benchmark containers read the `clickhouse-marts` secret that the clickhouse chart's ExternalSecret already creates in `data` for `CLICKHOUSE_MARTS_PASSWORD`; a second ExternalSecret of the same name would collide. They receive only storage and ClickHouse credentials.
- **Values:**
  - `dp3.enabled`, `dp3.profile`, `dp3.reportRoot`, `dp3.activeDeadlineSeconds`;
  - `marts.clickhouseUrl`, `marts.resources`, `marts.retentionDays` (180 in `values.yaml`, so in both environments);
  - `warehouseBenchmark.enabled`, `warehouseBenchmark.reportRoot`;
  - `dashboards.enabled`.
- **Prod enablement.** `values-prod.yaml` sets DP3 enablement the same way as Silver's.

**Spark sizing:**

| Environment | Profile | Basis |
|---|---|---|
| dev | `dev`: 1 executor × 2 cores, 16 shuffle partitions | v18 is under 1M attempts, and the gold tables are each under 1M rows |
| prod | `large`, as Silver uses it: 11 executors × 3 cores, 9 GiB plus 3 GiB overhead, 24 GiB ephemeral storage, on spot `aws-cpu-burst`; the driver on the on-demand `aws-workers` pool through `spark.driverNodeSelector`; 2,048 shuffle partitions set by `dp3.profile` | About 200M attempts produce about 1.2B interval-delta rows per window family before aggregation. The shuffle is estimated at 40–60 GB across 11 executors, under the 264 GiB of executor ephemeral storage. The first v19 run records tasks and time per phase, which confirms or lowers the profile, as for Silver. |

The marts container requests 100m CPU and 256 Mi, because ClickHouse does the work. ClickHouse itself is sized by the clickhouse chart. Every DP3 step pod uses `data-pipelines.workflowPlacement` and the SparkApplication uses the shared driver placement, so in prod no step or driver runs on spot nodes.

### 9b. Incremental mode

`dp3-gold` takes `mode=historical|incremental`. `historical` is decisions 2–8, a full recompute, and it is the default for manual submits and the mode for the v19 evidence. `incremental` must leave gold and the marts equal to a historical run over the same Silver, except for `created` (below). CI checks this.

**Input: Iceberg change data, not a feed table.** This is DP2 decision 17 and the "Silver change data for consumers" requirement.
- **State.** Every DP3 commit records the `main` snapshots it consumed as snapshot properties: `silver.attempts.snapshot_id`, `silver.labels.snapshot_id` and `silver.payment_events.snapshot_id`. `marts.build_info` records the same IDs. The next incremental run reads its start points from the latest published gold commit.
- **Change sets.** For `silver.attempts` and `silver.labels`, the run creates a changelog view between the consumed snapshot and the current `main` snapshot, with net changes:

  ```sql
  CALL lake.system.create_changelog_view(
    table => 'silver.attempts',
    options => map('start-snapshot-id', '<consumed>', 'end-snapshot-id', '<main>'),
    net_changes => true)
  ```

  Net changes cancel the carry-over rows from rewritten files, including DP2's delete-and-append of unchanged attempts and compaction rewrites. Only real changes remain, so the DELETE rows give the old image and the INSERT rows give the new one. That provides both images' `customer_id`, `payment_method_fingerprint`, `attempted_at` and `started_at` directly.
- **Rejected payments.** DP2 replaces whole payments, so a conflicting redelivery removes a payment's attempts. Those removals show up as DELETEs and drive the recompute of their old entities.
- **Why the attempts changelog also covers `dim_customer`'s profile.** The profile comes from `attempt_started` events. A changed `attempt_started` with the same `event_id` is a conflict, which deletes the attempt rows, and a new `attempt_started` inserts an attempt row. So every profile change surfaces in the attempts changelog, and `silver.payment_events` needs no changelog of its own. Its snapshot ID is still recorded and pinned for reads. The equivalence test includes a conflicting `attempt_started` case to protect this.

**Refusals.** Incremental mode fails, with no silent fallback, and asks for a historical DP3 run when:
- there is no published DP3 commit recording consumed Silver snapshots;
- any `silver.attempts` or `silver.labels` commit in the range has `dp2.mode = historical`, checked by walking the `snapshots` metadata table's summaries between start and end. A historical DP2 run rewrites every row, so it means a full reset;
- a consumed start snapshot no longer exists. Silver keeps snapshots for 7 days, with the last 10 always kept, so DP3 must run at least weekly to stay incremental;
- the earliest Silver attempt day `H` differs from the one the last run recorded (`dp3.history_start`), or coverage was still incomplete at the last anchor cap;
- the gold tables were last written by different DP3 runs;
- a `dp2.logic_version` in the range differs from the one recorded on the last consumed snapshot, or DP3's own logic or contract version changed.

**Per table:**

| Table | Incremental update |
|---|---|
| `feat_*_daily` | The entities to recompute are the affected customers and cards from both images, plus every entity with an attempt in `[C0 − 30 days, C1)`, where `C0` and `C1` are the previous and new anchor caps. Those entities need their new expiry or arrival rows even without changed data. For each one, recompute anchors from `min(earliest affected anchor, C0 + 1 day)`, reading its attempts from 30 days before that anchor. Seed the change-point comparison with its last existing row before that anchor, then replace its rows at and after that anchor in one `MERGE` on the audit branch. The shared engine's entity- and anchor-scoped entry points do this. |
| `dim_customer` | Rebuild the affected customers' full version history from Silver and replace their rows. The familiar-device set is evaluated only at attempts, so time passing alone changes nothing. |
| `label_attempt` | Replace the rows of the attempts named in either changelog. |
| `fact_payment_attempt` | Rebuild only the affected start days inside the retention window into staging. Those are the start days of changed attempts in both images, plus the attempts' start days for changed labels, looked up in the attempts changelog or on `main`. Then audit them, then `ALTER TABLE … REPLACE PARTITION <day> FROM …__staging` for each day in `publish-marts`. Each replace is atomic per day, matching the daily partitions. |
| `fact_merchant_day` | Rebuild the affected months (monthly partitions, small) from the published fact, then `REPLACE PARTITION`. Update `build_info`. |

**Anchors advance with Silver.** The anchor cap is the day of the pinned `silver.attempts` commit, so new anchors, including expiry rows, appear only after a DP2 run commits. A DP2 incremental run with nothing new commits nothing, which leaves the features at the last cap until data arrives.

**`created` in incremental mode.** A row keeps the Silver commit time of the run that last recomputed it. A historical run stamps every row with the current commit time. That is the only allowed difference, so the equivalence test compares every column except `created`. The `created >= event_timestamp` rule still holds, and Feast uses `created` only to break ties between rows with the same `event_timestamp`.

**Schedule.** The suspended daily `dp3-gold` CronWorkflow runs `mode=incremental`, after the six-hourly Kafka schedule's DP2 incremental runs. A refusal fails the scheduled run visibly; the operator then submits `mode=historical`.

**ClickHouse reads.** The marts stage reads the changed days from Silver `main` at the pinned snapshot IDs (`SETTINGS iceberg_snapshot_id`), so it stays consistent with Spark's view.

**Cost.** With the live simulator at one payment every 30 seconds (about 2,900 a day), an incremental run touches a few thousand payments and their entities' last 30 days. A historical run rereads all of v19 Silver. The prod evidence records one of each.

### 10. Contracts and checks

- **New products.** `contracts/data-products/gold-features/` (`dim-customer`, `feat-customer-daily`, `feat-payment-method-daily`, `label-attempt`) and `contracts/data-products/payment-marts/` (`fact-payment-attempt`, `fact-merchant-day`). Each has an ODPS descriptor with Silver input ports, and one v1 ODCS per table with `sqlDialect: clickhouse`.
- **Servers.** Each ODCS `servers[0].database` is `lakehouse` or `marts`.
- **Rule SQL.** Rules follow the Silver contracts: uniqueness groups by a `sipHash128` of the key columns, and a rule carries no `SETTINGS` clause, because the runner appends the snapshot pin and the spill settings.
- **Check runner.** `openmetadata.checks` resolves `{object}` and the entity FQN from the contract server, not from the `CLICKHOUSE_DATABASE` environment variable. For example, `marts.fact_payment_attempt` → `clickhouse.default.marts.fact_payment_attempt`, and `lakehouse.`gold.dim_customer`` → `clickhouse.default.lakehouse."gold.dim_customer"`. `CHECK_PRODUCT` accepts a comma-separated list. `warehouse.py` loses its environment-based schema helper (no compatibility shim).

**Rules, each a scalar violation count that must be 0:**

| Table | Rules |
|---|---|
| `dim_customer` | one current version per customer; no overlap or gap (`valid_to_ts` = the next `valid_from_ts`, via a `lead` window); `valid_from_ts < valid_to_ts`; unique `customer_sk`; `is_current` iff `valid_to_ts IS NULL` |
| `feat_customer_daily`, `feat_payment_method_daily` | unique (key, `event_timestamp`); `event_timestamp` is a UTC midnight; `created >= event_timestamp`; counts ≥ 0; `decided <= attempts`; `declined <= decided`; each rate is in [0, 1] and null iff its denominator is 0; a window's columns are null together, and a null 7-day window implies a null 30-day window; no two consecutive rows for one key are identical (the change-point rule) |
| `label_attempt` | unique (attempt key, `label_source`); known source; `label_available_at >= occurred_at`; every key exists in ``lakehouse.`silver.attempts` `` |
| `fact_payment_attempt` | unique attempt key; row count equals the lifecycle-valid ``lakehouse.`silver.attempts` `` count; `outcome` is a known value; `chargeback_available_at >= started_at` |
| `fact_merchant_day` | sums of `attempts` and `succeeded` equal the fact's; rates are in [0, 1] and null iff their denominator is 0; `decided = succeeded + risk_declined + processor_declined` |

**Profiling.** After `publish-marts`, a `profile` step runs a native `Profiler` workflow (`type: Profiler`) scoped to the `marts` schema with `schemaFilterPattern`. It uses `computeTableMetrics` and `computeColumnMetrics`, and `profileSampleConfig` set to a static 100%, because the marts are small. #102 removed profiling everywhere; this brings back only this scoped use.

### 11. Catalog and lineage

- **Where connectors live.** Since #102, `data-pipelines` owns the OpenMetadata connector configuration (`catalog.connectors`), its ExternalSecrets and a PostSync register job. Every checks run ingests the ClickHouse tables before its tests, and other connectors are ingested by the on-demand `metadata-discovery` WorkflowTemplate. Contracts and products are not imported into OpenMetadata (its 2.0.6 ODCS importer rejects them), so Git stays the authority for the gold contracts too.
- **Connector.** The ClickHouse connector in `catalog.connectors` drops `databaseSchema: lakehouse`, which limits ingestion to one schema, and uses `schemaFilterPattern: {includes: ["^lakehouse$", "^marts$"]}`. Its table filter adds `^fact_` and `^build_info$`, which the build-info rule needs as an entity, and excludes `__staging$`. `marts_benchmark` is outside the schema filter.
- **Pipelines.** `openmetadata.lineage` adds the pipelines `dp3-features` ("Gold features, dimension and labels from pinned Silver") and `dp3-marts` ("ClickHouse marts from Silver"), with the edges listed in the governance delta. `publish` records the Silver snapshot IDs from the run report in the pipeline status.
- **Dashboards are not catalogued.** OpenMetadata covers the tables, pipelines, tests and profiles. Grafana dashboards stay out of the catalog: no Grafana connector, no token, and no mart → dashboard lineage.

### 12. ClickHouse users and grants

Added to `infra/charts/clickhouse/templates/installation.yaml`, with `passwordKeys` extended so that each user gets an ExternalSecret:

```yaml
profiles:
  grafana/readonly: 2
  grafana/max_execution_time: 30
  grafana/max_memory_usage: 1073741824        # 1 GiB
  grafana/max_result_rows: 100000
  grafana/result_overflow_mode: break
  grafana/max_threads: 2
  grafana/constraints/readonly/const: ""
  grafana/constraints/max_execution_time/max: 30
  grafana/constraints/max_memory_usage/max: 1073741824
  grafana/constraints/max_result_rows/max: 100000
  marts/allow_database_iceberg: 1
  marts/max_memory_usage: 2147483648          # 2 GiB; prod instance limit is 4 GiB
  marts/join_algorithm: full_sorting_merge
  marts/max_bytes_before_external_sort: 400000000
  marts/max_bytes_before_external_group_by: 800000000
  marts/max_insert_threads: 2
users:
  grafana/password: {valueFrom: {secretKeyRef: {name: clickhouse-grafana, key: password}}}
  grafana/networks/ip: ["10.0.0.0/8"]
  grafana/profile: grafana
  grafana/grants/query: ["GRANT SELECT ON marts.*"]
  marts/password: {valueFrom: {secretKeyRef: {name: clickhouse-marts, key: password}}}
  marts/networks/ip: ["10.0.0.0/8"]
  marts/profile: marts
  marts/grants/query:
    - GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER, TRUNCATE ON marts.*
    - GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON marts_benchmark.*
    - GRANT SELECT ON lakehouse.*
    - GRANT SELECT ON system.query_log
    - GRANT SYSTEM DROP CACHE, SYSTEM FLUSH LOGS ON *.*
  openmetadata/grants/query:   # adds marts.*
    - GRANT SELECT ON lakehouse.*
    - GRANT SELECT ON marts.*
    - GRANT SELECT ON system.databases
    - GRANT SELECT ON system.tables
    - GRANT SELECT ON system.columns
```

- **Why `readonly = 2` for `grafana`.** ClickHouse recommends `readonly = 2` for BI clients: it still blocks `INSERT` and all DDL, but allows the `SET` statements the Grafana plugin sends. Profile constraints make `readonly` itself unchangeable and cap the time, memory and result-size limits, so a dashboard cannot raise them.
- **`EXCHANGE TABLES` privilege.** There is none of its own. The 26.x interpreter requires `SELECT`, `INSERT`, `CREATE TABLE` and `DROP TABLE` on both tables, which the `marts` grants cover.
- **`GRANT SELECT ON lakehouse.*`.** It is not documented for DataLakeCatalog databases, but `openmetadata` already depends on it in #102. #102's dev spike confirms it.
- **`system.query_log` scope.** ClickHouse shows other users' queries in `system.query_log` to anyone granted `SELECT` on it. That is acceptable for an in-cluster pipeline user, and it is noted as a risk.
- **Analytics MCP tool (#103).** It reuses this model: a `grafana`-like reader, or a second reader on `marts.*`.

### 13. Grafana plan

- **Dashboards.** Kept in `infra/charts/data-pipelines/dashboards/{payments-overview,merchant-drilldown,fraud-outcomes}.json` and rendered by `templates/dashboards.yaml` as ConfigMaps in `data`, with the label `grafana_dashboard: "1"` and the annotation `grafana_folder: Payments analytics`. Every panel's datasource is `{"type": "grafana-clickhouse-datasource", "uid": "clickhouse-marts"}`, with SQL using `$__timeFilter(day)` or `$__timeFilter(started_at)`.

| Dashboard | Panels | Source |
|---|---|---|
| Payments overview | daily attempts, approval rate, risk-decline rate, attempted and succeeded amounts; top 10 merchants by attempts; hot-merchant share (its attempts / all attempts) | `fact_merchant_day` |
| Merchant drill-down | `$merchant` variable (top 200 merchants by attempts); daily KPIs; outcome mix; recent attempts table | `fact_merchant_day`, and `fact_payment_attempt` filtered by the primary key |
| Fraud outcomes | truth fraud caught (risk declined) against missed (succeeded); chargeback rate, shaded where `chargeback_window_complete` is false; risk-score histogram; scenario breakdown (synthetic oracle, labelled as such) | `fact_payment_attempt`, `fact_merchant_day` |

**Platform requests (the platform repository makes these, not this change):**

1. **Plugin.** Add `grafana-clickhouse-datasource@4.23.3` to the VictoriaMetrics k8s stack's `grafana.plugins`. The embedded grafana chart renders it as `GF_PLUGINS_PREINSTALL_SYNC`.
2. **Datasource.** Provision it with name `ClickHouse marts` and `uid: clickhouse-marts`. `jsonData`: `host: clickhouse.data.svc.cluster.local`, `port: 9000`, `protocol: native`, `secure: false`, `username: grafana`, `defaultDatabase: marts`. `secureJsonData.password` comes from an environment variable fed by the Secret below.
3. **Doppler keys**, in the dev and prod configs: `CLICKHOUSE_GRAFANA_PASSWORD`, read by this repository's clickhouse chart and by the platform's Grafana, and `CLICKHOUSE_MARTS_PASSWORD`, read by this repository only.
4. **ExternalSecret.** One in `observability` that maps `CLICKHOUSE_GRAFANA_PASSWORD` into the Grafana pod's environment.
5. **Sidecar coverage.** The dashboard sidecar must watch namespace `data` (`sidecar.dashboards.searchNamespace` includes `data` or `ALL`), with folder annotations enabled (`folderAnnotation: grafana_folder`).
6. **Network.** If any NetworkPolicy restricts `data`, allow `observability/grafana` → `data/clickhouse` on TCP 9000.

These requests block only the dashboard evidence.

### 14. Rubric mapping

| Rubric row | Covered by |
|---|---|
| Datawarehouse (indexing), 3 points | decisions 6 and 8, with `storage.md` § Warehouse |
| DP3 ingest stage / validate stage | decision 9 (`features` and `marts` / `checks`) |
| DP3 lineage / data validation | decisions 10 and 11 |
| Dim table with SCD2 | decision 4 |
| `feat_` tables with `event_timestamp`, `created` | decision 3 |
| Dim–fact relationships | decision 1's ER diagram plus a DBeaver export |
| Naming convention | decision 1 |
| Final coursework "label table joinable with features" | decision 5 |
| Final coursework "Grafana metrics" (supporting), and edai-2's analytics MCP tool (later) | decision 13, plus mart names stable for the tool |

## Risks / Trade-offs

- **[The prod ClickHouse is small (4 GiB, 30 Gi)]** → Monthly batches, sort-merge joins with spill and the 2 GiB `marts` profile limit bound memory. Disk is the tighter limit: the 180-day v19 window is about 95M attempts, the staging copy doubles it during a build, the benchmark adds an unsorted copy, and a v19 full-history check join wrote 17.8 GiB of sort spill. The marts build, the checks and the benchmark therefore never run at the same time (the `dp3-gold-writer` mutex), and the benchmark drops its copies before it ends. Full-history gold rules, such as the `label_attempt` key-existence join and the change-point rule, may take tens of minutes on prod, as the Silver rules do. If one exceeds the limits, its SQL is rewritten under the same scope rather than narrowed. The first v19 build records its peak memory and disk. Marts keep a rolling 180-day `TTL` window, which fits the existing PVC with room for the staging copy.
- **[ClickHouse Iceberg pruning on DataLakeCatalog tables is weaker than Spark's]** → The monthly batches filter on partition columns. Variant A of the benchmark measures it, and whatever the result is goes into the report as evidence.
- **[Snapshot consistency between Spark and ClickHouse]** → The DP2 mutex, plus the recorded snapshot-ID comparison that fails the marts stage on a mismatch.
- **[Change-point features are unfamiliar to Feast users]** → The definition file and the `feat_` contract state the "latest row ≤ t" semantics. A contract rule rejects two identical consecutive rows, and the conformance examples include expiry.
- **[Daily anchors skew training against serving by the DP3 delay]** → Documented in the definition's availability block. The roadmap's TTL step accounts for it.
- **[The hot merchant dominates Q1]** → Reported separately, not averaged away.
- **[Snapshot pinning on DataLakeCatalog tables is undocumented]** → Dev spike first. The mutex alone guarantees consistency, and a failed spike falls back to comparing snapshot IDs.
- **[The `marts` user can read all query-log entries]** → In-cluster only; no credentials appear in query text.
- **[#102's change is not archived yet]** → Shared spec deltas use ADDED requirements only, so they do not collide with #102's deltas, whichever change archives first.
- **[The wall-clock `TTL` shrinks the marts while live data is off]** → Accepted: the live simulator is to be enabled eventually. Evidence is captured promptly after the prod DP3 run, and every report records its window.
- **[Prod is the first full-scale test]** → The prod ClickHouse is the tightest resource. The prod run records the marts stage's peak memory and disk against the 180-day window.
- **[The platform requests are not met]** → DP3, the marts and the evidence run anyway. Only the Grafana screenshots wait.

## Migration Plan

#102 is merged to `main`, and this branch is rebased onto it.

1. **Start gate.** Implementation starts only after the prod Bronze and Silver v19 evidence run finishes. A prod-evidence thread will send the start instruction. Until then the branch holds only this proposal, unpushed.
2. **Implement and open the PR.** Implement on this branch with local non-test checks only, then push and open a PR. The trusted PR deploys to dev, which serves as a v18 smoke run: `dp3-gold`, then `warehouse-benchmark`, on the existing v18 Silver. The new Doppler keys must exist in dev before that deploy.
3. **Prod evidence after merge.** Prod releases come only from `main`. Run `dp3-gold`, then `warehouse-benchmark`, on the pinned v19 Silver, on the `large` profile and the prod ClickHouse. Record the results in the docs: Spark event logs and UI through the History Server pattern of #102, Argo graphs, OpenMetadata lineage, tests and profiles, the benchmark tables, and the ER export.
4. **Platform requests.** Send the requests in decision 13. Once they are met, capture the dashboard screenshots in prod.

**Rollback:** revert the PR, then drop `marts`, `marts_benchmark` and the `gold` namespace in the affected environment. Silver is untouched.

## Open Questions

None remain. Decisions recorded from the user:

- DP3 writes the marts as a new `marts` user, with Doppler key `CLICKHOUSE_MARTS_PASSWORD`, and reuses every other DP1 and DP2 credential.
- The ClickHouse database is `marts`.
- Marts keep 180 days through a wall-clock `TTL` (`ttl_only_drop_parts` on daily partitions), because the live simulator will be enabled. The PVC is unchanged.
- DP3 is tested on prod v19.
- Grafana dashboards are not catalogued in OpenMetadata.
- `dim_customer` tracks devices as the familiar-device set (decision 4).
- The mart keys are time-first (decision 6).
