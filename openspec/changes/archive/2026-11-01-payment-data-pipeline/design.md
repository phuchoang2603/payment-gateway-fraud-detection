## Context

This change merges `rebuild-generation-to-silver` and `dp3-features-and-marts` into one data pipeline, and records the implementation as it runs on prod v19 (2026-11-01).

- **Platform:** Spark 4.1.3 on the Apache Spark Operator; Iceberg 1.12 through the Polaris REST catalog; ClickHouse 26.10 reads Iceberg through a DataLakeCatalog database and holds the native marts; OpenMetadata 2.0.6 is the catalog; Argo Workflows runs every batch stage.
- **Prod capacity:** an on-demand `aws-workers` pool of three `m6a.xlarge` nodes (step pods, Spark drivers, databases), and a spot `aws-cpu-burst` pool of up to 12 nodes, 48 vCPU and 192 GiB (Spark executors and generator shards). The ClickHouse server has about 3.4 GiB of memory and a 30 GiB volume shared with Langfuse.
- **Data:** v18 in dev (90 days, 10k customers, 0.8M events); v19 in prod (365 days from 2025-10-29, 3M customers, 983.7M events, 192.2M labels, about 56% of purchases at one merchant).
- **Kafka topics:** `payments.events.v1` and `fraud.labels.v1` carry raw protobuf bytes with 7-day retention.
- **Repository rules:** tests and image builds run only in CI, there is no legacy compatibility within the changed area, and platform-owned resources are changed only through the platform repository.

## Goals / Non-Goals

**Goals:**
- One implementation per stage, each small enough to read in one sitting.
- One model for every layer after Bronze: `job(start, end)` reads a bounded window before its range, overwrites only its own days on an audit branch, and is a pure function of its pinned inputs. The daily run and a backfill are the same code.
- Write-audit-publish per layer, so no reader of `main` or the live marts sees unaudited data.
- Every cost (Spark, checks, marts) follows the run's range, not the history.
- Gold tables that Feast, training and ClickHouse read with exact point-in-time semantics, and marts whose keys and indexes serve fixed queries.

**Non-Goals:**
- Feast objects, materialization and TTLs; Flink's live and replayed short windows; training; drift.
- A raw S3 archive of Kafka beyond topic retention: Iceberg Bronze is the archive.
- Running the on-demand benchmarks for evidence: optimization evidence comes from the production backfill (decision 15).

## Decisions

### 1. Entity world as one shared module

`simulator/world.py` is the only domain module. It holds the configuration models, calibration loading, identity functions and constants, and the entity world that both generators use.

- **`WorldConfig`:** seed, integration id, synthetic key, start date, entity counts, behaviour (including the foreign-IP probability), scenarios and label delays.
- **`World.build(config)`,** plus `world.digest` (configuration, calibration bytes and tzdb version) and `world.start_date`.
- **`world.day_purchases(day, customers)`** returns the `(customer, number)` purchases of a customer range. Counts come from one per-day draw over all customers, so the same purchases come out however the range is split. **`world.purchase(rng, day, customer)`** and **`world.card_test(rng, day, burst)`** build one purchase each, with streams from `purchase_rng(day, customer, number)` and `burst_rng(day, burst, number)`.
- **Constants and helpers:** `SCENARIOS`, `CATEGORIES`, `CONDITIONS`, `entity_id` and `fingerprint` (byte-compatible with the identity vectors), `StrictModel` and `stage_rng`.

Entity pools, Zipf weights, familiar-merchant lists and compromise windows stay internal and are seeded from the world seed. Customer activity and region are per-customer traits fixed by the seed.

`live/` also uses `world.py`. `HistoricalModel` builds a `World` from the manifest's recorded world configuration and rejects a manifest whose `world_digest` differs. `profiles.py`, `behavior.py`, `identity.py`, `calibration.py` and `config.py` are deleted in the same change. The live simulator's behaviour is otherwise unchanged.

*Alternative:* keep the old modules until the live change is done. Rejected: it keeps two implementations alive.

### 2. Generation unit is one purchase-start day, split into parts

- **Keys.** Each purchase is keyed by its purchase-start day, its customer and that customer's purchase number that day. Card-testing bursts are keyed by (day, burst, number). A purchase's random stream and identity depend only on its key, never on how the day is split or how many workers run.
- **Counts.** A day's purchase counts come from one Poisson draw over all customers, scaled by each customer's activity and the region's local-hour, weekday and seasonal intensity. The draw is computed once per process and is independent of the split.
- **Parts.** Internal constants size a part: customers per part (`PART_CUSTOMERS`, 750,000, so the full dataset has about four parts per day and table, on the order of 70 MB each as an estimate from v8's size) and rows per Parquet row group. A part holds one table's rows for a customer range. Names are `payment_events/date=D/part-NNNNN.v{2|3}.parquet` (events are split by `schema_version`, see decision 3) and `fraud_labels/date=D/part-NNNNN.parquet`. Part names are deterministic, so regenerating a day overwrites the same keys. The partition date is the purchase-start day, not the row timestamp.
- **Scheduling.** `produce` submits every pending (day, part) pair of a shard to the pool at once. Each day's `_day.json` marker is written as soon as all of that day's parts have finished, with no per-day barrier.
- **Bounded resources.** A worker holds one part's rows per writer and one local temporary file at a time. It uploads the file and deletes it. Memory and scratch are bounded by part size and worker count, not by day or dataset size.
- **Atomic files.** A part is written to a local temporary file and uploaded with a single PUT (multipart for large parts), or moved into place locally.
- **Start.** `config.json` (the logical configuration, the generator digest and their hash) is written first. A rerun with a different hash refuses to run.
- **Day completion.** A `date=D/_day.json` marker records each part's rows, bytes and SHA-256 once all of the day's parts are uploaded. A rerun skips days with a marker and regenerates days without one.
- **Publication.** Once every day has a marker, `publish` writes `manifest.json` last with an if-absent condition. It lists the logical configuration, generator digest, contract identities, row and byte totals and the file list. If the configuration declares `minimum_source_bytes` and the total is lower, publication refuses with both numbers. A version that already has a manifest is never modified.

*Alternative:* keep the old customer-block tasks with receipts and resume adoption. Rejected: day markers give the same restart granularity with one rule, and parts bound resources without a separate budget model.

### 3. Schema versions declared by the contract

- **Single version column.** `schema_version` (field 7 of `PaymentEvent`) is the only version of a payment event. There is no separate physical-version column.
- **Declaration.** The historical `payment-events` ODCS contract is the only declaration of which fields exist in which version. Each field added after version 2 carries a property-level `customProperties` entry `sinceSchemaVersion: 3`. Today those are `assessment.assessment_id`, `assessment.evidence_sha256` and `signals.user_agent_family`.
- **Versions.** The historical source starts at `schema_version` 2. The configuration's `schema_changes: [{effective_at, version}]` declares a change to `version: 3`, dated per dataset: dev on 2026-09-15, full on 2026-04-30. An event's version is chosen by its `occurred_at`. Version 3 is the gateway's current shape, so Kafka rows need no special handling.
- **Meaning of "version 2".** Version 2 is defined by the v18 historical contract, not by the gateway's older wire format. A later gateway change (for example to 4) uses the same mechanism: a new contract field marked `sinceSchemaVersion: 4`, and a configuration change entry to version 4. The config accepts only versions the contract declares, and produce refuses others before writing.
- **Projection.** The generator builds each event in the full shape and projects it onto the fields available at its `schema_version`, using the paths the contract declares. Field names are not coded.
- **Files.** Events of each version go to separate part files named `.v2.parquet` or `.v3.parquet`. Their Parquet schemas really differ: v2 parts physically lack the declared columns. A day that spans the change has parts of both versions.
- **Contract rules.** `schema_version` must be in (2, 3). The declared fields are present exactly when `schema_version >= 3`.

*Alternative:* one file per table holding both shapes, with the omitted fields null. Rejected: the Parquet schema would not show which version lacks the fields, so Bronze would have to infer the layout from the rows.

### 4. Generation runs as an on-demand cluster workflow

A `generate-historical` Argo `WorkflowTemplate` in the data-pipelines chart runs the simulator image as a DAG: `generator.shards` shard pods in parallel, then one publish pod. The configuration, shard count and CPU come from the chart. Each environment's `SIMULATOR_CONFIG` file is baked into the image:

| Environment | Config | Output |
|---|---|---|
| dev | `historical-dev.yaml`: v18, 90 days from 2026-07-31, 10k customers, one shard | `s3://platform-dev/datasets/payment-gateway/historical/v18` |
| prod | `historical-full.yaml`: v19, 365 days from 2025-10-29, 3M customers, change to version 3 on 2026-04-30, `minimum_source_bytes: 100000000000`, twelve shards | `s3://platform-prod-142737101504-us-east-1/datasets/payment-gateway/historical/v19` |

- **Settings:** the YAML holds only the logical dataset. Run settings come from the environment: `GENERATOR_MODE` (`generate`, `shard` or `publish`), `SHARD_INDEX` and `SHARD_COUNT` for shard steps, `GENERATOR_WORKERS` (set from the CPU request), `GENERATOR_CONTRACTS` (the contract directory, baked into the image) and `SIMULATOR_CONFIG`.
- **Shards:** shard `i` of `n` generates the days whose index modulo `n` is `i`. A publish step refuses until every day has a marker. Prod uses twelve shards, the `aws-cpu-burst` pool's node limit, with one shard per spot xlarge node.
- **Digest:** the generator digest is a SHA-256 over `world.py`, `lifecycle.py`, `historical.py`, `store.py`, the calibration file and the historical contracts. The configuration hash includes it, and `config.json` and the manifest record it. The image carries the same files, so the digest is reproducible between CI and the cluster.
- **Credentials:** the existing gateway object identity, held in the `gateway-storage` secret from the `datasets/payment-gateway/` scope.
- **Resources:** CPU, memory and ephemeral-storage requests sized to fit one spot xlarge node, plus `activeDeadlineSeconds`. Ephemeral storage stays modest because each worker's scratch is one part.
- **Retries:** `retryStrategy` on errors. A retried pod skips completed days by their markers, so a node replacement only costs the day in progress.
- **RBAC:** a `historical-generator` ServiceAccount, a Role with `workflowtaskresults` `create, patch` and its RoleBinding. The platform repository has no executor ServiceAccount bound in `data`, so this one is created here. The `gateway-storage` ExternalSecret supplies the S3 credentials.
- **Shared spec:** the common container spec (image, env, credentials, resources, node selector, retry and deadline) is one named template in `_helpers.tpl`, used by the shard and publish steps.

*Alternatives:*
- A Helm-managed `Job`: rejected, because a Job runs at sync time, is immutable, and needs a rename for every run.
- Workstation CLI: rejected by the user; a multi-hour run should not depend on a laptop.

The local CLI remains for tests and development only.

### 5. Bronze tables are shared by both sources

`bronze.payment_events` and `bronze.fraud_labels` hold business columns typed as the payments protobuf, plus provenance:

| Column | Meaning |
|---|---|
| `_source` | `historical` or `kafka` |
| `_dataset_version`, `_source_file` | historical files |
| `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp` | Kafka records |
| `_ingested_at`, `_run_id` | every row |

Events are partitioned by `_source` and `days(occurred_at)`; labels by `_source` and `days(label_available_at)`.

**Historical mode.**
1. Read the manifest and list the dataset root once, metadata only: its parquet objects must equal the listed files, with each size equal to the listed `bytes`.
2. Read the files once with `mergeSchema`.
3. Cast to the Bronze schema; any lossy cast fails.
4. Replace this dataset's rows with `writeTo(t).overwrite(_dataset_version = v)`, so a rerun is idempotent.
5. Group the written rows by source file and compare each count with the manifest `rows`; a mismatch names the file. Bronze does not recompute the generator's SHA-256 values.

**Kafka mode.**
- **Start offsets.** Each partition's next offset is `max(_kafka_offset) + 1` over the rows the table already holds for the topic: `_source = 'kafka'` rows of `bronze.payment_events` or `bronze.fraud_labels`, and every row of `bronze.rejected_records`. Partitions with no rows start at the configured starting offsets (`KAFKA_STARTING_OFFSETS`, `earliest` or `latest`). No table property records progress, so compaction and snapshot expiry cannot move it.
- **Reading.** Spark reads `format("kafka")` from the start offsets to `latest`, with `failOnDataLoss=true`, so expired offsets fail the run instead of being skipped.
- **Decoding.** `from_protobuf` decodes records using the payments descriptor set, generated by the existing `codegen:proto` `descriptor` option into the batch package.
- **Commit.** Progress is the rows, so each table's good rows and their offsets commit together. Records that fail decoding are written to `bronze.rejected_records` in their own commit, which runs first: the rejected table never lags the accepted one, so the read window starts from accepted progress alone and covers only unprocessed records. A failed run leaves a retry to re-read from the last committed rows. Partial failure between the two commits cannot lose quarantined records, because the rejected table is written first and holds its own rows.
- **Empty runs.** A run with no new records commits nothing.
- **Rejected records.** `bronze.rejected_records` holds the raw key and value bytes, topic, partition, offset, Kafka timestamp, error, `_run_id` and `_ingested_at` of each record that fails protobuf decoding or a required-field check taken from the Bronze contract (an empty string counts as missing). A value that decodes but cannot be cast to the Bronze schema fails the run. Historical mode has no such table: any bad file fails the run, because published sources are immutable. The run report and an OpenMetadata check show the rejected count.

*Alternatives:*
- Kafka Connect S3 sink (rejected: raw protobuf bytes have no registry schema, so it cannot write typed files without a framing change across Flink, Feast and Fraud).
- Keep the Python archiver (rejected: a second ingestion system and landing format).
- Separate Bronze tables per source (rejected: Silver would union them anyway).

### 6. Silver: payment days from pinned Bronze

Silver runs `job(start, end)` over UTC days, a pure function of pinned Bronze (`batch/silver/curate.py`).

- **Payment day.** Every event of a payment occurs within its 60-minute expiry, so a payment belongs to the UTC day of its first event, its `payment_day`. Its events, attempts and rejected rows carry it, and the tables are partitioned by it. A run overwrites whole payment days and nothing outside them.
- **Bounded reads.** A run reads Bronze `occurred_at` days from `start − 1` to `end + 1`. The first day tells a payment that began before midnight from one that began after; the last day holds the tails of payments that began just before midnight.
- **`silver.payment_events`:** a business hash covers every column except provenance and `recorded_at`, and includes `schema_version`. Copies with equal hashes collapse to the earliest `recorded_at`, because republished exports are duplicates, not conflicts. More than one distinct hash for an `event_id` is a conflict. Deduplication is one shuffle on `event_id` within the run's payments, so its cost follows the range.
- **`silver.rejected_events`:** every row of a conflicting payment, partitioned by `partition_day` (the payment's day, or a rejected label's availability day). No row of a conflicting payment enters the accepted tables. A check requires zero rejected rows from the historical source, because published sources are immutable.
- **`silver.attempts`:** one row per `(integration_id, payment_id, attempt_id)`, with top-level `merchant_id`, `customer_id`, `payment_method_fingerprint` and `amount_minor` from the AttemptStarted context, server `started_at` and client `attempted_at`, the risk decision, `outcome` and `outcome_at`, and the payment's `payment_status`. Lifecycle violations get `lifecycle_valid = false`, and a check counts them.
- **`silver.labels`:** one row per distinct observation, overwritten by availability day and linked to the attempts and rejections of the 45 days before (`LABEL_LOOKBACK`), which covers the 7–21 day chargeback delay. Each label is `linked`, `pending` (live, payment or outcome not yet arrived), `orphan` (historical, payment has no events) or `violation` (missing attempt on a terminal payment, a chargeback on a non-succeeded attempt, or availability before the attempt start or outcome).
- **Layout.** Events are written in `occurred_at` order and attempts in `started_at` order (`WRITE LOCALLY ORDERED BY`), so file statistics prune Gold's and ClickHouse's time filters inside a payment day.
- **Commits.** Each commit records the Bronze snapshot IDs and the range (`dp2.range`). The run report records Bronze snapshots, input rows, distinct events, duplicates, conflicts, rejected rows, label linkage counts, the Silver snapshots and each phase's tasks and wall time.
- **Verification.** CI runs a backfill and day-by-day runs over the same Bronze and compares every table, and checks a payment across midnight.

### 7. Gold layers, naming and relationships

| Layer | Engine and location | Tables | Built by |
|---|---|---|---|
| Bronze | Iceberg `bronze.` | `payment_events`, `fraud_labels`, `rejected_records` | DP1 Spark |
| Silver | Iceberg `silver.` | `payment_events`, `attempts`, `labels`, `rejected_events` | DP2 Spark |
| Gold, ML | Iceberg `gold.` | `dim_customer`, `feat_customer_daily`, `feat_payment_method_daily`, `label_attempt` | DP3 Spark |
| Gold, analytics | ClickHouse native database `marts` | `fact_payment_attempt`, `fact_merchant_day` | DP3 ClickHouse `INSERT … SELECT` |

- **Why the ML tables are in Iceberg.** Feast, training and ClickHouse all read the ML tables, so they live in Iceberg; ClickHouse sees them as ``lakehouse.`gold.feat_customer_daily` ``.
- **Why the facts are in ClickHouse.** They exist only for SQL analytics and indexing, so they live in ClickHouse.
- **Naming conventions.** The namespace or database states the layer and engine; the prefix states the role (`dim_`, `fact_`, `feat_`, `label_`). Bronze and Silver keep schema-based layer names rather than `raw_` and `stg_` prefixes. The rubric accepts "prefix or similar".
- **Column naming.** Keys are integration-scoped. Timestamps end in `_at` (business time) or `_ts` (SCD validity). Surrogate keys end in `_sk`, and booleans start with `is_`. The exceptions are Feast's own `event_timestamp` and `created`.
- **Why the database is not called `gold`.** `marts` avoids having both an Iceberg `gold` namespace and a ClickHouse `gold` database. Confirmed by the user.

**Relationships (documented as a Mermaid ER diagram in `docs/data/offline.md`):**

- `fact_payment_attempt` (attempt grain) → `dim_customer` on (`integration_id`, `customer_id`), point-in-time on `started_at`.
- `fact_payment_attempt` → `fact_merchant_day` on (`integration_id`, `merchant_id`, `toDate(started_at)`).
- `label_attempt` ↔ `fact_payment_attempt` on the attempt key.
- `label_attempt` → `feat_customer_daily` and `feat_payment_method_daily` on the entity key, with `feat.event_timestamp <= label.event_timestamp`, taking the latest row.
- `dim_customer` → `feat_customer_daily` on the customer key.

*Alternative:* gold facts in Iceberg, built by Spark. Rejected by the agreed scope: ClickHouse already reads the lake, and a Spark mart would add a write path without adding evidence.

### 8. Gold: day ranges continued from stored state

`batch/gold/features.py` reads Silver at the snapshots the run's own Silver stage left (its audit branch heads, which are published by then), or `main` when Silver ran separately, and overwrites only:

- feature rows with anchors in `(start, end + 1]`;
- `label_attempt` rows of attempts started in the range, or on a day whose attempts gained a linked label available in it;
- the `dim_customer` versions opened in the range.

**Exact from one window of history.** `windows.window_states` adds a baseline anchor at the range's first midnight for every entity it reads. Values there and after are exact once the run reads every attempt still in a window then: the longest window, plus one day for client clocks, before the range. `windows.changes` keeps a daily row only where it differs from the entity's previous row, with the baseline as the first previous row. A range's rows therefore equal the same days of any longer run; CI checks that daily runs build what one backfill builds.

**`dim_customer` continues from stored versions.** SCD2 versions are sequential, so this table is the one exception to independent ranges. A run reads each in-range customer's stored version open before the range (the profile to compare the first attempt against, and the version number to continue) and its first stored version after the range (which closes the last new version), deletes those and the versions opened in the range, and appends their rebuild. Device uses from 90 days before the range give the familiar devices. Familiar devices are found in one ordered pass per customer in which each familiar interval's start and end toggle its device's `xxhash64` in a running `bit_xor`, so change detection compares that fingerprint and only the opened versions join the intervals to list the devices. The delete and the append are two commits, so the run first resets the table's audit branch to `main` (`lakehouse.restage`): a retried attempt never reads a failed attempt's delete. `LOGIC_VERSION` is 3.

**Records.** Every gold commit records the Silver snapshot IDs, the logic version, the coverage start and the range (`dp3.range`). `created` is the commit time of the pinned `silver.attempts` snapshot, deterministic for one input.

**Short windows (Kappa).** The 5m/1h/24h `fraud-snapshot-v2` windows have one engine, Flink, for history and live. DP3 builds no short windows. `gold.feat_attempt_rt`, written by an earlier DP3, stays in prod with its October–January rows for an interim training dry run until the Flink history exists; no code writes it.

**Late Silver changes.** The two lookback days cover late outcomes. A Silver change to an older day needs a re-run from the first changed day onward, because `dim_customer` is sequential. There is no hidden state to repair.

### 9. Feature grain: daily anchors, change-point rows

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

**Shared engine.** `batch.gold.windows.window_states` and `changes` compute the interval deltas and change points for both feature tables (decision 8 covers the range rules).

**Computation (all quantities are exact):**

- **Contribution intervals.** Each eligible attempt contributes to the inclusive anchor-day interval `[enter, exit)` for each window, where:
  - `enter = max(day(attempt_time), day(started_at)) + 1`, also bounded by `day(outcome_at) + 1` for outcome-based counts;
  - `exit = day(attempt_time) + w + 1`.

  An attempt whose `enter >= exit` contributes nothing.
- **Counts and sums.** These are cumulative sums of +value at `enter` and −value at `exit`, ordered by anchor day within each entity.
- **Distinct counts.** First merge the intervals of each (entity, value), then apply the same +1/−1 deltas. No collect-set windows are needed.
- **Rows.** The change points are the distinct delta days, plus the coverage-start days `H + 7` and `H + 30`, where `H` is the first Silver attempt day. The H + 7 and H + 30 rows exist only for entities whose window holds something on that day.
- **Coverage.** At anchor `s`, window `w` is complete when `s − w >= H`; otherwise every column for that window is null.
- **Cutoff.** A run writes anchors in `(start, end + 1]`, and never past the commit day of the pinned `silver.attempts` snapshot.

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

- One row per entity per active day. Rejected: values go stale after expiry, so point-in-time lookups would be wrong.
- One row per attempt at `t`. Rejected because of the training/serving skew above.

### 10. `dim_customer` SCD2 over the observed profile

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

### 11. `label_attempt`

One row per (attempt key, `label_source`), taken from `silver.labels` where `linkage = 'linked'`. The earliest `label_available_at` wins, with ties broken by `observation_id`.

**Columns:**

- the attempt, customer and payment-method keys (from `silver.attempts`);
- `event_timestamp`, the same attempt time as the features;
- `label` (`is_fraud`), `label_source`, `label_available_at` and `occurred_at`.

**Point-in-time use.** Training takes `label_attempt` filtered by `label_available_at <= cutoff` as the Feast entity dataframe, and joins features on entity key and `event_timestamp`.

**Label sources.** Simulation truth and chargebacks stay separate rows. A missing chargeback is never a negative, as in the Silver labels contract.

**Checks:** the key is unique; `label_available_at >= occurred_at`; the source is a known value; every attempt key exists in `silver.attempts`.

### 12. Marts: DDL, keys and indexes

**Database.** The ClickHouse setup job creates `marts` and `marts_benchmark` as admin, next to `langfuse`. The DDL lives in `src/data-pipeline/batch/batch/gold/sql/marts/*.sql`. The build job owns it and runs `CREATE TABLE IF NOT EXISTS` for the live tables, so a schema change is a `DROP` plus rebuild during the run (no legacy compatibility).

```sql
CREATE TABLE marts.fact_payment_attempt (
  integration_id LowCardinality(String),
  payment_id String CODEC(ZSTD(3)), attempt_id String CODEC(ZSTD(3)),
  merchant_id String, customer_id String CODEC(ZSTD(3)), payment_method_fingerprint String CODEC(ZSTD(3)),
  payment_method_type LowCardinality(String), device_fingerprint Nullable(String) CODEC(ZSTD(3)),
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

**Codecs.** The UUID and SHA-256 hex columns are random, so the default LZ4 barely compresses them: they took 6.8 of the fact's 6.4 GiB uncompressed (about 240 bytes a row over May–June 2026). ZSTD's entropy stage stores them near their information content: on a 2M-row sample, 18.5 bytes instead of 35.5 for a UUID string (16 for the native `UUID` type) and 32.2 instead of 63.4 for a fingerprint (32 for `FixedString(32)`), so the columns keep their string types and their consumers, at about half the fact's size. With the 90-day window the live fact is about 6 GiB, leaving the shared 30 GiB volume room for a build's staging copies. A build still writes and merges about 13 GiB of staging parts within minutes; with ClickHouse's default 480 s `old_parts_lifetime` the merged-away parts filled the volume in September 2026's build, so the server keeps them 60 s.

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
  - Daily partitions line up with the day-granular TTL (decision 7), so expiry drops whole partitions instead of rewriting parts. That gives about 90 partitions at the 90-day retention, within ClickHouse's guidance of keeping the partition count in the hundreds to low thousands. Each partition holds about 0.5M rows in prod and about 9k in dev. `fact_merchant_day` is small, so it keeps monthly partitions and row-level TTL.
- **Integration first.** `integration_id` has one value today, but it leads the key so per-integration queries stay correct.
- **Hot merchant.** One merchant takes about 56% of rows, so Q1 on the hot merchant still reads about half of each day. The evidence reports Q1 for both the hot merchant and a median merchant, so the skew is visible instead of hidden.
- **Key choice considered:** `(merchant_id, day)` first. It is slightly faster for one merchant over a long range, but every network-wide time query would scan whole partitions. A time-first key serves both.
- **Granularity.** Bloom filters use `GRANULARITY 4`, one index block per 4 × 8192 rows, to balance index size against skipping. Customer IDs are near-unique per granule block, which is why a bloom filter is chosen over a set index.
- **Projections.** A customer-ordered projection would beat the bloom filter on Q2 but double storage. Prod's 30 Gi PVC is shared with Langfuse, so projections are not used. Revisit them if the evidence shows a weak bloom result.

### 13. Mart build: staged attempts and labels joined in hash buckets

`batch/gold/marts.py` runs as a container on the Spark image and talks to ClickHouse over HTTP as user `marts`. It reads published Silver only, at the snapshots the `plan-gold` step resolves, so it needs nothing from Gold.

1. Drop `marts.*__staging` tables left by a failed run, then create empty staging tables from the DDL.
2. Compute the window from the wall clock, matching the tables' `TTL`: `window_start = today − 89 days`. `window_end` is the run's last day, and `build_info`'s window end never moves backwards.
3. Rebuild the run's days and the 45-day label lookback before them, inside the window. A label available in the range belongs to an attempt started at most 45 days earlier, and the days whose 21-day chargeback window completes in the range fall inside the lookback, so this is a superset of every day the range can change; rebuilding an unchanged day rewrites the same rows.
4. Aggregate the linked labels available from the first rebuilt day to 45 days after the last into `attempt_labels__staging`, the earliest simulation truth and scenario and the earliest chargeback per attempt, reading only the eight columns aggregated, in 8 passes of `cityHash64(payment_id)`. One pass of June 2026's window measured 822 MiB and 18 s.
5. Stage the rebuilt days' lifecycle-valid attempts once into `attempts__staging`, a streaming read of their Iceberg partitions.
6. Both staging tables are ordered by `(cityHash64(payment_id) % 64, attempt key)`. Insert `fact_payment_attempt__staging` in 64 buckets: one bucket of labels `RIGHT JOIN` the same bucket of attempts with `join_algorithm = 'hash'`, the attempts as the build side. Each insert reads one sixty-fourth of each table through the primary key, so the label summary is read once in total; the former per-day inserts read all of it for every rebuilt day (8.4 s each, 13.5M rows, 26 days for May).
7. Regroup the months containing the rebuilt days into `fact_merchant_day__staging` from the live fact and the staged days, write `build_info__staging` with the run's range and code version, and reconcile the staged row count with the lifecycle-valid Silver count on the rebuilt days.
8. `check-marts` audits the staging tables (`{object}` resolves to `marts.<table>__staging`), and its results attach to the live entities.
9. `publish-marts` replaces each rebuilt day and month with `ALTER TABLE … REPLACE PARTITION … FROM` staging, drops days older than the window, exchanges `build_info` and drops the staging tables.

A crash between partition replaces is the only partly published state; the step retries on error, and re-running the range rebuilds and audits the same days. Until the live simulator feeds new days, the wall-clock `TTL` empties the marts about 90 days after v19's last day (2026-10-28).

*Alternatives:* one monthly insert joined with `full_sorting_merge`, replaced when May 2026 exceeded the 2 GiB limit in `MergeJoinTransform`; then one insert per day with the day's attempts as the hash build side, replaced because every day rescanned the whole label summary.

### 14. One pipeline workflow: Gold and the marts side by side, unchanged stages skipped

```text
dp-pipeline (WorkflowTemplate; the pipeline template holds mutex dp-writer; onExit: publish)
├── plan-silver       openmetadata.plan: skip Silver?
├── silver            SparkApplication batch/silver/curate.py → audit-<workflow> branch
├── check-silver      openmetadata.checks: silver-payments on the branch heads
├── publish-silver    openmetadata.wap: fast-forward Silver main
├── plan-gold         openmetadata.plan: skip Gold?, and the published Silver snapshots
├── gold ─ check-gold ─ publish-gold                    (gold-features)
└── plan-marts ─ marts ─ check-marts ─ publish-marts ─ profile   (payment-marts)
dp-daily    (CronWorkflow, suspended): dp1-bronze from Kafka, then the pipeline for yesterday and 2 lookback days
dp-backfill (WorkflowTemplate): the pipeline once per month of [start, end], parallelism 1, failFast
dp1-bronze  (WorkflowTemplate, mutex dp1-bronze-writer): spark → checks → publish-tables → publish
```

- **Per layer.** Silver is audited and published before Gold and the marts read it, so a Silver problem stops the run before any downstream work.
- **Gold and the marts in parallel.** Both read published Silver and nothing of each other, so the Gold Spark job overlaps the ClickHouse mart build. The mart build, `check-gold`, `check-marts` and the mart `profile` share the `dp-clickhouse` mutex, so the 3.4 GiB server runs one of them at a time; September 2026's `gold_dim_contiguous` check was stopped by the server's memory limit while the profiler, then outside the mutex, ran beside it. A failure in one branch does not stop the other from publishing; the run fails, and re-running the range rebuilds only what failed (next point).
- **Skip unchanged stages.** Every Silver and Gold commit records its range (`dp2.range`, `dp3.range`), its input snapshots and its code version (`dp.code_version`, the batch image), and `build_info` records the marts' range, Silver snapshots and code version. `plan-silver`, `plan-gold` and `plan-marts` compare the head of main with what the run would read; when every table matches, the stage's build, check and publish are skipped (`when` on the plan's decision). A re-run after a later stage failed therefore resumes there. A new image always rebuilds, a commit without a code version never matches, and daily runs never match because their inputs move.
- **Publish.** `openmetadata.wap` fast-forwards each table's `main` to its branch head in one catalog transaction, only when `main` is still the branch's base.
- **One mutex, on the template.** The daily run, manual runs, backfill months and maintenance serialize wherever the template is referenced.
- **Names.** A Spark application is named after its step pod, unique per step, loop item and retry, so a backfill's months never collide.
- **Retries.** Spark steps retry on any failure (prod: limit 10) for spot loss; containers retry on error; checks never retry a failed rule. A retried Spark step creates a new SparkApplication; a failed attempt's application that still holds a driver must be deleted before the retry's driver starts, because both would write the same branch.
- **Ranges.** A single run can cover more than a month, but three months of Silver overflowed the executors' 24 GiB of local disk in its shuffle (June–August 2026), so backfills stay monthly.
- **Lifecycle.** A platform admission policy (`dp1-workflow-lifecycle`) rejects deleting or terminating active workflows in `data`; runs are stopped with `shutdown: Stop`.
- **Placement and sizing (prod).** Step pods and Spark drivers run on the on-demand `aws-workers` pool, because a spot reclaim of a driver restarts the whole application; executors run on spot `aws-cpu-burst`. Silver uses the `large` profile (11 executors × 3 cores, 9g + 3g, 4g + 1.5g driver). Gold uses the `gold` profile: 11 × 3 cores at 7g heap + 5g overhead, and a 2g + 1.5g driver. Gold runs no Python, but its executors were OOMKilled at 9g + 3g while tasks peaked at 331 MB of execution memory, so the same 12 GiB pod holds a smaller heap and more native memory. Every executor sets `MALLOC_ARENA_MAX=2`. Spark keeps graceful decommission, failure exclusion and a 200-failure, 30-minute executor failure budget.

### 15. Optimization techniques, from the production backfill

Evidence is the v19 backfill itself: each technique below was adopted after a measured failure or cost on prod.

| Technique | Problem it solved | Evidence |
|---|---|---|
| Range model (`job(start, end)`) | Full-history recomputes grew with history: the `dim_customer` full rebuild was OOMKilled in January | Silver stays at 9–19 min a month from November to May |
| Payment-day partitions + ordered writes | Time filters scanned whole tables | Gold and the marts prune to the run's days |
| Local fanout writes for day-laid input | The hash shuffle cost 202 GB of shuffle write on DP1 historical (Spark UI) | Silver and historical Bronze write without the shuffle |
| Narrow dedup on `event_id` within the range | A full-history broadcast of 20M duplicated keys exceeded the driver | One shuffle per month |
| Change-point feature rows | One row per entity per day is about 1.1B rows a year per table | 292M customer and 304M card rows through August |
| XOR sweep for familiar devices | Joining every attempt to its customer's intervals was OOMKilled even range-scoped | One ordered window pass |
| `persist(MEMORY_AND_DISK)`, not `localCheckpoint` | Spot reclaims lost checkpointed blocks | Recomputed from lineage instead |
| Range-scoped checks (`additional_table_filters`) | Whole-table checks grew every month; a `SELECT *` subquery scope ran ClickHouse out of memory | check-silver + check-gold: Nov 5 + 7, Jan 11 + 21, Mar 18 + 28 min unscoped; Apr 3 + 9 min scoped |
| Spill-safe ClickHouse settings | Hash joins in full-history checks exceeded the server | external sort and group-by, `full_sorting_merge` |
| Label summary in hash passes of its own columns | June's single group-by over every label column exceeded 2 GiB | 822 MiB, 18 s a pass |
| Fact joined in 64 hash buckets of staged attempts and labels | The monthly sort-merge insert exceeded 2 GiB in May; the per-day joins after it rescanned all labels for every day (22 s a day in June) | 16.5 s a day in August |
| ZSTD on ID and fingerprint columns, 90-day window | About 240 bytes a row under LZ4; a 180-day window and its staging copies did not fit the 30 GiB volume | 147 bytes a row |
| Gold and the marts side by side | The mart build waited for Gold though it reads only Silver | 44 minutes a month in July and August |
| Skip stages already on `main` | A re-run after a later failure rebuilt every layer (22 minutes of Silver and Gold for June) | A re-run resumes at the failed stage |
| Heap → overhead shift, `MALLOC_ARENA_MAX` | Gold executors OOMKilled in native memory | 7g + 5g pods |
| Branch reset before a multi-commit rewrite | A retry would rebuild `dim_customer` without its history | `restage` in `replace_customers` |

### 16. Checks are contract-derived ClickHouse SQL, scoped to the run

ODCS contracts declare `quality` rules as scalar violation-count SQL with `mustBe: 0`. `openmetadata.checks` turns them into native OpenMetadata test cases, ingests the run's ClickHouse tables, runs the suite and fails unless every expected test reports Success.

- **Pinning.** Each rule runs with `SETTINGS iceberg_snapshot_id` pinned to its table's audited head. Other audited tables a rule references are pinned to their own audited snapshots in subqueries.
- **Scoping.** The checks read the range recorded on the audited commit (`dp2.range`, `dp3.range`) and add `additional_table_filters = {'lakehouse.<schema.table>': '<filter>'}` for the rule's own table: `payment_day` for Silver events and attempts, `partition_day` for rejected events, the availability day for labels, `event_timestamp` or `valid_from_ts` for Gold. The key must be written without backticks (`lakehouse.silver.attempts`); a quoted key filters nothing. A commit without a range is audited in full.
- **Spill.** Every rule gets the external sort and group-by settings and `join_algorithm = 'full_sorting_merge'`.
- **Scope of Spark accounting.** The Spark jobs own only conservation and quarantine counts (manifest rows, offsets, rejected records), which the run reports and checks read.

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

**Profiling.** After `publish-marts`, a `profile` step runs a native `Profiler` workflow (`type: Profiler`) scoped to the `marts` schema with `schemaFilterPattern`. It uses `computeTableMetrics` and `computeColumnMetrics`, and `profileSampleConfig` set to a static 100%, because the marts are small. Profiling runs only here.

### 17. Contracts at v1

- `contracts/data-products/{historical-payments,bronze-payments,silver-payments}/` each contain `product.odps.yaml` plus one `*.odcs.yaml` per table, all at version 1.0.0. There are no `versions/` copies and no `physical-schemas-*.yaml`.
- Silver business rules live in the Silver contracts; `contracts/quality/` is removed.
- Generated references under `docs/data/products/` are regenerated by the existing CI check.
- The Bronze product adds a `rejected-records` port for the quarantined Kafka records, and the Silver product adds a `rejected-events` port. Each port's contract declares its rule: the Silver contract requires zero rejected rows with source `historical`.

### 18. Catalog, registration and lineage

The `data-pipelines` application owns the OpenMetadata connector configuration, its ExternalSecrets and the register job. A PostSync job creates the services on each deploy. Every checks run ingests the ClickHouse tables before its native tests, so no scheduled ClickHouse discovery remains. Kafka topics are ingested by the on-demand `metadata-discovery` WorkflowTemplate. The `metadata-integrations` application is removed.

- **Where connectors live.** `data-pipelines` owns the OpenMetadata connector configuration (`catalog.connectors`), its ExternalSecrets and a PostSync register job. Every checks run ingests the ClickHouse tables before its tests, and other connectors are ingested by the on-demand `metadata-discovery` WorkflowTemplate. Contracts and products are not imported into OpenMetadata (its 2.0.6 ODCS importer rejects them), so Git stays the authority for the gold contracts too.
- **Connector.** The ClickHouse connector in `catalog.connectors` drops `databaseSchema: lakehouse`, which limits ingestion to one schema, and uses `schemaFilterPattern: {includes: ["^lakehouse$", "^marts$"]}`. Its table filter adds `^fact_` and `^build_info$`, which the build-info rule needs as an entity, and excludes `__staging$`. `marts_benchmark` is outside the schema filter.
- **Pipelines.** `openmetadata.lineage` adds the pipelines `dp3-features` ("Gold features, dimension and labels from pinned Silver") and `dp3-marts` ("ClickHouse marts from Silver"), with the edges listed in the governance delta. `publish` records the Silver snapshot IDs from the run report in the pipeline status.
- **Dashboards are not catalogued.** OpenMetadata covers the tables, pipelines, tests and profiles. Grafana dashboards stay out of the catalog: no Grafana connector, no token, and no mart → dashboard lineage.

Lineage is published on every run, success or failure: the edges are the pipeline's structure, and a backfill that stops at a later month has already published earlier ones.

### 19. ClickHouse instance and users

- **Ownership.** `infra/charts/clickhouse` (own app-of-apps entry, namespace `data`, enabled in dev and prod) defines the `ClickHouseInstallation`, the `clickhouse` Service, one ExternalSecret per user and a PostSync setup Job. The platform installs only the Altinity clickhouse-operator 0.27.4, which watches `data`.
- **Contract for Langfuse.** Service `clickhouse.data.svc.cluster.local` (HTTP 8123, native 9000), image `clickhouse/clickhouse-server:26.10.2.103`, one shard and one replica, and `allow_database_iceberg = 1` in the default profile.
- **Users.** `admin` (`CLICKHOUSE_ADMIN_PASSWORD`, access management, used by the setup jobs), `langfuse` (`CLICKHOUSE_LANGFUSE_PASSWORD`, `ALL` on `langfuse.*`, `SELECT` on `system.*`) and `openmetadata` (`CLICKHOUSE_OPENMETADATA_PASSWORD`, profile `openmetadata` with `readonly = 2` and `allow_database_iceberg = 1`, `SELECT` on `lakehouse.*`, `system.tables` and `system.columns`). The operator's management user is added by the Altinity normalizer with its own profile and is not defined here.
- **Passwords.** Each comes from an ExternalSecret on ClusterSecretStore `doppler`. Password references use `valueFrom.secretKeyRef`, the 0.27.4 form; the removed `k8s_secret_` prefix is not used.
- **Lakehouse split.** `lakehouse` now creates only the `lakehouse` DataLakeCatalog database, as admin, after the instance accepts connections. It authenticates the Polaris client through `catalog_credential` with `auth_scope` and `oauth_server_uri`, and recreates the database only when its rendered definition changes; the definition digest is stored in the database comment. A recreate drops the database object while it is rebuilt, so checks that run in that window fail. Iceberg tables are not touched.
- **Spike.** Before the chart is accepted, confirm on the 26.10 server: the exact OpenMetadata FQN and schema listing for the `clickhouse` connector; `catalog_credential` and the OAuth settings; that `COMMENT` on `CREATE DATABASE` round-trips through `system.databases`; that `DROP DATABASE` on a DataLakeCatalog leaves the Polaris tables in place; that `SHOW CREATE DATABASE lakehouse` as `openmetadata` and `system.query_log` show the client secret masked; and that the connector lists `lakehouse` with only `system.tables` and `system.columns` granted, adding the minimal grant to the instance if not.
- **Sizing.** Dev keeps the draft size, 2 CPU and 8 GiB requests with a 12 GiB limit, against the dev Proxmox workers, about 20 vCPU and 64 GiB across three VMs. Prod requests 500m CPU and 3 GiB with a 4 GiB memory limit and no CPU limit, sized beside the other databases on two On-Demand `m6a.xlarge` workers (4 vCPU and 16 GiB each). The platform docs do not record rendered prod requests, so the fit is unconfirmed until rollout.

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
  grafana/constraints/max_execution_time/max: 65  # clickhouse-go sends the 60 s plugin timeout + 5 s
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
- **`GRANT SELECT ON lakehouse.*`.** It is not documented for DataLakeCatalog databases, but `openmetadata` depends on it, and the dev spike confirmed it.
- **`system.query_log` scope.** ClickHouse shows other users' queries in `system.query_log` to anyone granted `SELECT` on it. That is acceptable for an in-cluster pipeline user, and it is noted as a risk.
- **Analytics MCP tool (#103).** It reuses this model: a `grafana`-like reader, or a second reader on `marts.*`.

### 20. The Polaris catalog belongs to this repository

The `lakehouse` application deploys the Polaris REST catalog that Spark, ClickHouse and the warehouse registration call, with its CNPG store, signing keys, object-storage secret and network policy. Its ExternalSecrets are in wave -2, the CNPG cluster in wave -1, and Polaris in wave 0 of that application. The `lakehouse` application is wave 1 and data-pipelines is wave 2. These waves order when Applications are created, not when they become healthy: this Argo CD has no Application health customization, so a later wave does not wait for an earlier application's health. Ordering therefore rests on retries. The warehouse registration and ClickHouse database jobs are PostSync hooks that retry until Polaris and ClickHouse answer, and both are idempotent. ClickHouse stays a separate application, and nothing in `lakehouse` depends on a later wave being healthy. The Service `iceberg-catalog.data.svc.cluster.local:8181`, realm `PLATFORM` and client `platform` do not change. Polaris chart 1.8.0 is vendored under `infra/charts/lakehouse/charts` because CI lints without fetching dependencies. That chart exposes liveness and readiness values but no startup probe, so liveness waits 60 seconds before its first check and tolerates six failures. Whether `CREATE DATABASE ... ENGINE = DataLakeCatalog` contacts the catalog at creation is a spike item; the job retries until ClickHouse answers and is idempotent either way.

### 21. Lakehouse layout and maintenance

- **Partitioning.** Bronze events by `_source` and `days(occurred_at)`, labels by `_source` and `days(label_available_at)`. Silver by `payment_day` (labels by availability day, rejected events by `partition_day`). Gold features and labels by day of `event_timestamp`, `dim_customer` by month of `valid_from_ts`.
- **Writes.** Every table has `write.distribution-mode = hash` and a 256 MB target file size, except the historical Bronze and Silver event writes, which write locally with the fanout writer because their input is already laid out by day.
- **Nightly (`maintain-lakehouse`, task `nightly`, suspended 03:43 UTC CronWorkflow).** Expire snapshots older than seven days, keeping the last ten; drop `audit-*` branches older than seven days, which would otherwise pin their files; bin-pack the Bronze files ingested in the last three days, the only small files the pipeline makes (Kafka appends). Silver and Gold need no compaction: every run rewrites whole partitions at the target size.
- **On demand (task `zorder`, `start`, `end`).** Z-order one month of `silver.attempts` by `customer_id` and `payment_method_fingerprint`, reporting one customer's planned files and bytes before and after.
- **Safety.** Maintenance holds `dp-writer` and `dp1-bronze-writer`, so it never commits alongside a pipeline run, and never moves Kafka progress, which lives in the Bronze rows.

### 22. Grafana dashboards as code

- **Dashboards.** Kept in `infra/charts/data-pipelines/dashboards/{payments-overview,merchant-drilldown,fraud-outcomes}.json` and rendered by `templates/dashboards.yaml` as ConfigMaps in `data`, with the label `grafana_dashboard: "1"` and the annotation `grafana_folder: Payments analytics`. Every panel's datasource is `{"type": "grafana-clickhouse-datasource", "uid": "clickhouse-marts"}`, with SQL using `$__timeFilter(day)` or `$__timeFilter(started_at)`.

| Dashboard | Panels | Source |
|---|---|---|
| Payments overview | daily attempts, approval rate, risk-decline rate, attempted and succeeded amounts; top 10 merchants by attempts; hot-merchant share (its attempts / all attempts) | `fact_merchant_day` |
| Merchant drill-down | `$merchant` variable (top 200 merchants by attempts); daily KPIs; outcome mix; recent attempts table | `fact_merchant_day`, and `fact_payment_attempt` filtered by the primary key |
| Fraud outcomes | truth fraud caught (risk declined) against missed (succeeded); chargeback rate, shaded where `chargeback_window_complete` is false; risk decision against truth (the historical policy records no risk score); scenario breakdown (synthetic oracle, labelled as such) | `fact_payment_attempt`, `fact_merchant_day` |

**Platform requests (the platform repository makes these, not this change):**

1. **Plugin.** Add `grafana-clickhouse-datasource@4.23.3` to the VictoriaMetrics k8s stack's `grafana.plugins`. The embedded grafana chart renders it as `GF_PLUGINS_PREINSTALL_SYNC`.
2. **Datasource.** Provision it with name `ClickHouse marts` and `uid: clickhouse-marts`. `jsonData`: `host: clickhouse.data.svc.cluster.local`, `port: 9000`, `protocol: native`, `secure: false`, `username: grafana`, `defaultDatabase: marts`. `secureJsonData.password` comes from an environment variable fed by the Secret below.
3. **Doppler keys**, in the dev and prod configs: `CLICKHOUSE_GRAFANA_PASSWORD`, read by this repository's clickhouse chart and by the platform's Grafana, and `CLICKHOUSE_MARTS_PASSWORD`, read by this repository only.
4. **ExternalSecret.** One in `observability` that maps `CLICKHOUSE_GRAFANA_PASSWORD` into the Grafana pod's environment.
5. **Sidecar coverage.** The dashboard sidecar must watch namespace `data` (`sidecar.dashboards.searchNamespace` includes `data` or `ALL`), with folder annotations enabled (`folderAnnotation: grafana_folder`).
6. **Network.** If any NetworkPolicy restricts `data`, allow `observability/grafana` → `data/clickhouse` on TCP 9000.

These requests block only the dashboard evidence.

### 23. Images and CI

- **Spark image:** adds `spark-sql-kafka-0-10_2.13:4.1.3` with its transitive `kafka-clients` and `commons-pool2`, plus `spark-protobuf_2.13:4.1.3`, each pinned by checksum. The image copies `contracts/data-products`.
- **Metadata image:** copies the same contracts.
- **Rebuild paths:** `.github/service-images.json` lists every copied directory as an image rebuild path.
- **CI:** the Python CI job points at `src/data-pipeline/batch` and the rewritten simulator; there are no new jobs.

## Risks / Trade-offs

- **[The prod ClickHouse is small (3.4 GiB, 30 GiB)]** → Per-day mart inserts with a small hash build side, spill settings on every check, range-scoped checks, and one writer mutex so builds, checks and benchmarks never overlap.
- **[The on-demand pool is nearly full]** → Drivers stay on on-demand nodes by user decision; the pool was raised to three nodes on 2026-11-01 when drivers could not schedule.
- **[Spot executors are reclaimed]** → Spark retries, decommission and failure exclusion; every Spark write is an idempotent overwrite, and `dim_customer` resets its branch before a retry.
- **[`dim_customer` is sequential]** → Backfill months run in order and stop at the first failure; a Silver change to an older day needs a re-run from that day onward.
- **[Kafka retention is 7 days]** → `failOnDataLoss` fails Bronze loudly; enabling `dp-daily` must keep its cadence inside retention.
- **[Daily anchors skew training against serving by the DP3 delay]** → Recorded in the feature definition's availability block.
- **[The hot merchant dominates one-merchant queries]** → Reported for the hot and a median merchant, not averaged away.
- **[The wall-clock mart `TTL` shrinks the marts while live data is off]** → Accepted; evidence records `build_info`'s window.
- **[OpenMetadata cannot import the ODCS v3 files]** → Git stays the authority for contracts and products.
- **[Grafana evidence depends on the platform]** → The sidecar namespace, the plugin and the datasource are platform requests; the marts and checks do not depend on them.

## Migration Plan

Done on prod: a clean cluster, `dp1-bronze` for v19, then `dp-backfill` from 2025-10-29. Silver, Gold and the marts are published through 2026-08-31; `dp-backfill-8h7gq` continues to 2026-10-28.

**Rollback:** revert on `main`, then drop the `silver` and `gold` namespaces and the `marts` database in the affected environment. Bronze and the source datasets are untouched.

## Open Questions

- OpenMetadata 2.0.6 rejects the ODCS v3 files as written (schema-level `customProperties` as a list, `servers.environment` as a string, a server-side null pointer). Retry the import on a newer OpenMetadata; publish data products once ODPS is supported natively.
