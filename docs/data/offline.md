# Offline data

Historical generation produces reproducible payment lifecycles and labels in Parquet. Spark preserves deliveries in Bronze, canonicalizes events and attempts in Silver, then builds Gold history features. ClickHouse builds analytics marts from Silver separately. Each stage changes the row's meaning as well as its schema.

## Historical generation

[`historical-full.yaml`](../../src/data-pipeline/simulator/configs/historical-full.yaml) defines v19: 365 purchase-start days from 2025-10-29, seed 2603, 3M customers, 20K merchants, 3.9M payment methods and 4.2M devices. [`historical-dev.yaml`](../../src/data-pipeline/simulator/configs/historical-dev.yaml) uses a smaller population with the same generator.

### Model and sampling techniques

The generator builds a synthetic world first, then purchases, then valid payment lifecycles. It does not sample every event column independently.

| Technique | Implementation and purpose |
|---|---|
| Deterministic random streams | [`world.py`](../../src/data-pipeline/simulator/simulator/world.py) derives NumPy `SeedSequence` streams from the seed, generation stage and stable day/customer/purchase keys. UUIDv5 identities and normalized HMAC fingerprints keep entities consistent. Worker count, shard assignment and reruns do not change the generated records. |
| Unequal customer activity | A mean-normalized lognormal activity factor varies each customer's Poisson purchase count around `purchases_per_customer_day: 0.17`. This creates occasional and frequent buyers rather than one uniform rate. |
| Calendar-aware arrival times | Region/timezone, local-hour weights, weekday multipliers and month multipliers shape purchase volume and timestamps. The [calibration file](../../src/data-pipeline/simulator/simulator/us-calibration.json) records assumptions; this is an illustrative synthetic population, not a fitted production traffic model. |
| Popularity and hot-key skew | Merchant/category selection uses truncated Zipf distributions (exponents 1.1/1.2). Half of purchases explicitly select the hot merchant; familiar/Zipf selection can select it too, giving about 56% observed share. This stresses downstream merchant aggregations. |
| Correlated context and spend | Purchases usually reuse familiar merchants (80%) and context (95%). Category/condition prices, customer spend and lognormal amount variation determine integer minor-unit amounts; device/network rotation and foreign-IP draws introduce changes. |
| Fraud scenarios | High-value purchases, stolen cards, account takeover and card-testing bursts alter related amount/context/attempt behavior together. Friendly fraud adds a later chargeback case. Simulation truth remains separate from the rules policy's decision. |
| Lifecycle expansion | [`lifecycle.py`](../../src/data-pipeline/simulator/simulator/lifecycle.py) expands each purchase into creation, attempt, risk decision and processor/result events. It models declines, retries, unknown processor outcomes and expiry; aggregate versions preserve payment order. |
| Deliberate data problems | Each event has a 2% chance of one republished copy with the same `event_id` and business content but `recorded_at` 1–300 seconds later. Chargebacks are reported 7–21 days after successful eligible attempts; absent chargebacks are unknown, not negative. |

Observed Silver cardinality for **2026-10-16–28**: 6,816,149 attempts, 2,077,836 customers and all 20,000 merchants. Distinct-key/row ratios are **0.304840** (customer) and **0.002934** (merchant), with zero null keys. The hot merchant contributes 3,843,873 attempts (**56.39%**). These are measured counts, separate from configured population sizes.

![Observed customer and merchant cardinality in ClickHouse](images/generator-cardinality-2026-11-03.png)

### Source schema and version change

[`arrow.py`](../../src/data-pipeline/simulator/simulator/arrow.py) defines typed Arrow schemas; [`historical.py`](../../src/data-pipeline/simulator/simulator/historical.py) writes Zstd-compressed Parquet in customer chunks with buffered row groups. Files are organized by purchase-start day, not by label availability.

| Source output | Row grain | Schema |
|---|---|---|
| `payment_events/date=…/part-….v2.parquet` or `.v3.parquet` | One lifecycle event delivery; duplicates intentionally remain | Envelope: `event_id`, `payment_id`, `integration_id`, `aggregate_version`, `schema_version`, `occurred_at`, `recorded_at`. One nested payload, such as `attempt_started` or `risk_decision_recorded`, supplies the business fields. |
| `fraud_labels/date=…/part-….parquet` | One label observation per attempt/source | `integration_id`, `payment_id`, `attempt_id`, `is_fraud`, `occurred_at`, `label_available_at`, `source` and optional `scenario`. Truth and chargeback observations remain separate. |

The event's `occurred_at` selects the schema version: v2 before 2026-04-30, v3 from that instant. A single purchase-day partition can contain both versions when its lifecycle crosses the cutover.

| Nested field added in v3 | Physical v2 Parquet | v3 meaning |
|---|---|---|
| `attempt_started.signals.user_agent_family` | Field absent | Client user-agent family |
| `risk_decision_recorded.assessment.assessment_id` | Field absent | Durable assessment identity |
| `risk_decision_recorded.assessment.evidence_sha256` | Field absent | Assessment evidence digest |

The generator removes these fields from both records and Arrow schemas for v2; it does not merely fill a v3-shaped file with nulls. [Generation tests](../../src/data-pipeline/simulator/tests/test_generation.py) verify physical schemas, occurrence-time cutover and equal records across worker counts, shards and interrupted runs.

### Publication

Submit `generate-historical` in `data`. Shards write per-day Parquet, separate `.v2`/`.v3` event files and `_day.json` markers; `config.json` pins inputs and the generator digest, and `manifest.json` publishes the inventory. Complete days are skipped, incomplete days regenerated, and changed inputs or an already published version are refused. Full-scale v19 publication also enforces the configured 100 GB minimum source size. Credentials come from Secrets; roots and shard settings come from chart values.

| Dataset / run | Events | Labels | Files / size |
|---|---:|---:|---|
| Dev v18, `generate-historical-v18-77fzr` | 823,963 | 161,007 | 181 / 112.4 MB; 11m50s |
| Recorded AWS v19, `generate-historical-v19-nvp7l` | 983,727,083 | 192,204,996 | 2,924 / 133.5 GB; 49 min |
| GCP v19, `generate-historical-v19-gcp-mstrk` | 983,727,083 | 192,204,996 | 2,924 / 133,498,693,796 bytes; succeeded |

Dev uses `s3://platform-dev/datasets/payment-gateway/historical/v18`; GCP uses `s3://talos-proxmox-prod-platform/datasets/payment-gateway/historical/v19`. GCP's manifest SHA-256 is `78ee54fe2a607ae0c128bc6765d89238f28d00270fca5ad48a8fed44fd0f8608`, matching the existing live-traffic pin. Its per-file rows/bytes, unique paths and 365 dates reconcile; this check did not reread every Parquet object or run downstream jobs. The recorded dev/AWS readbacks found no empty parts and verified physical v2/v3 schemas.

## Spark transformations

<a id="pipeline"></a>

`dp1-bronze` ingests the published source. `dp-pipeline(start, end)` builds Silver, then Gold features and the marts in parallel because both consume Silver. Each output goes to an audit branch or staging table, passes contract checks, then publishes. Daily runs and monthly backfills share these range-bounded jobs; plan steps skip matching range/input-snapshot/image outputs.

```mermaid
flowchart LR
    raw["Parquet: nested event deliveries + label observations"] --> bronze["Bronze: same business schema + provenance"]
    bronze --> silver["Silver: canonical events + flat attempts + linked labels"]
    silver --> gold["Gold: entity/time features, SCD2 profiles, training labels"]
    silver --> marts["Marts: attempt fact + merchant/day aggregates"]
```

### DP1: source deliveries → Bronze

[`bronze/ingest.py`](../../src/data-pipeline/batch/batch/bronze/ingest.py) validates manifest paths/sizes, reads all event parts with `mergeSchema=true` and conforms them by field name to the target [Bronze schema](../../src/data-pipeline/batch/batch/bronze/schema.py). Historical ingestion replaces only that dataset version and verifies written row counts per source file.

| Change at this boundary | Bronze result |
|---|---|
| Mixed physical v2/v3 schemas | One typed event schema; missing nested v2 fields become null. Existing payloads, IDs, event versions and delivery timestamps are retained; `payload_case` identifies the populated payload. Schema version still determines which fields a contract requires. |
| Source file → queryable Iceberg row | Add `_source`, `_dataset_version`, `_source_file`, `_ingested_at` and `_run_id`. Kafka provenance fields are null for historical input. |
| Delivery and label grain | `bronze.payment_events` keeps every delivery, including duplicates; `bronze.fraud_labels` keeps label observations. No attempt-level flattening or feature calculation happens here. |

For the Kafka input mode, the same tables also hold decoded records with topic/partition/offset/timestamp provenance. Invalid records go to `bronze.rejected_records`; committed row offsets determine restart progress even after compaction.

Historical files already touch few day partitions, so Bronze uses local fanout writes instead of hash redistribution; see the [write-distribution results](storage.md#write-distribution).

### DP2: Bronze → canonical Silver

[`silver/curate.py`](../../src/data-pipeline/batch/batch/silver/curate.py) reads Bronze from `start − 1` through `end + 1`, assigns each payment to its first event's UTC day and overwrites only the requested payment days. This keeps a lifecycle crossing midnight together. Labels are rebuilt by availability day and linked against 45 days of attempts.

| Silver output | Schema and grain change | Transformation |
|---|---|---|
| `silver.payment_events` | One canonical row per `event_id`; keeps the nested envelope/payload and adds `event_hash` and `payment_day`, retaining `_source` rather than file/offset provenance | Hash business fields excluding `recorded_at`; group by event ID, retain the earliest identical delivery. |
| `silver.rejected_events` | Rejection metadata plus serialized offending rows | Different hashes for one event ID quarantine every event of the affected payment, preventing a partly accepted lifecycle. |
| `silver.attempts` | One flat row per `(integration_id, payment_id, attempt_id)` | Group start/decision/result events by attempt and aggregate version, join payment status, derive outcome and `lifecycle_valid`. |
| `silver.labels` | One deduplicated observation with `observation_id`, attempt/outcome references and `linkage` | Distinguish linked, pending, orphan and violation; orphan/violation observations fail publication checks. |

For example, nested `attempt_started.context.snapshot.total.minor_units` becomes `silver.attempts.amount_minor` (`bigint`), customer context becomes `customer_id`, and assessment outcome/ID become `risk_outcome`/`assessment_id`. `started_at` is server event time; `attempted_at` preserves client time. Terminal events produce `outcome`/`outcome_at`; pending/unknown attempts remain unresolved rather than becoming invented declines. v2 rows retain null assessment IDs after flattening. Raw evidence digests remain in canonical events rather than becoming attempt features.

**Deduplication and schema evolution.** Published-table results on 2026-11-04 use identical occurrence-date/source predicates in both zones:

| Historical events, 2026-10-20 | Deliveries | Distinct event IDs | Extra deliveries |
|---|---:|---:|---:|
| Bronze | 2,558,103 | 2,507,498 | 50,605 (1.98% of deliveries) |
| Silver | 2,507,498 | 2,507,498 | 0 |

![Same-day Bronze and Silver deduplication result](images/spark-dedup-2026-11-04.png)

Across the April 30 cutover, Bronze has 480,421 v2 / 495,868 v3 risk decisions; Silver has 471,209 / 486,120 after deduplication. In both zones, every v2 assessment ID is null and every v3 assessment ID is present. This proves the added field survives schema merging and curation without inventing values for older records.

![Schema evolution across the v2/v3 cutover in Bronze and Silver](images/spark-schema-evolution-2026-11-04.png)

The [Bronze-to-Silver integration test](../../src/data-pipeline/batch/tests/bronze/test_bronze.py) writes physically different v2/v3 Parquet schemas plus a redelivery: 11 Bronze deliveries become 10 canonical events and two valid attempts. [Conflict tests](../../src/data-pipeline/batch/tests/silver/test_silver.py) also verify that differing business hashes quarantine the whole payment.

**Scale techniques.** A narrow event-ID/hash aggregation replaces a broadcast of about 20M duplicate IDs; attempt construction uses one grouped aggregation instead of separate payload windows and joins. Range overwrites avoid full-history recomputation. `persist(MEMORY_AND_DISK)` allows lost Spot blocks to be recomputed from lineage instead of relying on local checkpoints.

<a id="prod-v19-run"></a>

AWS v19 publication, 2025-10-29 to 2026-10-28: 964,433,589 canonical events, 191,148,630 attempts, 192,163,964 labels and zero rejected events.

**Controlled skew comparison.** [`silver/benchmark.py`](../../src/data-pipeline/batch/batch/silver/benchmark.py) ran as `dp2-rubric-split-nqvm4` on GCP on 2026-11-04, using the same cached 6,816,149 attempts from October 16–28, pinned to Silver snapshot `306587680516707839`. Two one-core executors (2 GiB heap + 1 GiB overhead each), 64 shuffle partitions, no static/adaptive broadcast; coalescing disabled and skew/advisory thresholds 1 MiB so this bounded input exercises actual splitting.

| Variant | Wall time | Tasks | Result |
|---|---:|---:|---|
| Skew handling off | 24.68 s | 174 | Ordinary sort-merge join |
| AQE skew handling | 20.48 s | 417 | One skewed partition, 13 splits |
| 16-bucket salting by attempt ID | 20.29 s | 381 | Salted join |

All variants produced **202,070 rows** with checksum `-2964469759385603852493`. AQE reduced wall time 17.0%, salting 17.8%, while scheduling more tasks. Single sequential warm-cache run.

![Spark SQL UI comparing baseline, AQE and salted queries](images/spark-skew-comparison-2026-11-04.png)

![AQE physical plan showing one skewed partition and 13 splits](images/spark-aqe-split-2026-11-04.png)

### DP3: Silver → Gold history features

[`gold/features.py`](../../src/data-pipeline/batch/batch/gold/features.py) and [`gold/windows.py`](../../src/data-pipeline/batch/batch/gold/windows.py) implement [`fraud-history-batch-v1`](../../contracts/features/fraud-history-batch-v1.yaml). The grain changes from an attempt to an entity at a UTC-midnight anchor.

| Gold output | Key / time fields | Derived schema |
|---|---|---|
| `feat_customer_daily` | `integration_id`, `customer_id`, `event_timestamp`; `created` identifies the source snapshot's commit time | Nullable counts such as `attempt_count_7d` (`bigint`), `amount_sum_minor_30d` (`bigint`), `decline_rate_7d` (`double`), and exact distinct merchants/devices/payment methods |
| `feat_payment_method_daily` | `integration_id`, `payment_method_fingerprint`, `event_timestamp`, `created` | Equivalent card-history measures, including distinct customers |
| `dim_customer` | `customer_sk`, customer keys, `valid_from_ts`/`valid_to_ts`, `version_number`, `is_current` | SCD2 profile fields, `profile_hash` and familiar-device state; a new version represents a change, not another payment |
| `label_attempt` | Attempt keys, entity keys and scoring-time `event_timestamp` | Earliest linked label per attempt/source, with `is_fraud` and separate `label_available_at` |

AWS v19 output: 34,004,866 customer versions, 348,030,903 customer-feature rows, 362,959,360 card-feature rows and 192,163,964 attempt labels.

For an anchor `s`, features read `[s − window, s)` using `coalesce(attempted_at, started_at)` and require that the attempt was observed before `s`. Outcome measures additionally require `outcome_at < s`. Incomplete windows are null; a complete empty window can be zero. Training takes the latest feature row at/before scoring time and only labels available by the cutoff.

**Scale techniques.** Emit rows only when a value changes or coverage completes, rather than every entity every day: recorded output was 348M customer/363M card rows instead of about 1.1B rows per daily table. Familiar devices (used at least twice in 90 days) use an ordered XOR sweep rather than an interval join. Retries reset the audit branch before the dimension's delete/append rewrite. Gold's 12 GiB executor budget shifted from 9 GiB heap + 3 GiB overhead to 7 + 5, with `MALLOC_ARENA_MAX=2`, after native-memory OOMs.

The 5-minute/1-hour/24-hour stream-feature tables are produced by Flink and its writer, including replay; this Spark stage owns the 7/30-day history.

### Silver → ClickHouse marts

[`gold/marts.py`](../../src/data-pipeline/batch/batch/gold/marts.py) builds a rolling 90-day analytics window independently of Gold feature computation.

| Mart | Schema change from Silver |
|---|---|
| `fact_payment_attempt` | Keep one flat attempt row, join label summaries, and add nullable `truth_is_fraud`, `truth_scenario` and `chargeback_available_at`. Convert Spark timestamps to UTC `DateTime64` and categorical fields to `LowCardinality` strings. |
| `fact_merchant_day` | Aggregate attempts by `(integration_id, merchant_id, day)` into counts, minor-unit sums, exact distinct customers and rates; `chargeback_window_complete` marks delayed-label coverage. |
| `build_info` | One build record with code version, rebuilt days, retention window, Silver snapshot IDs and build time for reproducibility. |

**Bounded construction.** Stage only rebuilt days, build label summaries in eight hash passes and insert the attempt fact in bounded hash buckets. Validate staging before `REPLACE PARTITION`. This keeps the mart build within ClickHouse's 2 GiB query budget and prevents a failed build from publishing partial outputs.

AWS v19 marts: 44,209,535 attempts for 2026-08-04 to 2026-10-28 (6.05 GiB) and 1,327,898 merchant-day rows. All 58 quality checks passed across 15 Silver, Gold and mart tables. [Range-scoped checks](orchestration-governance.md#range-scoped-validation) reduce repeated audit work; Gold and marts run concurrently after Silver.
