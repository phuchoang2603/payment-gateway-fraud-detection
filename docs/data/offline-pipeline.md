# Offline pipeline

**Status: in progress** in [PR #102](https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/102). The design is fixed by the [`rebuild-generation-to-silver`](../../openspec/changes/rebuild-generation-to-silver/design.md) change; results are added here as runs complete.

Spark jobs on the Apache Spark Operator turn the [offline generator](offline-generator.md) datasets and the gateway's Kafka topics into Iceberg tables. Each pipeline is an Argo workflow with three stages (Spark job → StarRocks SQL checks → OpenMetadata publication); see [orchestration and governance](orchestration-governance.md).

## DP1: raw → Bronze

Bronze keeps every delivery, including duplicates, in two tables shared by both sources: `bronze.payment_events` and `bronze.fraud_labels`.

- **Historical mode** reads exactly the files listed in a dataset's `manifest.json`, verifies their SHA-256 and row counts, reads the `.v2` and `.v3` files together with `mergeSchema` (absent version-2 fields become null), casts losslessly to the Bronze contract and replaces only that dataset version's rows. It checks that Bronze row totals equal the manifest.
- **Kafka mode** reads `payments.events.v1` and `fraud.labels.v1` directly, decodes protobuf with the payments descriptor set, and appends. Each run starts from `max(_kafka_offset) + 1` per partition, read from Bronze itself, so progress and rows commit together and survive compaction. Records that fail decoding or the cast go to `bronze.rejected_records` instead of blocking later runs.
- **Provenance columns:** `_source`, `_dataset_version`, `_source_file`, `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_ingested_at`, `_run_id`.

## DP2: Bronze → Silver

A full recompute from Bronze snapshots fixed at the start of the run writes three tables:

- **`silver.payment_events`:** one row per `event_id`. Republished copies (same content, later `recorded_at`) collapse to the earliest. Copies that differ in any other business field are conflicts: the whole payment goes to `silver.rejected_events`.
- **`silver.attempts`:** one row per attempt with `customer_id`, `merchant_id`, `payment_method_fingerprint`, `amount_minor`, server start time, client `attempted_at`, risk decision, outcome and payment status. Lifecycle violations fail the checks.
- **`silver.labels`:** one row per distinct label observation, classified `linked`, `pending` or `violation` against the attempt's start and success. Violations fail the checks.

A run report records the Bronze snapshot IDs, input rows, duplicates, conflicts, rejected rows and label linkage counts.

## DP3: offline features

**Planned.** A daily Spark batch over Silver computes point-in-time features per customer and payment method, including the long 7- and 30-day windows that the streaming path does not keep, and materializes them into Feast. See [feature store](../ml/feature-store.md).

## Handling the offline problems

| Problem | Baseline | Optimized | Evidence |
|---|---|---|---|
| Skew (hot merchant) | Aggregation by merchant with AQE skew handling off | AQE skew join and salting of the hot key | Pending: benchmark job on v19 Silver, median of 3 runs, equal outputs, Spark UI |
| High cardinality | Distinct devices and attempt counts per customer and method over 24 h | Optimized variant in the same job | Pending |
| Schema evolution | Reading `.v2` and `.v3` files without schema merge loses the version-3 columns | `mergeSchema` plus one cast to the Bronze contract | Pending |
| Duplicates | Naive totals over Bronze overcount by about 2% | Silver deduplication on `event_id`, ignoring `recorded_at` | Pending |

## Schemas and naming

Bronze and Silver table schemas are the v1 Bronze and Silver data-product contracts, with generated references under [`products/`](products). Gold dimension and fact tables (SCD2 `valid_from_ts`, `valid_to_ts`, `is_current`) and `feat_` tables with `event_timestamp` and `created` are planned with DP3.
