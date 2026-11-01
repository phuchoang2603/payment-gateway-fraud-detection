# Feature store

Feast holds the fraud features in two stores: an online store (Redis) for decision-time lookups by Go Fraud, and an offline store for point-in-time training data. The daily and streaming definitions govern both paths. Code: [`src/data-pipeline/feast`](../../src/data-pipeline/feast); definition: [`contracts/features`](../../contracts/features).

The build order for the offline store, materialization, TTLs and feature services is in the [roadmap](../overview/roadmap.md#feature-store-build-order).

## Features

Keys are integration-scoped customer and payment-method identities. Request features come from the attempt itself; history features count earlier attempts in `[t − window, t)` with explicit coverage, so missing or stale history is null, never zero.

| Group | Features | Computed by | Status |
|---|---|---|---|
| Request | amount, customer and merchant account ages, nullable IP/shipping mismatch, item count, device presence | Feast pure Python request_features on-demand view | Shared by training and serving |
| Short history | customer and method attempt counts over 5 minutes, 1 hour and 24 hours; distinct customer devices over 24 hours | Flink | Flink writes the v2 journal; feature writers write Redis and the offline stream tables |
| Long history | 7- and 30-day attempt, amount, decided, declined and distinct counts and decline rates per customer and payment method, at daily anchors | DP3 Spark over Silver into `gold.feat_customer_daily` and `gold.feat_payment_method_daily` ([`fraud-history-batch-v1`](../../contracts/features/fraud-history-batch-v1.yaml)); Feast materialization | Implemented; cluster evidence pending |

Labels, scenario and oracle fields, and anything that happens after the attempt (decision, outcome, payment status) are never features.

## Online and offline paths

- **Online:** the Flink journal feeds `feature-writer-online` for Redis and `feature-writer-offline` for Iceberg. `payment_risk_v1` exposes customer and payment-method daily and stream views under natural keys, plus request_features from raw fields in the same request. Go rules retain their own logic.
- **Offline:** a training row uses plain `started_at` and takes the latest daily anchor at or before that timestamp, including an anchor at exactly that instant. Both writers map stream rows to `event_timestamp = online_write_at + 1µs`, so a snapshot written at the scored attempt's start is invisible then. The remaining skew is the DP3 delay after midnight. Long windows are batch-only; Flink keeps only the short windows, with no HybridSource bootstrap.
- **Materialization:** `feast-materialize` runs local Spark in one Argo container, loading from each registry last end minus lookback plus one day through now (first run: now minus TTL). Use `since=<backfill start>` after a backfill; end is always now. The check samples newest keys only. If DP3 deletes every row of an entity, its prior online value remains until TTL.

`fraud_feast.training.entity_rows(spark, start, end, cutoff)` joins lifecycle-valid Silver attempts and their attempt_started request context to `label_attempt` rows with `label_source = LABEL_SOURCE_SIMULATION_TRUTH` and `label_available_at <= cutoff`, and their `label` as its target. Retrieval requests `payment_risk_v1` with `full_feature_names=True`. Time-ordered splits need no label-maturity gaps. The initial history replay uses `start=2026-07-29`, `end=2026-10-29` (exclusive); the workflow adds the one-day lead-in.

The training image installs `fraud_feast[spark]` and runs `python -m fraud_feast.spark_jars <spark-jars-directory>` during its build to use the same pinned, checksum-verified JARs as `data-platform-spark`. Set `SPARK_MASTER=local[*]` for retrieval in one Kubeflow Pipelines step, with local disk for Spark spill. Local Spark is also the default for materialization. No Feast SparkApplications are created.

History deletes each month's history-epoch rows before the offline writer appends normally, dropping lead-in rows. A failed month must be rerun before training. The gate requires zero decode rejects and consumption of all seeded offsets; duplicates and too-late rejects are reported only. `feature-writer-replay` repairs the online writer only with `from`/`until`.

## TTL and freshness

Daily views use 31 days 12 hours; stream views use 24 hours 5 minutes. Each TTL covers its longest window plus pipeline delay. Feast applies TTLs offline, and Go Fraud applies the same TTLs at scoring time, with independent evidence for each view.
