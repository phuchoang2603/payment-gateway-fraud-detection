# Feature store

Feast holds the fraud features in two stores: an online store (Redis) for decision-time lookups by Go Fraud, and an offline store for point-in-time training data. The daily and streaming definitions govern both paths. Code: [`src/data-pipeline/feast`](../../src/data-pipeline/feast); definition: [`contracts/features`](../../contracts/features); reference implementation: [`src/data-pipeline/features`](../../src/data-pipeline/features).

The build order for the offline store, materialization, TTLs and feature services is in the [roadmap](../overview/roadmap.md#feature-store-build-order).

## Features

Keys are integration-scoped customer and payment-method identities. Request features come from the attempt itself; history features count earlier attempts in `[t − window, t)` with explicit coverage, so missing or stale history is null, never zero.

| Group | Features | Computed by | Status |
|---|---|---|---|
| Request | amount, customer account age, IP and shipping country mismatch | Request context | Done |
| Short history | customer and method attempt counts over 5 minutes, 1 hour and 24 hours; distinct customer devices over 24 hours | Flink | Flink writes the v2 journal; Feast push jobs write Redis and the offline stream tables |
| Long history | 7- and 30-day attempt, amount, decided, declined and distinct counts and decline rates per customer and payment method, at daily anchors | DP3 Spark over Silver into `gold.feat_customer_daily` and `gold.feat_payment_method_daily` ([`fraud-history-batch-v1`](../../contracts/features/fraud-history-batch-v1.yaml)); Feast materialization | Implemented; cluster evidence pending |

Labels, scenario and oracle fields, and anything that happens after the attempt (decision, outcome, payment status) are never features.

## Online and offline paths

- **Online:** the Flink journal feeds `feast-push-online` for Redis and `feast-push-offline` for Iceberg. `fraud_model_v1` exposes customer and payment-method daily and stream views under natural keys.
- **Offline:** a training row uses plain `started_at` and takes the latest daily anchor at or before that timestamp, including an anchor at exactly that instant. Both push jobs map stream rows to `event_timestamp = online_write_at + 1µs`, so a snapshot written at the scored attempt's start is invisible then. The remaining skew is the DP3 delay after midnight. Long windows are batch-only; Flink keeps only the short windows, with no HybridSource bootstrap.
- **Materialization:** `feast-materialize` pins the daily tables, loads incremental rows and refreshes entities affected by DP3 reruns, including deletion tombstones. Its online check must pass before consumption state advances.

Training uses `label_attempt` rows with `label_source = LABEL_SOURCE_SIMULATION_TRUTH` and `label_available_at <= cutoff`, and their `label` as its target. Retrieval requests `fraud_model_v1` with `full_feature_names=True`. Time-ordered splits need no label-maturity gaps. The initial history replay uses `start=2026-07-29`, `end=2026-10-29` (exclusive); the workflow adds the one-day lead-in.

The training image installs `fraud_feast[spark]` and runs `python -m fraud_feast.spark_jars <spark-jars-directory>` during its build to use the same pinned, checksum-verified JARs as `data-platform-spark`. Set `SPARK_MASTER=local[*]` for retrieval in one Kubeflow Pipelines step, with local disk for Spark spill. Without that variable, Feast leaves the master to the SparkApplication.

## TTL and freshness

Daily views use 31 days 12 hours; stream views use 24 hours 5 minutes. Each TTL covers its longest window plus pipeline delay. Feast applies TTLs offline, and Go Fraud applies the same TTLs at scoring time, with independent evidence for each view.
