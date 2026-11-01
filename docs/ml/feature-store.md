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
- **Offline:** DP3 computes the long windows from Silver at daily UTC-midnight anchors, not at each attempt time: serving receives them from the daily batch, so a training row uses `started_at − 1µs` and takes the latest anchor at or before that timestamp. The remaining skew is the DP3 delay after midnight. Long windows are batch-only; Flink keeps only the short windows, with no HybridSource bootstrap.
- **Materialization:** `feast-materialize` pins the daily tables, loads incremental rows and refreshes entities affected by DP3 reruns, including deletion tombstones. Its online check must pass before consumption state advances.

## TTL and freshness

Daily views use 31 days 12 hours; stream views use 24 hours 5 minutes. Each TTL covers its longest window plus pipeline delay. Feast applies TTLs offline, and Go Fraud applies the same TTLs at scoring time, with independent evidence for each view.
