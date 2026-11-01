# Feature store

Feast holds the fraud features in two stores: an online store (Redis) for decision-time lookups by Go Fraud, and an offline store for point-in-time training data. One versioned feature definition governs both. Code: [`src/data-pipeline/feast`](../../src/data-pipeline/feast); definition: [`contracts/features`](../../contracts/features); reference implementation: [`src/data-pipeline/features`](../../src/data-pipeline/features).

The build order for the offline store, materialization, TTLs and feature services is [below](#build-order).

## Features

Keys are integration-scoped customer and payment-method identities. Request features come from the attempt itself; history features count earlier attempts in `[t − window, t)` with explicit coverage, so missing or stale history is null, never zero.

| Group | Features | Computed by | Status |
|---|---|---|---|
| Request | amount, customer account age, IP and shipping country mismatch | Request context | Done |
| Short history | customer and method attempt counts over 5 minutes, 1 hour and 24 hours; distinct customer devices over 24 hours | Flink | Flink computes v2 window features into the feature journal topic; online and offline stores follow the [build order](#build-order) |
| Long history | 7- and 30-day attempt, amount, decided, declined and distinct counts and decline rates per customer and payment method, at daily anchors | DP3 Spark over Silver into `gold.feat_customer_daily` and `gold.feat_payment_method_daily` ([`fraud-history-batch-v1`](../../contracts/features/fraud-history-batch-v1.yaml)); Feast materialization planned | Offline tables implemented |

Labels, scenario and oracle fields, and anything that happens after the attempt (decision, outcome, payment status) are never features.

## Build order

Feast is built as one change after Bronze and Silver merge. Until then the streaming job ends at the feature journal topic, and the existing online-only Feast writer and Go Fraud's shadow retrieval are disabled until the feature-store change.

1. **DP3 offline features.** A daily Spark job over Silver writes `feat_customer_*` and `feat_payment_method_*` Iceberg tables keyed by integration, customer or payment method, with `event_timestamp` and `created`. It computes the long 7- and 30-day windows that Flink does not keep.
2. **Offline store.** Feast reads the `feat_` tables as its offline store, through a Spark source over the Iceberg catalog or a Parquet export, whichever Feast supports reliably. Training builds its dataset with `get_historical_features` against the label table, joining point-in-time on attempt time and label availability.
3. **Streaming features to offline.** A Kafka batch reader, the same pattern as Bronze Kafka mode, appends the feature journal topic into an Iceberg `feat_stream_*` table that the offline store also reads.
4. **Streaming features to online.** A Feast writer consumes the feature journal topic and pushes each update to Redis, committing offsets only after the write is accepted.
5. **Materialization.** An Argo workflow runs `feast materialize-incremental` from the offline store into Redis after each DP3 run, so batch features reach the online store.
6. **TTL and feature services.** Each feature view gets a TTL equal to its longest window plus the expected pipeline delay, with the reason recorded on the [feature store](#ttl-and-freshness) page. A versioned `FeatureService` (for example `fraud_model_v1`) names the features a model uses; training and Go Fraud both request it.
7. **Natural keys.** Feature views key on integration-scoped customer and payment-method IDs instead of the current synthetic snapshot key, so offline joins with labels are direct. Go Fraud's feature client moves to the new views and feature service in the same change.
8. **Governance.** OpenMetadata's native Flink pipeline connector, run from `data-pipelines` against the JobManager REST API, records the streaming job and its runs. If it does not resolve Kafka lineage for the PyFlink job, a deploy-time step publishes `payments.events.v1` → streaming job → feature journal and reject topics with the OpenMetadata SDK, as Bronze and Silver do. Feast has no OpenMetadata connector, so the governed assets are the `feat_` tables with DP3 lineage from Silver. Neither the Flink job nor Feast embeds the SDK, so a catalog outage cannot affect feature computation.

## Online and offline paths

- **Online:** the Flink job writes window features through one ordered writer that reads back each accepted revision before committing Kafka offsets. Two Feast views store typed nullable quantities and snapshot metadata.
- **Offline:** DP3 computes the long windows from Silver at daily UTC-midnight anchors, not at each attempt time: serving receives them from the daily batch, so a training row takes the latest anchor at or before its attempt, exactly what serving had. The remaining skew is the DP3 delay after midnight. Long windows are batch-only; Flink keeps only the short windows, with no HybridSource bootstrap.
- **Materialization:** a planned incremental job moves the latest offline values to the online store with checkpoints; older late writes never replace newer online state.

## TTL and freshness

**Planned:** a TTL per feature view, chosen from its longest window plus the expected pipeline delay, with the rationale recorded here. Go Fraud already rejects snapshots older than its configured anchor and progress budgets (30 seconds each, unmeasured initial values).
