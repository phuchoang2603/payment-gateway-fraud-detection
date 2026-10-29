# Feature store

Feast holds the fraud features in two stores: an online store (Redis) for decision-time lookups by Go Fraud, and an offline store for point-in-time training data. One versioned feature definition governs both. Code: [`src/data-pipeline/feast`](../../src/data-pipeline/feast); definition: [`contracts/features`](../../contracts/features); reference implementation: [`src/data-pipeline/features`](../../src/data-pipeline/features).

The build order for the offline store, materialization, TTLs and feature services is in the [roadmap](../overview/roadmap.md#feature-store-build-order).

## Features

Keys are integration-scoped customer and payment-method identities. Request features come from the attempt itself; history features count earlier attempts in `[t − window, t)` with explicit coverage, so missing or stale history is null, never zero.

| Group | Features | Computed by | Status |
|---|---|---|---|
| Request | amount, customer account age, IP and shipping country mismatch | Request context | Done |
| Short history | customer and method attempt counts over 5 minutes, 1 hour and 24 hours; distinct customer devices over 24 hours | Flink, written online | Online done (shadow), contract v2 |
| Long history | 7- and 30-day counts and amounts per customer and method | DP3 daily Spark batch over Silver, materialized into Feast | Planned |

Labels, scenario and oracle fields, and anything that happens after the attempt (decision, outcome, payment status) are never features.

## Online and offline paths

- **Online:** the Flink job writes window features through one ordered writer that reads back each accepted revision before committing Kafka offsets. Two Feast views store typed nullable quantities and snapshot metadata.
- **Offline:** DP3 computes the same definitions from Silver as of each attempt time, so training and serving share one definition. Long windows are batch-only; Flink keeps only the short windows, with no HybridSource bootstrap.
- **Materialization:** a planned incremental job moves the latest offline values to the online store with checkpoints; older late writes never replace newer online state.

## TTL and freshness

**Planned:** a TTL per feature view, chosen from its longest window plus the expected pipeline delay, with the rationale recorded here. Go Fraud already rejects snapshots older than its configured anchor and progress budgets (30 seconds each, unmeasured initial values).
