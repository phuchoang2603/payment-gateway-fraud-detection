# Feature store

Feast holds the fraud features in two stores: an online store (Redis) for decision-time lookups by Go Fraud, and an offline store for point-in-time training data. The daily and streaming definitions govern both paths. Code: [`src/data-pipeline/feast`](../../src/data-pipeline/feast); definition: [`contracts/features`](../../contracts/features).

The build order for the offline store, materialization, TTLs and feature services is in the [roadmap](../overview/roadmap.md#feature-store-build-order).

## Features

Keys are integration-scoped customer and payment-method identities. Request features come from the attempt itself; history features count earlier attempts in `[t − window, t)` with explicit coverage, so missing or stale history is null, never zero.

| Group | Features | Computed by | Status |
|---|---|---|---|
| Request | amount, customer and merchant account ages, nullable IP/shipping mismatch, item count, device presence | Feast pure Python request_features on-demand view | Shared by training and serving |
| Short history | customer and method attempt counts over 5 minutes, 1 hour and 24 hours; distinct customer devices over 24 hours | Flink | Flink writes the v2 journal; the feature writer writes Redis and the offline stream tables |
| Long history | 7- and 30-day attempt, amount, decided, declined and distinct counts and decline rates per customer and payment method, at daily anchors | DP3 Spark over Silver into `gold.feat_customer_daily` and `gold.feat_payment_method_daily` ([`fraud-history-batch-v1`](../../contracts/features/fraud-history-batch-v1.yaml)); Feast materialization | Implemented; cluster evidence pending |

Labels, scenario and oracle fields, and anything that happens after the attempt (decision, outcome, payment status) are never features.

## Online and offline paths

- **Online:** the Flink journal feeds one `feature-writer` that pushes Redis before buffering its Iceberg appends. `payment_risk_v1` exposes customer and payment-method daily and stream views under natural keys, plus request_features from raw fields in the same request. Go rules retain their own logic.
- **Offline:** a training row uses plain `started_at` and takes the latest daily anchor at or before that timestamp, including an anchor at exactly that instant. The writer maps stream rows to `event_timestamp = online_write_at + 1µs`, so a snapshot written at the scored attempt's start is invisible then. The remaining skew is the DP3 delay after midnight. Long windows are batch-only; Flink keeps only the short windows, with no HybridSource bootstrap.
- **Materialization:** `feast-materialize` runs local Spark in one Argo container, loading from each registry last end minus lookback plus one day through now (first run: now minus TTL). Use `since=<backfill start>` after a backfill; end is always now. The check samples newest keys only. If DP3 deletes every row of an entity, its prior online value remains until TTL.

`fraud_feast.training.entity_rows(spark, start, end, cutoff)` joins lifecycle-valid Silver attempts and their attempt_started request context to `label_attempt` rows with `label_source = LABEL_SOURCE_SIMULATION_TRUTH` and `label_available_at <= cutoff`, and their `label` as its target. Retrieval requests `payment_risk_v1` with `full_feature_names=True`. Time-ordered splits need no label-maturity gaps. The initial history replay uses `start=2026-07-29`, `end=2026-10-29` (exclusive); the workflow adds the one-day lead-in.

The training image installs `fraud_feast[spark]` and runs `python -m fraud_feast.spark_jars <spark-jars-directory>` during its build to use the same pinned, checksum-verified JARs as `data-platform-spark`. Set `SPARK_MASTER=local[*]` for retrieval in one Kubeflow Pipelines step, with local disk for Spark spill. Local Spark is also the default for materialization. No Feast SparkApplications are created.

History deletes each table’s epoch and month range in a short commit, then commits bounded batches without lead-in rows separately. Live and history writes share refresh and conflict retries. A month is partially visible while running or after failure; nothing trains until the entire replay passes its gates. A failed unit restarts from the range delete and offset zero through committed partition EOF; live recovery instead uses offsets in table properties that survive snapshot expiry. The gate requires zero decode rejects and consumption of all seeded offsets; duplicates and too-late rejects are reported only. `feature-writer-replay` rebuilds Redis only with `from`/`until`.

## TTL and freshness

Daily views use 31 days 12 hours; stream views use 24 hours 5 minutes. Each TTL covers its longest window plus pipeline delay. Feast applies TTLs offline, and Go Fraud applies the same TTLs at its server decision time, recorded as `EvaluatedAt`, with independent evidence for each view.

## Train/serve consistency

| Rule | Enforcement |
|---|---|
| **Exclude the scored attempt.** Training joins at `started_at`. Stream rows use `online_write_at + 1µs`, so a snapshot including that attempt is invisible at its own start. Daily rows are anchored at the midnight closing their window; both paths see only closed days. Redis receives those same rows after materialization, so it can lag the latest daily anchor. | [Training timestamp](../../src/data-pipeline/feast/fraud_feast/training.py), [stream mapper](../../src/data-pipeline/feast/fraud_feast/stream_writer.py), [daily anchors](../../src/data-pipeline/batch/batch/gold/windows.py) and [materialization](../../src/data-pipeline/feast/fraud_feast/materialize.py). |
| **Apply the same TTL.** At scoring time `t`, a value older than `t − ttl` is unknown. Feast applies the view TTL offline; Go applies it online using its server clock because Feast's online read ignores TTL. Flink emits snapshots on window expiry, so the stored counts already exclude expired contributions. | [View TTLs](../../src/data-pipeline/feast/fraud_feast/repo.py), [server clock](../../src/payment-gateway/fraud/internal/application/assessment.go), [online freshness checks](../../src/payment-gateway/fraud/internal/adapters/feast/client.go) and [Flink expiry timers](../../src/data-pipeline/streaming/fraud_streaming/operators.py). |
| **Use one engine per feature.** Spark computes daily 7/30-day features; Flink computes 5m/1h/24h windows. Stream history is a Kappa replay of the same Flink job, so training and serving share the stream implementation. | [Daily features](../../src/data-pipeline/batch/batch/gold/features.py), [stream windows](../../src/data-pipeline/streaming/fraud_streaming/model.py) and [history workflow](../../infra/charts/data-pipelines/templates/stream-history.yaml). |
| **Use one feature list.** Training retrieves `payment_risk_v1`, and Go Fraud requests that service. Different model inputs require a new service version. | [Service definition](../../src/data-pipeline/feast/fraud_feast/repo.py), [historical/server retrieval check](../../src/data-pipeline/feast/tests/test_repo.py) and [Go request](../../src/payment-gateway/fraud/internal/adapters/feast/client.go). |
