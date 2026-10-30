# Roadmap

Status of each capability, grouped like the course rubric. **Done** means implemented and exercised with recorded results on the linked page; **in progress** means implementation is under review; **planned** means designed but not built. Course tools are replaced by platform equivalents where noted (Argo Workflows for Airflow, OpenMetadata for DataHub, Kubeflow Model Registry for MLflow, GitHub Actions for Jenkins).

The active plan is the OpenSpec change [`rebuild-generation-to-silver`](../../openspec/changes/rebuild-generation-to-silver/proposal.md).

## Data platform

| Area | Capability | Page | Status |
|---|---|---|---|
| Engineering fundamentals | Multi-stage Docker images and size comparison | [Docker images](../payment-gateway/docker.md) | Images done; size comparison planned |
| Offline generator | Skew, schema evolution, duplicates; ≥ 100 GB stored for Bronze ingestion | [Offline data](../data/offline.md) | Done (v19: 133.5 GB) |
| Online generator | Late arrivals and streaming duplicates | [Online data](../data/online.md) | Done; see [evidence](../data/online.md#evidence) |
| Spark jobs | Baseline, skew, schema evolution, duplicates, integrated into pipelines | [Offline data](../data/offline.md) | In progress |
| Flink jobs | Baseline, late arrivals, duplicates, windows, dead-letter topic; output ends at the feature journal topic | [Online data](../data/online.md) | Done except a Flink UI metrics-panel capture; see [evidence](../data/online.md#evidence) |
| Storage | Lakehouse compaction, partitioning, Z-order; ClickHouse warehouse indexing (`ORDER BY`, skip indexes) | [Storage](../data/storage.md) | Planned |
| Orchestration | DP1 (raw → Bronze), DP2 (Bronze → Silver/Gold), DP3 (offline features), each with ingest and validate stages | [Orchestration and governance](../data/orchestration-governance.md) | DP1/DP2 in progress; DP3 planned |
| Incremental curation | A `mode=historical\|incremental` toggle for DP2 and DP3. DP2 merges only payments touched by new Kafka offsets; DP3 recomputes only affected customers and cards and replaces affected mart days; CI checks incremental output against historical mode | [Offline data](../data/offline.md) | Planned (before the live simulator runs on prod) |
| Governance | Lineage and validation for DP1–DP3 in the catalog | [Orchestration and governance](../data/orchestration-governance.md) | In progress |
| Schemas | SCD2 dimensions, `feat_` tables with `event_timestamp`/`created`, naming conventions, relationships | [Offline data](../data/offline.md) | Planned |

## ML platform

| Area | Capability | Page | Status |
|---|---|---|---|
| Feature store | Streaming features to online and offline stores, incremental materialization, TTL | [Feature store](../ml/feature-store.md) | Planned: see [feature store build order](#feature-store-build-order) |
| Generator | Data drift and a label table joinable with features | [Offline data](../data/offline.md) | Labels done; drift planned |
| Model | Notebook baseline from the offline store, training pipeline, distributed training | [Training](../ml/training.md) | Planned |
| Versioning | Model registry and incremental data versions | [Training](../ml/training.md) | Planned |
| Serving | Feature-retrieval API, KServe inference, rolling update, autoscaling, A/B traffic split | [Serving](../ml/serving.md) | Planned |
| Drift | Periodic drift comparison and retraining trigger | [Training](../ml/training.md) | Planned |

## Feature store build order

Feast is built as one change after Bronze and Silver merge. Until then the streaming job ends at the feature journal topic, and the existing online-only Feast writer and Go Fraud's shadow retrieval are disabled until the feature-store change.

1. **DP3 offline features.** A daily Spark job over Silver writes `feat_customer_*` and `feat_payment_method_*` Iceberg tables keyed by integration, customer or payment method, with `event_timestamp` and `created`. It computes the long 7- and 30-day windows that Flink does not keep.
2. **Offline store.** Feast reads the `feat_` tables as its offline store, through a Spark source over the Iceberg catalog or a Parquet export, whichever Feast supports reliably. Training builds its dataset with `get_historical_features` against the label table, joining point-in-time on attempt time and label availability.
3. **Streaming features to offline.** A Kafka batch reader, the same pattern as Bronze Kafka mode, appends the feature journal topic into an Iceberg `feat_stream_*` table that the offline store also reads.
4. **Streaming features to online.** A Feast writer consumes the feature journal topic and pushes each update to Redis, committing offsets only after the write is accepted.
5. **Materialization.** An Argo workflow runs `feast materialize-incremental` from the offline store into Redis after each DP3 run, so batch features reach the online store.
6. **TTL and feature services.** Each feature view gets a TTL equal to its longest window plus the expected pipeline delay, with the reason recorded on the [feature store](../ml/feature-store.md) page. A versioned `FeatureService` (for example `fraud_model_v1`) names the features a model uses; training and Go Fraud both request it.
7. **Natural keys.** Feature views key on integration-scoped customer and payment-method IDs instead of the current synthetic snapshot key, so offline joins with labels are direct. Go Fraud's feature client moves to the new views and feature service in the same change.
8. **Governance.** OpenMetadata's native Flink pipeline connector, run from `data-pipelines` against the JobManager REST API, records the streaming job and its runs. If it does not resolve Kafka lineage for the PyFlink job, a deploy-time step publishes `payments.events.v1` → streaming job → feature journal and reject topics with the OpenMetadata SDK, as Bronze and Silver do. Feast has no OpenMetadata connector, so the governed assets are the `feat_` tables with DP3 lineage from Silver. Neither the Flink job nor Feast embeds the SDK, so a catalog outage cannot affect feature computation.

## LLM and agent platform

| Area | Capability | Page | Status |
|---|---|---|---|
| Agents | Inference platform, agent gateway, registry, RAG pipeline, MCP tools, coordinator agent | [Agents](../llm/agents.md) | Planned |

## Delivery and operations

| Area | Capability | Page | Status |
|---|---|---|---|
| Verification | Coverage, equivalence partitioning and boundary values, mutation testing (> 80% on changed code), property-based tests, load tests | [CI](../deployment/ci.md) | Partial |
| CI/CD | Test, build and deploy for services and pipelines | [CI](../deployment/ci.md), [CD](../deployment/cd.md) | Done for services and generator; pipelines in progress |
| Routing | Gateway in front of services, HTTPS | [Security](../deployment/security.md) | Done for Edge |
| Infrastructure as code | Cluster and cloud provisioning, configuration | [Infrastructure](../deployment/infra.md) | Done (platform repository) |
| Observability | Metrics, logs and traces for services | [Observability](../deployment/observability.md) | Done for Go services |
| Security | Central secrets, service-to-service authorization | [Security](../deployment/security.md) | Done |

## Next steps

- **Silver reads only validated Bronze.** Bronze checks set an Iceberg `validated` tag on the checked snapshot, and Silver reads that tag instead of the current snapshot. Today a standalone `dp2-silver` run can consume a Bronze snapshot that failed its checks; the Kafka cron is gated by step order.

- **OpenMetadata contracts and products.** Import ODCS contracts when OpenMetadata's importer accepts ODCS v3 as written (blocked on 2.0.6), and publish data products then.
- **Bronze and Silver evidence (prod v19).** Screenshots and recorded results are open tasks in `openspec/changes/rebuild-generation-to-silver/tasks.md`, section 12.

## Limits

- Synthetic data cannot establish real-world fraud-model quality. Unlabelled drift measures distribution change, not accuracy.
- Real card custody, live processor connectivity, settlement, payouts and FX are out of scope. The gateway demonstrates PCI-oriented boundaries with synthetic data, not compliance.
