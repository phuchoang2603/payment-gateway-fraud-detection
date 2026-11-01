# Roadmap

Status of each capability, grouped like the course rubric. **Done** means implemented and exercised with recorded results on the linked page; **in progress** means implementation is under review; **planned** means designed but not built. Course tools are replaced by platform equivalents where noted (Argo Workflows for Airflow, OpenMetadata for DataHub, Kubeflow Model Registry for MLflow, GitHub Actions for Jenkins).

The active plan is the OpenSpec change [`payment-data-pipeline`](../../openspec/changes/payment-data-pipeline/proposal.md).

## Data platform

| Area | Capability | Page | Status |
|---|---|---|---|
| Engineering fundamentals | Multi-stage Docker images and size comparison | [Docker images](../payment-gateway/docker.md) | Images done; size comparison planned |
| Offline generator | Skew, schema evolution, duplicates; ≥ 100 GB stored for Bronze ingestion | [Offline data](../data/offline.md) | Done (v19: 133.5 GB) |
| Online generator | Late arrivals and streaming duplicates | [Online data](../data/online.md) | Done; see [evidence](../data/online.md#evidence) |
| Spark jobs | Baseline, skew, schema evolution, duplicates, integrated into pipelines | [Offline data](../data/offline.md) | Running on prod v19; optimizations recorded from the backfill |
| Flink jobs | Baseline, late arrivals, duplicates, windows, dead-letter topic; output ends at the feature journal topic | [Online data](../data/online.md) | Done except a Flink UI metrics-panel capture; see [evidence](../data/online.md#evidence) |
| Storage | Lakehouse compaction, partitioning, Z-order; ClickHouse warehouse indexing (`ORDER BY`, skip indexes) | [Storage](../data/storage.md) | Layout done on v19; mart `EXPLAIN` evidence waits for the backfilled window |
| Orchestration | DP1 (raw → Bronze), then `dp-pipeline` (Silver, Gold, marts), each layer with ingest, validate and publish stages | [Orchestration and governance](../data/orchestration-governance.md) | DP1 done; backfill through April done, May onward running |
| Incremental curation | Silver and Gold take a day range and overwrite only those days, so the daily run and a backfill are the same job; CI checks daily runs against one backfill for both | [Offline data](../data/offline.md) | Done; prod backfill month by month |
| Governance | Lineage and validation for DP1–DP3 in the catalog | [Orchestration and governance](../data/orchestration-governance.md) | Done: source-to-marts lineage, 58 passing tests |
| Schemas | SCD2 dimensions, `feat_` tables with `event_timestamp`/`created`, naming conventions, relationships | [Offline data](../data/offline.md) | Done |

## ML platform

| Area | Capability | Page | Status |
|---|---|---|---|
| Feature store | Streaming features to online and offline stores, incremental materialization, TTL | [Feature store](../ml/feature-store.md) | Implemented; prod runs pending |
| Generator | Data drift and a label table joinable with features | [Offline data](../data/offline.md) | Labels done; drift planned |
| Model | Notebook baseline from the offline store, training pipeline, distributed training | [Training](../ml/training.md) | Planned |
| Versioning | Model registry and incremental data versions | [Training](../ml/training.md) | Planned |
| Serving | Feature-retrieval API, KServe inference, rolling update, autoscaling, A/B traffic split | [Serving](../ml/serving.md) | Planned |
| Drift | Periodic drift comparison and retraining trigger | [Training](../ml/training.md) | Planned |

## Feature store build order

Steps 2–8 are implemented; prod runs are pending.

1. **DP3 offline features.** A daily Spark job over Silver writes `feat_customer_daily` and `feat_payment_method_daily` with `event_timestamp` and `created`, for the 7- and 30-day windows.
2. **Offline store.** Feast's Spark offline store reads the `feat_` tables through `SparkSource(table=…)` on the Iceberg catalog. Training builds its dataset with `get_historical_features(payment_risk_v1)` from `label_attempt`.
3. **Streaming features to offline.** The `feature-writer` appends journal snapshots to `gold.feat_stream_*` with PyIceberg, keeping the Kafka offsets in the same commit. History comes from a Kappa replay of Bronze through the same Flink job.
4. **Streaming features to online.** The same process pushes each batch to Redis with Feast's `push` before buffering its Iceberg append.
5. **Materialization.** `feast-materialize` loads the daily views into Redis after DP3 and checks a sample against the offline rows.
6. **TTL and feature services.** 31 days 12 hours for daily views and 24 hours 5 minutes for stream views, with reasons on the [feature store](../ml/feature-store.md#ttl-and-freshness) page; one `payment_risk_v1` service for training and Go Fraud.
7. **Natural keys.** Views key on integration plus customer or payment-method IDs, and Go Fraud requests the new service.
8. **Governance.** OpenMetadata's Flink connector records the live job, with SDK lineage as the fallback; the writer, history and materialization publish lineage and run status.

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

- **OpenMetadata contracts and products.** Import ODCS contracts when OpenMetadata's importer accepts ODCS v3 as written (blocked on 2.0.6), and publish data products then.
- **Remaining prod v19 evidence.** The rest of the backfill, the schema-version count, the Grafana dashboards and the mart `EXPLAIN` plans are open tasks in `openspec/changes/payment-data-pipeline/tasks.md`, section 7.

## Limits

- Synthetic data cannot establish real-world fraud-model quality. Unlabelled drift measures distribution change, not accuracy.
- Real card custody, live processor connectivity, settlement, payouts and FX are out of scope. The gateway demonstrates PCI-oriented boundaries with synthetic data, not compliance.
