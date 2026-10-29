# Roadmap

Status of each capability, grouped like the course rubric. **Done** means implemented and exercised with recorded results on the linked page; **in progress** means implementation is under review; **planned** means designed but not built. Course tools are replaced by platform equivalents where noted (Argo Workflows for Airflow, OpenMetadata for DataHub, Kubeflow Model Registry for MLflow, GitHub Actions for Jenkins).

The active plan is the OpenSpec change [`rebuild-generation-to-silver`](../../openspec/changes/rebuild-generation-to-silver/proposal.md).

## Data platform

| Area | Capability | Page | Status |
|---|---|---|---|
| Engineering fundamentals | Multi-stage Docker images and size comparison | [Docker images](../payment-gateway/docker.md) | Images done; size comparison planned |
| Offline generator | Skew, schema evolution, duplicates; ≥ 100 GB stored for Bronze ingestion | [Offline data](../data/offline.md) | Done (v19: 133.5 GB) |
| Online generator | Late arrivals and streaming duplicates | [Online data](../data/online.md) | In progress |
| Spark jobs | Baseline, skew, schema evolution, duplicates, integrated into pipelines | [Offline data](../data/offline.md) | In progress |
| Flink jobs | Baseline, late arrivals, duplicates, windows | [Online data](../data/online.md) | Windows done; late/duplicate handling in progress |
| Storage | Lakehouse compaction, partitioning, Z-order; warehouse indexing | [Storage](../data/storage.md) | Planned |
| Orchestration | DP1 (raw → Bronze), DP2 (Bronze → Silver/Gold), DP3 (offline features), each with ingest and validate stages | [Orchestration and governance](../data/orchestration-governance.md) | DP1/DP2 in progress; DP3 planned |
| Governance | Lineage and validation for DP1–DP3 in the catalog | [Orchestration and governance](../data/orchestration-governance.md) | In progress |
| Schemas | SCD2 dimensions, `feat_` tables with `event_timestamp`/`created`, naming conventions, relationships | [Offline data](../data/offline.md) | Planned |

## ML platform

| Area | Capability | Page | Status |
|---|---|---|---|
| Feature store | Streaming features to online and offline stores, incremental materialization, TTL | [Feature store](../ml/feature-store.md) | Online writer done (shadow); offline and materialization planned |
| Generator | Data drift and a label table joinable with features | [Offline data](../data/offline.md) | Labels done; drift planned |
| Model | Notebook baseline from the offline store, training pipeline, distributed training | [Training](../ml/training.md) | Planned |
| Versioning | Model registry and incremental data versions | [Training](../ml/training.md) | Planned |
| Serving | Feature-retrieval API, KServe inference, rolling update, autoscaling, A/B traffic split | [Serving](../ml/serving.md) | Planned |
| Drift | Periodic drift comparison and retraining trigger | [Training](../ml/training.md) | Planned |

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

## Limits

- Synthetic data cannot establish real-world fraud-model quality. Unlabelled drift measures distribution change, not accuracy.
- Real card custody, live processor connectivity, settlement, payouts and FX are out of scope. The gateway demonstrates PCI-oriented boundaries with synthetic data, not compliance.
