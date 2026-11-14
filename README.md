# Payment Gateway and Real-Time Fraud Detection

A payment gateway for a refurbished-electronics marketplace, built to connect reliable checkout with real-time fraud decisions and explainable investigations. Payment events feed streaming and historical data pipelines for model training, scoring and analyst agents, using synthetic cards and fraud labels. The documentation starts at [`docs/Home.md`](docs/Home.md), which holds the status roadmap, and is also published as the [project wiki](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki).

## Table of contents

- [Payment gateway, data pipeline and ML](#payment-gateway-data-pipeline-and-ml)
- [LLM and agents](#llm-and-agents)
- [Deployment and security](#deployment-and-security)
- [Repository structure](#repository-structure)
- [Documentation map](#documentation-map)
- [Getting started](#getting-started)

## Payment gateway, data pipeline and ML

- **Gateway:** Go services, Postgres, Kafka CDC, idempotent payment attempts, timeout recovery and signed webhooks. Integrates with the [marketplace](https://github.com/phuchoang2603/refurbished-marketplace). [Architecture](docs/payment-gateway/architecture.md).
- **Lakehouse:** Spark Bronze → Silver → Gold, audited publication and OpenMetadata lineage. **133.5 GB** synthetic v19 source dataset (AWS run). [Offline data](docs/data/offline.md).
- **Warehouse:** ClickHouse marts and skip indexes. Same-query GCP plan: **5,541 → 156 granules, 97.18% fewer**, with query-condition cache disabled. [Storage](docs/data/storage.md).
- **Streaming:** shared Java Flink operators for live traffic and historical replay; Feast/Redis for online features. Parity tests compare **2,498 snapshots from 503 deliveries**. [Historical replay](docs/data/historical-replay.md) · [Feature store](docs/ml/feature-store.md).
- **ML:** Kubeflow/Katib, GPU XGBoost, Triton and drift-triggered retraining. Dev synthetic test set: **PR-AUC 0.9370**, **91.1% recall at 1% FPR**. [Training](docs/ml/training.md) · [Pipeline](docs/ml/pipeline.md).

[![Payment gateway, lakehouse, streaming features and ML pipeline](docs/payment-gateway/images/payment-flow.png)](docs/payment-gateway/images/payment-flow.png)

## LLM and agents

- **Agents:** coordinator, evidence and risk agents; read-only tools, cited knowledge retrieval and Langfuse prompts/traces. [Agents](docs/llm/agents.md).
- **Model access:** agentgateway API keys, rate limits, PII masking and request screening; vLLM/KServe inference. [Gateway](docs/llm/gateway-and-jev-policy.md) · [Inference](docs/llm/inference.md).
- **Knowledge API:** dev load test, 60 users over 5 minutes: **181.7 requests/s**, **p95 86 ms**, **0 failures across 54,453 requests**, with KEDA scaling 2 → 4 replicas. [Autoscaling](docs/llm/scaling.md) · [RAG](docs/llm/rag.md).

![Agents, model gateway, inference and knowledge tools](docs/llm/images/llm-architecture.png)

## Deployment and security

- **Platform:** Talos on Proxmox (dev) and GCP (prod, now retired), OpenTofu, Argo CD and shared operators, owned by [talos-proxmox](https://github.com/phuchoang2603/talos-proxmox).
- **Delivery:** GitHub Actions → GHCR → Argo CD; Istio checkout canary stages at **10%, 25%, 50%**. [CI](docs/deployment/ci.md) · [Delivery](docs/deployment/cd.md) · [Analysis-gated promotion](docs/ml/analysis-gated-promotion.md).
- **Access:** Cloudflare Tunnel/HTTPS, Istio ambient mTLS, service-account policies and Doppler/External Secrets. [Networking](docs/deployment/networking.md) · [Security](docs/deployment/security.md).

![Application delivery, infrastructure, secrets and service mesh](docs/deployment/images/deployment-security.png)

## Repository structure

| Path | Contents |
|---|---|
| `src/payment-gateway/` | Go payment services and synthetic vault/processor |
| `src/data-pipeline/` | Java Flink, Python simulators, Spark, Feast and catalog jobs |
| `src/ml/` | Training, evaluation, registry, serving and drift |
| `src/llm/` | Agents, RAG pipeline and knowledge API/MCP tools |
| `contracts/` | Protobuf, data-product contracts and feature definitions |
| `infra/` | Application Dockerfiles, Helm charts and Argo CD releases |
| `openspec/` | Specifications and changes |
| `docs/` | Project wiki sources |
| `scripts/` | CI, generation, releases and wiki publication |

## Documentation map

| Section | Pages |
|---|---|
| Overview and status | [Home](docs/Home.md) · [Novel ideas](docs/Home.md#novel-ideas) |
| Payment gateway | [Architecture](docs/payment-gateway/architecture.md) · [Conventions](docs/payment-gateway/conventions.md) |
| Data | [Offline](docs/data/offline.md) · [Online](docs/data/online.md) · [Storage](docs/data/storage.md) · [Orchestration and governance](docs/data/orchestration-governance.md) · [Write–audit–publish](docs/data/write-audit-publish.md) · [Historical replay](docs/data/historical-replay.md) |
| ML | [Feature store](docs/ml/feature-store.md) · [Training](docs/ml/training.md) · [Pipeline](docs/ml/pipeline.md) · [Drift](docs/ml/drift.md) · [Serving](docs/ml/serving.md) · [NVIDIA DRA GPU sharing](docs/ml/nvidia-dra.md) · [Analysis-gated promotion](docs/ml/analysis-gated-promotion.md) · [Background](docs/ml/background.md) |
| LLM | [Agents](docs/llm/agents.md) · [Inference](docs/llm/inference.md) · [Gateway and Jev policy](docs/llm/gateway-and-jev-policy.md) · [Custom Go ADK runtime](docs/llm/custom-go-adk-runtime.md) · [RAG](docs/llm/rag.md) · [Autoscaling](docs/llm/scaling.md) · [MCP releases](docs/llm/mcp-releases.md) · [Langfuse](docs/llm/langfuse.md) |
| Deployment | [CI](docs/deployment/ci.md) · [Infrastructure](docs/deployment/infra.md) · [CD](docs/deployment/cd.md) · [Observability](docs/deployment/observability.md) · [Security](docs/deployment/security.md) · [Networking](docs/deployment/networking.md) |

## Getting started

- Install Nix and devenv; run `devenv shell`.
- See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and local checks.
