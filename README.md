# Payment Gateway and Real-Time Fraud Detection

A payment gateway for a refurbished-electronics marketplace, with a real-time fraud decision on every payment attempt. Its events and a synthetic history feed a lakehouse, a feature store and a fraud model, and read-only analyst agents explain declines and risk decisions on top. Synthetic cards only. The [project wiki](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki) covers the domain, the data problems, the AI problem, the architecture and the roadmap.

- [Overview](#overview)
- [Payment gateway, data pipeline and ML](#payment-gateway-data-pipeline-and-ml)
- [LLM and agents](#llm-and-agents)
- [Deployment and security](#deployment-and-security)
- [Repository structure](#repository-structure)
- [Getting started](#getting-started)

## Overview

_Diagram to come._

Docs: [domain](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki#domain) · [architecture](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki#architecture) · [roadmap](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki#roadmap)

## Payment gateway, data pipeline and ML

The [refurbished marketplace](https://github.com/phuchoang2603/refurbished-marketplace) prepares orders and reserves stock through its Checkout saga before opening the gateway's hosted checkout. The Go gateway owns attempts, fraud decisions, simulated processing and signed outcomes that drive stock settlement and order finalization.

Kafka events and synthetic history feed Spark's Bronze/Silver/Gold lakehouse and Flink's streaming features. Feast serves daily and streaming features from Redis to Go Fraud, which applies rules and the KServe model score. Kubeflow builds point-in-time training examples, trains and evaluates XGBoost, registers passing versions and rolls them out to KServe.

![Payment gateway, data and ML flow: marketplace checkout, Go payment services, Kafka, Spark and Flink processing, Feast and Redis, and Kubeflow training and KServe rollout](docs/payment-gateway/images/payment-flow.png)

Diagram source: [`payment-flow.excalidraw`](docs/payment-gateway/images/payment-flow.excalidraw) · model integration: [#112](https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/112).

Gateway docs: [architecture](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/architecture) · [flows](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/flows) · [conventions](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/conventions)

Data and ML docs: [offline data](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/offline) · [online data](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/online) · [storage](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/storage) · [orchestration and governance](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/orchestration-governance) · [feature store](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/feature-store) · [training](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/training) · [serving](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/serving) · [background](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/background)

## LLM and agents

A coordinator agent hands each analyst question to the evidence agent (knowledge base, cited passages) or the risk agent (online features, the fraud model's score, the knowledge base). Every model, tool and agent call passes agentgateway: client keys, rate and token limits, PII masking and a request screen by the d3-edge Jev decision model. Models run on vLLM through KServe and llm-d on a shared GPU; Langfuse holds prompts, A/B tests and traces.

![LLM and agents: clients, agentgateway, kagent agents on Agent Substrate, inference on KServe, MCP tools and ML inference, data pipelines and operations](docs/llm/images/llm-architecture.png)

Docs: [agents](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/agents) · [inference](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/inference) · [agent gateway](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/gateway) · [policy](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/policy) · [RAG](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/rag) · [observability](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/llm-observability) · diagram source [`llm-architecture.excalidraw`](docs/llm/images/llm-architecture.excalidraw)

## Deployment and security

The platform (Talos clusters on Proxmox for dev and GCP for prod, Cluster Autoscaler, object storage, Argo CD, Doppler secrets, the operators and the mesh) is owned by [talos-proxmox](https://github.com/phuchoang2603/talos-proxmox). This repository ships the application images, charts and Argo CD applications.

> **Migration note:** Production moved from AWS to GCP on 2026-11-02. The AWS account could no longer be used, so prod now runs on the GCP free-trial credit. Evidence and run records captured on AWS before that date are left as recorded.

_Diagram to come._

Docs: [CI](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/ci) · [CD](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/cd) · [Docker images](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/docker) · [infrastructure](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/infra) · [security](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/security) · [observability](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki/deployment-observability)

## Repository structure

| Path | Contents |
|---|---|
| `src/payment-gateway/` | Go services: Edge, Payments, Fraud, Notifier and the vault and processor simulators, with their shared module |
| `src/data-pipeline/` | Python: historical and live simulators, `batch` (Spark and ClickHouse jobs for Bronze, Silver, Gold, marts, maintenance), `openmetadata` (checks, publish, lineage, plan), `streaming` (Flink), `feast` and `features` |
| `src/llm/` | Agent runtime, RAG pipeline and RAG serving (knowledge API and MCP tools) |
| `contracts/` | Wire formats (`payments`, `fraud`, `vault`, `processor`), data products as ODPS and ODCS contracts, feature definitions and synthetic fixtures |
| `infra/charts/` | Helm charts per application; `infra/argocd/` the Argo CD roots and release file; `infra/docker/` the Dockerfiles |
| `openspec/` | Specifications and planned or archived changes |
| `docs/` | The documentation, published to the [project wiki](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki) |
| `scripts/` | CI checks, code generation, release planning and wiki publishing |

## Getting started

Install Nix and devenv, then run `devenv shell`; details in [CONTRIBUTING.md](CONTRIBUTING.md). Documentation lives in [`docs/`](docs) and is published to the [project wiki](https://github.com/phuchoang2603/payment-gateway-fraud-detection/wiki).
