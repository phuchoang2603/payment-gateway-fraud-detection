# Payment Gateway and Real-Time Fraud Detection

An ecommerce payment gateway with real-time fraud decisioning, and the data, ML and agent platform built on its events. Edit these pages in the repository's `docs/` folder; changes publish to the wiki from `main`. Contributor setup is in [CONTRIBUTING.md](https://github.com/phuchoang2603/payment-gateway-fraud-detection/blob/main/CONTRIBUTING.md).

## Overview

- [Proposal](overview/proposal.md): domain, entities and events, data problems, AI problem, requirements
- [Architecture](overview/architecture.md): deployable units and deployment view
- [Roadmap](overview/roadmap.md): status by rubric area

## Payment gateway

- [Architecture](payment-gateway/architecture.md), [flows](payment-gateway/flows.md), [conventions](payment-gateway/conventions.md), [Docker images](payment-gateway/docker.md)

## Data

- [Offline data](data/offline.md): generator and Bronze/Silver pipeline
- [Online data](data/online.md): live simulator and streaming features
- [Storage](data/storage.md), [orchestration and governance](data/orchestration-governance.md)

## ML

- [Feature store](ml/feature-store.md), [training](ml/training.md), [serving](ml/serving.md), [background](ml/background.md)

## LLM

- [Agents](llm/agents.md)

## Deployment

- [CI](deployment/ci.md), [CD](deployment/cd.md), [infrastructure](deployment/infra.md), [security](deployment/security.md), [observability](deployment/observability.md)
