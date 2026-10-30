## Why

The LLM part of the coursework needs a self-hosted model behind a governed agent entry point before any fraud-analyst agent can be built. Neither cluster runs anything LLM-related today: KServe 0.21 ships its `LLMInferenceService` controller but no presets, the Gateway API Inference Extension CRDs are installed but unused, and there is no agent gateway, agent runtime or registry. This change builds phase 1: the parts that do not depend on the data pipeline or Feast, plus the RAG knowledge pipeline. It is shaped so that the RAG consumers, the MCP tools, the evidence, risk and coordinator agents, and the MCP/A2A gateway routes attach later without rework.

## What Changes

- Serve Qwen3-8B (FP8) for chat and Qwen3-Embedding-0.6B for embeddings on vLLM through KServe `LLMInferenceService`s. KServe runs llm-d's endpoint picker (EPP) and InferencePool for the chat model.
  - Both share one GPU through a single DRA claim, each process with a fixed memory fraction: the RTX 5060 Ti on dev, and on prod the shared `aws-gpu-training` Spot pool, which the platform restricts to `g6.xlarge` (L4).
  - One chat replica serves every agent through vLLM's continuous batching. The benchmark measures its capacity, and a second GPU replica behind the same InferencePool is the scale-out path.
  - Model weights live once per environment in the platform bucket (`artifacts/models/`) and stream straight to the GPU with Run:ai Model Streamer. No serving node writes them to disk, which matters because the dev GPU node is also the control plane.
- Run one agentgateway `Gateway` in the `llm` namespace. It is both the inference gateway (an OpenAI-compatible `/v1` route to the chat pool and the embedding service) and the future single entry point for MCP and A2A.
  - The LLM route requires a per-client API key and applies request and token rate limits and PII masking (card numbers, e-mail, phone) on prompts and responses.
  - Istio ambient stays the mesh: mesh authorization admits only the gateway (and the endpoint picker) to the model servers.
- Add a global kagent `ModelConfig`, `default-model-config`, that points every agent at the gateway's `/v1` with the agents' gateway key.
- Run `evidence-agent` 0.1.0 on kagent 0.10.4. It is a read-only fraud-analyst agent without tools, and the first version of the evidence agent that later gains the MCP evidence tool.
- Run the same agent as `evidence-agent-sandbox`, a `SandboxAgent` on Agent Substrate 0.0.9 (gVisor). Its conversations suspend to warm snapshots and resume on a small worker pool, which covers the sandbox and warm-up rubric rows.
- Publish the agent's catalog entry to agentregistry from Git with a sync hook.
- Run self-hosted Langfuse v3 in `llm`, on the shared ClickHouse that replaces StarRocks (owned by this repository, added in PR #102), with app-owned Postgres and Redis. In phase 2 it becomes the prompt store. It receives the agents' own LLM traces from kagent's OpenTelemetry instrumentation, which needs kagent tracing switched on in the platform. Gateway traces go to the platform's VictoriaTraces directly.
- Build RAG in `llm` (design.md decision 15; corpus in `rag-corpus.md`): a knowledge pipeline, a Web API and an MCP tool. It never reads payment data.
  - **Corpus:** about 14 documents about gateway behaviour. Most are rendered from the repository's specs and contracts; six are short, hand-written and labelled synthetic (decline codes, risk policy, analyst runbook, chargebacks, merchant terms, FAQ). The corpus ships inside the `rag-pipeline` image built from `src/rag/pipeline/`, so CI never writes to the bucket.
  - **Workflow:** an Argo `WorkflowTemplate` in the new `rag-pipeline` chart (Python, no Spark). Each new pipeline image starts one run, rendered as a `Workflow` named after the image tag, so a failed run never blocks a sync. It publishes the corpus to an immutable version prefix, chunks it, embeds through the gateway's embedding route and writes through Feast. The offline history is an Iceberg table; the online store is pgvector on an app-owned CNPG Postgres.
  - **Governance:** the run validates its output and records its checks and lineage in OpenMetadata like the other pipelines.
  - **Web API and MCP tool in one service:** FastAPI with pydantic and health checks, as a Deployment whose rolling update keeps the last ready version serving. It fetches chunks by id, searches them, and later returns customer features by id from the online feature store. It also serves the knowledge MCP tools behind the gateway's `/mcp/knowledge` route; they are published to the registry and used by `evidence-agent`.
  - **Quality gate:** every quality check runs before the write. After the write, the pipeline types the Feast pgvector column and adds an HNSW index, which Feast does not create.
- Register the AI assets in OpenMetadata from the RAG workflow's catalog step: the LLM service (the gateway endpoint), both models, the agent as an AI application linked to its model, and the knowledge MCP server. Further agents attach later.
- Add an on-demand benchmark (`vllm bench serve` from the vLLM image, as plain manifests applied with `kubectl`) with an agent-shaped shared-prefix workload. It records throughput, TTFT and inter-token latency before and after each optimization (prefix caching, FP8 KV cache, n-gram speculative decoding, streamed cold start, FP8 vs AWQ weights). Results are recorded only from real dev runs, with the command and date.
- Record the autoscaling decision against the rubric's KEDA:
  - Knative for this repo's own plain HTTP services that need no caller identity (later MCP servers and web APIs); the knowledge API is a Deployment, because its callers are admitted by mesh identity and Knative's cluster-local gateway runs outside the mesh;
  - KEDA for kagent agent Deployments and the Substrate WorkerPool (from phase 2);
  - a fixed single replica per model server, because each cluster has one GPU.
- Document the platform on new, focused `docs/llm/` pages (inference, gateway, agents, observability, benchmark, RAG), and update `docs/llm/agents.md`, the roadmap and the architecture where the behaviour changes.
- Planned for a later phase, not built in this change: a read-only ClickHouse analytics MCP tool. It answers questions such as "decline rate by merchant this week" or "top payment methods by volume" from the ClickHouse analytics marts that the next data slice builds from the Iceberg Silver tables (`fact_payment_attempt` and a daily merchant mart).
  - It is deployed like the other MCP tools: behind the agent gateway, published to the registry, wired into kagent, and covered by the same CI/CD.
  - It exposes a few typed, parameterised tools (metric, dimension, time range) rather than free-form SQL. If free-form SQL is ever offered, it is limited to SELECT over an allowlist of mart tables and capped.
  - It connects as its own ClickHouse user, limited to SELECT on the analytics database with `readonly = 1` and quotas (`max_execution_time`, `max_result_rows`, `max_memory_usage`). It never uses the `admin` or `openmetadata` users. The password is a new platform Doppler key, read through the ClusterSecretStore `doppler`.
  - The coordinator routes analytics questions to it, alongside the feature-retrieval API (online store) and RAG.
  - Contract owners: the data slice owns the mart names and schema. The user and quotas are added to the app's `infra/charts/clickhouse`.
- Platform prerequisites are a separate talos-proxmox change set that needs user approval:
  - restrict the prod GPU pool to `g6.xlarge`;
  - install the shared controllers and CRDs: KServe LLM presets, agentgateway, kagent 0.10.4 with Agent Substrate 0.0.9, agentregistry, and KEDA for phase 2;
  - let the Argo Workflows controller run workflows in `llm`, and let `llm` reach OpenMetadata, for the RAG pipeline.

  The clusters stay on Kubernetes 1.36. This change consumes these components and does not install them.

## Capabilities

### New Capabilities

- `llm-inference-platform`: the self-hosted chat and embedding models, their shared GPU claim and memory split, model storage, endpoint picker and InferencePool, serving placement per environment, and model-server telemetry.
- `agent-gateway`: the agentgateway entry point, the LLM route and its authentication, rate-limit and PII policy, client keys, mesh authorization, and the route layout that MCP and A2A extend.
- `agent-runtime`: the global model config, the published agent and its sandboxed, warm copy on Agent Substrate, the agent registry catalog, and the autoscaling choice per workload.
- `llm-benchmark`: the benchmark harness, workload, measured optimizations and evidence rules.
- `rag-knowledge-pipeline`: the document corpus and its versioning, the release-triggered chunk, embed and Feast write workflow, the offline and pgvector online stores, the run's validation, the Web API and the knowledge MCP tool.

### Modified Capabilities

- `talos-gitops-deployment`: adds the `llm-platform` application in the platform-owned `llm` namespace. It is enabled in dev and disabled in prod until the user confirms launching the prod GPU pool. Its credentials come from `gateway-doppler`.
- `data-catalog-governance`: adds AI assets (LLM service, models, agents, MCP server) to the catalog from the RAG workflow, and the knowledge-base product (contract checks and lineage for the RAG run).

## Impact

- **Infra:**
  - A new chart `infra/charts/llm-platform`: chat and embedding `LLMInferenceService`s, a shared GPU ResourceClaim, the model upload Job, Gateway, HTTPRoute, AgentgatewayPolicy, API-key ExternalSecrets, Istio AuthorizationPolicies, kagent ModelConfig, Agent and SandboxAgent, agent prompts as files, the registry catalog (agents and prompts) and publish hook, and scrape configuration. The benchmark manifests live in `infra/llm-benchmark/`.
  - The `llm-platform` entry in `infra/argocd/app-of-apps/values.yaml`, disabled in `values-prod.yaml`.
  - A new chart `infra/charts/rag-pipeline`, application `rag-pipeline` in `llm` (enabled in dev, disabled in prod), containing:
    - the `rag-knowledge` WorkflowTemplate and its per-image `Workflow`;
    - the CNPG `rag-pgvector` Cluster and `Database`;
    - the `rag-api` Deployment (Web API and MCP tool in one);
    - ExternalSecrets, mesh policies and the registry `MCPServer` entry.
  - In `llm-platform`: the `ragingest` and `ragapi` client keys, the `/mcp/knowledge` route and policy, the `knowledge` RemoteMCPServer, and `evidence-agent` 0.2.0 with the knowledge tools.
  - After #102: `llm` admitted by the Polaris, ClickHouse and OpenMetadata network rules.
- **Code:**
  - a new `src/rag/` uv workspace (`common`, `pipeline`, `serving`) and images `rag-pipeline` and `rag-serving`: corpus sources, renderer, chunker, embed client, Feast definitions, validation, OpenMetadata client, Web API and MCP server, with their CI tests;
  - the knowledge-base ODPS product and ODCS contract in `contracts/data-products/knowledge-base/`; the `openmetadata_jobs` unit tests are removed at the user's request.
  - No change to the data-pipeline code, the `data-pipelines` or `lakehouse` charts, or any existing contract.
- **Secrets:**
  - new keys in the `payment-gateway-fraud-detection` Doppler configs (`dev`, `prd`): `LLM_GATEWAY_AGENTS_API_KEY` and `LLM_GATEWAY_BENCHMARK_API_KEY`, already provisioned with random values. No model-provider key is needed, because the models are public on Hugging Face.
  - for RAG: `LLM_GATEWAY_RAG_INGEST_API_KEY`, `LLM_GATEWAY_RAG_API_KEY`, `RAG_DB_WRITER_PASSWORD` and `RAG_DB_READER_PASSWORD`, provisioned with random values in `dev` and `prd` on 2026-10-29.
- **Platform dependency:** the talos-proxmox components in design.md decision 13 must be live in an environment before the chart's resources can reconcile there.
- **Docs:**
  - new: `docs/llm/inference.md`, `docs/llm/gateway.md`, `docs/llm/observability.md`, `docs/llm/benchmark.md`, `docs/llm/rag.md` (and `docs/llm/agents.md` updated);
  - updated where behaviour changes: `docs/llm/agents.md`, `docs/overview/roadmap.md`, `docs/overview/architecture.md`, `docs/deployment/cd.md` (applications table), `docs/deployment/security.md` (keys), `docs/data/orchestration-governance.md` (AI assets in the catalog), `docs/_Sidebar.md`.
- **No change** to the payment gateway, data pipelines or their contracts. Agents never sit in the payment decision path.
