## Why

The LLM part of the coursework needs a self-hosted model behind a governed agent entry point before any fraud-analyst agent can be built. Neither cluster runs anything LLM-related today: KServe 0.21 ships its `LLMInferenceService` controller but no presets, the Gateway API Inference Extension CRDs are installed but unused, and there is no agent gateway, agent runtime or registry. This change builds the parts that do not depend on the data pipeline's online features: the inference platform, the gateway with its MCP and A2A routes, the RAG knowledge pipeline, and a coordinator agent with the evidence and risk agents as its sub-agents. It is shaped so that the risk agent's scoring and feature tools and the analytics tool attach later without rework.

## What Changes

- Serve Qwen3-8B (FP8) for chat and Qwen3-Embedding-0.6B for embeddings on vLLM through KServe `LLMInferenceService`s. KServe runs llm-d's endpoint picker (EPP) and InferencePool for the chat model.
  - Both share one GPU through a single DRA claim, each process with a fixed memory fraction: the RTX 5060 Ti on dev, and on prod the shared `aws-gpu-training` Spot pool, which the platform restricts to `g6.xlarge` (L4).
  - One chat replica serves every agent through vLLM's continuous batching. The benchmark measures its capacity, and a second GPU replica behind the same InferencePool is the scale-out path.
  - Model weights live once per environment in the platform bucket (`artifacts/models/`) and stream straight to the GPU with Run:ai Model Streamer. No serving node writes them to disk, which matters because the dev GPU node is also the control plane.
- Run one agentgateway `Gateway` in the `llm` namespace. It is both the inference gateway (an OpenAI-compatible `/v1` route to the chat pool and the embedding service) and the single entry point for MCP and A2A.
  - The LLM route requires a per-client API key and applies request and token rate limits and PII masking (card numbers, e-mail, phone) on prompts and responses.
  - Istio ambient stays the mesh: mesh authorization admits only the gateway (and the endpoint picker) to the model servers.
- Add a global kagent `ModelConfig`, `default-model-config`, that points every agent at the gateway's `/v1` with the agents' gateway key.
- Run the agents on kagent 1.0.0-rc1 with Agent Substrate 1.0.0-rc1 (gVisor), after the platform's upgrade to Talos 1.14.5 / Kubernetes 1.37.2. They live in the platform-created namespace `llm-agents` (Pod Security `privileged`, outside the mesh) with the app's own WorkerPool; each conversation is an actor restored from a golden snapshot, suspended when idle and resumed on any worker. KEDA scales the WorkerPool on Substrate's busy-worker metric. Actors hold no credentials: Substrate's egress gateway injects the gateway and Langfuse keys. `llm` stays `restricted`.
- Build the agent runtime image from kagent's own Go ADK entrypoint plus one ADK plugin: before each model call it reads the calling agent's prompt and A/B variant from Langfuse and links the call to that prompt version.
- Deploy `coordinator` 0.2.0, the analysts' only entry agent, behind the gateway's A2A route `/a2a/coordinator` (key, rate limit, a signed token to the kagent controller). `evidence-agent` 0.3.0 (knowledge MCP tools) and `risk-agent` 0.1.0 (read-only prompt, no tools yet; its scoring and feature tools come with the feature-store work) are kagent Shared sub-agents compiled into the coordinator, which transfers each question to the specialist that owns it.
- Run laya (`convaiinnovations/laya`, a 421M non-autoregressive decision model) on CPU as the `decision` service in `llm` (`src/llm/decision`): an agentgateway prompt-guard webhook on the LLM route that rejects prompts asking an agent to act (approve, block, refund, retry, change a rule, promote a model) or to ignore its rules. Prompts never leave the cluster for this.
- Manage prompts and A/B tests in Langfuse: each agent's prompt is seeded once from Git and then edited and versioned in Langfuse; a test is part of the prompt's config and splits conversations by a stable hash, and each trace is tagged with every tested agent's side. A/B tests of LLM versions are left for prod, because dev's one GPU holds one chat model.
- Ship an "LLM agents" Grafana dashboard: A2A calls per entry agent, MCP tool calls and failures, screening decisions, tokens and latency.
- Publish the entry agent and the MCP servers to agentregistry from Git with a catalog job.
- Run self-hosted Langfuse v3 in `llm`, on the shared ClickHouse that replaces StarRocks (owned by this repository, added in PR #102), with app-owned Postgres and Redis. Agents send their own traces to Langfuse through kagent's OpenTelemetry export. The gateway exports no traces, and there is no collector.
- Build RAG in `llm` (design.md decision 15; corpus in `rag-corpus.md`): a knowledge pipeline, a Web API and an MCP tool. It never reads payment data.
  - **Corpus:** about 14 documents about gateway behaviour. Most are rendered from the repository's specs and contracts; six are short, hand-written and labelled synthetic (decline codes, risk policy, analyst runbook, chargebacks, merchant terms, FAQ). The corpus ships inside the `rag-pipeline` image built from `src/llm/rag_pipeline/`, so CI never writes to the bucket.
  - **Workflow:** an Argo `WorkflowTemplate` in the new `rag-pipeline` chart (Python, no Spark). Each new pipeline image starts one run, rendered as a `Workflow` named after the image tag, so a failed run never blocks a sync. It publishes the corpus to an immutable version prefix, chunks it, embeds through the gateway's embedding route and writes through Feast. The offline history is an Iceberg table; the online store is pgvector on an app-owned CNPG Postgres.
  - **Governance:** the run validates its output and records its checks and lineage in OpenMetadata like the other pipelines.
  - **Web API and MCP tool in one service:** FastAPI with pydantic and health checks, as a Deployment whose rolling update keeps the last ready version serving. Its REST API fetches chunks by id, and later customer features by id from the online feature store. Search is only an MCP tool: the service serves the knowledge MCP tools (search and chunk by id) behind the gateway's `/mcp/knowledge` route; they are published to the registry and used by `evidence-agent`.
  - **Quality gate:** every quality check runs before the write. After the write, the pipeline types the Feast pgvector column and adds an HNSW index, which Feast does not create.
- Add an on-demand benchmark (`vllm bench serve` from the vLLM image, as plain manifests applied with `kubectl`) with an agent-shaped shared-prefix workload. It records throughput, TTFT and inter-token latency before and after each optimization (prefix caching, FP8 KV cache, n-gram speculative decoding, streamed cold start, FP8 vs AWQ weights). Results are recorded only from real dev runs, with the command and date.
- Record the autoscaling decision against the rubric's KEDA:
  - Knative for this repo's own plain HTTP services that need no caller identity (later MCP servers and web APIs); the knowledge API is a Deployment autoscaled by KEDA (2 to 4 replicas on request rate), because its callers are admitted by mesh identity and Knative's cluster-local gateway runs outside the mesh;
  - KEDA on the agents' Substrate WorkerPool (2 to 6 workers), on Substrate's own metric of booked workers;
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
  - install the shared controllers and CRDs: KServe LLM presets, agentgateway, kagent 1.0.0-rc1 with Agent Substrate 1.0.0-rc1, agentregistry, and KEDA;
  - let the Argo Workflows controller run workflows in `llm`, and let `llm` reach OpenMetadata, for the RAG pipeline;
  - admit only the gateway to kagent's A2A endpoint, lower kagent's per-actor reservation, and let the Grafana dashboard sidecar load dashboards from `llm`.

  The platform upgraded the clusters to Talos 1.14.5 / Kubernetes 1.37.2 for kagent 1.0. This change consumes these components and does not install them.

## Capabilities

### New Capabilities

- `llm-inference-platform`: the self-hosted chat and embedding models, their shared GPU claim and memory split, model storage, endpoint picker and InferencePool, serving placement per environment, and model-server telemetry.
- `agent-gateway`: the agentgateway entry point, the LLM route and its authentication, rate-limit and PII policy, client keys, mesh authorization, and the route layout that MCP and A2A extend.
- `agent-runtime`: the global model config, the agents on Agent Substrate and their runtime image, credential injection at egress, the coordinator and its sub-agents, prompts and A/B tests in Langfuse, the agent registry catalog, and the autoscaling choice per workload.
- `llm-benchmark`: the benchmark harness, workload, measured optimizations and evidence rules.
- `rag-knowledge-pipeline`: the document corpus and its versioning, the release-triggered chunk, embed and Feast write workflow, the offline and pgvector online stores, the run's validation, the Web API and the knowledge MCP tool.

### Modified Capabilities

- `talos-gitops-deployment`: adds the `llm-platform` application in the platform-owned `llm` namespace. It is enabled in dev and disabled in prod until the user confirms launching the prod GPU pool. Its credentials come from `gateway-doppler`.
- `data-catalog-governance`: adds the knowledge-base product (contract checks and lineage for the RAG run).

## Impact

- **Infra:**
  - A new chart `infra/charts/llm-platform`: chat and embedding `LLMInferenceService`s, a shared GPU ResourceClaim, the model upload Job, Gateway, HTTPRoutes, AgentgatewayPolicies, API-key ExternalSecrets, Istio AuthorizationPolicies, kagent ModelConfig, RemoteMCPServer, AgentTemplates, Harness, Agent, EgressPolicy, WorkerPool and its ScaledObject, prompt seeds as files, the catalog job (registry and prompt seeds), Langfuse, and scrape configuration. The benchmark manifests live in `infra/charts/llm-platform/benchmark/`.
  - The `llm-platform` entry in `infra/argocd/app-of-apps/values.yaml`, disabled in `values-prod.yaml`.
  - A new chart `infra/charts/rag-pipeline`, application `rag-pipeline` in `llm` (enabled in dev, disabled in prod), containing:
    - the `rag-knowledge` WorkflowTemplate and its per-image `Workflow`;
    - the CNPG `rag-pgvector` Cluster and `Database`;
    - the `rag-api` Deployment (Web API and MCP tool in one);
    - ExternalSecrets, mesh policies and the registry `MCPServer` entry.
  - In `llm-platform`: the `ragingest` and `ragapi` client keys, the `/mcp/knowledge` route and policy, the `knowledge` RemoteMCPServer, and `evidence-agent` 0.3.0 with the knowledge tools.
  - In `llm-platform`: the `decision` Deployment, the screening webhook, the A2A route, backend and policy, `risk-agent`, `coordinator`, and the Grafana dashboard ConfigMap.
  - After #102: `llm` admitted by the Polaris, ClickHouse and OpenMetadata network rules.
- **Code:**
  - a new `src/llm/` uv workspace (`common`, `rag_pipeline`, `rag_serving`, `decision`); `decision` is image `decision`, the laya screening webhook and weight fetch, with their CI tests;
  - the agent runtime, a Go module in `src/llm/agent_runtime` (image `agent-runtime`): kagent's Go ADK entrypoint and the Langfuse prompt plugin, with CI tests;
  - the release script resolves each image's registry digest, because kagent accepts only digest-pinned runtime images; the RAG members build images `rag-pipeline` and `rag-serving`: corpus sources, renderer, chunker, embed client, Feast definitions, validation, OpenMetadata client, Web API and MCP server, with their CI tests;
  - the knowledge-base ODPS product and ODCS contract in `contracts/data-products/knowledge-base/`; the `openmetadata_jobs` unit tests are removed at the user's request.
  - No change to the data-pipeline code, the `data-pipelines` or `lakehouse` charts, or any existing contract.
- **Secrets:**
  - new keys in the `payment-gateway-fraud-detection` Doppler configs (`dev`, `prd`): `LLM_GATEWAY_AGENTS_API_KEY` and `LLM_GATEWAY_BENCHMARK_API_KEY`, already provisioned with random values. No model-provider key is needed, because the models are public on Hugging Face.
  - for RAG: `LLM_GATEWAY_RAG_INGEST_API_KEY`, `LLM_GATEWAY_RAG_API_KEY`, `RAG_DB_WRITER_PASSWORD` and `RAG_DB_READER_PASSWORD`, provisioned with random values in `dev` and `prd` on 2026-10-29.
- **Platform dependency:** the talos-proxmox components in design.md decision 13 must be live in an environment before the chart's resources can reconcile there.
- **Docs:**
  - new: `docs/llm/inference.md`, `docs/llm/gateway.md`, `docs/llm/observability.md`, `docs/llm/benchmark.md`, `docs/llm/rag.md` (and `docs/llm/agents.md` updated);
  - updated where behaviour changes: `docs/llm/agents.md`, `docs/overview/roadmap.md`, `docs/overview/architecture.md`, `docs/deployment/cd.md` (applications table), `docs/deployment/security.md` (keys), `docs/data/orchestration-governance.md` (RAG lineage), `docs/_Sidebar.md`.
- **No change** to the payment gateway, data pipelines or their contracts. Agents never sit in the payment decision path.
