## Why

The LLM part of the coursework needs a self-hosted model behind a governed agent entry point before any fraud-analyst agent can be built. Neither cluster runs anything LLM-related today: KServe 0.21 ships its `LLMInferenceService` controller but no presets, the Gateway API Inference Extension CRDs are installed but unused, and there is no agent gateway, agent runtime or registry. This change builds phase 1: the parts that do not depend on the data pipeline or Feast, shaped so that RAG, the MCP tools, the evidence, risk and coordinator agents, and the MCP/A2A gateway routes attach later without rework.

## What Changes

- Serve Qwen3-8B (FP8) for chat and Qwen3-Embedding-0.6B for embeddings on vLLM through KServe `LLMInferenceService`s. KServe runs llm-d's endpoint picker (EPP) and InferencePool for the chat model.
  - Both share one GPU through a single DRA claim, each process with a fixed memory fraction: the RTX 5060 Ti on dev, and on prod the shared `aws-gpu-training` Spot pool, which the platform restricts to `g6.xlarge` (L4).
  - One chat replica serves every agent through vLLM's continuous batching. The benchmark measures its capacity, and a second GPU replica behind the same InferencePool is the scale-out path.
  - Model weights are cached on a PVC, so a restart or Spot replacement does not download them again.
- Run one agentgateway `Gateway` in `ml`. It is both the inference gateway (an OpenAI-compatible `/v1` route to the chat pool and the embedding service) and the future single entry point for MCP and A2A.
  - The LLM route requires a per-client API key and applies request and token rate limits and PII masking (card numbers, e-mail, phone) on prompts and responses.
  - Istio ambient stays the mesh: mesh authorization admits only the gateway (and the endpoint picker) to the model servers.
- Add a global kagent `ModelConfig`, `default-model-config`, that points every agent at the gateway's `/v1` with the agents' gateway key.
- Run `evidence-agent` 0.1.0 on kagent 0.10.4. It is a read-only fraud-analyst agent without tools, and the first version of the evidence agent that later gains the MCP evidence tool.
- Run the same agent as `evidence-agent-sandbox`, a `SandboxAgent` on Agent Substrate 0.0.9 (gVisor). Its conversations suspend to warm snapshots and resume on a small worker pool, which covers the sandbox and warm-up rubric rows.
- Publish the agent's catalog entry to agentregistry from Git with a sync hook.
- Register the AI assets in OpenMetadata through the existing `metadata-integrations` chart: the LLM service (the gateway endpoint), both models, and the agent as an AI application linked to its model. MCP servers, further agents and knowledge bases attach to these later.
- Add an on-demand benchmark harness (inference-perf, llm-d's benchmark tool) with an agent-shaped shared-prefix workload. It records throughput, TTFT and inter-token latency before and after each optimization (prefix caching, FP8 KV cache, n-gram speculative decoding, PVC-cached cold start, FP8 vs AWQ weights). Results are recorded only from real dev runs, with the command and date.
- Record the autoscaling decision against the rubric's KEDA:
  - Knative for this repo's own HTTP services (the later MCP servers and web API);
  - KEDA for kagent agent Deployments and the Substrate WorkerPool (from phase 2);
  - a fixed single replica per model server, because each cluster has one GPU.
- Document the platform on new `docs/llm/platform.md` and `docs/llm/benchmark.md` pages, and update `docs/llm/agents.md`, the roadmap and the architecture where the behaviour changes.
- Platform prerequisites are a separate talos-proxmox change set that needs user approval:
  - restrict the prod GPU pool to `g6.xlarge`;
  - install the shared controllers and CRDs: KServe LLM presets, agentgateway, kagent 0.10.4 with Agent Substrate 0.0.9, agentregistry, and KEDA for phase 2.

  The clusters stay on Kubernetes 1.36. This change consumes these components and does not install them.

## Capabilities

### New Capabilities

- `llm-inference-platform`: the self-hosted chat and embedding models, their shared GPU claim and memory split, model cache, endpoint picker and InferencePool, serving placement per environment, and model-server telemetry.
- `agent-gateway`: the agentgateway entry point, the LLM route and its authentication, rate-limit and PII policy, client keys, mesh authorization, and the route layout that MCP and A2A extend.
- `agent-runtime`: the global model config, the published agent and its sandboxed, warm copy on Agent Substrate, the agent registry catalog, and the autoscaling choice per workload.
- `llm-benchmark`: the benchmark harness, workload, measured optimizations and evidence rules.

### Modified Capabilities

- `talos-gitops-deployment`: adds the `llm-platform` application in `ml`. It is enabled in dev and disabled in prod until the user confirms launching the prod GPU pool. Its credentials come from `gateway-doppler`.
- `data-catalog-governance`: adds AI assets (LLM service, models, agents) to the catalog through the metadata-integrations registration.

## Impact

- **Infra:**
  - A new chart `infra/charts/llm-platform`: chat and embedding `LLMInferenceService`s, a shared GPU ResourceClaim, the model cache PVC and its fill Job, Gateway, HTTPRoute, AgentgatewayPolicy, API-key ExternalSecrets, Istio AuthorizationPolicies, kagent ModelConfig, Agent and SandboxAgent, the registry catalog and publish hook, scrape configuration, and the benchmark Job template.
  - The `llm-platform` entry in `infra/argocd/app-of-apps/values.yaml`, disabled in `values-prod.yaml`.
  - New `llm` and `ai` values in `infra/charts/metadata-integrations`.
- **Code:** an `llm` service kind and `ai` asset registration in the OpenMetadata register job (`src/data-pipeline/openmetadata_jobs/` after PR #102 merges).
- **Secrets:** new keys in the `payment-gateway-fraud-detection` Doppler configs (`dev`, `prd`): `LLM_GATEWAY_AGENTS_API_KEY` and `LLM_GATEWAY_BENCHMARK_API_KEY`, already provisioned with random values. No model-provider key is needed, because the models are public on Hugging Face.
- **Platform dependency:** the talos-proxmox components in design.md decision 13 must be live in an environment before the chart's resources can reconcile there.
- **Docs:**
  - new: `docs/llm/platform.md`, `docs/llm/benchmark.md`;
  - updated where behaviour changes: `docs/llm/agents.md`, `docs/overview/roadmap.md`, `docs/overview/architecture.md`, `docs/deployment/cd.md` (applications table), `docs/deployment/security.md` (keys), `docs/data/orchestration-governance.md` (AI assets in the catalog), `docs/_Sidebar.md`.
- **No change** to the payment gateway, data pipelines or their contracts. Agents never sit in the payment decision path.
