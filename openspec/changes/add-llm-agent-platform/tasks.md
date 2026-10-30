## 0. Platform prerequisites (talos-proxmox, user approval required)

- [x] 0.1 Get approval for the platform set in design.md decision 13 (P0–P5; P6 is for phase 2).
- [x] 0.2 Implement P0: restrict `aws-gpu-training` to `g6.xlarge` in `apps/components/karpenter-nodes/environments/prod/values.yaml`, and update the two operations pages that list the types. Verify with read-only `kubectl get nodepool aws-gpu-training` after the prod platform sync (prod sync on user confirmation).
- [x] 0.3 Implement P1–P5 and sync to dev; P7 (vmagent in the mesh) follows. Verify read-only that:
  - the `kserve-config-llm-*` presets exist in `ml`;
  - GatewayClass `agentgateway` is Accepted;
  - kagent 0.10.4 and its UI, agentregistry and Substrate 0.0.9 pods are Ready;
  - WorkerPool `kagent-default` reports one ready worker.
- [x] 0.4 Provision the Doppler keys `LLM_GATEWAY_AGENTS_API_KEY` and `LLM_GATEWAY_BENCHMARK_API_KEY` in `payment-gateway-fraud-detection` `dev` and `prd`. Done 2026-10-29 with random 64-hex values, distinct per config, verified by name and length only.
- [x] 0.5 Platform: set `otel.tracing.enabled: true` (no exporter endpoint) in talos-proxmox's kagent values, so agents export to the endpoint each sets (the collector in `llm`). Done on dev in talos-proxmox 93080a9; `kagent-controller` reports `OTEL_TRACING_ENABLED=true`.

## 1. Chart and application wiring

- [x] 1.1 Create `infra/charts/llm-platform` (Chart.yaml, values.yaml, values-dev.yaml, values-prod.yaml) and add the `llm-platform` application (`llm`) to `infra/argocd/app-of-apps/values.yaml`, disabled in `values-prod.yaml`. Verify locally with `helm lint` and `helm template` for both roots: dev contains the Application and prod does not.
- [x] 1.2 Add the ExternalSecrets for the client keys (`llm-gateway-agents`, `llm-gateway-benchmark`), labelled for the gateway key selector and carrying the client id. Verify with `helm template` that no secret value is rendered.

## 2. Model servers

- [x] 2.1 Add:
  - the shared ResourceClaim `llm-gpu`;
  - the idempotent `model-upload` Job, which copies the pinned revisions of `Qwen/Qwen3-8B-FP8`, `Qwen/Qwen3-Embedding-0.6B` and the `Qwen/Qwen3-8B-AWQ` fallback into `artifacts/models/` with the Kubeflow artifacts identity.

  Verify with `helm template` for both environments (prod renders nothing while disabled).
- [x] 2.2 Add the chat `LLMInferenceService` `qwen3-8b`:
  - `s3://` model location streamed with `runai_streamer`, storage initializer off;
  - pinned `vllm/vllm-openai:v0.32.0` image;
  - args: 16k context, memory fraction 0.80, tool parser, reasoning off;
  - the shared claim;
  - per-environment requests and prod placement;
  - `router.gateway.refs` and `router.route.http.refs`, with `router.scheduler: {}`.

  Verify with `helm template` and a server-side dry run against dev once P1 is live.
- [x] 2.3 Add the embedding `LLMInferenceService` `qwen3-embedding`: pooling runner, memory fraction 0.12, max length 2048, the shared claim, no scheduler. Verify with a server-side dry run on dev.
- [x] 2.4 Add VMPodScrape configuration for both vLLM servers, the endpoint picker and agentgateway. vLLM scraping needs vmagent enrolled in the mesh (P7). Verify with `helm template`.

## 3. Agent gateway

- [x] 3.1 Add the `agentgateway` Gateway (ClusterIP, port 80) and the `llm` HTTPRoute (`/v1/embeddings` → embedding Service, `/v1` → InferencePool `qwen3-8b-inference-pool`, long request timeout). Verify with a server-side dry run on dev.
- [x] 3.2 Add the single `AgentgatewayPolicy` for `llm`: strict API keys by selector, request and token limits, and request/response PII masking (built-ins plus the PAN pattern). Verify with a server-side dry run on dev.
- [x] 3.3 Add the Istio AuthorizationPolicies:
  - chat vLLM ← gateway, EPP, vmagent;
  - embedding ← gateway, vmagent;
  - EPP ← gateway;
  - gateway ← labelled LLM clients, Substrate workers, vmagent.

  Verify with `helm template`.

## 4. Agents and registry (kagent 0.10.4)

- [x] 4.1 Add `ModelConfig` `default-model-config` and `Agent` `evidence-agent` (system message, no tools, replicas unset, `restricted` contexts), rendered from one values list by a helper. Verify with a server-side dry run on dev.
- [x] 4.2 Add `SandboxAgent` `evidence-agent-sandbox` from the same helper on WorkerPool `kagent-default`, with `sandbox.network` egress limited to the gateway. Verify with a server-side dry run on dev once P5 is live.
- [x] 4.3 Render the registry catalog from the agent values (`evidenceagent` 0.1.0) and add the PostSync Job that POSTs it to the registry's `/v0/apply`. Verified with `helm template` and a registry `dryRun=true` apply on dev.

## 4b. Langfuse

- [x] 4b.1 Vendor `langfuse` 1.5.41 (v3, image `3.225.11`) into the chart. Add its CNPG Postgres, Redis, ExternalSecrets and UI VirtualService, and point it at the shared ClickHouse contract agreed with #102. Provision its app keys in Doppler `dev` and `prd`. Verify with `helm lint`/`helm template`; live checks wait for the rebuild.
- [ ] 4b.2 After the rebuild and #102's ClickHouse: the platform adds the tunnel, proxy hosts and Dex redirect URIs; then verify on dev that the migrations complete, login works behind the proxy, and the init project's API keys authenticate.
- [x] 4b.3 Run an OpenTelemetry Collector in `llm` that receives OTLP from the gateway and `evidence-agent` and exports every span to Langfuse (basic auth from the init project keys) and to VictoriaTraces through `vtagent`. Verified with `helm template` and a server-side dry-run on dev.
- [x] 4b.4 Confirm on dev that a question to `evidence-agent` produces its trace in both Langfuse and VictoriaTraces (agent span, two `qwen3-8b` generations with token usage, the `search_knowledge` tool call), and that the gateway's chat and MCP spans reach both. Recorded 2026-10-30 on 7c44415. The gateway spans are separate traces, because the kagent runtime does not propagate `traceparent`.
- [ ] 4b.5 Run the agents on kagent's Python ADK so the gateway's spans join the agent's trace; confirm on dev one trace id across the agent, the gateway's chat and MCP spans and the model call, in both Langfuse and VictoriaTraces, and that the sandboxed copy still starts and resumes.
- [ ] 4b.6 Let Substrate's API server read the sandbox's two Secrets in `llm` (Role `substrate-sandbox-secrets`, `get` by name). Without it the golden actor never started on dev (`secrets \"evidence-agent-sandbox\" is forbidden` since 04:44). Confirm the SandboxAgent becomes Ready and answers, then remove the stale ActorTemplates holding the pool's only worker if they remain.

## 5. OpenMetadata (after PR #102 merges)

- [ ] 5.1 In `src/rag`, register the AI assets (decision 12) over OpenMetadata's REST API from the workflow's catalog step, with the facts in `rag-pipeline` values matching `llm-platform`. Verify the endpoints against the live OpenMetadata 2.0.6 API first.

## 5b. RAG: pipeline, Web API and MCP tool (after PR #102 merges; design.md decision 15)

- [x] 5b.1 Research the corpus sources and record the proposed corpus in `rag-corpus.md`.
- [x] 5b.2 Settle the layout with the user: `src/rag`, chart `rag-pipeline`, namespace `llm`, corpus in the image, release-triggered runs, Iceberg offline, Web API and MCP in scope (2026-10-29).
- [x] 5b.3 Run the workflow in `data`, where Argo, Polaris and OpenMetadata already admit pipelines; record P9 and P10 as optional follow-ups for an all-`llm` layout.
- [x] 5b.4 Create `src/rag/` (uv project):
  - `corpus/`: the hand-written synthetic documents with front matter, `sources.yaml` and the renderer;
  - `pipeline/`: publish, structure-aware chunker, batched embed client, pre-write validation gate, PyIceberg writer, Feast refresh with HNSW index, post-write verify;
  - `store/`: the Feast definitions (`knowledge_base`, `knowledge_chunks`, Postgres online store with `vector_enabled`, SQL registry);
  - `service/`: FastAPI with pydantic and probes, with the stateless streamable-HTTP MCP app mounted at `/mcp`;
  - CI tests for the code and threshold sync, chunk-id stability, version hashing, gate and contract rule parity, the Feast table name and index DDL, API validation and MCP tool mapping.

  Verify locally only with ruff and format.
- [x] 5b.5 Add the `rag-pipeline` and `rag-serving` images to `.github/service-images.json`. The pipeline's build paths are the workspace root, `common`, `pipeline` plus every corpus source in `sources.yaml`, and the Dockerfile copies those sources into the image.
- [x] 5b.6 Add the knowledge-base ODPS product and ODCS contract to `contracts/data-products/knowledge-base/`, read by the shared checks image. Publish the run status, corpus container and lineage from `src/rag` over OpenMetadata's REST API, without changing `openmetadata_jobs`.
- [x] 5b.7 Create `infra/charts/rag-pipeline` and the `rag-pipeline` application (dev on, prod off), with:
  - the executor ServiceAccount and Role;
  - the `rag-knowledge` WorkflowTemplate and a `Workflow` per `rag-pipeline` image tag, in the last sync wave, never a sync hook;
  - the CNPG `rag-pgvector` Cluster (`18.3-standard-trixie`) and `Database` with the `vector` extension and the writer and reader roles;
  - the `rag-api` Deployment (REST and MCP);
  - ExternalSecrets (gateway storage identity, Polaris client, DB roles, gateway keys, OpenMetadata token, ClickHouse checks user);
  - mesh AuthorizationPolicies;
  - the registry `MCPServer` entry.

  Verify with `helm template` and `helm lint`.
- [x] 5b.8 Give the chart its own ExternalSecrets in `data` and `llm` and a ClickHouse connector filtered to the `knowledge` tables, so no `lakehouse`, `data-pipelines` or platform change is needed.
- [x] 5b.9 In `llm-platform`:
  - add the `ragingest` and `ragapi` client keys;
  - add the `/mcp/knowledge` HTTPRoute, MCP backend and policy;
  - add a kagent `RemoteMCPServer` for the gateway route;
  - give `evidence-agent` the two tools as 0.2.0, updating its prompt and registry entry.

  Provision `LLM_GATEWAY_RAG_INGEST_API_KEY`, `LLM_GATEWAY_RAG_API_KEY`, `RAG_DB_WRITER_PASSWORD` and `RAG_DB_READER_PASSWORD` in Doppler `dev` and `prd`.
- [ ] 5b.10 Add the customer-features endpoint once the DP3 slice has customer features online in Feast, and admit `rag-api` to that feature server.

## 6. Benchmark harness

- [x] 6.1 Add `infra/charts/llm-platform/benchmark/job.yaml` (`vllm bench serve` concurrency sweep with the `prefix_repetition` workload, results printed as JSON) and `server.yaml` (standalone variant server with its mesh policy), applied with `kubectl`. Verify with a server-side dry run on dev.

## 7. Deploy to dev and collect evidence (after the PR is open and CI is green; tell the user first)

- [ ] 7.1 Release to dev through `deploy/dev`. Verify:
  - the model upload completes;
  - both `LLMInferenceService`s are Ready on the shared GPU (record `nvidia-smi` memory per process, FP8 on sm_120 or the AWQ fallback, and the time to Ready).
- [ ] 7.2 Check the gateway policy live and capture a screenshot:
  - no key and an unknown key get 401;
  - the agents' key gets a chat answer and an embedding;
  - a card number, phone and e-mail are masked;
  - a burst gets 429;
  - a direct call to vLLM is refused by the mesh.
- [ ] 7.3 Chat with `evidence-agent` and `evidence-agent-sandbox` in the kagent UI (port-forward), and capture the ModelConfig and agent pages. Run three concurrent sandboxed conversations on one worker and capture the worker count, actor states and answer times.
- [ ] 7.4 Verify that the registry lists `evidence-agent` 0.2.0, its system prompt and the `knowledge` MCP server, and capture the registry UI.
- [ ] 7.5 After section 5, verify that OpenMetadata shows the LLM service, both models and the agent application linked to `qwen3-8b`, and capture it.
- [ ] 7.6 Run the benchmark baseline and each optimization (design.md decision 9) on dev by applying the rendered Job with `kubectl`, plus the gateway end-to-end run. Keep only improving settings, and set the gateway limits from the measured capacity. Record each run's command, image digest, args and date.

- [ ] 7.7 Release `rag-pipeline` to dev and let its per-image `Workflow` run `rag-knowledge`. Record:
  - the version, chunk count, step durations and check results;
  - a re-sync that exits early;
  - a run after deleting a section, showing the stale id gone online;
  - a run with a deliberately broken document that fails at `validate` and leaves both stores unchanged;
  - the `EXPLAIN` showing the HNSW index;
  - the OpenMetadata lineage and test results, with screenshots.
- [ ] 7.8 Exercise the Web API (chunk by id, search, the 404 and 422 paths) and run a Locust load test, keeping its HTML report. Chat with `evidence-agent` 0.2.0 so that it calls the knowledge tools through the gateway. Capture the tool-call metrics, a stalled rollout of a deliberately broken `rag-serving` image with the previous pods still serving (the automatic fallback), and the registry entry for the MCP server.

## 7b. Rubric screenshots (dev; stored in `docs/llm/images/`, each embedded in the page named, with a sentence on what it proves)

- [ ] 7b.1 Inference platform (`inference.md`): both `LLMInferenceService`s Ready with their pods, `nvidia-smi` showing both processes on the one GPU, and the custom model's name and revision.
- [ ] 7b.2 Benchmark (`benchmark.md`): the baseline and each optimization's results table, with the command and date.
- [ ] 7b.3 Agent gateway security and policy (`gateway.md`): the 7.2 terminal run (401 without or with a wrong key, chat and embedding with the agents key, PII masked, 429 burst, mesh refusal), plus links to the Gateway, HTTPRoute and `AgentgatewayPolicy` YAML.
- [ ] 7b.4 MCP gateway (`rag.md`): `tools/list` and a `search_knowledge` call through `/mcp/knowledge` with the agents key, the same call refused without a key and refused when calling `rag-api` directly, plus a link to the MCP route and policy YAML.
- [ ] 7b.5 Global ModelConfig and agent (`agents.md`): the kagent UI's ModelConfig page pointing at the gateway, and a chat with `evidence-agent` that calls the knowledge tools and cites chunk ids, plus links to the Agent YAML.
- [ ] 7b.6 Agent UI authentication (`agents.md`): the kagent UI behind the platform's Dex login.
- [ ] 7b.7 Registry (`agents.md`): the agentregistry UI showing `evidence-agent`, its prompt and the `knowledge` MCP server, plus a link to the registry catalog template.
- [ ] 7b.8 Sandbox and warm-up (`agents.md`): `evidence-agent-sandbox` on Agent Substrate, with several conversations served by few workers (worker count, actor states, answer times), plus a link to the SandboxAgent YAML.
- [ ] 7b.9 RAG pipeline (`rag.md`): the Argo Workflows graph of a successful `rag-knowledge` run, the validate and verify reports, the OpenMetadata lineage from the corpus container through `rag-knowledge` to `knowledge.chunks`, and the knowledge-base contract test results.
- [ ] 7b.10 Web API (`rag.md`): the FastAPI OpenAPI page, chunk-by-id and search responses, the 404 and 422 paths, the readiness probe, and the Locust HTML report (throughput and latency).
- [ ] 7b.11 Web API tests (`rag.md`): the `rag-serving` test run from CI with coverage above 90%, showing the fixtures and mocks used. The serving member's pytest options enforce `--cov-fail-under=90` with `pytest-cov`.
- [ ] 7b.12 MCP deployment rollout (`rag.md`): the stalled rollout from 7.8 with the previous version still serving.
- [ ] 7b.13 OpenMetadata AI assets (`observability.md`): the LLM service, both models, the `knowledge` MCP server and the `evidence-agent` application linked to its model, tool and `knowledge.chunks`.
- [ ] 7b.14 LLM and agent telemetry (`observability.md`): the metrics are enabled now. vmagent scrapes vLLM, the endpoint picker, agentgateway, and `rag-serving`'s REST request and MCP tool-call counters (with failures). Capture them in the metrics explorer. The Grafana dashboard is deferred to a later change at the user's request.
- [ ] 7b.15 Langfuse (`observability.md`): the Langfuse UI behind Dex with the `payment-gateway-fraud-detection` project (prompt sourcing from Langfuse is phase 2), the gateway's and the agent's spans in Langfuse, and the same trace in VictoriaTraces through Grafana's trace view.
- [ ] 7b.16 CI/CD (`rag.md`, `inference.md`): the GitHub Actions runs that test, build and release the `rag-pipeline` and `rag-serving` images and the LLM charts, and the `deploy/dev` release they produced.
- [ ] 7b.17 Web API authentication and rate limit (`rag.md`): the REST knowledge API is routed through the gateway as `/knowledge/v1/...` with client keys and limits, like `/mcp/knowledge`; capture 401 and 429.

## 8. Documentation and cleanup

- [ ] 8.1 Write the LLM pages as separate, focused notes, each with its captures and links to code at a commit:
  - `docs/llm/inference.md`: model servers, the GPU split and rollout strategy, weight streaming, placement, cache;
  - `docs/llm/gateway.md`: the Gateway, routes (`/v1`, `/mcp/<server>`, `/<api>/v1`), keys, limits, PII masking, mesh rules;
  - `docs/llm/agents.md` (update): ModelConfig, `evidence-agent` and its tools, the sandboxed warm copy on Substrate, the registry, the autoscaling decision table;
  - `docs/llm/observability.md`: metrics and scrapes, gateway and agent traces in Langfuse and VictoriaTraces through the collector, OpenMetadata AI assets;
  - `docs/llm/benchmark.md`: the workload, recorded results and conclusions.

  Add the pages to `docs/_Sidebar.md`.
- [ ] 8.1b Write `docs/llm/rag.md` from the 7.7 and 7.8 runs: corpus, versioning, workflow, stores, checks, lineage, the Web API with its load-test results, and the MCP tool.
- [ ] 8.2 Update only the affected parts of (no single catch-all platform page):
  - `docs/llm/agents.md` (status and component table);
  - `docs/overview/roadmap.md` (LLM rows and the Knative/KEDA mapping);
  - `docs/overview/architecture.md` (deployable units);
  - `docs/deployment/cd.md` (applications table);
  - `docs/deployment/security.md` (new keys);
  - `docs/deployment/infra.md` (the prod GPU pool's instance type);
  - `docs/data/orchestration-governance.md` (AI assets).

  Verify with `rg` that no stale "planned" claim remains for built parts.
- [ ] 8.2b Before merging, revert `infra/argocd/app-of-apps/values-dev.yaml`, which turns off Kafka, the gateway, live traffic, Flink/Feast and the Spark pipelines on dev for the LLM evidence runs, so `main` keeps dev's full stack.
- [ ] 8.3 Run `devenv shell -- openspec validate add-llm-agent-platform --strict`, `helm lint` and the diff review locally. Open the PR, link it, and report CI results honestly.

## 9. Prod (explicit user confirmation required)

- [x] 9.1 On confirmation, sync P0–P5 to prod. Done 2026-10-29, with a second On-Demand `aws-workers` node added so the On-Demand Postgres pods schedule.
- [ ] 9.2 On confirmation, enable `llm-platform` in prod and let the pool launch one `g6.xlarge` Spot node. Verify that both models are Ready on the shared claim (or apply the affinity fallback from decision 6) and that a gateway call succeeds. Then disable it again unless the user wants it kept running.
