## 0. Platform prerequisites (talos-proxmox, user approval required)

- [x] 0.1 Get approval for the platform set in design.md decision 13 (P0–P5; P6 is for phase 2).
- [x] 0.2 Implement P0: restrict `aws-gpu-training` to `g6.xlarge` in `apps/components/karpenter-nodes/environments/prod/values.yaml`, and update the two operations pages that list the types. Verify with read-only `kubectl get nodepool aws-gpu-training` after the prod platform sync (prod sync on user confirmation).
- [x] 0.3 Implement P1–P5 and sync to dev; P7 (vmagent in the mesh) follows. Verify read-only that:
  - the `kserve-config-llm-*` presets exist in `ml`;
  - GatewayClass `agentgateway` is Accepted;
  - kagent 0.10.4 and its UI, agentregistry and Substrate 0.0.9 pods are Ready;
  - WorkerPool `kagent-default` reports one ready worker.
- [x] 0.4 Provision the Doppler keys `LLM_GATEWAY_AGENTS_API_KEY` and `LLM_GATEWAY_BENCHMARK_API_KEY` in `payment-gateway-fraud-detection` `dev` and `prd`. Done 2026-10-29 with random 64-hex values, distinct per config, verified by name and length only.

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
- [x] 2.3 Add the embedding `LLMInferenceService` `qwen3-embedding`: pooling runner, memory fraction 0.12, max length 8192, the shared claim, no scheduler. Verify with a server-side dry run on dev.
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

- [x] 4b.1 Vendor `langfuse` 1.5.41 (v3, image `3.224.1`) into the chart. Add its CNPG Postgres, Redis, ExternalSecrets and UI VirtualService, and point it at the shared ClickHouse contract agreed with #102. Provision its app keys in Doppler `dev` and `prd`. Verify with `helm lint`/`helm template`; live checks wait for the rebuild.
- [ ] 4b.2 After the rebuild and #102's ClickHouse: the platform adds the tunnel, proxy hosts and Dex redirect URIs; then verify on dev that the migrations complete, login works behind the proxy, and the init project's API keys authenticate.

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
- [x] 5b.5 Add the `rag` image to `.github/service-images.json`. Its build paths are `src/rag/` plus every corpus source in `sources.yaml`, and the Dockerfile copies those sources into the image.
- [x] 5b.6 Add the knowledge-base ODPS product and ODCS contract to `infra/charts/rag-pipeline/files/knowledge-base/`, mounted into the shared checks job. Publish the run status, corpus container and lineage from `src/rag` over OpenMetadata's REST API, without changing `openmetadata_jobs`.
- [x] 5b.7 Create `infra/charts/rag-pipeline` and the `rag-pipeline` application (dev on, prod off), with:
  - the executor ServiceAccount and Role;
  - the `rag-knowledge` WorkflowTemplate and its PostSync `Workflow` hook with a deadline;
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
- [ ] 7.4 Verify that the registry lists `evidence-agent` 0.1.0, and capture the registry UI.
- [ ] 7.5 After section 5, verify that OpenMetadata shows the LLM service, both models and the agent application linked to `qwen3-8b`, and capture it.
- [ ] 7.6 Run the benchmark baseline and each optimization (design.md decision 9) on dev by applying the rendered Job with `kubectl`, plus the gateway end-to-end run. Keep only improving settings, and set the gateway limits from the measured capacity. Record each run's command, image digest, args and date.

- [ ] 7.7 Release `rag-pipeline` to dev and let its sync hook run `rag-knowledge`. Record:
  - the version, chunk count, step durations and check results;
  - a re-sync that exits early;
  - a run after deleting a section, showing the stale id gone online;
  - a run with a deliberately broken document that fails at `validate` and leaves both stores unchanged;
  - the `EXPLAIN` showing the HNSW index;
  - the OpenMetadata lineage and test results, with screenshots.
- [ ] 7.8 Exercise the Web API (chunk by id, search, the 404 and 422 paths) and run a Locust load test, keeping its HTML report. Chat with `evidence-agent` 0.2.0 so that it calls the knowledge tools through the gateway. Capture the tool-call metrics, a Knative revision rollback, and the registry entry for the MCP server.

## 8. Documentation and cleanup

- [ ] 8.1 Write `docs/llm/platform.md`:
  - request path, model servers and the GPU split, placement, cache;
  - gateway policy, ModelConfig, agent and warm-up;
  - registry, OpenMetadata assets, the autoscaling decision table;
  - links to code at a commit, with captures.

  Write `docs/llm/benchmark.md` with the workload, recorded results and conclusions. Add both to `docs/_Sidebar.md`.
- [ ] 8.1b Write `docs/llm/rag.md` from the 7.7 and 7.8 runs: corpus, versioning, workflow, stores, checks, lineage, the Web API with its load-test results, and the MCP tool.
- [ ] 8.2 Update only the affected parts of:
  - `docs/llm/agents.md` (status and component table);
  - `docs/overview/roadmap.md` (LLM rows and the Knative/KEDA mapping);
  - `docs/overview/architecture.md` (deployable units);
  - `docs/deployment/cd.md` (applications table);
  - `docs/deployment/security.md` (new keys);
  - `docs/deployment/infra.md` (the prod GPU pool's instance type);
  - `docs/data/orchestration-governance.md` (AI assets).

  Verify with `rg` that no stale "planned" claim remains for built parts.
- [ ] 8.3 Run `devenv shell -- openspec validate add-llm-agent-platform --strict`, `helm lint` and the diff review locally. Open the PR, link it, and report CI results honestly.

## 9. Prod (explicit user confirmation required)

- [x] 9.1 On confirmation, sync P0–P5 to prod. Done 2026-10-29, with a second On-Demand `aws-workers` node added so the On-Demand Postgres pods schedule.
- [ ] 9.2 On confirmation, enable `llm-platform` in prod and let the pool launch one `g6.xlarge` Spot node. Verify that both models are Ready on the shared claim (or apply the affinity fallback from decision 6) and that a gateway call succeeds. Then disable it again unless the user wants it kept running.
