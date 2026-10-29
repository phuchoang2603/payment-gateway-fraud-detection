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

- [x] 1.1 Create `infra/charts/llm-platform` (Chart.yaml, values.yaml, values-dev.yaml, values-prod.yaml) and add the `llm-platform` application (`ml`) to `infra/argocd/app-of-apps/values.yaml`, disabled in `values-prod.yaml`. Verify locally with `helm lint` and `helm template` for both roots: dev contains the Application and prod does not.
- [x] 1.2 Add the ExternalSecrets for the client keys (`llm-gateway-agents`, `llm-gateway-benchmark`), labelled for the gateway key selector and carrying the client id. Verify with `helm template` that no secret value is rendered.

## 2. Model servers

- [x] 2.1 Add:
  - the shared ResourceClaim `llm-gpu`;
  - PVC `llm-models`;
  - the idempotent `model-fill` sync-hook Job (pinned revisions of `Qwen/Qwen3-8B-FP8`, `Qwen/Qwen3-Embedding-0.6B` and the `Qwen/Qwen3-8B-AWQ` fallback; `restricted` security context).

  Verify with `helm template` for both environments (prod renders nothing while disabled).
- [x] 2.2 Add the chat `LLMInferenceService` `qwen3-8b`:
  - `pvc://` URI;
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

## 5. OpenMetadata (after PR #102 merges)

- [ ] 5.1 Rebase onto #102. In `openmetadata_jobs/integrations.py`, add the `llm` service kind (`CreateLLMServiceRequest`, `CustomLLM`) and idempotent registration of the `ai` entries (`llmModels`, `aiApplications`). Verify locally with ruff. CI runs the existing integration tests; add one behaviour test only if the AI registration adds a distinct failure path, such as an application whose primary model is missing.
- [ ] 5.2 Add the `llm` connector and the `ai` section to `infra/charts/metadata-integrations` values (dev), matching the llm-platform chart's model names, revisions and agent version. Verify with `helm template` that `connectors.json` contains them.

## 6. Benchmark harness

- [x] 6.1 Add `infra/docker/inference-perf.Dockerfile` (inference-perf 0.7.0 from PyPI) and its entry in `.github/service-images.json`. Add the disabled-by-default benchmark Job template with:
  - the shared-prefix workload and the concurrency stages;
  - target selection (vLLM direct or gateway);
  - upload to `runs/payment-gateway/llm-benchmark/<run>/`.

  Verify with `helm template` and the CI image build.
- [x] 6.2 Add the standalone chat vLLM variant pod template used for model-server runs while the chat `LLMInferenceService` is stopped. Embedding keeps running, so runs measure the shared-GPU reality. Verify with `helm template`.

## 7. Deploy to dev and collect evidence (after the PR is open and CI is green; tell the user first)

- [ ] 7.1 Release to dev through `deploy/dev`. Verify:
  - the model fill completes;
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

## 8. Documentation and cleanup

- [ ] 8.1 Write `docs/llm/platform.md`:
  - request path, model servers and the GPU split, placement, cache;
  - gateway policy, ModelConfig, agent and warm-up;
  - registry, OpenMetadata assets, the autoscaling decision table;
  - links to code at a commit, with captures.

  Write `docs/llm/benchmark.md` with the workload, recorded results and conclusions. Add both to `docs/_Sidebar.md`.
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
