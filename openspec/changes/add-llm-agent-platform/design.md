## Context

Inventory taken read-only on 2026-10-29 with `kubectl` against both clusters and from talos-proxmox `main` (`660446e`).

| | Dev (Proxmox) | Prod (AWS) |
|---|---|---|
| Kubernetes / Talos | 1.36.3 / 1.13.9, 3 nodes | 1.36.3 / 1.13.9, control plane, 1 On-Demand worker, Spot burst nodes |
| GPU | RTX 5060 Ti 16 GB (Blackwell, CC 12.0, driver 595.91, CUDA 13.2) on `dev-server1` (control plane, untainted), advertised by `nvidia-dra-driver-gpu` 25.12.0 as ResourceSlice device `gpu-0` of DeviceClass `gpu.nvidia.com`, with no device plugin | Karpenter NodePool `aws-gpu-training`: Spot, `g4dn`/`g5`/`g6.xlarge`, `us-east-1a`, limit 1 node / 4 CPU / 16 GiB, taint `burst.talos.dev/gpu-training=true:NoSchedule`, label `burst.talos.dev/workload: gpu-training`, consolidate when empty after 5 min, 100 GiB gp3 root, interruption queue configured. 0 nodes now |
| DRA objects | DeviceClasses `gpu.nvidia.com`, `mig.nvidia.com`, `vfio.gpu.nvidia.com`; ResourceClaimTemplate `ml/aws-gpu-training` (1 GPU) from the platform's Kubeflow component | same |
| GPU operator | v26.7.2 with CDI and DCGM exporter (DRA-aware); driver, toolkit and device plugin come from Talos extensions | same |
| Gateway API | CRDs 1.5.1; GatewayClasses `istio`, `istio-waypoint`, `istio-remote` (Istio 1.30.7 ambient, inference extension not enabled) | same |
| Inference Extension | CRDs `inferencepools.inference.networking.k8s.io` v1 (bundle v1.5.0-rc.2), `inference.networking.x-k8s.io`, `llm-d.ai` `InferenceObjective`/`InferenceModelRewrite`, from the KServe bundle | same |
| KServe | 0.21.0 in `ml` (default mode Serverless on Knative); `llmisvc-controller-manager` running; **no** well-known `LLMInferenceServiceConfig` presets installed; `LocalModelCache` CRDs present | same |
| Knative | Serving and Eventing 1.23.0 with net-istio and `cluster-local-gateway`, default autoscaler config | same |
| Mesh / namespaces | `ml` is ambient with Pod Security `restricted`; `payment-gateway` uses per-pod ambient and AuthorizationPolicies | same |
| Secrets | ClusterSecretStores `doppler` (platform) and `gateway-doppler` (this repo; namespaces `payment-gateway`, `data`, `ml`) | same |
| Observability | VictoriaMetrics operator stack: vmagent (scrapes every VMServiceScrape/VMPodScrape and ServiceMonitor/PodMonitor), vmsingle, vtsingle (traces), vlsingle (logs), Grafana, DCGM dashboard | same |
| Other | cert-manager 1.21.3, metrics-server, CNPG, local-path (dev) / gp3 EBS (prod) storage | same |
| LLM-related | nothing: no agentgateway, kagent, agentregistry, Agent Substrate, KEDA, llm-d charts or model servers | same |

`dev-server1` has about 1.3 CPU and 11.8 GiB of unrequested capacity. The GPU node in prod has 4 vCPU and 16 GiB for every instance type the pool allows.

The rubric (`edai-2`, LLM rows) asks for: an llm-d based inference platform with a custom model; agentgateway with security and policy; a model-server benchmark with optimizations; a global kagent ModelConfig through the gateway; agentregistry with a published agent; and Agent Substrate warm-up. Later rows add RAG, MCP tools, sandboxed and autoscaled agents, MCP and A2A gateway routes and a coordinator. The reference project (`../fsds/example-proj/card-fraud-platform`) used llama.cpp on the host only because its Pascal GPU could not run vLLM. Its patterns for gateway policy, the evidence shape and the benchmark workload carry over; its model-server path does not.

## Goals / Non-Goals

**Goals:**

- A self-hosted, GPU-backed, OpenAI-compatible chat model and embedding model, reachable only through an authenticated, rate-limited, PII-masking gateway.
- Agents configured once, through a global ModelConfig, with one published agent running sandboxed and warm on Agent Substrate.
- The model, the agent and the serving endpoint registered in OpenMetadata next to the data assets they will later read.
- Measured benchmark results with optimizations, from real runs.
- Route, key, namespace and catalog layout that the MCP servers, RAG, the three fraud agents and the A2A routes extend without renaming or moving anything.

**Non-Goals:**

- MCP servers other than the knowledge tool (decision 15), the scoring tool, the risk and coordinator agents, MCP/A2A routes, agent autoscaling, prompt management, LLM/agent dashboards and A/B tests. These come after the feature-store change.
- A self-hosted laya decision model on CPU, for the coordinator's routing and for gateway screening (an agentgateway prompt-guard webhook) of requests that ask agents to act. It belongs with the coordinator in phase 2. The hosted Jev API is not used, because it would send analyst prompts to a third party.
- Public exposure of the gateway, kagent UI or registry UI (port-forward only in this phase).
- Running prod GPU capacity without explicit user confirmation.

## Decisions

### 1. Model serving: KServe `LLMInferenceService` (llm-d under KServe), not standalone llm-d charts

KServe 0.21's `LLMInferenceService` deploys vLLM, llm-d's inference scheduler (EPP), an InferencePool and an HTTPRoute from one resource. It is the llm-d integration KServe and agentgateway both document. The controller already runs in both clusters as part of Kubeflow. It merges the well-known `kserve-config-llm-*` presets and stops with `ConfigNotFound` when they are absent, so the platform must install `kserve-llmisvc-resources` v0.21.0 (presets only, with the GIE CRDs already present). That is talos-proxmox change P1.

The workload overrides only what it needs:

- `model.uri: s3://<bucket>/artifacts/models/Qwen/Qwen3-8B-FP8/<revision>` and `model.name: qwen3-8b`;
- `template.containers[main]`: the vLLM image, args and resources;
- `template.resourceClaims`: the GPU claim;
- `router.gateway.refs`: the agentgateway Gateway;
- `router.route.http.refs`: our own HTTPRoute, so gateway policy targets a stable name;
- `router.scheduler: {}`: the default EPP config, which uses vLLM's queue, KV-cache and prefix-cache scorers.

Alternative: llm-d's quickstart (helmfile with `llm-d-infra`, the GIE `inferencepool` chart and a vLLM Deployment). It needs no platform preset install, but it puts three charts and a second gateway model into this repo, duplicates what Kubeflow already ships, and leaves the later KServe fraud model (rubric ML serving) on a different control plane. Rejected unless the user prefers the literal quickstart (open question 1).

### 2. One agentgateway Gateway for inference, LLM policy, MCP and A2A

agentgateway implements InferencePool backends natively (`inferenceExtension.enabled=true`) and is documented with KServe `LLMInferenceService`. So one Gateway, `agentgateway` in `llm` with an HTTP listener on 80 and a ClusterIP Service, serves as both the inference gateway and the agent gateway. A request takes one hop: client → agentgateway (policy) → EPP pick → vLLM pod.

Istio's Gateway API implementation stays for the payment-gateway ingress. Its inference extension is not enabled, so there is no second InferencePool-capable gateway.

agentgateway coexists with ambient: the proxy pod lives in the ambient `llm` namespace, so ztunnel secures client→gateway and gateway→EPP/vLLM traffic with mTLS. The gateway is not a waypoint, and no waypoint is deployed for `ml`.

Route layout, fixed now so later routes only add rules:

| Path | Route | Phase |
|---|---|---|
| `/v1/embeddings` | `llm` rule → Service of `LLMInferenceService` `qwen3-embedding` | 1 |
| `/v1/*` (chat completions, completions, models) | `llm` rule → InferencePool `qwen3-8b-inference-pool` | 1 |
| `/mcp/<server>` | one HTTPRoute + AgentgatewayBackend per MCP server | `/mcp/knowledge` in phase 1 (decision 15), others phase 2 |
| `/a2a/<agent>` | one HTTPRoute per agent | phase 2 |

agentgateway applies one policy object per target, so every route gets exactly one `AgentgatewayPolicy`. The same holds for the reference project's finding that policies overwrite each other.

### 3. Gateway security and policy (LLM route)

- **Authentication:** `apiKeyAuthentication: Strict`, with a `secretSelector` on label `llm.payment-gateway.dev/client-key: "true"`. Each client has its own key Secret, synced by ExternalSecret from `gateway-doppler`: `agents` and `benchmark` now, with analyst UI, MCP and evaluation clients later. Each key Secret carries the client id as metadata, so later MCP/A2A authorization rules can match on it. A missing or unknown key gets 401.
- **Rate limits:** local limits of 120 requests per minute (burst 20) and 60,000 tokens per minute per route. These are starting values from the reference workload; the benchmark sets the final ones (decision 9).
- **Prompt guard:** request and response `regex` with action `Mask`. It uses the built-ins `CreditCard`, `Email` and `PhoneNumber`, plus a PAN pattern for 13–19 digit numbers with separators that the built-in misses. Masking keeps an analyst conversation going where rejection would break it. Synthetic gateway tokens are not secrets, so they are not masked.
- **Mesh authorization (Istio `AuthorizationPolicy`, L4 in ambient):**
  - vLLM pods admit the gateway's ServiceAccount on 8000 and the EPP's ServiceAccount (metrics scraping on the same port);
  - the EPP admits the gateway (ext-proc);
  - the embedding server admits only the gateway;
  - the gateway admits `llm` ServiceAccounts labelled as LLM clients (benchmark, later MCP servers) and the Substrate worker identity in `ate-system`, where agent actors run.

  vLLM serves inference and `/metrics` on the same port 8000, so the platform enrolls `vmagent` in the ambient mesh (platform change P7). The model servers then admit its identity, `cluster.local/ns/observability/sa/vmagent-vmks`, instead of opening the port. The gateway's 15020 and the EPP's 9090 are dedicated metrics ports, so they stay open to any source. Nothing reaches the model servers around the gateway.
- **Credentials:** no model-provider key exists. vLLM listens without an API key inside the mesh, and the gateway key is the only credential a client holds.

### 4. Models: Qwen3-8B FP8 and Qwen3-Embedding-0.6B on one GPU; prod on L4 only

The chat model is `Qwen/Qwen3-8B-FP8` (revision `220b46e3`, 8.79 GiB of safetensors). The embedding model is `Qwen/Qwen3-Embedding-0.6B` (revision `97b0c614`, 1.11 GiB), served by vLLM's pooling runner as `qwen3-embedding`, which the RAG pipeline and its search tool will call. Both share the one GPU (decision 6).

Memory budget on the dev card (15.93 GiB):

| Process | `--gpu-memory-utilization` | Budget | Weights | Left for KV cache |
|---|---:|---:|---:|---|
| Chat | 0.80 | 12.7 GiB | 8.8 GiB | about 2.7 GiB after about 1.2 GiB of activations and CUDA graphs: about 39k tokens at FP8 KV (72 KiB/token for 36 layers × 8 KV heads × 128), about 19k at bf16 |
| Embedding | 0.12 | 1.9 GiB | 1.1 GiB | `--max-model-len 2048` (measured on dev: 0.29 GiB of KV cache is left, and 8192 tokens need 0.88 GiB) |

The total of 0.92 leaves headroom for the CUDA contexts. Prod's L4 (24 GB, about 22.5 GiB usable) keeps the same fractions, which gives chat about 8 GiB of KV cache: roughly 115k tokens at FP8 KV. If FP8 does not load on sm_120, or its KV room cannot hold two full 16k contexts at FP8 KV, the chat model switches to `Qwen/Qwen3-8B-AWQ` (revision `4da05a8e`, 5.68 GiB). That is still Qwen3-8B and adds about 3 GiB of KV cache. The benchmark compares both.

Qwen3-8B-FP8 runs on vLLM's native FP8 W8A8 path on both environments' GPUs: Blackwell on dev (CC 12.0) and Ada on prod (`g6.xlarge` L4, CC 8.9). At the user's request the platform pool itself is restricted to `g6.xlarge` (platform change P0), so the workload needs no instance-type affinity. The alternatives were weaker: the A10G (`g5`) supports FP8 only as weight-only Marlin, and the T4 (`g4dn`) has no bfloat16, only 16 GB and a caveated Marlin path.

Chat settings:

- `--max-model-len 16384`: the agents' longest expected prompt is a few thousand tokens;
- `--kv-cache-dtype` as decided by the benchmark (FP8 expected);
- Qwen3 reasoning off by default through the chat template kwargs, so agent turns do not spend tokens on `<think>`;
- tool-call parsing on (`--enable-auto-tool-choice --tool-call-parser hermes`), because kagent agents call tools.

**Capacity for several agents.** One chat replica serves every agent, in dev and in prod. vLLM batches concurrent requests continuously, so agents share the model instead of queueing behind one another. The limit is KV-cache room, not the number of agents.

An analyst question in phase 2 fans out to coordinator → evidence and risk agents, each with a few tool-loop calls of a few thousand tokens. Prompt-prefix caching makes the repeated system prompts and tool definitions cheap. As an estimate, to be replaced by the benchmark: dev's ~39k FP8 KV tokens hold roughly 10–15 such calls in flight, and prod's ~115k several times more. That is enough for a few concurrent analyst sessions, which is the load this project runs.

The benchmark measures it at concurrency 1–16 and the gateway's rate limits are set from that. If it is not enough, the design scales without rework:
- add a chat replica on a second GPU (raise the platform pool limit); the InferencePool and EPP then balance with prefix- and KV-aware routing, which is where llm-d pays off;
- give a light agent such as the coordinator a smaller Qwen3 through its own ModelConfig variant.

So one model is not a dev-only simplification; it is the production configuration for this load.

The embedding service has no scheduler: one replica needs no endpoint picker, so its route targets the service's Kubernetes Service directly.

"Custom model" means a model this project selects, quantizes for its hardware and serves under its own name and settings, rather than the quickstart default. A LoRA fine-tune is out of scope unless the user wants it (open question 2).

### 5. Image

Pin `vllm/vllm-openai:v0.32.0` (2026-10-18; vLLM requires CC ≥ 7.5 and supports sm_120) in the workload template, overriding the preset's `ghcr.io/llm-d/llm-d-cuda:v0.9.0`. The preset's start script works with any image that has the `vllm` CLI. The `llm` namespace enforces Pod Security `restricted`, so the container runs as UID 1000, with `HOME`, the Hugging Face cache and `/tmp` on emptyDirs and `/dev/shm` as a memory emptyDir, all already in the preset. `USER` is set so libraries that look up the user name work under a UID without a passwd entry. The first dev bring-up verifies FP8 on sm_120 and records the outcome.

Serving flags (checked against the vLLM v0.32.0 source):

| Flag | Setting | Status |
|---|---|---|
| `--max-model-len` | 16384 (chat), 2048 (embedding) | fixed |
| `--gpu-memory-utilization` | 0.80 / 0.12 | fixed (decision 4) |
| `--reasoning-parser qwen3`, `--default-chat-template-kwargs '{"enable_thinking": false}'` | chat | fixed: agents get answers, not `<think>` blocks |
| `--enable-auto-tool-choice --tool-call-parser hermes` | chat | fixed: kagent tool calls |
| `--runner pooling` | embedding | fixed |
| prefix caching, chunked prefill, async scheduling, `-O2` | vLLM defaults (on) | kept; prefix caching is measured off vs on |
| `--kv-cache-dtype fp8` | chat | benchmarked |
| `--max-num-seqs` | bounded to what the KV cache holds, to avoid preemption | benchmarked |
| `--performance-mode` | `balanced` (default) vs `interactivity` (small-batch latency, which suits agent turns) | benchmarked |
| `--speculative-config` n-gram | chat | benchmarked |
| `--load-format` | `runai_streamer` from object storage; `concurrency` and the CPU buffer `memory_limit` via `--model-loader-extra-config` | benchmarked as cold start |

KServe's start script `eval`s the container args, so JSON-valued args are written single-quoted inside the value.

fastsafetensors and InstantTensor also ship in the image, but they read local files, so they don't apply once weights stream from object storage. GPUDirect Storage is not used either:
- Talos ships no `nvidia-fs` extension (only gdrcopy, which is GPUDirect RDMA), and the GPU operator's GDS component needs an operator-managed driver, which Talos does not use;
- NVIDIA supports GDS only on data-center and workstation GPUs, which excludes the dev GeForce card. The dev card is also passed through to a VM on a virtio disk;
- weights stream from object storage, which GDS does not read. GDS would only help from the `g6.xlarge`'s ephemeral local NVMe, which would mean downloading again on every Spot replacement.

### 6. GPU through DRA, placement per environment

The chart owns one ResourceClaim `llm-gpu` in `llm` (a claim, not a template): one device of class `gpu.nvidia.com`, `ExactCount` 1. Both model pods reference it by `resourceClaimName` in `template.resourceClaims`, together with the container's `resources.claims`. A DRA claim shared by several pods gives them the same allocated GPU on the same node, which is how chat and embedding share the card. The memory split comes from each vLLM's utilization fraction (decision 4), not from the driver. KServe's experimental managed-DRA annotations are not used. The platform's `aws-gpu-training` template is not reused, because its name and lifecycle belong to training.

- Dev: no selector is needed, because the only GPU is on `dev-server1`. Chat requests 1 CPU and 8 GiB memory (limit 12 GiB); embedding requests 250m CPU and 2 GiB. Together they fit the node's free capacity.
- Prod: both pods get node selector `karpenter.sh/nodepool: aws-gpu-training` and the pool's toleration; the pool provides only `g6.xlarge`. Chat requests 2 CPU and 9 GiB memory, and embedding 500m and 2 GiB, which fit a 4 vCPU / 16 GiB node after Talos reservations and DaemonSets. Karpenter launching a node for a pod that references a not-yet-allocated shared claim is verified at prod enablement. If it does not work, embedding moves to the chat pod's node by pod affinity, and the claim stays shared.

The pool is shared with Kubeflow training and limited to one node, so serving and training cannot run at the same time in prod. Serving must therefore be off whenever training runs (decision 10).

### 7. Model weights in object storage, streamed to the GPU

Weights live once per environment in the platform bucket under `artifacts/models/<repo>/<revision>/`:
- dev: MinIO `platform-dev`;
- prod: S3 `platform-prod-…`.

They stream straight into GPU memory with Run:ai Model Streamer (`--load-format runai_streamer`, already in the vLLM image), so the serving node never writes weights to its disk. That matters because the dev GPU node is also the control plane: writing tens of gigabytes there starves etcd, and the GPU cannot move to a worker because of a physical constraint. On prod, every new Spot node streams in-region from S3, with no volume to reattach.

- **Upload:** an idempotent `model-upload` Job (sync wave -1) copies the pinned Hugging Face revisions file by file. It skips objects already present with the same size and deletes each local copy after upload, so the node only ever holds one file. It runs on the pinned `python:3.13-slim` image with pinned `huggingface-hub` and `boto3` installed at start, so the project builds no image for it.
- **Credentials:** the platform's Kubeflow artifacts identity (Doppler `OBJECT_ARTIFACTS_*`, scoped to `artifacts/`, `datasets/` and `runs/`), synced into `llm` as Secret `llm-model-store` from the platform `doppler` store, so no new platform policy is needed.
- **Serving:** the chart sets the container command to `vllm serve s3://…` because KServe's preset start script serves a local `/mnt/models`. `storageInitializer` is off, and `model.uri` records the object location. Because the args are no longer re-parsed by the preset's `eval`, JSON values need no extra quoting.

KServe's `LocalModelCache` and a model PVC were considered and dropped: both write the weights to a node disk.

### 8. Agent runtime: kagent 0.10.4 with Agent Substrate 0.0.9

The clusters stay on Kubernetes 1.36.3 (user decision). kagent 1.x requires 1.37, so the runtime is kagent 0.10.4, the current stable release. It keeps `Agent` → Deployment and adds `SandboxAgent` on Agent Substrate through `controller.substrate.*`. Substrate covers the sandbox and warm-up rubric rows.

kagent 0.10.4 pins Substrate v0.0.9 (`replace` in its `go.mod`) and ships it as an optional subchart (`substrate.enabled`). Substrate 0.0.9 defaults to JWT authentication, which needs no `PodCertificateRequest` or `ClusterTrustBundle` feature gates, so no Talos or control-plane change is required. It brings Valkey and snapshot storage. Snapshots go to an external S3/MinIO prefix with its own identity, not to the bundled RustFS with its default credentials.

The platform installs kagent 0.10.4 with:
- Substrate 0.0.9 and the `kagent-default` gVisor WorkerPool;
- built-in agents and tools off;
- the UI on.

Its Helm default provider is disabled or pointed at the gateway (open question 6).

This repo owns, in the platform-owned `llm` namespace (ambient, Pod Security `restricted`):

- **`ModelConfig` `default-model-config`:** `provider: OpenAI`, `model: qwen3-8b`, `openAI.baseUrl: http://agentgateway.llm.svc.cluster.local/v1`, `apiKeySecret: llm-gateway-agents`, `apiKeySecretKey: key`. This is the global model config. Every agent references it, and changing the model, gateway or key for all agents is one edit.
- **`Agent` `evidence-agent`:** declarative, `modelConfig: default-model-config`, with no tools and a read-only fraud-analyst system prompt that states it has no data access yet and must not invent payment facts. The prompt is `files/prompts/<agent>.md` in the chart, loaded through `systemMessageFrom` from the `agent-prompts` ConfigMap, The prompt is published to agentregistry as a versioned `Prompt` (decision 11). `replicas` is left unset: kagent's Deployment mutation sets replicas only when the Agent specifies them, so the phase 2 KEDA ScaledObject can own them. Pod and container security contexts satisfy `restricted`.
- **`SandboxAgent` `evidence-agent-sandbox`:** the same spec, rendered from one helper, on the platform's default WorkerPool `kagent/kagent-default`, with `sandbox.network.allowedDomains` limited to the gateway Service. Snapshots go under the platform's `substrate/` prefix. kagent 0.10.4's CRD accepts only `gs://`-style locations, which Substrate's S3 backend also uses, so the location is `gs://<environment bucket>/substrate/llm/<agent>/`. Each conversation is an actor in a gVisor sandbox. Idle actors suspend to snapshots and resume on any worker, so a one-worker pool serves several conversations. This is the warm-up ("Agent Fleet Architecture") evidence.

Later agents (risk, coordinator) and MCP tool references are additional entries in the same values list. Each renders both an `Agent` and a `SandboxAgent`. A new prompt version is a change to the file and the agent version, released together. Moving to kagent 1.x later is a platform upgrade plus re-rendering these resources from the same values.

### 9. Benchmark harness and optimizations

The tool is vLLM's own `vllm bench serve`, run from the same pinned vLLM image, so no benchmark image is built. The benchmark is not part of the chart: Plain manifests applied with `kubectl` on dev (user decision):
- `job.yaml`, a Job that runs the concurrency sweep and prints each result as JSON to its log;
- `server.yaml`, a standalone vLLM Pod, Service and mesh policy for one model-server variant.

The manifests live with the chart in `infra/charts/llm-platform/benchmark/`, outside `templates/`, so Argo CD never renders them. The chart only provides the benchmark's identity (ServiceAccount `llm-benchmark`, admitted by the mesh policies) and its gateway key. Results are copied from the Job log into `docs/llm/benchmark.md`.

The workload is vLLM's `prefix_repetition` dataset with 3 shared prefixes (future agents): 1,500-token system prompts, 100-token questions and 150 output tokens with `--ignore-eos`. Closed-loop concurrency stages are 1, 2, 4, 8 and 16. The reported metrics are output tokens/s, requests/s, TTFT p50/p90, inter-token latency (TPOT) p50/p90, end-to-end latency, prefix-cache hit share from vLLM's metrics, and failures.

Two targets:

- **Model-server runs** go straight to the vLLM Service, to isolate the server. One variant changes at a time. Argo CD self-heals the chat `LLMInferenceService`, so it can't be stopped with `kubectl`. A dev values flag (`models.chat.stopped`) sets the KServe stop annotation through a normal release. While it is set, `server.yaml` runs each variant, edited and applied with `kubectl`. The embedding server keeps running, so the runs measure the shared GPU.
- **One end-to-end run** goes through agentgateway with the chosen configuration, which shows the gateway and EPP overhead.

Optimizations measured, each against the previous kept configuration:

1. prefix caching off vs on;
2. KV cache bf16 vs FP8 (capacity and concurrency at 8–16);
3. `--max-num-seqs` bound and `--performance-mode interactivity` vs `balanced`;
4. n-gram speculative decoding vs none, on a grounded-answer workload where the answers quote context;
5. cold start, split into weight-load time and time to Ready:
   - streamer `concurrency` and buffer settings;
6. FP8 vs AWQ weights, if FP8 runs on sm_120.

Each result row records the command, image and digest, args, date and environment. Only measured numbers are written down.

### 10. Autoscaling decision (rubric names KEDA)

| Workload | Owner of the replicas | Mechanism | Why |
|---|---|---|---|
| Model server | `LLMInferenceService.replicas` | Fixed: 1 in dev, 0 in prod until confirmed; scale-to-zero by a values toggle | Each cluster has exactly one schedulable GPU (prod pool limit is 1 node), so there is nothing to scale out to. `LLMInferenceService.scaling` supports only KEDA and llm-d's WVA, and Knative cannot manage it. With KEDA in place, `scaling.keda` (scale 0↔1 on the gateway's queued LLM requests) becomes possible for prod cost; it is not built in phase 1, because a Spot GPU cold start of several minutes would time out agent calls. |
| kagent agents (phase 2) | KEDA `ScaledObject` on the Deployment kagent creates; `minReplicaCount` 2 for multi-replica | Prometheus trigger on agentgateway's A2A request rate per agent, from vmsingle's Prometheus API | Knative's KPA and HPA classes scale only Knative Revisions, and kagent creates plain Deployments with no Knative mode. CPU-based HPA misreads agents that mostly wait on the model. KEDA is acceptable to the user, so it is the platform equivalent here. |
| Substrate worker pool (phase 2) | KEDA `ScaledObject` on the `WorkerPool` scale subresource | Active-actor or request-rate trigger | `WorkerPool` exposes a scale subresource for exactly this purpose |
| MCP servers, web API (phase 2; the knowledge API is the exception in decision 15) | Knative `Service` | KPA with a concurrency target, `min-scale` ≥ 1 for MCP sessions, revisions for rolling update with automatic fallback to the last ready revision | They are this repo's own stateless HTTP services, which is Knative's model. Knative is already deployed and is the user's preference. |

The docs record this mapping the way the roadmap maps Airflow→Argo: Knative where the workload is a plain HTTP service, KEDA where the replicas belong to another controller.

### 11. Registry

agentregistry 0.4.0, the latest stable release, is installed by the platform with an external Postgres on CNPG and RBAC limited to `llm`. It keeps its ClusterIP Service, reached by port-forward in phase 1. This repo owns the catalog, rendered from the same agent values as the kagent resources so name and version have one source. The registry's agent names cannot contain hyphens, so `evidence-agent` is published as `evidenceagent`, tag `0.1.0`, with its model config named in the description; the registry's `Model` kind supports only Bedrock. A PostSync Job POSTs the catalog to the registry's `/v0/apply` endpoint, which is what `arctl apply` calls, from a pinned curl image. CI never contacts the cluster, so publication happens at sync. A changed catalog means a new version; later phases add MCP server entries and bump the agent to 0.2.0 with its tools.

agentregistry 0.4.0 materializes agents as kagent `v1alpha2` resources, the API kagent 0.10.4 serves. Even so, the agents are deployed by this chart through GitOps, and the registry is the governed catalog (published versions, UI). That keeps one deployment path, so the registry's deploy feature is not used.

**Prompts are versioned in the registry.** Each agent's system prompt is a file, `files/prompts/<agent>.md`. kagent reads it through `systemMessageFrom` from the `agent-prompts` ConfigMap. The same content is published to agentregistry as a `Prompt` (`<agent>-system-prompt`, tagged with the agent version), so every released prompt version is retained and browsable next to its agent. In phase 2 Langfuse becomes the prompt-management tool the rubric names, and the source of truth. It is deployed now by this chart (decision 14). A sync step renders each agent's `production`-labelled prompt into the `agent-prompts` ConfigMap, because kagent cannot fetch from Langfuse itself, and the registry `Prompt` entries are then dropped. Gateway traces go to the platform's VictoriaTraces, not Langfuse: agentgateway's frontend tracing policy posts OTLP straight to `vtagent` with no collector, as the reference project posts to Jaeger. Langfuse's OTLP endpoint needs basic-auth headers the gateway cannot send, and vtagent cannot yet forward OTLP. LLM-level traces in Langfuse come from the agents themselves, through kagent's own OpenTelemetry instrumentation of its runtime and OpenAI client. Each `Agent` sets the OTLP traces endpoint (Langfuse's `/api/public/otel/v1/traces`, `http/protobuf`) and a basic-auth header built by an ExternalSecret from the headless-init project keys. kagent appends its controller's `OTEL_*` variables after the agent's own, so its cluster-wide `OTEL_TRACING_ENABLED=false` wins: tracing starts only when the platform sets `otel.tracing.enabled: true` in its kagent values, with no endpoint, so each agent keeps its own exporter. That platform change is recorded as a blocker. The agent's calls carry `traceparent`, so the gateway's spans in VictoriaTraces share the agent's trace id. The sandboxed copy is not traced yet, because its Substrate actors run in `kagent` and their sandbox only allows the gateway. The rubric asks for telemetry, which the gateway, vLLM and `rag-serving` metrics provide, and names Langfuse only for prompt management. Skills use the same registry mechanism when agents get them. OpenMetadata's `promptTemplate` entity catalogues the same prompts with the AI assets (decision 12).

### 12. OpenMetadata: register the AI assets from this change's own code

OpenMetadata 2.0.6 has first-class AI entities: `llmService`, `llmModel`, `aiApplication` and `mcpServer`. It has no ingestion connector for a custom LLM service, so the assets are created through its REST API.

This change must stay independent of the data pipelines, so it does not change the `openmetadata_jobs` code or the `data-pipelines` chart, which now holds the OpenMetadata connections. Instead the RAG workflow's `catalog` step (decision 15) registers them on every run with idempotent `PUT`s from the `rag` image. That step already runs in `data`, the namespace OpenMetadata admits.
- `llmService` `agent-gateway`: `CustomLLM`, base URL the gateway's `/v1`, no key stored.
- `llmModels` `qwen3-8b` and `qwen3-embedding`: base model, pinned revision as version, capabilities, and the vLLM deployment.
- `aiApplication` `evidence-agent`: agent, kagent, primary model `qwen3-8b`, the current agent version, and its MCP server.
- `mcpServer` `knowledge`: the gateway route, its two tools, and lineage to `knowledge.chunks`.

The facts come from the `rag-pipeline` values and must match the `llm-platform` chart, because one chart cannot read another's values. Both charts change in the same commit when a model or the agent version changes.

### 14. Langfuse, app-owned, on the shared ClickHouse

Langfuse v3 runs in `llm` from the upstream chart `langfuse` 1.5.41 (the last v3 chart), vendored as a dependency of this chart with image `3.225.11`, the v3 patch release that carries the fix for ClickHouse 26.8+ misparsing Langfuse's DateTime64 inserts (langfuse/langfuse#16858, which stamped traces `9999-12-31` on dev). Every bundled subchart is off, so the backing services are:
- **ClickHouse:** the shared, app-owned instance that PR #102 adds as chart `infra/charts/clickhouse` in `data`, replacing StarRocks. The contract is fixed with #102:
  - Service `clickhouse.data.svc.cluster.local`, HTTP 8123 and native 9000;
  - `clickhouse/clickhouse-server:26.10.2.103`, single shard and replica;
  - user `langfuse` owning database `langfuse`.

  Langfuse runs its own migrations there with `CLICKHOUSE_CLUSTER_ENABLED=false`.
- **Postgres:** a CNPG `Cluster` `langfuse-postgres` in `llm`, connected with `sslmode=require`.
- **Redis:** an opstree `Redis` `langfuse-redis` in `llm`, with `noeviction`.
- **Object storage:** event uploads go under `artifacts/langfuse/` in the environment bucket through the Kubeflow artifacts identity, the same Secret as the model weights. Batch export and media upload are off.

Secrets:
- Langfuse's own secrets live in the app's Doppler config through `gateway-doppler`: salt, encryption key, NextAuth secret, Postgres and Redis passwords, and the headless-init project keys and admin password.
- The ClickHouse `langfuse` password stays the platform key `CLICKHOUSE_LANGFUSE_PASSWORD`, read through the platform `doppler` store. Moving the ClickHouse keys to the app's Doppler is a later ownership decision.

Headless init creates organisation `payment-gateway`, project `payment-gateway-fraud-detection` and an admin user. Sign-up is off. The UI uses the kagent pattern: the app owns a `VirtualService` on `ml/kubeflow-gateway` for `langfuse(-dev).phuchoang.sbs`, and the platform adds the TunnelBinding, the oauth2-proxy hosts and the Dex redirect URIs. The shared proxy is the access control.

### 15. RAG: knowledge pipeline, Web API and MCP tool (after #102 merges)

The RAG part turns a small corpus about how the gateway behaves into embedded chunks stored through Feast. It serves them through a Web API and an MCP tool. It never reads Silver, Gold or any payment data. The corpus, with source, sync rule and size for each document, is in [rag-corpus.md](rag-corpus.md): about 14 documents and 140–160 chunks.
- Eight documents are rendered from specs, contracts and docs, so they cannot drift from the code.
- Six are short, hand-written and labelled synthetic.
- Simulator configs are excluded, because they hold the generator's ground truth.

**Layout.**
- Code: a uv workspace in `src/rag/` with one lock file and three members:
  - `common` (`fraud_rag_common`): the Feast definitions and the embedding client. Both sides must agree on them, so they live once;
  - `pipeline` (`fraud_rag_pipeline`): the corpus, the workflow steps and the OpenMetadata client, built into image `rag-pipeline`;
  - `serving` (`fraud_rag_serving`): the Web API and the MCP server in one process, built into image `rag-serving`.

  Separate images keep the serving image small (no PyIceberg, no corpus) and let each change release on its own. Python because Feast and PyIceberg are Python. There is no Spark, since a few hundred rows do not need a cluster.
- Chart: `infra/charts/rag-pipeline`, application `rag-pipeline`, enabled where `llm-platform` is: dev now, prod only with the LLM platform.
- Namespaces: the store and the knowledge API run in `llm`, next to the embedding model and the gateway. The ingest workflow runs in `data`, because that is where Argo Workflows, Polaris and OpenMetadata already admit pipelines. Running it in `llm` instead would need two platform changes (Argo Workflows watching `llm`; OpenMetadata admitting `llm`) and a `lakehouse` network change; that is recorded as a follow-up.
- The chart owns everything it needs: its own ExternalSecrets in both namespaces, and a ClickHouse connector filtered to the `knowledge` tables. Its knowledge-base product and contract are a new folder, `contracts/data-products/knowledge-base/`, linted by the same CI as the other products and read by the shared checks image. It changes no data-pipeline code or chart, and no existing contract.

**Corpus inside the image.**
- The hand-written documents (with front matter `doc_id`, title, audience, `synthetic`, source of truth) and `sources.yaml`, which lists the rendered documents, live in `src/rag/pipeline/fraud_rag_pipeline/corpus/`.
- The image build copies the listed repository sources in. The image's build paths in `.github/service-images.json` cover every source, so a changed spec, contract or document produces a new `rag-pipeline` image through the normal release.
- CI tests keep the hand-written codes, thresholds and policy version equal to the code. CI never touches the bucket.
- The workflow's first step renders the corpus from the image and writes it to `datasets/payment-gateway/knowledge/<knowledge_version>/` with a `manifest.json` of per-document SHA-256 hashes. A prefix is immutable once its manifest exists.
- `knowledge_version` is a hash of the documents, the chunker settings and the embedding model with its revision. Changing the model or the chunker re-embeds everything.

**Workflow and trigger.**
- Argo `WorkflowTemplate` `rag-knowledge` in `data`. Steps pass object paths, never data:
  1. `publish-corpus` writes the version prefix if absent. It ends the run early when the offline table and the online store already hold the version.
  2. `chunk` → `runs/payment-gateway/knowledge/<run>/chunks.parquet`.
  3. `embed` calls the gateway's `/v1/embeddings` in batches with the `ragingest` client key. Documents are embedded without an instruction prefix.
  4. `validate` gates the write. It runs every quality check on the staged, embedded Parquet, so a bad corpus never reaches either store.
  5. `write` appends to the offline table, then refreshes the online store and rebuilds its ANN index.
  6. `verify` checks that the stores hold exactly what was validated.
  7. `checks` re-runs the contract rules on `knowledge.chunks` with the shared `openmetadata_jobs.checks` image, pointed at this chart's contract and a connector that ingests only the `knowledge` tables, so the results are recorded in the catalog.
  8. On exit, `catalog` (in the `rag-pipeline` image, over OpenMetadata's REST API) records the run status, the corpus container, the lineage to `knowledge.chunks` and the AI assets of decision 12.
- Each new `rag-pipeline` image runs it: the chart renders a `Workflow` named after a hash of that image tag, in the last sync wave. A changed corpus or pipeline code yields a new tag and so a new run; a serving-only change yields none.
- It is an ordinary resource, not a sync hook. On dev, a failing `PostSync` hook made Argo CD retry the old revision and hold back every newer release; now a failed run is visible in Argo Workflows and in the OpenMetadata run status but never blocks a sync.
- Argo CD prunes the superseded run when a new image arrives; its reports stay in object storage and its status in OpenMetadata. The current run has no TTL, because self-heal would recreate a deleted run and execute it again. Runs have a deadline, manual `argo submit --from workflowtemplate/rag-knowledge` stays available, and there is no cron.
- Access to object storage: the gateway storage identity (`OBJECT_GATEWAY_*` from `gateway-doppler`), which already covers `datasets/payment-gateway/`, `runs/payment-gateway/` and `lakehouse/`.
- Access to Polaris: the same `platform` client as `data-pipelines`. Polaris has STS disabled, so writers bring their own storage credentials.

**Stores.**
- **Offline:** Iceberg table `knowledge.chunks` in the app-owned Polaris catalog, written with PyIceberg. It is append-only by `knowledge_version`. ClickHouse sees it through the DataLakeCatalog, so #102's ODCS SQL checks and catalog discovery apply unchanged.
- **Online:** Feast project `knowledge_base` on its own CNPG Postgres, `rag-pgvector`, in `llm`.
  - Image `ghcr.io/cloudnative-pg/postgresql:18.3-standard-trixie`, which ships pgvector. A CNPG `Database` resource enables the `vector` extension, because Feast does not create it.
  - Online store `postgres` with `vector_enabled: true`; the 1024 dimension comes from the feature field's `vector_length` (Feast 0.67 has no store-level length). The Feast registry is a SQL registry in the same database.
  - Roles: `rag_writer` owns the schema (workflow); `rag_reader` gets SELECT only (Web API). Their passwords are app Doppler keys.
- **Feature view `knowledge_chunks`:** entity `chunk_id`, with fields `doc_id`, `title`, `section`, `audience`, `synthetic`, `source_path`, `text`, `embedding` (1024 × float32, vector index, cosine), `knowledge_version` and `content_sha256`, and no TTL.
- **ANN index, added by the pipeline:** Feast 0.67 is the latest release, and neither it nor its main branch builds an HNSW or IVFFlat index for Postgres. The common advice ("`CREATE INDEX … USING hnsw (embedding vector_cosine_ops)` on the Feast table") does not work as written, for three reasons:
  1. Feast stores one row per entity and feature, in `<project>_<feature_view>` with columns `entity_key`, `feature_name`, `value`, `value_text`, `vector_value`. There is no `embedding` column.
  2. `vector_value` is created as `vector` without dimensions, and pgvector cannot index such a column.
  3. `retrieve_online_documents_v2` ranks with `MIN(vector_value <=> q) … GROUP BY entity_key ORDER BY distance LIMIT k`. That aggregate reads every row, so no index can serve it.

  What works:
  - Right after Feast re-applies the table, the `write` step runs `ALTER TABLE … ALTER COLUMN vector_value TYPE vector(1024)` and builds `USING hnsw (vector_value vector_cosine_ops)`. The other features' rows have a NULL `vector_value`, and HNSW skips NULLs.
  - The service runs the nearest-neighbour query itself on Feast's table, `ORDER BY vector_value <=> q LIMIT k`, which the index serves. It decodes the chunk ids with Feast's entity-key deserializer and reads the features through Feast. On dev, Feast 0.67's v1 `retrieve_online_documents` on Postgres returned only the embedding column, with no ids or distances, and v2 can pair ids with the wrong rows.
  - `verify` asserts with `EXPLAIN` (with sequential scans disabled, since the planner rightly prefers a sequential scan on a table this small) that the search uses the index.
  - The table name and column come from Feast's internals, so the step resolves the name through Feast's own `compute_table_id`, and a CI test pins the behaviour, so an upgrade that changes it fails CI.

  At about 150 chunks the exact scan is already well under a millisecond. The index is for scale, and to show the technique.
- **Qdrant was considered and rejected (2026-10-29).** Feast 0.67's Qdrant store reads by id through a `collection_name` setting its config does not define, so reads crash. It also writes points with random ids, so a rewrite duplicates them. v2 search support is still an open PR (feast-dev/feast#6445). Postgres is the Feast vector store that works for writes, reads by id and search.

**Chunking and identity.**
- Chunking follows the markdown structure: one chunk per spec requirement, clause, section or FAQ entry.
- Sections over about 350 tokens split on paragraphs with about 50 tokens of overlap. Each chunk starts with "document title › section".
- `chunk_id` is `<doc_id>#<section-slug>[-n]`, stable while a heading is unchanged, and `content_sha256` detects edits.
- A new version fully refreshes the online store: the feature view's table is dropped and re-applied, its column typed and indexed, then every current chunk is written. Deleted and renamed chunks therefore disappear. Because the gate already passed, the refresh fails only on infrastructure errors, and a rerun repairs it.
- The offline table keeps every version.

**Validation.** A failed check fails the run.
- **Before the write** (`validate`, over the staged Parquet):
  - the contract rules below, implemented in Python;
  - every manifest document has chunks;
  - a golden question set reaches recall@3 against the expected `doc_id`s. The questions are embedded through the gateway and ranked in memory against the staged vectors, so retrieval quality is known before the store changes.

  A CI test asserts that the Python gate and the ODCS contract cover the same rule ids.
- **Contract rules** (ODCS `contracts/data-products/knowledge-base/knowledge-chunks.odcs.yaml`, scalar counts that must be 0, on the current version; the catalog records them after the write):
  - row count within the expected range;
  - unique `chunk_id`;
  - text non-empty and within bounds;
  - 1024 finite values per embedding, with unit norm;
  - synthetic documents flagged;
  - no card-number or e-mail patterns in the text.
- **After the write** (`verify`):
  - the online store equals the offline version: same hashes by id, no extra ids;
  - the search plan uses the HNSW index.
- Reports go to `runs/payment-gateway/knowledge/<run>/report.json`.

**Web API and MCP tool in one service (`rag-api`).** One FastAPI process serves both the REST API and the MCP tools, so there is one deployment, one reader identity and no extra hop.
- REST: pydantic models, async handlers, and `/healthz` and `/readyz` probes for Kubernetes. Endpoints:
  - `GET /v1/chunks/{chunk_id}`;
  - `POST /v1/chunks/search`: embeds the query through the gateway with Qwen3-Embedding's query instruction and the `ragapi` client key, then runs the indexed nearest-neighbour query and reads the results back by id through Feast;
  - `GET /v1/customers/{customer_id}/features`: online customer features from the fraud Feast project, once the DP3 slice puts customer features online.
- MCP: the official `mcp` SDK's streamable-HTTP app, mounted at `/mcp`, with two tools, `search_knowledge(query, top_k, audience)` and `get_knowledge_chunk(chunk_id)`. They call the same functions as the REST endpoints. It runs in stateless mode, so any replica or revision can answer any request with no session affinity.
- It is the only Feast reader of the knowledge store, through `rag_reader`.
- It runs as a Deployment, not a Knative Service. The platform's Knative cluster-local gateway runs outside the mesh, so traffic routed through Knative would reach the pods with no identity, and the mesh rule that only the gateway may call the service could not hold.
  - Rollouts use `maxUnavailable: 0`, `maxSurge: 1`, readiness gating and a progress deadline. A version that never becomes ready stalls the rollout while the previous pods keep serving, which is the automatic fallback.
  - Two replicas and a PodDisruptionBudget. It is load-tested with Locust.
- It loads the Feast store on first use: the registry exists only after the first pipeline run, and the reader role cannot create it. Readiness checks the database only, so the first Argo CD sync does not wait on the pipeline that its PostSync hook starts.
- The gateway routes `/mcp/knowledge` to the service's `/mcp` (decision 2), with its own policy: the agents' key, and tool-call metrics for the agent telemetry rows. Mesh policy admits only the gateway to the service.
- It is published to agentregistry as an `MCPServer` and catalogued in OpenMetadata as an `mcpServer`. `evidence-agent` gains the tools (kagent `RemoteMCPServer` → gateway), which bumps it to 0.2.0.

### 13. Platform changes (talos-proxmox, separate approval)

| # | Component | Placement | Notes |
|---|---|---|---|
| P0 | Restrict prod NodePool `aws-gpu-training` to `g6.xlarge` (L4) only: `instanceTypes` in `apps/components/karpenter-nodes/environments/prod/values.yaml`, plus the two operations pages that list the types | prod | User decision. It also applies to Kubeflow training, which shares the pool. Spot capacity in `us-east-1a` for one instance type is narrower. The platform's own training evidence recorded g6 `InsufficientInstanceCapacity` retries. |
| P1 | `kserve-llmisvc-resources` v0.21.0 (presets only, `createGIECRDs: false`) | `ml`, in the Kubeflow component | Required by decision 1 |
| P2 | agentgateway v1.7.0 (`agentgateway-crds`, `agentgateway`, `inferenceExtension.enabled=true`) | controller in `operators`; GatewayClass `agentgateway` | Gateways stay in app namespaces |
| P3 | kagent 0.10.4 (`kagent-crds`, `kagent`), built-in agents and tools off, Postgres on CNPG instead of the bundled development database, UI on, `controller.substrate.enabled` with `substrateWorkerPool.create` | `kagent` | Watches `ml` |
| P4 | agentregistry 0.4.0, CNPG Postgres, `rbac.watchedNamespaces: [ml]` | `agentregistry` | Catalog (decision 11) |
| P5 | Agent Substrate 0.0.9 (`substrate-crds` and `substrate`, the version kagent 0.10.4 pins), JWT mode, Valkey, snapshots in an S3/MinIO prefix with its own identity (not bundled RustFS) | `ate-system` (privileged Pod Security) | No Kubernetes feature gates needed. Its workers must reach the `ml` gateway in the mesh. |
| P6 | KEDA (latest stable) | `operators` | Phase 2 only (decision 10) |
| P8 | Namespace `llm` (ambient, Pod Security `restricted`), with kagent and agentregistry watching it | `llm` | The application's namespace; the app never creates or deletes it |
| P9 (follow-up, optional) | Add `llm` to the Argo Workflows controller's `workflowNamespaces` | `operators` | Only if the RAG workflow moves from `data` to `llm` (decision 15) |
| P10 (follow-up, optional) | Admit `llm` to OpenMetadata on 8585 | `data` | Same condition as P9 |
| P7 | Enroll the platform vmagent in Istio ambient (`istio.io/dataplane-mode: ambient` on its pods) | `observability` | Lets app mesh policies admit the metrics agent by identity, rather than opening inference ports to sources with no identity |

The clusters stay on Talos 1.13.9 / Kubernetes 1.36.3. No change is needed to the GPU operator, the DRA driver, Istio or Knative.

## Risks / Trade-offs

- [vLLM FP8 on sm_120 may hit a kernel gap in v0.32.0] → AWQ is the fallback and is recorded on first bring-up. The image is pinned, never `latest`.
- [`dev-server1` is the control plane and is 82% CPU-requested] → model-server requests are kept at 1 CPU / 8 GiB. If it still does not fit, other dev requests are reviewed with the user instead of overcommitting.
- [Prod GPU pool is Spot, shared with training and limited to one node] → serving is off in prod by default and has to be enabled deliberately. Streaming from in-region object storage bounds cold start after interruption, and agents get errors (not hangs) while the pool has no Ready endpoint.
- [Agent Substrate 0.0.9 is pre-1.0] → it is isolated to `SandboxAgent`. The normal `Agent`, model servers, gateway, benchmark and catalog do not depend on it, so the warm-up row can slip without blocking the rest.
- [The prod pool is L4-only Spot in one zone] → launches may wait on capacity. Serving is off in prod by default, and streaming from S3 shortens each launch.
- [Chat and embedding share one 16 GB GPU] → fixed memory fractions per process, with AWQ chat as the in-family fallback. If both still do not fit, embedding moves to CPU (0.6B is small) and the decision is recorded.
- [The OpenMetadata part depends on PR #102] → it lands after #102 merges and is rebased onto its layout. The rest of the change does not wait for it.
- [agentgateway policies overwrite per target] → the chart renders policies from the route list, one per route, so a second policy for the same route cannot be declared in values.
- [RAG reuses #102's Polaris, ClickHouse and checks image while staying out of the data-pipeline code] → the workflow runs in `data` with chart-owned secrets, connector and contract, and publishes lineage itself; no platform, lakehouse or data-pipelines change is needed.
- [The customer-features endpoint needs customer features online] → it lands with or after the DP3 slice; the chunk endpoints do not wait for it.
- [Hand-written synthetic documents could drift from the code or be mistaken for real policy] → each is labelled synthetic in its text, and CI tests keep the codes and thresholds equal to the code.
- [Benchmark variants need the GPU] → the `LLMInferenceService` is stopped for the duration of a run, and dev agents are unavailable then.

## Migration Plan

1. The user approves the talos-proxmox set. P1–P5 are implemented and synced in dev; P0 changes only the prod pool definition.
2. This change's chart goes to dev through `deploy/dev` once the PR is open and CI is green, after telling the user.
3. Dev bring-up: model upload, both `LLMInferenceService`s Ready on the shared GPU, gateway policy checks, agent chat, registry publication, warm-up with few workers, benchmark runs, OpenMetadata assets (after #102). Evidence and docs follow from those runs.
4. Prod platform components and prod enablement (chart, then GPU pool launch) happen only on explicit user confirmation.

Rollback: disable the `llm-platform` application. Nothing in the payment or data paths depends on it.

## Open Questions

1. KServe `LLMInferenceService` (recommended) or llm-d's standalone quickstart charts?
2. Does "custom model" mean a self-selected, quantized and named model (as planned), or should a LoRA adapter be fine-tuned?
3. Should the platform's kagent default provider be disabled (preferred) or pointed at this repo's gateway?
4. Is `evidence-agent` 0.1.0 without tools the right published agent for phase 1?

Resolved with the user on 2026-10-29:
- Qwen3-8B with the embedding model on the same GPU.
- Clusters stay on Kubernetes 1.36 with kagent 0.10.4.
- Prod pool restricted to `g6.xlarge`.
- KEDA allowed where Knative does not fit.
- The Doppler keys were provisioned by me: `LLM_GATEWAY_AGENTS_API_KEY` and `LLM_GATEWAY_BENCHMARK_API_KEY`, random 64-hex values, distinct per config, in `dev` and `prd`.
- Benchmark Jobs on dev may be started with `kubectl`.
