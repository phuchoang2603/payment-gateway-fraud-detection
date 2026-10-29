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

- The RAG pipeline (only the embedding endpoint it will call is built here), MCP servers, the scoring tool, the risk and coordinator agents, MCP/A2A routes, agent autoscaling, prompt management, LLM/agent dashboards and A/B tests. These come after the feature-store change.
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

agentgateway implements InferencePool backends natively (`inferenceExtension.enabled=true`) and is documented with KServe `LLMInferenceService`. So one Gateway, `agentgateway` in `ml` with an HTTP listener on 80 and a ClusterIP Service, serves as both the inference gateway and the agent gateway. A request takes one hop: client → agentgateway (policy) → EPP pick → vLLM pod.

Istio's Gateway API implementation stays for the payment-gateway ingress. Its inference extension is not enabled, so there is no second InferencePool-capable gateway.

agentgateway coexists with ambient: the proxy pod lives in the ambient `ml` namespace, so ztunnel secures client→gateway and gateway→EPP/vLLM traffic with mTLS. The gateway is not a waypoint, and no waypoint is deployed for `ml`.

Route layout, fixed now so later routes only add rules:

| Path | Route | Phase |
|---|---|---|
| `/v1/embeddings` | `llm` rule → Service of `LLMInferenceService` `qwen3-embedding` | 1 |
| `/v1/*` (chat completions, completions, models) | `llm` rule → InferencePool `qwen3-8b-inference-pool` | 1 |
| `/mcp/<server>` | one HTTPRoute + AgentgatewayBackend per MCP server | phase 2 |
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
  - the gateway admits `ml` ServiceAccounts labelled as LLM clients (benchmark, later MCP servers) and the Substrate worker identity in `ate-system`, where agent actors run.

  vLLM serves inference and `/metrics` on the same port 8000, so the platform enrolls `vmagent` in the ambient mesh (platform change P7). The model servers then admit its identity, `cluster.local/ns/observability/sa/vmagent-vmks`, instead of opening the port. The gateway's 15020 and the EPP's 9090 are dedicated metrics ports, so they stay open to any source. Nothing reaches the model servers around the gateway.
- **Credentials:** no model-provider key exists. vLLM listens without an API key inside the mesh, and the gateway key is the only credential a client holds.

### 4. Models: Qwen3-8B FP8 and Qwen3-Embedding-0.6B on one GPU; prod on L4 only

The chat model is `Qwen/Qwen3-8B-FP8` (revision `220b46e3`, 8.79 GiB of safetensors). The embedding model is `Qwen/Qwen3-Embedding-0.6B` (revision `97b0c614`, 1.11 GiB), served by vLLM's pooling runner as `qwen3-embedding`, which the RAG pipeline and its search tool will call. Both share the one GPU (decision 6).

Memory budget on the dev card (15.93 GiB):

| Process | `--gpu-memory-utilization` | Budget | Weights | Left for KV cache |
|---|---:|---:|---:|---|
| Chat | 0.80 | 12.7 GiB | 8.8 GiB | about 2.7 GiB after about 1.2 GiB of activations and CUDA graphs: about 39k tokens at FP8 KV (72 KiB/token for 36 layers × 8 KV heads × 128), about 19k at bf16 |
| Embedding | 0.12 | 1.9 GiB | 1.1 GiB | `--max-model-len 8192`, small batches |

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

Pin `vllm/vllm-openai:v0.32.0` (2026-10-18; vLLM requires CC ≥ 7.5 and supports sm_120) in the workload template, overriding the preset's `ghcr.io/llm-d/llm-d-cuda:v0.9.0`. The preset's start script works with any image that has the `vllm` CLI. The `ml` namespace enforces Pod Security `restricted`, so the container runs as UID 1000, with `HOME`, the Hugging Face cache and `/tmp` on emptyDirs and `/dev/shm` as a memory emptyDir, all already in the preset. `USER` is set so libraries that look up the user name work under a UID without a passwd entry. The first dev bring-up verifies FP8 on sm_120 and records the outcome.

Serving flags (checked against the vLLM v0.32.0 source):

| Flag | Setting | Status |
|---|---|---|
| `--max-model-len` | 16384 (chat), 8192 (embedding) | fixed |
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
| torch.compile / CUDA-graph cache | `VLLM_CACHE_ROOT` on a small writable PVC instead of an emptyDir, so restarts skip recompilation | benchmarked as cold start |

KServe's start script `eval`s the container args, so JSON-valued args are written single-quoted inside the value.

fastsafetensors and InstantTensor also ship in the image, but they read local files, so they don't apply once weights stream from object storage. GPUDirect Storage is not used either:
- Talos ships no `nvidia-fs` extension (only gdrcopy, which is GPUDirect RDMA), and the GPU operator's GDS component needs an operator-managed driver, which Talos does not use;
- NVIDIA supports GDS only on data-center and workstation GPUs, which excludes the dev GeForce card. The dev card is also passed through to a VM on a virtio disk;
- weights stream from object storage, which GDS does not read. GDS would only help from the `g6.xlarge`'s ephemeral local NVMe, which would mean downloading again on every Spot replacement.

### 6. GPU through DRA, placement per environment

The chart owns one ResourceClaim `llm-gpu` in `ml` (a claim, not a template): one device of class `gpu.nvidia.com`, `ExactCount` 1. Both model pods reference it by `resourceClaimName` in `template.resourceClaims`, together with the container's `resources.claims`. A DRA claim shared by several pods gives them the same allocated GPU on the same node, which is how chat and embedding share the card. The memory split comes from each vLLM's utilization fraction (decision 4), not from the driver. KServe's experimental managed-DRA annotations are not used. The platform's `aws-gpu-training` template is not reused, because its name and lifecycle belong to training.

- Dev: no selector is needed, because the only GPU is on `dev-server1`. Chat requests 1 CPU and 8 GiB memory (limit 12 GiB); embedding requests 250m CPU and 2 GiB. Together they fit the node's free capacity.
- Prod: both pods get node selector `karpenter.sh/nodepool: aws-gpu-training` and the pool's toleration; the pool provides only `g6.xlarge`. Chat requests 2 CPU and 9 GiB memory, and embedding 500m and 2 GiB, which fit a 4 vCPU / 16 GiB node after Talos reservations and DaemonSets. Karpenter launching a node for a pod that references a not-yet-allocated shared claim is verified at prod enablement. If it does not work, embedding moves to the chat pod's node by pod affinity, and the claim stays shared.

The pool is shared with Kubeflow training and limited to one node, so serving and training cannot run at the same time in prod. Serving must therefore be off whenever training runs (decision 10).

### 7. Model weights in object storage, streamed to the GPU

Weights live once per environment in the platform bucket under `artifacts/models/<repo>/<revision>/`:
- dev: MinIO `platform-dev`;
- prod: S3 `platform-prod-…`.

They stream straight into GPU memory with Run:ai Model Streamer (`--load-format runai_streamer`, already in the vLLM image), so the serving node never writes weights to its disk. That matters because the dev GPU node is also the control plane: writing tens of gigabytes there starves etcd, and the GPU cannot move to a worker because of a physical constraint. On prod, every new Spot node streams in-region from S3, with no volume to reattach.

- **Upload:** an idempotent `model-upload` Job (sync wave -1) copies the pinned Hugging Face revisions file by file. It skips objects already present with the same size and deletes each local copy after upload. Node affinity keeps it off GPU and control-plane nodes.
- **Credentials:** the platform's Kubeflow artifacts identity (Secret `mlpipeline-minio-artifact` in `ml`, scoped to `artifacts/`, `datasets/` and `runs/`), so no new platform policy is needed.
- **Serving:** the chart sets the container command to `vllm serve s3://…` because KServe's preset start script serves a local `/mnt/models`. `storageInitializer` is off, and `model.uri` records the object location. Because the args are no longer re-parsed by the preset's `eval`, JSON values need no extra quoting.
- **Compile cache:** a small RWO PVC keeps torch.compile and CUDA-graph artifacts per model, so restarts skip recompilation.

KServe's `LocalModelCache` and a model PVC were considered and dropped: both write the weights to a node disk.

### 8. Agent runtime: kagent 0.10.4 with Agent Substrate 0.0.9

The clusters stay on Kubernetes 1.36.3 (user decision). kagent 1.x requires 1.37, so the runtime is kagent 0.10.4, the current stable release. It keeps `Agent` → Deployment and adds `SandboxAgent` on Agent Substrate through `controller.substrate.*`. Substrate covers the sandbox and warm-up rubric rows.

kagent 0.10.4 pins Substrate v0.0.9 (`replace` in its `go.mod`) and ships it as an optional subchart (`substrate.enabled`). Substrate 0.0.9 defaults to JWT authentication, which needs no `PodCertificateRequest` or `ClusterTrustBundle` feature gates, so no Talos or control-plane change is required. It brings Valkey and snapshot storage. Snapshots go to an external S3/MinIO prefix with its own identity, not to the bundled RustFS with its default credentials.

The platform installs kagent 0.10.4 with:
- Substrate 0.0.9 and the `kagent-default` gVisor WorkerPool;
- built-in agents and tools off;
- the UI on.

Its Helm default provider is disabled or pointed at the gateway (open question 6).

This repo owns, in `ml`:

- **`ModelConfig` `default-model-config`:** `provider: OpenAI`, `model: qwen3-8b`, `openAI.baseUrl: http://agentgateway.ml.svc.cluster.local/v1`, `apiKeySecret: llm-gateway-agents`, `apiKeySecretKey: key`. This is the global model config. Every agent references it, and changing the model, gateway or key for all agents is one edit.
- **`Agent` `evidence-agent`:** declarative, `modelConfig: default-model-config`, with a read-only fraud-analyst system message that states it has no data access yet and must not invent payment facts, and no tools. `replicas` is left unset: kagent's Deployment mutation sets replicas only when the Agent specifies them, so the phase 2 KEDA ScaledObject can own them. Pod and container security contexts satisfy `restricted`.
- **`SandboxAgent` `evidence-agent-sandbox`:** the same spec, rendered from one helper, on the platform's default WorkerPool `kagent/kagent-default`, with `sandbox.network.allowedDomains` limited to the gateway Service. Snapshots go under the platform's `substrate/` prefix. kagent 0.10.4's CRD accepts only `gs://`-style locations, which Substrate's S3 backend also uses, so the location is `gs://<environment bucket>/substrate/ml/<agent>/`. Each conversation is an actor in a gVisor sandbox. Idle actors suspend to snapshots and resume on any worker, so a one-worker pool serves several conversations. This is the warm-up ("Agent Fleet Architecture") evidence.

Later agents (risk, coordinator) and MCP tool references are additional entries in the same values list. Each renders both an `Agent` and a `SandboxAgent`. The prompt source becomes a prompt-management reference (`systemMessageFrom`) when that rubric row lands. Moving to kagent 1.x later is a platform upgrade plus re-rendering these resources from the same values.

### 9. Benchmark harness and optimizations

The tool is inference-perf v0.7.0, the harness llm-d's benchmark tooling drives, built into a small image from the PyPI release. The chart renders a disabled-by-default Job template. A dev run is started by rendering it with `helm template --set benchmark.run=<name>` and applying the Job with `kubectl` (user decision); it is deleted after its results are uploaded. Results go to the pod log and `runs/payment-gateway/llm-benchmark/<run>/` in the environment bucket through the existing gateway storage identity, and are copied into `docs/llm/benchmark.md` by hand.

The workload is a `shared_prefix` dataset of 3 groups (future agents), each with a 1,500-token system prompt, 100-token questions and 150 output tokens with `ignore_eos`. Closed-loop concurrency stages are 1, 2, 4, 8 and 16. The reported metrics are output tokens/s, requests/s, TTFT p50/p90, inter-token latency (TPOT) p50/p90, end-to-end latency, prefix-cache hit share from vLLM's metrics, and failures.

Two targets:

- **Model-server runs** go straight to the vLLM Service, to isolate the server. One variant changes at a time. Argo CD self-heals the chat `LLMInferenceService`, so it can't be stopped with `kubectl`. A dev values flag (`models.chat.stopped`) sets the KServe stop annotation through a normal release. While it is set, standalone vLLM pods rendered from the same values run each variant, applied with `kubectl`. The embedding server keeps running, so the runs measure the shared GPU.
- **One end-to-end run** goes through agentgateway with the chosen configuration, which shows the gateway and EPP overhead.

Optimizations measured, each against the previous kept configuration:

1. prefix caching off vs on;
2. KV cache bf16 vs FP8 (capacity and concurrency at 8–16);
3. `--max-num-seqs` bound and `--performance-mode interactivity` vs `balanced`;
4. n-gram speculative decoding vs none, on a grounded-answer workload where the answers quote context;
5. cold start, split into weight-load time and time to Ready:
   - streamer `concurrency` and buffer settings;
   - with and without a persisted compile cache;
6. FP8 vs AWQ weights, if FP8 runs on sm_120.

Each result row records the command, image and digest, args, date and environment. Only measured numbers are written down.

### 10. Autoscaling decision (rubric names KEDA)

| Workload | Owner of the replicas | Mechanism | Why |
|---|---|---|---|
| Model server | `LLMInferenceService.replicas` | Fixed: 1 in dev, 0 in prod until confirmed; scale-to-zero by a values toggle | Each cluster has exactly one schedulable GPU (prod pool limit is 1 node), so there is nothing to scale out to. `LLMInferenceService.scaling` supports only KEDA and llm-d's WVA, and Knative cannot manage it. With KEDA in place, `scaling.keda` (scale 0↔1 on the gateway's queued LLM requests) becomes possible for prod cost; it is not built in phase 1, because a Spot GPU cold start of several minutes would time out agent calls. |
| kagent agents (phase 2) | KEDA `ScaledObject` on the Deployment kagent creates; `minReplicaCount` 2 for multi-replica | Prometheus trigger on agentgateway's A2A request rate per agent, from vmsingle's Prometheus API | Knative's KPA and HPA classes scale only Knative Revisions, and kagent creates plain Deployments with no Knative mode. CPU-based HPA misreads agents that mostly wait on the model. KEDA is acceptable to the user, so it is the platform equivalent here. |
| Substrate worker pool (phase 2) | KEDA `ScaledObject` on the `WorkerPool` scale subresource | Active-actor or request-rate trigger | `WorkerPool` exposes a scale subresource for exactly this purpose |
| MCP servers, web API (phase 2) | Knative `Service` | KPA with a concurrency target, `min-scale` ≥ 1 for MCP sessions, revisions for rolling update with automatic fallback to the last ready revision | They are this repo's own stateless HTTP services, which is Knative's model. Knative is already deployed and is the user's preference. |

The docs record this mapping the way the roadmap maps Airflow→Argo: Knative where the workload is a plain HTTP service, KEDA where the replicas belong to another controller.

### 11. Registry

agentregistry 0.4.0, the latest stable release, is installed by the platform with an external Postgres on CNPG and RBAC limited to `ml`. It keeps its ClusterIP Service, reached by port-forward in phase 1. This repo owns the catalog, rendered from the same agent values as the kagent resources so name and version have one source. The registry's agent names cannot contain hyphens, so `evidence-agent` is published as `evidenceagent`, tag `0.1.0`, with its model config named in the description; the registry's `Model` kind supports only Bedrock. A PostSync Job POSTs the catalog to the registry's `/v0/apply` endpoint, which is what `arctl apply` calls, from a pinned curl image. CI never contacts the cluster, so publication happens at sync. A changed catalog means a new version; later phases add MCP server entries and bump the agent to 0.2.0 with its tools.

agentregistry 0.4.0 materializes agents as kagent `v1alpha2` resources, the API kagent 0.10.4 serves. Even so, the agents are deployed by this chart through GitOps, and the registry is the governed catalog (published versions, UI). That keeps one deployment path, so the registry's deploy feature is not used.

### 12. OpenMetadata: register the AI assets through the metadata-integrations chart

OpenMetadata 2.0.6 has first-class AI entities: `llmService`, `llmModel`, `aiApplication`, `mcpServer` and `promptTemplate`. It has no ingestion connector for a custom LLM service, so the assets are created through the SDK by the existing `metadata-integrations` register Job, the same chart and image that register the StarRocks, Kafka, Argo and object-storage services.

The chart gains:

- an `llm` connector kind: `CreateLLMServiceRequest`, `serviceType: CustomLLM`, `baseURL` set to the gateway's `/v1`, with no API key stored in the catalog;
- an `ai` section that the same Job applies with `create_or_update`:
  - `llmModels`: `qwen3-8b` (`modelType: Quantized`, `baseModel: Qwen/Qwen3-8B`, provider `Qwen`, the pinned HF revision as `modelVersion`, capabilities `Chat`/`FunctionCalling`/`ToolUse`, deployment info for vLLM and the GPU) and `qwen3-embedding` (`Embeddings`);
  - `aiApplications`: `evidence-agent` (`applicationType: Agent`, framework kagent, `primaryModel` `qwen3-8b`, `developmentStage: Development`, version 0.1.0, source-code link).

Later phases add `mcpServers` (the evidence and scoring tools, linked to the Feast and KServe assets they read), the risk and coordinator applications with `upstreamApplications`/`downstreamApplications`, and `knowledgeBases` pointing at the RAG tables. These are only new values entries.

The register code lives where open PR #102 moves it (`src/data-pipeline/openmetadata_jobs/openmetadata_jobs/integrations.py`), so this part is implemented on top of #102 after it merges. The `metadata-integrations` application is enabled in dev and disabled in prod, so AI assets are catalogued in dev until prod metadata integration is enabled. Bring-up checks that the pinned `openmetadata-ingestion` 2.0.6 SDK ships the generated `api.ai` requests and has `create_or_update` routes for them; if not, the Job uses the SDK's REST client against `/v1/services/llmServices`, `/v1/llmModels` and `/v1/aiApplications`.

The catalog facts (model names, revisions, agent version) are values in the metadata chart that must match the llm-platform chart, because one Helm chart cannot read another's files. Both live in the same release, and the agent version is bumped in both places in the same commit as the registry catalog.

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
