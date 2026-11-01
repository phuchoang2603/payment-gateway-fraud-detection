## ADDED Requirements

### Requirement: Self-hosted chat model on vLLM
The platform SHALL serve Qwen3.5-4B in FP8 (`RedHatAI/Qwen3.5-4B-FP8-dynamic`) at a pinned revision on a pinned vLLM image through a KServe `LLMInferenceService` in `llm`, under the served model name `qwen35-4b`. The model SHALL expose the OpenAI-compatible chat completions, completions and models APIs, with tool calling enabled and reasoning output off by default. The maximum context length SHALL be 32,768 tokens. No model-provider API key SHALL exist.

#### Scenario: Chat completion through the platform
- **WHEN** an authorized client sends a chat completion for model `qwen35-4b`
- **THEN** a vLLM replica of the service answers with a completion and token usage

#### Scenario: Unknown model name
- **WHEN** a client requests a model other than `qwen35-4b`, `qwen3-embedding` or `d3-edge`
- **THEN** the request is rejected and no other model is loaded

### Requirement: Self-hosted embedding model
The platform SHALL serve `Qwen/Qwen3-Embedding-0.6B` at a pinned revision on the same pinned vLLM image through a second `LLMInferenceService` in `llm`, under the served model name `qwen3-embedding`, exposing the OpenAI-compatible embeddings API.

#### Scenario: Embedding request
- **WHEN** an authorized client posts text to `/v1/embeddings` with model `qwen3-embedding`
- **THEN** it receives one fixed-length vector per input

### Requirement: Shared GPU with fixed memory split
The chat, embedding and decision model servers SHALL share one GPU through a single DRA claim. Each SHALL be limited to a fixed fraction of GPU memory whose sum leaves headroom for the CUDA contexts. The chat model SHALL keep KV-cache room for at least two concurrent 32,768-token contexts.

#### Scenario: All model servers loaded
- **WHEN** every model server is Ready on the dev GPU
- **THEN** each serves requests concurrently and neither fails with out-of-memory under the benchmark load

### Requirement: Endpoint picking through an InferencePool
Requests to the model SHALL reach vLLM replicas only through the InferencePool and endpoint picker that the `LLMInferenceService` creates, using the default scheduler profile with queue, KV-cache and prefix-cache scoring. A replica SHALL receive traffic only after vLLM reports healthy.

#### Scenario: Model still loading
- **WHEN** the only replica has not finished loading weights
- **THEN** the pool has no ready endpoint and requests fail fast with an error instead of hanging

### Requirement: Explicit GPU claim and environment placement
The model servers SHALL obtain their GPU through a chart-owned ResourceClaim requesting exactly one device of DeviceClass `gpu.nvidia.com`, not through extended resources. In prod they SHALL schedule only onto the platform's `aws-gpu-training` NodePool, which provides `g6.xlarge` (L4) instances, and tolerate that pool's taint. The application SHALL NOT change the pool definition.

#### Scenario: Prod scheduling
- **WHEN** the prod model server is enabled and no GPU node exists
- **THEN** its pending pod causes the pool to launch one `g6.xlarge` Spot node in `us-east-1a`

#### Scenario: GPU already claimed
- **WHEN** another workload holds the environment's only GPU
- **THEN** the model server pod stays pending without disturbing the claim holder

### Requirement: Model weights in the local model cache
Each model SHALL be named by its pinned Hugging Face revision and kept on the GPU nodes' local disk by a KServe local model cache in the app's namespace, on the platform's GPU node group. Model servers SHALL load the cached files with InstantTensor and SHALL NOT download weights at start once the cache holds them.

#### Scenario: Pod restart
- **WHEN** the model server pod is deleted
- **THEN** its replacement loads the same pinned weights from the node's cache without a new download

### Requirement: One chat replica serves all agents
A single chat model replica SHALL serve every agent's calls concurrently through continuous batching, in dev and prod. Its measured concurrency capacity SHALL set the gateway's rate limits. Adding capacity SHALL be done by adding replicas on additional GPUs behind the same InferencePool, without changing agents or their model config.

#### Scenario: Several agents at once
- **WHEN** several agents send chat requests at the same time within the measured capacity
- **THEN** all are served by the one replica without queueing failures

### Requirement: Serving replicas per environment
Each model server SHALL run one replica in dev and zero in prod until the prod deployment is explicitly enabled. It SHALL NOT be autoscaled while each environment has a single schedulable GPU.

#### Scenario: Prod default
- **WHEN** the prod release is rendered without the explicit enablement
- **THEN** no model-server pod, GPU claim or GPU node request exists

### Requirement: Model-server telemetry
vLLM, endpoint-picker and gateway metrics (requests, prompt and generation tokens, TTFT, inter-token latency, queue length, KV-cache use, prefix-cache hits, preemptions, speculative acceptance) SHALL be scraped by the platform's metrics agent through application-owned scrape configuration, without an application-owned collector, store, dashboard or alert. The model servers SHALL admit the metrics agent by its mesh identity rather than opening their inference port.

#### Scenario: Metrics after a request
- **WHEN** a completion has been served
- **THEN** its vLLM token and latency series and the gateway's series for the route are queryable in the platform metrics store
