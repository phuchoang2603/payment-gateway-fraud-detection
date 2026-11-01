# LLM inference

Self-hosted chat and embedding models for the agents and the RAG pipeline. Every caller reaches them through the [agent gateway](gateway.md); nothing calls a model server directly.

- Chart: [`infra/charts/llm-platform`](../../infra/charts/llm-platform) (`models.yaml`, values `models`). Application `llm-platform` in namespace `llm`, enabled on dev and disabled on prod until the prod GPU pool is launched.
- Platform prerequisites (KServe 0.21 `LLMInferenceService` presets, the GPU DRA driver, agentgateway) come from the platform repository.

## Models

| Model | Served name | Weights (pinned revision) | Runtime | Role |
|---|---|---|---|---|
| [Qwen3.5-4B FP8 (RedHatAI, dynamic)](https://huggingface.co/RedHatAI/Qwen3.5-4B-FP8-dynamic) | `qwen35-4b` | `397b7ba47a99b3221ebfc0cfc5a279118cb733ad` | vLLM v0.32, FP8, text only, max length 32 768, 32 concurrent sequences, `qwen3_coder` tool-call parser, thinking off; sampling 0.7 / top-p 0.8 / presence 1.5 | Agents' chat model with tool calling |
| [d3-edge](https://huggingface.co/vllm-sr/d3-edge) | `d3-edge` | `c454ee39b862248d70abd7f84195d2f54ffe6d43` | vLLM nightly `2b009fc6` (`/v1/systemone`), text only, max length 2 048, 16 concurrent sequences | Gateway request screening |
| [Qwen3-Embedding-0.6B](https://huggingface.co/Qwen/Qwen3-Embedding-0.6B) | `qwen3-embedding` | `97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3` | vLLM pooling runner, 1024 dimensions, max length 2 048 | Knowledge-base and query embeddings |

Both are KServe `LLMInferenceService`s. The chat model runs behind llm-d's endpoint picker (EPP) and an InferencePool, so a second replica would be added to the same pool rather than to a new route.

## One GPU, two models

Each environment has one GPU, so the three models share it through a single DRA `ResourceClaim` (`llm-gpu`), each vLLM process with a fixed memory fraction: 0.76 for chat (8192-token context), 0.12 for embeddings and 0.10 for the `d3-edge` decision model that screens gateway requests ([gateway](gateway.md#security-and-policy)). The decision model runs on a vLLM nightly pinned by digest, because release 0.32 does not serve it on `/v1/systemone`. Its weights stream from the bucket like the others; its readout head ships as a second file that vLLM's streamer loader does not read, so the model-upload Job writes it into the backbone's file as `score.weight`. The chat server keeps its KV cache in FP8, which the [benchmark](benchmark.md) kept for throughput and cache room. The embedding server's context is capped at 2 048 tokens so its KV cache fits the small share; knowledge chunks are at most about 400 tokens.

On dev both processes run on the RTX 5060 Ti (16 GiB), reported by each pod's `nvidia-smi` under the same GPU UUID:

```text
$ kubectl -n llm get llminferenceservice
NAME              URL                     READY
qwen3-8b          http://10.103.5.98/v1   True
qwen3-embedding                           True

$ kubectl -n llm get resourceclaim
NAME      STATE
llm-gpu   allocated,reserved

qwen3-8b pod:         VLLM::EngineCore, 12862 MiB   GPU-e37bfa28-1289-0d08-eec9-4bd49bca82ea
qwen3-embedding pod:  VLLM::EngineCore,  2068 MiB   GPU-e37bfa28-1289-0d08-eec9-4bd49bca82ea
GPU total:            14945 MiB / 16311 MiB used, NVIDIA GeForce RTX 5060 Ti

$ curl localhost:8000/v1/models   # inside each pod
[{"id": "qwen3-8b", "root": "s3://platform-dev/artifacts/models/Qwen/Qwen3-8B-FP8/220b46e3...", "max_model_len": 16384}]
[{"id": "qwen3-embedding", "root": "s3://platform-dev/artifacts/models/Qwen/Qwen3-Embedding-0.6B/97b0c614...", "max_model_len": 2048}]
```

## Weights and start-up

- Weights live once per environment in the platform bucket under `artifacts/models/<repo>/<revision>/`, uploaded by the chart's upload Job. vLLM streams them straight to the GPU with the Run:ai Model Streamer, so no node writes them to local disk; the dev GPU node is also the control plane.
- The rollout strategy is `maxSurge: 0, maxUnavailable: 1`: a surge pod would ask for a second share of the one GPU and never schedule.
- The start-up probe allows up to 180 failures (30 minutes) for the first weight load and CUDA graph capture; liveness only starts after it.

## Autoscaling

The model servers are not autoscaled: each environment has one GPU, so a second replica cannot start. Scaling out means a second GPU replica behind the same InferencePool. The agent-side and API-side autoscaling choices are in [agents](agents.md#autoscaling).

## Evidence

The full terminal records are kept with the change's evidence: model servers Ready, the shared claim, both processes on the same GPU and the pinned weights each server loaded (recorded 2026-10-30 on dev).
