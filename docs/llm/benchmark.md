# LLM benchmark

How fast the chat model serves agent-shaped traffic on dev, which server settings were tried, which one was kept, and what the gateway adds.

- Manifests: [`infra/charts/llm-platform/benchmark/`](../../infra/charts/llm-platform/benchmark): [`job.yaml`](../../infra/charts/llm-platform/benchmark/job.yaml) (the load generator) and [`server.yaml`](../../infra/charts/llm-platform/benchmark/server.yaml) (a standalone server for one variant), applied with `kubectl`. Argo CD never renders them.
- Tool: `vllm bench serve` from the serving image `vllm/vllm-openai:v0.32.0` (`sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34`), so no benchmark image is built.
- Hardware: the dev RTX 5060 Ti (16 GiB, Blackwell), shared with the embedding server (0.12 of its memory) during every run.
- Model: `Qwen/Qwen3-8B-FP8` at revision `220b46e3`, served as `qwen3-8b`, 16 384-token context.
- Date: 2026-10-31, 18:11 to 23:10 UTC.

## Workload

Agents send long, shared system prompts and short questions, so the workload is vLLM's `prefix_repetition` dataset: 3 shared 1 500-token prefixes (one per agent), 100-token questions and 150-token answers (`--ignore-eos`). Each run is a closed-loop sweep at 1, 2, 4, 8 and 16 concurrent requests, eight requests per concurrent slot:

```sh
vllm bench serve --backend openai-chat --base-url "$TARGET" --endpoint /v1/chat/completions \
  --model qwen3-8b --tokenizer Qwen/Qwen3-8B \
  --dataset-name prefix_repetition --prefix-repetition-num-prefixes 3 \
  --prefix-repetition-prefix-len 1500 --prefix-repetition-suffix-len 100 \
  --prefix-repetition-output-len 150 --ignore-eos --seed 20261029 \
  --num-prompts $((c * 8)) --max-concurrency "$c" --percentile-metrics ttft,tpot,itl,e2el --metric-percentiles 50,90
```

Model-server runs go straight to the vLLM server, to isolate it. The chat `LLMInferenceService` was stopped through a dev release (`models.chat.stopped`) and each variant ran as `server.yaml` with one change to its arguments, each against the best configuration so far. The standalone server gets 0.84 of the GPU instead of the chart's 0.80: on a fresh start vLLM 0.32 counts CUDA graph memory inside the budget, and at 0.80 a bf16 KV cache no longer fits one 16 384-token request.

## Baseline

The live chat server as deployed (bf16 KV cache, prefix caching on), no failed requests:

| Concurrency | Output tok/s | Requests/s | TTFT p50 / p90 (ms) | TPOT p50 (ms) | End-to-end p50 (ms) |
|---|---|---|---|---|---|
| 1 | 44.3 | 0.30 | 225 / 269 | 21.4 | 3 416 |
| 2 | 82.2 | 0.55 | 112 / 221 | 22.1 | 3 399 |
| 4 | 157.5 | 1.05 | 142 / 317 | 22.8 | 3 569 |
| 8 | 302.5 | 2.02 | 208 / 334 | 24.5 | 3 861 |
| 16 | 520.3 | 3.47 | 248 / 624 | 28.4 | 4 507 |

Throughput grows almost linearly to 16 concurrent requests while the time per output token rises from 21 to 28 ms, so one replica serves every agent with room to spare. The standalone default (0.84) matched it: 521.2 output tokens/s at 16.

## Optimizations

| Variant (server arguments added) | Output tok/s at 1 / 16 | TTFT p50 at 16 (ms) | TPOT p50 at 16 (ms) | KV cache (tokens) | Kept |
|---|---|---|---|---|---|
| Standalone default | 44.9 / 521.2 | 276 | 28.3 | 16 608 | reference |
| Prefix caching off (`--no-enable-prefix-caching`) | 43.9 / 230.7 | 5 834 | 36.4 | 16 608 | no |
| FP8 KV cache (`--kv-cache-dtype=fp8`) | 45.7 / 601.1 | 327 | 24.0 | 27 488 | **yes** |
| FP8 KV + `--max-num-seqs=16` | 45.6 / 594.6 | 381 | 23.9 | 34 432 | no |
| FP8 KV + `--performance-mode=interactivity` | 45.7 / 598.6 | 338 | 24.0 | 25 408 | no |
| FP8 KV + n-gram speculative decoding (4 tokens, lookup 2 to 4) | 37.5 / 421.7 | 139 | 36.1 | 26 192 | no |
| FP8 KV + AWQ 4-bit weights (`Qwen/Qwen3-8B-AWQ` `4da05a8e`) | 65.6 / 744.9 | 583 | 16.8 | 73 824 | not adopted |

- **Prefix caching** is what makes shared agent prompts cheap: without it every request recomputes its 1 500-token prefix, throughput at 16 falls by 56% and the first token takes 5.8 s instead of 0.3 s. It stays on (vLLM's default).
- **FP8 KV cache** is kept: 15% more output tokens/s at 16 concurrent requests, a lower time per token at every level (24.0 vs 28.3 ms at 16), and 65% more KV cache room. It also lets the chart's 0.80 share hold the 16 384-token context on a fresh start. The chat server now runs with it ([`values.yaml`](../../infra/charts/llm-platform/values.yaml)); after the restart its cache holds 18 480 tokens.
- **`--max-num-seqs` and `--performance-mode interactivity`** change nothing measurable at these concurrencies (within 1%).
- **N-gram speculative decoding** is 30% slower here: a draft was accepted for only 1.07 tokens on average, because these answers do not quote the prompt. It would need a workload whose answers quote retrieved passages to pay off, so it is not used.
- **AWQ weights** are faster (+24% output tokens/s at 16, 16.8 ms per token, 3.1 GiB less memory, 2.7× the KV cache) but start answers later (TTFT p50 361 ms at 1 and 583 ms at 16, against 165 and 327 ms) and are 4-bit; their answer quality was not measured, so the FP8 model stays served.

## Cold start

Weights stream from the environment bucket to the GPU with Run:ai Model Streamer, no disk on the node:

| | Weight load | Time to Ready |
|---|---|---|
| FP8 weights, streamer concurrency 16 (default) | 120 to 137 s over seven starts (8.85 GiB) | 481 to 929 s |
| FP8 weights, streamer concurrency 32 | 123 s | 502 s |
| AWQ weights, concurrency 16 | 73 s (5.71 GiB) | 290 s |

Most of the time to Ready is `torch.compile` and CUDA graph capture, which vary with the compile cache (graph capture 12 to 330 s). A higher streamer concurrency is within the noise, so it is not kept; raising its CPU buffer to 4 GiB as well failed to start within the server's 12 GiB memory limit.

## Through the gateway

The gateway adds API keys, limits, PII masking and laya screening ([gateway](gateway.md)). The `prefix_repetition` prompts are random tokens, and the screen rejected all of them (403): laya reads them as jailbreak attempts. So the gateway run uses 48 distinct analyst questions with 150-token answers, sent the same way to vLLM and through the gateway with the benchmark client's key:

| Concurrency | TTFT p50 direct / gateway (ms) | Output tok/s direct / gateway | TPOT p50 (ms) | Failed |
|---|---|---|---|---|
| 1 | 54 / 1 051 | 47.8 / 36.3 | 20.7 / 20.7 | 0 / 0 |
| 2 | 73 / 1 004 | 95.0 / 71.2 | 20.7 / 20.7 | 0 / 0 |
| 4 | 91 / 1 522 | 188.3 / 125.2 | 20.7 / 20.8 | 0 / 0 |

Generation itself is unchanged; the gateway adds about one second before the first token, almost all of it the laya screen, then on CPU (a forward pass averaged 3.4 s over 83 screens that evening, cached per prompt), and screens queue at higher concurrency. laya has since moved to the GPU. Agents re-send the same user turn on every model call of a conversation, so only the first call of a turn pays it.

The per-client limits stay as set (120 requests and 60 000 tokens per minute): one client at concurrency 2 of the analyst workload sends about 28 requests and 6 000 tokens a minute, and the server's measured capacity of about 4 requests/s on agent-shaped prompts is shared by all clients.
