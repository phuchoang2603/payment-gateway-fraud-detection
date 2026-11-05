# LLM observability

What the platform records about the models, the gateway, the agents and the knowledge service, and where to look.

| Signal | Source | Store | Look at |
|---|---|---|---|
| Metrics | vLLM, the endpoint picker, agentgateway, `rag-api` | VictoriaMetrics (scraped by the platform's vmagent) | Grafana Explore |
| Agent traces | kagent's Go runtime (OpenTelemetry), linked to prompt versions by our plugin | Langfuse | Langfuse traces |
| Prompts, versions and A/B tests | Langfuse, seeded once from Git | Langfuse | Langfuse prompts |
| Agent, tool and screening metrics | agentgateway, `rag-api`, `risk-serving` | VictoriaMetrics | Grafana "LLM agents" dashboard |

The gateway exports no traces; its view is covered by its metrics. There is no OpenTelemetry Collector: each source exports to one store natively.

## Metrics

Scrape configuration: [`infra/charts/llm-platform/templates/scrape.yaml`](../../infra/charts/llm-platform/templates/scrape.yaml) and the `VMPodScrape` in [`infra/charts/rag-pipeline/templates/api.yaml`](../../infra/charts/rag-pipeline/templates/api.yaml). The queries below run in Explore; the agent-facing ones are also on the [dashboard](#grafana-dashboard).

| What | Metric |
|---|---|
| Web API requests and failures | `knowledge_api_requests_total{exported_endpoint, status}` (the scrape's own `endpoint` label shadows the app's, so the path is in `exported_endpoint`) |
| MCP tool calls and failures | `knowledge_mcp_tool_calls_total{tool, outcome}`; at the gateway `agentgateway_requests_total{route="llm/mcp-knowledge"}` and `agentgateway_mcp_requests_total` |
| Requests per route, keys refused, rate-limited | `agentgateway_requests_total{route, status}` |
| Tokens per request (input, output) | `agentgateway_gen_ai_client_token_usage` |
| Round-trip time of a generation | `agentgateway_gen_ai_server_request_duration` |
| Time to first token, inter-token latency, queue, KV cache | `vllm:time_to_first_token_seconds`, `vllm:inter_token_latency_seconds`, `vllm:num_requests_waiting`, `vllm:kv_cache_usage_perc` |
| Prompts caught by PII masking | `agentgateway_guardrail_checks_total{action, phase}` |

Current dev captures are kept with the components they describe: [gateway routing](gateway.md#routes), [Jev screening and PII guard decisions](novel-1-jev-policy.md#checks-on-dev), and [model time to first token](inference.md#checks-on-dev).

The risk server exports `risk_mcp_tool_calls_total{tool,outcome}`, `risk_tool_latency_seconds{tool}` and `risk_view_status_total{view,status}`. Tool failures are counted even when MCP transports the error with HTTP 200.

## Grafana dashboard

The "LLM agents" dashboard ([`files/dashboards/llm-agents.json`](../../infra/charts/llm-platform/files/dashboards/llm-agents.json)) shows A2A calls and failures per agent route, MCP tool calls by outcome, screen route calls, guard checks and screen latency, and the model's tokens, time to first token and round-trip. Its MCP panels count the knowledge and risk servers, and a Risk tool row shows the tool's mean and p95 latency, the feature view statuses, and an Autoscaling row with the KEDA replicas of both MCP servers and the agent worker pool. The ConfigMap carries the `grafana_folder` annotation from `dashboards.folder`, so the dashboard lands in the `LLM` folder like the other components' dashboards. On dev, `score_payment_risk` read 1.04 s mean and 4.63 s p95 over the 3 hours shown, every feature view `available`, and 2 replicas for both MCP servers:

![Grafana, LLM folder: LLM agents with the Risk tool row, latency, feature views by status and MCP server replicas](images/risk-dashboard.png)

## Langfuse

Self-hosted Langfuse v4 (4.72.0, chart 2.2.0) in `llm`, on the shared ClickHouse, behind the platform's Dex proxy ([`langfuse.yaml`](../../infra/charts/llm-platform/templates/langfuse.yaml)). It holds the agents' prompts and their A/B tests, and their traces.

### Traces

kagent's Go runtime exports each conversation's spans over OTLP to Langfuse; Substrate's egress gateway adds the project's basic-auth key. A conversation is one trace: the A2A request, the coordinator's `invoke_agent` span with its model call, `transfer_to_agent`, the sub-agent's `invoke_agent` span with its tool calls and model calls, and the task-store calls. Each model call (`generate_content qwen35-4b`) carries its tokens and is linked to the prompt version it used; the [runtime capture](novel-2-go-adk-runtime.md#evidence-and-limits) shows a risk generation linked to production prompt v3.

Each model call's messages are recorded on its span (`OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=SPAN_ONLY` on the agents), so Langfuse shows each generation's input and output. They stay in the self-hosted Langfuse in the cluster.

### Prompts

Each agent's system prompt is the Langfuse prompt `<agent>-system-prompt`. Its `production` label is what agents use; the release catalog job only creates a prompt that does not exist yet, from [`files/prompts/<agent>.md`](../../infra/charts/llm-platform/files/prompts), so edits made in Langfuse are kept. The [runtime note](novel-2-go-adk-runtime.md) describes prompt caching and includes the dev prompt-version evidence.

### A/B tests

A test is part of the prompt: the `production` version's config names the other label and the share of conversations it gets, for example `{"abTest": {"label": "variant-b", "share": 0.5}}`. The runtime hashes the prompt name and the conversation, so a conversation keeps its side for every model call, and each agent in it is split on its own. The trace is tagged with each tested agent's side (`evidence-agent:variant-b`), and every model call is linked to its version, so Langfuse compares the versions directly. Ending a test is a config edit in Langfuse; adopting the variant is moving the `production` label.

The seeds give every agent a variant that adds one instruction to its prompt. Prompt metrics on dev after 15 conversations on 2026-10-31 (the variant counts also include earlier runs):

| Prompt | Version | Median latency | Median output tokens | Model calls |
|---|---|---|---|---|
| evidence-agent | v3 production | 2.38 s | 86 | 3 |
| evidence-agent | v2 variant-b ("at most five sentences") | 2.09 s | 57 | 35 |
| coordinator | v1 production | 0.83 s | 24 | 5 |
| coordinator | v2 variant-b ("say which specialist") | 1.44 s | 23 | 36 |
| risk-agent | v1 production | 4.56 s | 182 | 20 |
| risk-agent | v2 variant-b ("risk level in one word first") | 0.97 s | 24 | 2 |

The shorter-answer variants produce fewer output tokens; the risk variant has two calls, too few to judge.

RAG governance uses OpenMetadata: the workflow publishes corpus → `knowledge.chunks` lineage, run status and contract-derived quality results.
