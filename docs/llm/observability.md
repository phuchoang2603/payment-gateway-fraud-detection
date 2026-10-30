# LLM observability

What the platform records about the models, the gateway, the agents and the knowledge service, and where to look.

| Signal | Source | Store | Look at |
|---|---|---|---|
| Metrics | vLLM, the endpoint picker, agentgateway, `rag-api` | VictoriaMetrics (scraped by the platform's vmagent) | Grafana Explore |
| Agent traces | kagent's own OpenTelemetry instrumentation | Langfuse | Langfuse traces |
| Prompt versions | The catalog job, from Git | Langfuse | Langfuse prompts |
| AI assets | The catalog job, from the chart values and `tools/list` | OpenMetadata | OpenMetadata API |

The gateway exports no traces; its view is covered by its metrics. There is no OpenTelemetry Collector: each source exports to one store natively.

## Metrics

Scrape configuration: [`infra/charts/llm-platform/templates/scrape.yaml`](../../infra/charts/llm-platform/templates/scrape.yaml) and the `VMPodScrape` in [`infra/charts/rag-pipeline/templates/api.yaml`](../../infra/charts/rag-pipeline/templates/api.yaml). The Grafana dashboard is a later change; the queries below run in Explore.

| What | Metric |
|---|---|
| Web API requests and failures | `knowledge_api_requests_total{exported_endpoint, status}` (the scrape's own `endpoint` label shadows the app's, so the path is in `exported_endpoint`) |
| MCP tool calls and failures | `knowledge_mcp_tool_calls_total{tool, outcome}`; at the gateway `agentgateway_requests_total{route="llm/mcp-knowledge"}` and `agentgateway_mcp_requests_total` |
| Requests per route, keys refused, rate-limited | `agentgateway_requests_total{route, status}` |
| Tokens per request (input, output) | `agentgateway_gen_ai_client_token_usage` |
| Round-trip time of a generation | `agentgateway_gen_ai_server_request_duration` |
| Time to first token, inter-token latency, queue, KV cache | `vllm:time_to_first_token_seconds`, `vllm:inter_token_latency_seconds`, `vllm:num_requests_waiting`, `vllm:kv_cache_usage_perc` |
| Prompts caught by PII masking | `agentgateway_guardrail_checks_total{action, phase}` |

Web API, MCP and gateway traffic over the evidence runs (the 429s are the rate-limit check, the 501s are `GET /v1/models`, which the LLM route does not serve):

![Grafana Explore: Web API requests by path and status, MCP tool calls by outcome, gateway requests by route and status](images/observability-api-mcp-gateway.png)

Tokens, time to first token, generation round-trip and PII masking:

![Grafana Explore: tokens per second by type, TTFT p95 per model, generation round-trip p95, PII guardrail checks by action](images/observability-tokens-ttft-pii.png)

Values over the six hours of evidence runs on 2026-10-30:

```text
Web API /v1/chunks/{chunk_id}     200: 144   404: 3   422: 2
MCP tool calls                    search_knowledge success: 12
Gateway routes                    llm 200: 245, 401: 6, 429: 20 · mcp-knowledge 200: 726, 202: 661, 401: 4, 500: 2 · api-knowledge 200: 182, 401: 3
Tokens per request (average)      input 301, output 112
Generation round-trip p95         1.21 s
vLLM time to first token p95      qwen3-8b 0.24 s, qwen3-embedding 0.41 s
PII guardrail                     request: allow 14, mask 12 · response: allow 26
```

The two MCP 500s are the cold-start timeout that loading the Feast store at start-up fixed.

## Langfuse

Self-hosted Langfuse v3 (3.225.11) in `llm`, on the shared ClickHouse, behind the platform's Dex proxy ([`langfuse.yaml`](../../infra/charts/llm-platform/templates/langfuse.yaml)).

**Agent traces.** kagent's Go runtime exports each agent's spans over OTLP to Langfuse through the sandbox egress proxy, with a basic-auth header built from the project keys. A question becomes one trace: the agent span, one generation per model call with its tokens, and the tool calls. The platform turns kagent tracing on (`otel.tracing.enabled`); each agent sets its own endpoint. The trace of the UI chat above, 9.43 s:

![Langfuse trace: invoke_agent evidence_agent, generate_content qwen3-8b (809 → 41 tokens), search_knowledge, generate_content qwen3-8b (2,323 → 249 tokens)](images/observability-langfuse-trace.png)

**Prompts.** Git is the source; the release catalog job publishes each agent's prompt as a new `production` version when its text changes, with the agent version as commit message:

![Langfuse prompt evidence-agent-system-prompt: version 1, labels production and latest, "evidence-agent 0.3.0 from Git"](images/observability-langfuse-prompt.png)

## AI assets in OpenMetadata

The release catalog job registers the LLM platform from the `llm-platform` values over OpenMetadata's REST API: the LLM service (the gateway endpoint), both models with their pinned revisions, each MCP server with the tools it lists, and each agent as an AI application linked to its model, its MCP servers and the knowledge table. OpenMetadata 2.0.7's UI has no pages for these entity types yet, so they are read through the API:

```text
GET /api/v1/services/llmServices/name/agent-gateway       serviceType CustomLLM, "Self-hosted models behind the agent gateway at http://agentgateway.llm.svc.cluster.local/v1; clients use API keys."
GET /api/v1/llmModels/name/agent-gateway.qwen3-8b         baseModel Qwen/Qwen3-8B-FP8, modelVersion 220b46e3b2180893580a4454f21f22d3ebb187d3, provider Qwen
GET /api/v1/llmModels/name/agent-gateway.qwen3-embedding  baseModel Qwen/Qwen3-Embedding-0.6B, modelVersion 97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3, provider Qwen
GET /api/v1/mcpServers/name/agent-gateway-mcp.knowledge   DataAccess, StreamableHTTP, tools search_knowledge and get_knowledge_chunk (descriptions from the server)
GET /api/v1/aiApplications/name/evidence-agent            Agent, primary model agent-gateway.qwen3-8b, tools [mcpServer knowledge], knowledgeBases [table knowledge.chunks]
```

The RAG run's lineage and quality checks are on the [RAG](rag.md#governance) page.
