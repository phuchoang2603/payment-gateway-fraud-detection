# Agent gateway

One agentgateway `Gateway` in `llm` is the only entry to the models, the MCP tools and the knowledge Web API. Callers present a per-client API key; the gateway applies limits and PII masking and routes to the backend. The mesh makes sure nothing can reach a backend around it.

- Chart: [`infra/charts/llm-platform`](../../infra/charts/llm-platform): [`gateway.yaml`](../../infra/charts/llm-platform/templates/gateway.yaml) (Gateway, LLM backends and policy), [`mcp.yaml`](../../infra/charts/llm-platform/templates/mcp.yaml) (MCP routes and policies), [`apis.yaml`](../../infra/charts/llm-platform/templates/apis.yaml) (REST API routes), [`secrets.yaml`](../../infra/charts/llm-platform/templates/secrets.yaml) (client keys), [`mesh.yaml`](../../infra/charts/llm-platform/templates/mesh.yaml) (who may connect).
- agentgateway v1.7, installed by the platform; the app owns the Gateway, routes and policies.

## Routes

| Path | Backend | Used by |
|---|---|---|
| `/v1/chat/completions`, `/v1/embeddings` | `qwen3-8b` InferencePool (through the endpoint picker), `qwen3-embedding` | agents, the RAG pipeline, `rag-api`, the benchmark |
| `/mcp/<server>` | MCP server `<server>` (today `knowledge` → `rag-api` `/mcp`) | agents (kagent), the catalog job |
| `/<api>/v1/...` | REST API `<api>` (today `knowledge` → `rag-api` `/v1`) | API clients |

An MCP server or REST API is one entry under `gateway.mcpServers` or `gateway.apiRoutes` in the values; the chart renders its route, backend and policy. The OpenAI-compatible `/v1` route uses `backend.ai.routes` so chat completions, embeddings and model listing each get the right request shape.

## Security and policy

| Control | Setting | Why |
|---|---|---|
| API key per client | `apiKeyAuthentication: Strict` on every route; one key per client (`agents`, `benchmark`, `ragingest`, `ragapi`, `catalog`) from Doppler, labelled for the gateway's key store | Every call is attributable to a client, and one client's key can be rotated without touching others |
| Rate limit per client | 120 requests per minute with a burst of 20, keyed on `apiKey.client`, on the LLM, MCP and REST routes; a token budget of 60 000 tokens per minute on the LLM route | A runaway agent or script exhausts its own budget, not the GPU |
| PII masking | Built-in card, e-mail and phone recognisers plus a pattern for separated card numbers, on prompts and responses | Agents can be pasted payment data; the model never sees full card numbers, and answers cannot echo them |
| Mesh authorization | Istio ambient `AuthorizationPolicy`: only the gateway (and the endpoint picker, benchmark and metrics agent where needed) may reach vLLM and `rag-api`; only listed identities may reach the gateway | A pod that skips the gateway is refused at the connection, before any key check |
| Sandbox egress | Sandboxed agents run outside the mesh and reach the gateway only through `sandbox-egress`, which forwards only `/v1/` and `/mcp/` (and Langfuse trace intake) and is admitted by its own identity | See [agents](agents.md#sandbox-and-warm-up) |

Gateway clients admitted by identity: the agents' egress proxy, `rag-api`, the RAG workflow (`data/rag-pipeline`), the catalog job, the kagent controller (MCP tool discovery) and the benchmark.

## Checks on dev

Run on 2026-10-30 through a port-forward to the gateway; keys are read from the cluster and never printed.

```text
# LLM route: no key, then an unknown key
401
401

# Chat and embedding with the agents key
{"model": "qwen3-8b", "answer": "A payment gateway processes and secures the payment information between a customer's bank and a merchant's bank during an online transaction.", ...}
{"model": "qwen3-embedding", "dimensions": 1024, ...}

# PII masking: the prompt asks the model to repeat a card number, an e-mail and a phone number
card <masked>, email <EMAIL_ADDRESS>, phone <PHONE_NUMBER>

# Rate limit: 160 requests at once with one client's key (120 per minute, burst 20)
    140 200
     20 429

# Mesh: a pod without a gateway identity calls vLLM directly
http 000 curl exit 56      # connection reset by the mesh

# MCP gateway: tools/list and a search_knowledge call through /mcp/knowledge with the agents key
[{"name": "search_knowledge", ...}, {"name": "get_knowledge_chunk", ...}]
[{"distance": 0.298, "chunk_id": "decline-codes#new-account-geo-mismatch"}, {"distance": 0.422, "chunk_id": "fraud-features#feature-ip-shipping-country-mismatch"}, ...]
# the same route without a key, then a pod calling rag-api directly
401
http 000 curl exit 56

# Knowledge Web API through /knowledge/v1: no key, then a 160-request burst with one client's key
401
    140 200
     20 429
```

The same counts appear in the gateway's metrics by route and status (see [observability](observability.md)).
