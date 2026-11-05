# Agent gateway

One agentgateway `Gateway` in `llm` is the only entry to the agents (A2A), the models, the MCP tools and the knowledge Web API. Callers present a per-client API key; the gateway applies limits and the content policy (request screen, PII masking) and routes to the backend. The mesh makes sure nothing can reach a backend around it.

- Chart: [`infra/charts/llm-platform`](../../infra/charts/llm-platform): [`gateway.yaml`](../../infra/charts/llm-platform/templates/gateway.yaml) (Gateway, LLM backends and policy), [`mcp.yaml`](../../infra/charts/llm-platform/templates/mcp.yaml) (MCP routes and policies), [`apis.yaml`](../../infra/charts/llm-platform/templates/apis.yaml) (REST API routes), [`a2a.yaml`](../../infra/charts/llm-platform/templates/a2a.yaml) (A2A routes), [`secrets.yaml`](../../infra/charts/llm-platform/templates/secrets.yaml) (client keys), [`mesh.yaml`](../../infra/charts/llm-platform/templates/mesh.yaml) (who may connect).
- agentgateway v1.7, installed by the platform; the app owns the Gateway, routes and policies.

## Routes

| Path | Backend | Used by |
|---|---|---|
| `/a2a/<agent>` | kagent's A2A endpoint for an entry agent (today `coordinator`), `/agents/llm-agents/<agent>` on the kagent controller | analysts' clients, the benchmark |
| `/v1/chat/completions`, `/v1/embeddings` | `qwen35-4b` InferencePool (through the endpoint picker), `qwen3-embedding` | agents, the RAG pipeline, `rag-api`, the benchmark |
| `/v1/systemone` | `d3-edge`, the Jev decision model (agentgateway's `SystemOne` format), on its own route without the prompt guard | clients with a key, and the LLM route's request screen |
| `/mcp/<server>` | MCP server `<server>` (today `knowledge` → `rag-api` `/mcp`) | agents, kagent's tool discovery |
| `/<api>/v1/...` | REST API `<api>` (today `knowledge` → `rag-api` `/v1`) | API clients |

An MCP server or REST API is one entry under `gateway.mcpServers` or `gateway.apiRoutes` in the values, and an A2A route one entry agent under `agents.list`; the chart renders its route, backend and policy. The A2A backend rewrites the agent card's URLs to the gateway's address and signs a short-lived token (ES256, 300 s) for each call to the kagent controller, which admits only the gateway. The OpenAI-compatible `/v1` route uses `backend.ai.routes` so chat completions, embeddings and model listing each get the right request shape.

## Security and policy

| Control | Setting | Why |
|---|---|---|
| API key per client | `apiKeyAuthentication: Strict` on every route; one key per client (`agents`, `benchmark`, `ragingest`, `ragapi`) from Doppler, labelled for the gateway's key store. A2A, model and REST routes take it as a bearer token, MCP routes as `x-api-key` | Every call is attributable to a client, and one client's key can be rotated without touching others |
| Rate limit per client | 120 requests per minute with a burst of 20, keyed on `apiKey.client`, on the A2A, LLM, MCP and REST routes; a token budget of 60 000 tokens per minute on the LLM route | A runaway agent or script exhausts its own budget, not the GPU |
| Request screen and PII masking | Mask request/response PII; reject actions/jailbreaks with 403 when the decision model probability reaches 0.5 | Agents are read-only, and the model never sees full card numbers |
| Mesh authorization | Istio ambient `AuthorizationPolicy`: only the gateway (and the endpoint picker, benchmark and metrics agent where needed) may reach vLLM and `rag-api`; only listed identities may reach the gateway | A pod that skips the gateway is refused at the connection, before any key check |
| Agent egress | Agents run in Substrate actors outside the mesh; their only way out is Substrate's egress gateway, which injects the agents' key and is admitted by its own identity | Credentials stay outside the actor; the egress gateway injects them from Secrets |

Gateway clients admitted by identity: Substrate's egress gateway (agents), `rag-api`, the RAG workflow (`data/rag-pipeline`), the kagent controller (MCP tool discovery) and the benchmark.

## Checks on dev

A2A and MCP, run on 2026-10-31 through a port-forward to the gateway (keys read from the cluster, never printed):

| Check | Result |
|---|---|
| `/a2a/coordinator` agent card without a key, with an unknown key | 401, 401 |
| The card with a client key | `coordinator`, its endpoint rewritten to the gateway's `/a2a/coordinator` |
| A message to `/a2a/coordinator` | Completed; 12 at once all accepted; workers scaled 2 → 3 |
| `/mcp/knowledge` without a key | 401 |
| `/mcp/knowledge` with the agents' key as `x-api-key` | A session, then `tools/list`: `search_knowledge`, `get_knowledge_chunk` |

Model-route and mesh results from 2026-10-30:

| Check | Result |
|---|---|
| Model route without a key / with an unknown key | 401 / 401 |
| Chat / embedding with the agents key | Chat answer / 1024-dimensional embedding |
| 160 concurrent requests with one client key | 140 × 200, 20 × 429 |
| Unauthorised pod calling vLLM or `rag-api` directly | Connection reset by the mesh |
| Knowledge search through MCP | Returned ranked chunks |

The same counts appear in `agentgateway_requests_total{route, status}` on the Grafana "LLM agents" dashboard.
