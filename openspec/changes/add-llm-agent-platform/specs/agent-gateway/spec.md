## ADDED Requirements

### Requirement: Single agent gateway entry point
One agentgateway `Gateway` in `llm`, reachable by a ClusterIP Service, SHALL be the only entry point for LLM calls, MCP servers and agent-to-agent calls. The LLM route SHALL serve `/v1/embeddings` from the embedding model server and the rest of `/v1` from the chat model's InferencePool. MCP and A2A routes SHALL attach as additional routes under `/mcp/<server>` and `/a2a/<agent>` to the same Gateway, without renaming the Gateway, its Service or the LLM route. The Gateway SHALL NOT be publicly routed.

#### Scenario: Agent reaches the model
- **WHEN** an agent calls `http://agentgateway.llm.svc.cluster.local/v1/chat/completions`
- **THEN** the request is served by the model server through the gateway and endpoint picker

### Requirement: Client API keys
The LLM route SHALL require a valid API key in strict mode. Each client (agents, benchmark, and later clients) SHALL have its own key, synced from Doppler through `gateway-doppler` into a labelled Secret that carries the client identity, and no key SHALL appear in Git or rendered manifests.

#### Scenario: Missing or unknown key
- **WHEN** a request has no key or a key that is not provisioned
- **THEN** the gateway answers 401 and the model server receives nothing

#### Scenario: Key rotation
- **WHEN** a client's key is rotated in Doppler
- **THEN** after the secret refresh the old key is rejected and the new key is accepted without changing the chart

### Requirement: Rate limits
The LLM route SHALL enforce a request-rate limit with a burst allowance and a token-rate limit per minute. Requests over either limit SHALL receive 429 without reaching the model server.

#### Scenario: Burst over the limit
- **WHEN** a client sends more requests at once than the burst allowance
- **THEN** the excess requests get 429 and the admitted ones are answered

### Requirement: PII masking on prompts and responses
The LLM route SHALL mask payment card numbers, e-mail addresses and phone numbers in request messages before they reach the model, and in model responses before they reach the client. Requests SHALL continue with masked values instead of being rejected.

#### Scenario: Card number in a prompt
- **WHEN** a prompt contains a 16-digit card number with or without separators
- **THEN** the model receives a masked placeholder and the answer is still returned

### Requirement: One policy per route
Each gateway route SHALL be governed by exactly one agentgateway policy object holding its authentication, limits and guards.

#### Scenario: Render the gateway policies
- **WHEN** the chart is rendered
- **THEN** every route has exactly one policy targeting it

### Requirement: No path around the gateway
Mesh authorization SHALL admit connections to the model servers only from the gateway, the endpoint picker, the benchmark client and the metrics agent's mesh identity. Connections to the gateway SHALL be admitted only from labelled LLM-client service accounts in `llm`, the listed external clients, and Substrate's egress gateway, which carries the agent actors' requests. Identity-less sources SHALL reach only the gateway's and endpoint picker's dedicated metrics ports.

#### Scenario: Direct call to vLLM
- **WHEN** a pod other than the gateway or endpoint picker connects to the vLLM port
- **THEN** the connection is refused by the mesh

### Requirement: Knowledge MCP route
The gateway SHALL route `/mcp/knowledge` to the `/mcp` endpoint of the `rag-api` service through an MCP backend, with its own policy:
- strict API keys, read from the `x-api-key` header so that an agent's MCP key and model key are separate credentials at egress;
- the agents' client id allowed;
- tool-call metrics exported for each tool, with success and failure counts.

Mesh authorization SHALL admit only the gateway to `rag-api`.

#### Scenario: Agent lists tools
- **WHEN** a client with the agents' key lists tools on `/mcp/knowledge`
- **THEN** it receives `search_knowledge` and `get_knowledge_chunk`, and a request without a key gets 401

#### Scenario: Direct call
- **WHEN** a pod other than the gateway calls `rag-api` directly
- **THEN** the mesh refuses the connection

### Requirement: A2A routes for entry agents
The gateway SHALL be the entry point for A2A calls to agents. Each entry agent SHALL have an A2A route at `/a2a/<agent>`, rewritten to kagent's public A2A endpoint for that agent on the kagent controller through an A2A backend, with the same strict API key and per-client rate limit as the other routes. The backend SHALL sign a short-lived token for each call as the gateway's machine principal, with a platform-provided key, and the kagent controller SHALL admit A2A calls only from the gateway. Sub-agents compiled into an entry agent have no route of their own.

#### Scenario: Agent card through the gateway
- **WHEN** a client with a key fetches `/a2a/<agent>/.well-known/agent-card.json`
- **THEN** the card's URLs name the address the client used, not the kagent controller

#### Scenario: Call without a key
- **WHEN** a client sends an A2A message to `/a2a/<agent>/` without a key
- **THEN** the gateway answers 401 and the agent receives nothing

#### Scenario: Agent called
- **WHEN** a client with a key sends a message to `/a2a/coordinator`
- **THEN** the coordinator answers and the call appears on its A2A route in the gateway's request metrics

### Requirement: Request screening with a self-hosted decision model
The LLM route SHALL screen each request's latest user message with a webhook guard on the app's laya decision service, after PII masking. A message that asks an agent to act (approve, block, override or change a risk decision, retry or refund a payment, change a rule, promote or deploy a model) or tries to make it ignore its rules SHALL be rejected with 403 and a read-only refusal when laya's probability reaches the configured threshold. The guard SHALL fail open, because agents hold no tools that change anything and screening is an early refusal rather than the only control. Prompts SHALL NOT leave the cluster for screening.

#### Scenario: Request to act
- **WHEN** an analyst asks an agent to refund a payment
- **THEN** the gateway answers 403 with the refusal and the model server receives nothing

#### Scenario: Question
- **WHEN** an analyst asks why a payment was declined
- **THEN** the request passes the screen and reaches the model

#### Scenario: Decision service down
- **WHEN** the decision service is unavailable
- **THEN** requests pass without screening and PII masking still applies
