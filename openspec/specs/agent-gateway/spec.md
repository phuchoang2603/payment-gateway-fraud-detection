## Purpose

Give every model, tool and agent call one authenticated, rate-limited and screened entry point.

## Requirements

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

#### Scenario: Opaque identifier in a prompt
- **WHEN** a prompt contains a payment-method fingerprint or a UUID, which can hold runs of digits
- **THEN** the model receives it unchanged, and a phone number written with separators or a leading plus is still masked

### Requirement: One policy per route
Each gateway route SHALL be governed by exactly one agentgateway policy object holding its authentication, limits and guards.

#### Scenario: Render the gateway policies
- **WHEN** the chart is rendered
- **THEN** every route has exactly one policy targeting it

### Requirement: No path around the gateway
Mesh authorization SHALL admit connections to the model servers only from the gateway, the endpoint picker, the benchmark client and the metrics agent's mesh identity. Connections to the gateway SHALL be admitted only from:
- labelled LLM-client service accounts in `llm`;
- the listed external clients;
- Substrate's egress gateway, which carries the agent actors' requests.

Connections to the payments service's MCP port SHALL be admitted only from the gateway. Identity-less sources SHALL reach only the gateway's and endpoint picker's dedicated metrics ports.

Knowledge and risk REST routes SHALL use explicit client allowlists that exclude merchant clients and the shared agent model credential, so the model origin cannot bypass audience-scoped tool access.

#### Scenario: Direct call to vLLM
- **WHEN** a pod other than the gateway or endpoint picker connects to the vLLM port
- **THEN** the connection is refused by the mesh

#### Scenario: Direct call to the payments tools
- **WHEN** a pod other than the gateway connects to the payments MCP port
- **THEN** the connection is refused by the mesh

### Requirement: Knowledge MCP route
The gateway SHALL route `/mcp/knowledge` and `/mcp/knowledge-merchant` to the `/mcp` endpoint of the `rag-api` service through MCP backends. Each route SHALL have its own policy:
- `/mcp/knowledge` SHALL exist only on the analyst listener.
- `/mcp/knowledge-merchant` SHALL exist only on the merchant listener, and SHALL overwrite the audience header with `merchant`.
- Tool calls SHALL require a verified native caller JWT matching a configured profile. Initialization and listing MAY omit the credential on these mesh-restricted listeners.
- Tool-call metrics SHALL be exported for each tool, with success and failure counts.

Mesh authorization SHALL admit only the gateway to `rag-api`.

#### Scenario: Agent lists tools
- **WHEN** `analyst-assistant` lists tools on its permitted listener on `/mcp/knowledge`
- **THEN** it receives `search_knowledge` and `get_knowledge_chunk`, and a tool call without a valid caller JWT is refused

#### Scenario: Merchant actor selects the analyst origin
- **WHEN** `merchant-assistant` attempts to reach the analyst listener
- **THEN** Substrate refuses the ungranted origin; the merchant listener has no analyst knowledge or risk route

#### Scenario: Direct call
- **WHEN** a pod other than the gateway calls `rag-api` directly
- **THEN** the mesh refuses the connection

### Requirement: A2A routes for entry agents
The gateway SHALL be the entry point for every A2A call to an agent, including calls between agents. Routes:
- **External entry routes.** `/a2a/analyst-assistant` SHALL admit only analyst client keys. `/a2a/merchant-assistant` SHALL admit only merchant client keys.
- **Internal route.** `/a2a/internal/decline-reviewer` SHALL be exposed only on the merchant assistant listener and SHALL require the verified merchant JWT for invocation.
- **Model routes.** Agents that receive human input SHALL use the model route with the request screen. `decline-reviewer` SHALL use a model route on its own listener with the same key, limits and PII masking and without the screen; the gateway SHALL NOT rewrite or exempt a message by its content.

Every route SHALL be rewritten to kagent's A2A endpoint for its agent on the kagent controller through an A2A backend, with a per-client rate limit. External entry routes SHALL require a strict client API key; internal review SHALL use native bearer passthrough on the merchant origin. The backend SHALL sign a short-lived token for each call with the configured client principal on external entry calls; internal calls SHALL preserve the validated delegated user credential, with the configured gateway signing key. The kagent controller SHALL admit A2A calls only from the gateway. Sub-agents compiled into an entry agent have no route of their own.

#### Scenario: Agent card through the gateway
- **WHEN** a client with a key fetches `/a2a/<agent>/.well-known/agent-card.json`
- **THEN** the card's URLs name the address the client used, not the kagent controller

#### Scenario: Call without a key
- **WHEN** a client sends an A2A message to `/a2a/<agent>/` without a key
- **THEN** the gateway answers 401 and the agent receives nothing

#### Scenario: Agent called
- **WHEN** an analyst client with a key sends a message to `/a2a/analyst-assistant`
- **THEN** `analyst-assistant` answers and the call appears on its A2A route in the gateway's request metrics

#### Scenario: Merchant key on the analyst route
- **WHEN** a merchant client key calls `/a2a/analyst-assistant`
- **THEN** the gateway answers 403 and the agent receives nothing

#### Scenario: Agent-to-agent call
- **WHEN** `merchant-assistant` calls `decline-reviewer`'s `review_decline` skill
- **THEN** the call passes `/a2a/internal/decline-reviewer` and appears in the gateway's request metrics for that route

### Requirement: Request screening with a self-hosted decision model
The LLM route SHALL screen each request's latest user message with a webhook guard answered by a self-hosted decision model through the gateway, after PII masking. A message that asks an agent to act (approve, block, override or change a risk decision, retry or refund a payment, change a rule, promote or deploy a model) or tries to make it ignore its rules SHALL be rejected with 403 and a read-only refusal when the decision model's probability reaches the configured threshold. The guard SHALL fail open, because agents hold no tools that change anything and screening is an early refusal rather than the only control. Prompts SHALL NOT leave the cluster for screening.

#### Scenario: Request to act
- **WHEN** an analyst asks an agent to refund a payment
- **THEN** the gateway answers 403 with the refusal and the model server receives nothing

#### Scenario: Question
- **WHEN** an analyst asks why a payment was declined
- **THEN** the request passes the screen and reaches the model

#### Scenario: Decision model down
- **WHEN** the decision model is unavailable
- **THEN** requests pass without screening and PII masking still applies

### Requirement: Risk MCP route
The gateway SHALL route `/mcp/risk` to the `/mcp` endpoint of the `risk` server through an MCP backend, with its own policy:
- strict API keys, read from the `x-api-key` header, so that an agent's MCP key and model key stay separate credentials at egress;
- the same per-client rate limit as the other MCP routes;
- tool-call metrics exported for each tool, with success and failure counts.

Mesh authorization SHALL admit the gateway to the `risk` server, with narrowly scoped exceptions for metrics scraping and the dedicated rollout-analysis identity's bounded preview checks. Agent and analyst tool calls SHALL still pass through gateway authentication and rate limiting.

#### Scenario: Agent lists tools
- **WHEN** a client with the agents' key lists tools on `/mcp/risk`
- **THEN** it receives `get_risk_features` and `score_payment_risk`, and a request without a key gets 401

#### Scenario: Direct call
- **WHEN** a pod without an explicitly admitted gateway, metrics or rollout-analysis identity calls the `risk` server directly
- **THEN** the mesh refuses the connection

### Requirement: Native authenticated caller context
Each configured external client SHALL have a key-restricted entry route rule. Its backend SHALL sign a short-lived JWT whose client identity and optional merchant `integration_id` are fixed by that rule. The subject SHALL be the gateway's own trusted subject, and a `user_id` claim naming the client (and, for a merchant, its integration) SHALL own the kagent session. Supplied caller headers SHALL NOT establish identity.

The internal A2A route SHALL be mounted only on the merchant assistant listener, validate the merchant JWT and pass that bearer credential to kagent unchanged. Substrate SHALL enforce exact per-agent egress origins; only its egress gateway and kagent controller SHALL reach agent listeners by mesh identity. It SHALL NOT replace the merchant's `client`, `integration_id` and `user_id` claims. Card discovery MAY use a gateway service credential on a separate GET rule without invoking the agent.

#### Scenario: Forged identity
- **WHEN** a merchant client sends its own caller or integration headers, or an arbitrary bearer token, to its external entry route
- **THEN** kagent receives only the identity and integration signed for that authenticated client

#### Scenario: Session ownership
- **WHEN** a configured client attempts to resume another client's conversation
- **THEN** native kagent ownership checks refuse the request

### Requirement: Payments MCP route
The gateway SHALL route `/mcp/payments` to the payments service's MCP port through an MCP backend, with its own policy:
- separate merchant and analyst listener routes using native caller JWTs;
- `get_payment_status` allowed to `merchant-assistant` and `analyst-assistant`;
- `get_attempt_risk_inputs` allowed only to `analyst-assistant` and `decline-reviewer`;
- a per-client rate limit and per-tool call metrics.

A tool not permitted on a listener SHALL be absent from its tool list and refused when called. MCP and internal review routes SHALL NOT be mounted on the model or public listeners.

#### Scenario: Merchant listener lists payment tools
- **WHEN** `merchant-assistant` lists tools on `/mcp/payments`
- **THEN** it receives `get_payment_status` only

#### Scenario: Merchant listener refuses risk inputs
- **WHEN** `merchant-assistant` calls `get_attempt_risk_inputs`
- **THEN** the gateway refuses the call and the payments service receives nothing
