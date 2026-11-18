## MODIFIED Requirements

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

## ADDED Requirements

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
