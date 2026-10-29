## ADDED Requirements

### Requirement: Single agent gateway entry point
One agentgateway `Gateway` in `ml`, reachable by a ClusterIP Service, SHALL be the only entry point for LLM calls, and later for MCP servers and agent-to-agent calls. The LLM route SHALL serve `/v1/embeddings` from the embedding model server and the rest of `/v1` from the chat model's InferencePool. Later MCP and A2A routes SHALL attach as additional routes under `/mcp/<server>` and `/a2a/<agent>` to the same Gateway, without renaming the Gateway, its Service or the LLM route. The Gateway SHALL NOT be publicly routed.

#### Scenario: Agent reaches the model
- **WHEN** an agent calls `http://agentgateway.ml.svc.cluster.local/v1/chat/completions`
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
Mesh authorization SHALL admit connections to the model servers only from the gateway, the endpoint picker, the benchmark client and the metrics agent's mesh identity. Connections to the gateway SHALL be admitted only from labelled LLM-client service accounts in `ml` and the Agent Substrate workers that run agent actors. Identity-less sources SHALL reach only the gateway's and endpoint picker's dedicated metrics ports.

#### Scenario: Direct call to vLLM
- **WHEN** a pod other than the gateway or endpoint picker connects to the vLLM port
- **THEN** the connection is refused by the mesh
