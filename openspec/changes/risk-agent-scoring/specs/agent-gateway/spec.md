## ADDED Requirements

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
