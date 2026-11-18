## MODIFIED Requirements

### Requirement: Knowledge Web API and MCP tool in one service
The `rag-api` service SHALL be one FastAPI application that serves both REST endpoints and MCP tools. It SHALL be the only reader of the knowledge online store, and SHALL connect with the reader role.

REST is retrieval by id: pydantic response models, async handlers, and liveness and readiness endpoints used by its Kubernetes probes. It SHALL provide:
- chunk retrieval by `chunk_id`;
- customer feature retrieval by `customer_id` from the online feature store, once customer features are online.

MCP: the streamable-HTTP tools at `/mcp`, in stateless mode:
- `search_knowledge`, which embeds the query through the gateway with the embedding model's query instruction and its own client key, runs the indexed Feast vector search, and reads the chunks back by id;
- `get_knowledge_chunk`, backed by the same function as the REST chunk endpoint.

The audience of an MCP call SHALL come from the audience header set by the gateway route, never from a tool argument:
- With audience `merchant`, `search_knowledge` SHALL return only `merchant` and `both` chunks, and `get_knowledge_chunk` SHALL report an `analyst` chunk as not found.
- Without the header, every chunk is visible.

Search is offered only as the MCP tool, because its callers are agents; REST serves lookups by id.

Deployment:
- It SHALL run in `llm` with an update that keeps the previous version serving until a new version is ready.
- Its readiness SHALL depend on the database only, not on a loaded knowledge version.
- It SHALL be load-tested for throughput and latency.
- Agents SHALL reach the MCP tools only through the gateway routes `/mcp/knowledge` and `/mcp/knowledge-merchant`.
- It SHALL be published to the agent registry as an MCP server.

Both audiences of `evidence-agent` SHALL use these tools through the gateway.

#### Scenario: Unknown chunk id
- **WHEN** a client requests a `chunk_id` that is not in the current version
- **THEN** the API returns 404 with a validated error body

#### Scenario: Invalid search request
- **WHEN** a `search_knowledge` call has an empty query or `top_k` outside the allowed range
- **THEN** the tool returns a validation error without calling the gateway or the store

#### Scenario: Agent cites a chunk
- **WHEN** an analyst asks `evidence-agent` what `NEW_ACCOUNT_GEO_MISMATCH` means
- **THEN** the agent calls `search_knowledge` through the gateway and answers citing the returned `chunk_id`

#### Scenario: Merchant route asks for an analyst chunk
- **WHEN** a call through `/mcp/knowledge-merchant` asks for an analyst-only chunk by id
- **THEN** the tool reports it as not found

#### Scenario: Failed revision
- **WHEN** a new `rag-api` revision fails its readiness check
- **THEN** traffic stays on the last ready revision
