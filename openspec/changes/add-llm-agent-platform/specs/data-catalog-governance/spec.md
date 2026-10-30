## ADDED Requirements

### Requirement: AI assets in the governance catalog
The LLM platform's release catalog job SHALL publish the LLM platform to OpenMetadata as native AI entities from that chart's own values, without changing the data pipelines' metadata code or chart:
- a custom LLM service for the agent gateway endpoint, without storing any key;
- one LLM model per served model, with base model, pinned revision, provider and deployment details;
- one MCP server per gateway MCP route, with the tools the server itself lists;
- one AI application per agent, with type, framework, version, development stage, primary model, MCP servers and knowledge base.

Registration SHALL be idempotent and SHALL run once per change of the catalog content.

#### Scenario: Registration after a sync
- **WHEN** the catalog job runs in an environment with the LLM platform enabled
- **THEN** OpenMetadata shows the LLM service, the `qwen3-8b` and `qwen3-embedding` models, and the `evidence-agent` application whose primary model is `qwen3-8b`

#### Scenario: Tool added to an MCP server
- **WHEN** an MCP server's code adds a tool and the catalog job runs again
- **THEN** OpenMetadata lists the new tool on that server with the description from the server, with no chart change

#### Scenario: Agent version bump
- **WHEN** the agent's version changes and registration runs again
- **THEN** the existing application entity is updated to the new version instead of a duplicate being created

### Requirement: Knowledge-base product in the catalog
The RAG knowledge pipeline SHALL be governed like the other data pipelines:
- an ODPS product and ODCS contract owned by the `rag-pipeline` chart, whose SQL quality rules the shared checks job runs against `knowledge.chunks`;
- the Argo pipeline `rag-knowledge` with its run status;
- lineage from the corpus version container through `rag-knowledge` to `knowledge.chunks`.

#### Scenario: Successful knowledge run
- **WHEN** a `rag-knowledge` run completes
- **THEN** OpenMetadata shows the run status, the passing contract test results on `knowledge.chunks`, and lineage from the corpus version container to that table
