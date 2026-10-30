## ADDED Requirements

### Requirement: AI assets in the governance catalog
The RAG workflow's catalog step SHALL publish the LLM platform to OpenMetadata as native AI entities, without changing the data pipelines' metadata code or chart:
- a custom LLM service for the agent gateway endpoint, without storing any key;
- one LLM model per served model, with base model, quantization type, pinned revision, capabilities and deployment details;
- one AI application per agent, with type, framework, version, development stage and primary model.

Registration SHALL be idempotent and SHALL run on every RAG workflow run. Later MCP servers, agents and knowledge bases SHALL be added as further entries linked to these entities.

#### Scenario: Registration after a sync
- **WHEN** the metadata registration job runs in an environment with the LLM platform enabled
- **THEN** OpenMetadata shows the LLM service, the `qwen3-8b` and `qwen3-embedding` models, and the `evidence-agent` application whose primary model is `qwen3-8b`

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
