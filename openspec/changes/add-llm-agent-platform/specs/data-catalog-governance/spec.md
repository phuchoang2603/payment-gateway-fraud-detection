## ADDED Requirements

### Requirement: AI assets in the governance catalog
The metadata-integrations registration SHALL publish the LLM platform to OpenMetadata as native AI entities:
- a custom LLM service for the agent gateway endpoint, without storing any key;
- one LLM model per served model, with base model, quantization type, pinned revision, capabilities and deployment details;
- one AI application per agent, with type, framework, version, development stage and primary model.

Registration SHALL be idempotent, and SHALL run from the same chart, image and catalog identity as the other platform service registrations. Later MCP servers, agents and knowledge bases SHALL be added as further entries linked to these entities.

#### Scenario: Registration after a sync
- **WHEN** the metadata registration job runs in an environment with the LLM platform enabled
- **THEN** OpenMetadata shows the LLM service, the `qwen3-8b` and `qwen3-embedding` models, and the `evidence-agent` application whose primary model is `qwen3-8b`

#### Scenario: Agent version bump
- **WHEN** the agent's version changes and registration runs again
- **THEN** the existing application entity is updated to the new version instead of a duplicate being created
