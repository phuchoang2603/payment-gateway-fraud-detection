## ADDED Requirements

### Requirement: Global model config
A kagent `ModelConfig` named `default-model-config` in `llm` SHALL point at the agent gateway's `/v1` route with the OpenAI provider, model `qwen3-8b` and the agents' gateway key. Every agent defined by this repository SHALL reference it unless a deliberate variant config is declared. No agent SHALL hold a model-server address or a model-provider key.

#### Scenario: Change the model for all agents
- **WHEN** the model name or gateway address changes
- **THEN** only `default-model-config` changes and every agent uses the new value after reconciliation

### Requirement: Read-only fraud-analyst agent
The repository SHALL define agent `evidence-agent` in `llm` as a declarative kagent agent using the global model config. Its instructions SHALL make it read-only, forbid inventing payment facts it cannot retrieve, and keep it out of any payment decision. Until the evidence tool exists it SHALL have no tools. It SHALL leave the Deployment replica count to an autoscaler and SHALL run under the `restricted` Pod Security level.

#### Scenario: Analyst asks about a payment
- **WHEN** the agent is asked about a specific payment ID in phase 1
- **THEN** it states that it cannot retrieve payment evidence yet instead of fabricating details

### Requirement: Sandboxed warm agent on Agent Substrate
The repository SHALL define `evidence-agent-sandbox`, a `SandboxAgent` rendered from the same specification as `evidence-agent`, running each conversation in a gVisor sandbox on the platform's Substrate worker pool. Its sandbox egress SHALL be limited to the agent gateway. Idle conversations SHALL suspend to snapshots and resume on any worker with capacity, so that a pool with fewer workers than concurrent conversations still serves all of them.

#### Scenario: More conversations than workers
- **WHEN** three conversations start at once on a pool with one worker
- **THEN** all three are answered and the worker count stays at one

#### Scenario: Egress outside the gateway
- **WHEN** the sandboxed agent tries to reach a destination other than the agent gateway
- **THEN** the connection is refused

### Requirement: Self-hosted Langfuse
The application SHALL run self-hosted Langfuse v3 at a pinned chart and image, using the shared ClickHouse in `data` through its fixed Service contract, and app-owned Postgres and Redis instances. Its secrets SHALL come from Doppler through External Secrets. Public sign-up SHALL be disabled, and its UI SHALL be reachable only through the platform's authenticating proxy.

#### Scenario: First start
- **WHEN** Langfuse starts against an empty ClickHouse `langfuse` database and Postgres
- **THEN** it applies its migrations and creates the initial organisation, project and admin user from the provisioned keys

### Requirement: Agent registry publication
The registry catalog SHALL live in Git and be published to the platform's agentregistry by a deployment-time job, never by CI. It SHALL record the agent's name, version, description, model config and tools. A behaviour change of a published agent SHALL publish a new version. Agents SHALL be deployed by the application chart, not by the registry.

#### Scenario: Sync publishes the catalog
- **WHEN** the application syncs with a catalog containing `evidence-agent` 0.1.0
- **THEN** the registry lists `evidence-agent` at 0.1.0 with its model config and no MCP servers

### Requirement: Autoscaling mechanism per workload
Workloads whose replicas another controller owns (kagent agent Deployments, Substrate worker pools) SHALL be autoscaled by KEDA from request-rate or active-actor metrics when autoscaling is enabled. Stateless HTTP services owned by this repository (MCP servers, web API) SHALL run as Knative Services. The model servers SHALL NOT be autoscaled while each environment has one GPU. The documentation SHALL record this as the platform equivalent of the rubric's KEDA requirement, with the reason for each choice.

#### Scenario: Agent Deployment under autoscaling
- **WHEN** an agent's Deployment is scaled by its autoscaler
- **THEN** kagent reconciliation does not reset the replica count
