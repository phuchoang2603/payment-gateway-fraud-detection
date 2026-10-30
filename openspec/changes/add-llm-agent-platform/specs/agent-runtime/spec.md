## ADDED Requirements

### Requirement: Global model config
A kagent `ModelConfig` named `default-model-config` in the agents' namespace SHALL reach the agent gateway's `/v1` route, through the sandbox egress proxy, with the OpenAI provider, model `qwen3-8b` and the agents' gateway key. Every agent defined by this repository SHALL reference it unless a deliberate variant config is declared. No agent SHALL hold a model-server address or a model-provider key.

#### Scenario: Change the model for all agents
- **WHEN** the model name or gateway address changes
- **THEN** only `default-model-config` changes and every agent uses the new value after reconciliation

### Requirement: Agents run on Agent Substrate in their own namespace
Agents SHALL be kagent `SandboxAgent`s that run each conversation in a gVisor sandbox on an app-owned Substrate `WorkerPool`, in a platform-created namespace with Pod Security `privileged` and no mesh enrollment that holds only the agents and their pool. The release namespace SHALL stay `restricted`. Idle conversations SHALL suspend to snapshots and resume on any worker with capacity.

#### Scenario: More conversations than workers
- **WHEN** more conversations are open than the pool has workers
- **THEN** all are answered, idle ones are suspended, and a resumed conversation keeps its context

### Requirement: Agent pool autoscaling
The agents' `WorkerPool` SHALL run at least two workers and SHALL be scaled by a KEDA `ScaledObject` on its scale subresource, from the gateway's chat request rate, up to a fixed maximum.

#### Scenario: Load rises
- **WHEN** agent traffic exceeds the per-worker request-rate target
- **THEN** KEDA adds workers up to the maximum, and removes them after the cooldown once traffic falls

### Requirement: Sandbox egress through a proxy
Sandboxed agents SHALL leave their sandbox only through an app-owned egress proxy in the release namespace, which forwards only the gateway's model and MCP routes and Langfuse's trace intake and calls them with its own mesh identity. A network policy SHALL admit only the agents' worker pods, the kagent controller and kubelet probes to the proxy.

#### Scenario: Egress outside the allowed routes
- **WHEN** a sandboxed agent tries to reach a destination other than the egress proxy, or a proxy path other than the allowed routes
- **THEN** the connection or request is refused

#### Scenario: Gateway access from a sandbox
- **WHEN** a sandboxed agent calls the model or its MCP tool
- **THEN** the request reaches the gateway through the egress proxy with the proxy's mesh identity and the agents' API key

### Requirement: Read-only fraud-analyst agent
The repository SHALL define agent `evidence-agent` as a declarative `SandboxAgent` using the global model config and the knowledge tools. Its instructions SHALL make it read-only, forbid inventing payment facts it cannot retrieve, and keep it out of any payment decision.

#### Scenario: Analyst asks about a payment
- **WHEN** the agent is asked about a specific payment ID it has no evidence for
- **THEN** it states that it cannot retrieve that payment's evidence instead of fabricating details

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
