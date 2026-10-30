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

### Requirement: Agent pool capacity
The agents' `WorkerPool` SHALL run a fixed three workers, so a coordinator conversation and its delegation can be active at once with a third conversation, while idle ones stay suspended. It SHALL NOT be autoscaled until Substrate's WorkerPool scale subresource publishes a selector that the Kubernetes HPA accepts.

#### Scenario: Coordinator delegation
- **WHEN** a coordinator conversation delegates to a sub-agent
- **THEN** the coordinator and the sub-agent each run on their own worker and the analyst gets the combined answer

### Requirement: Sandbox egress through a proxy
Sandboxed agents SHALL leave their sandbox only through an app-owned egress proxy in the release namespace, which forwards only the gateway's model, MCP and agents' A2A routes and Langfuse's trace intake and calls them with its own mesh identity. A network policy SHALL admit only the agents' worker pods, the kagent controller and kubelet probes to the proxy.

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
The registry catalog SHALL live in Git and be published to the platform's agentregistry by a deployment-time job, never by CI. It SHALL record the agent's name, version, description, model config and MCP servers; it SHALL NOT hold prompt copies or tool lists, which come from Langfuse and the MCP servers. A behaviour change of a published agent SHALL publish a new version. Agents SHALL be deployed by the application chart, not by the registry.

#### Scenario: Release publishes the catalog
- **WHEN** a release changes an agent, MCP server or prompt
- **THEN** one catalog job runs and the registry lists the agent at its new version with its model config and MCP servers

### Requirement: Prompts versioned in Langfuse from Git
Each agent's system prompt SHALL be a file in Git, read by the agent from a ConfigMap rendered from that file. The same deployment-time job SHALL publish it to Langfuse as `<agent>-system-prompt` with the `production` label, creating a new version only when the text differs from the current production version. Langfuse SHALL NOT be written back to the agent.

#### Scenario: Prompt changed in Git
- **WHEN** a release changes an agent's prompt file
- **THEN** Langfuse shows a new production version with the new text, and the agent uses the same text after reconciliation

#### Scenario: Release without a prompt change
- **WHEN** a release leaves the prompt file unchanged
- **THEN** Langfuse keeps the same production version

### Requirement: Autoscaling mechanism per workload
The agents' Substrate worker pool SHALL be autoscaled by KEDA once Substrate's WorkerPool scale subresource publishes a selector, and SHALL run at a fixed size until then. The knowledge API SHALL run as a Deployment, because its callers are admitted by mesh identity, which Knative's cluster-local gateway cannot carry, and SHALL be autoscaled by KEDA between two and four replicas on its combined REST and MCP request rate. The model servers SHALL NOT be autoscaled while each environment has one GPU. The documentation SHALL record this as the platform equivalent of the rubric's KEDA requirement, with the reason for each choice.

#### Scenario: Knowledge API under load
- **WHEN** the knowledge API's request rate exceeds the per-replica target during a load test
- **THEN** KEDA adds replicas up to four, and removes them after the stabilisation window once the load stops

#### Scenario: Worker pool without a scale selector
- **WHEN** the pinned Substrate WorkerPool reports no `status.selector` on its scale subresource
- **THEN** the chart sets a fixed replica count and renders no ScaledObject for it

### Requirement: Risk agent
The repository SHALL define agent `risk-agent` as a declarative `SandboxAgent` using the global model config, with a read-only prompt for assessing the fraud risk of one payment, card, customer or merchant. Until its scoring and feature tools are attached, it SHALL say that it cannot retrieve a payment's features or score, explain which signals an analyst should check, and never invent identifiers, feature values, scores or outcomes.

#### Scenario: Risk question without tools
- **WHEN** the risk agent is asked for a specific payment's risk
- **THEN** it says it cannot retrieve that payment's features or score and lists the signals to check

### Requirement: Coordinator agent
The repository SHALL define agent `coordinator` as a declarative `SandboxAgent` that is the analysts' entry agent. For each question it SHALL call the `route_question` tool of the `decision` MCP server, which answers with laya which specialist (`evidence-agent` or `risk-agent`) should answer, and delegate to that specialist as a kagent Agent tool over A2A through the gateway. Each delegation SHALL start a fresh sub-agent session. The coordinator SHALL combine the specialists' answers, keep their citations, and refuse requests to act.

#### Scenario: Knowledge question
- **WHEN** an analyst asks the coordinator what a decline code means
- **THEN** it calls `route_question`, delegates to `evidence-agent`, and answers with the evidence agent's cited answer

#### Scenario: Risk question
- **WHEN** an analyst asks the coordinator for the risk of a specific payment
- **THEN** it routes to `risk-agent` and relays its answer

### Requirement: Routing decisions from the agents' own descriptions
The decision service SHALL run the laya decision model (a pinned revision of `convaiinnovations/laya`, English checkpoint) on CPU in the release namespace, with its weights copied from the environment bucket at start. Its routing choices SHALL be the coordinator's sub-agents, each described by the `routing` text of that agent's entry in the chart values, so adding a specialist is a values change. It SHALL count routing and screening decisions and expose its forward-pass latency as metrics.

#### Scenario: New specialist
- **WHEN** a release adds an agent to the coordinator's sub-agents with a routing text
- **THEN** the routing tool offers it without a code change

### Requirement: A/B test of agent model configs
An agent MAY declare an A/B test: a variant `SandboxAgent` with the same prompt and tools and another named model config, behind the same A2A route, receiving the declared share of that route's traffic by weight. Each arm SHALL be its own gateway backend, so its calls, latency and errors are separable in the gateway's metrics and its traces are separable in Langfuse by agent name. `evidence-agent` SHALL run such a test against `model-config-b`. A variant SHALL replace the agent's config only after its results are reviewed, by changing the agent's model config and removing the test.

#### Scenario: Split
- **WHEN** many A2A messages are sent to `/a2a/evidence-agent`
- **THEN** both arms answer, in roughly the declared proportion, and the dashboard shows each arm's calls, latency and errors

### Requirement: Agent dashboard
The chart SHALL ship a Grafana dashboard, picked up by the platform's dashboard sidecar, that shows the calls per agent, MCP tool calls and failures per tool, routing and screening decisions, the A/B arms, tokens, time to first token and generation latency.

#### Scenario: Agent called
- **WHEN** an agent is called through the gateway
- **THEN** the dashboard's calls-per-agent panel counts it under the agent's route
