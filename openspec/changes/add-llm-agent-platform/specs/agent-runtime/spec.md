## ADDED Requirements

### Requirement: Global model config
A kagent `ModelConfig` named `default-model-config` in the agents' namespace SHALL reach the agent gateway's `/v1` route with the OpenAI provider, model `qwen3-8b` and the agents' gateway key. Every agent defined by this repository SHALL use it. No agent SHALL hold a model-server address or a model-provider key.

#### Scenario: Change the model for all agents
- **WHEN** the model name or gateway address changes
- **THEN** only `default-model-config` changes and every agent uses the new value after reconciliation

### Requirement: Agents run on Agent Substrate in their own namespace
Agents SHALL be kagent `Agent`s whose `Harness` runs each conversation as a gVisor actor on an app-owned Substrate `WorkerPool`, in a platform-created namespace with Pod Security `privileged` and no mesh enrollment that holds only the agents and their pool. The release namespace SHALL stay `restricted`. A new conversation SHALL start from the agent revision's golden snapshot, and idle conversations SHALL suspend to snapshots and resume on any worker with room.

#### Scenario: More conversations than workers
- **WHEN** more conversations are open than the pool has workers
- **THEN** all are answered, idle ones are suspended, and a resumed conversation keeps its context

#### Scenario: Cold conversation
- **WHEN** a client sends the first message of a new conversation
- **THEN** the message is accepted while the actor is restored and its runtime starts, without a retry

### Requirement: Agent runtime
Every agent SHALL run one app-built runtime image: kagent's Go ADK entrypoint at the pinned kagent release, with one ADK plugin added and no other change to kagent's behaviour. Before each model call the plugin SHALL read the calling agent's prompt from Langfuse, choose the conversation's A/B variant, put the prompt first in the system instruction, and link the model call's span to the prompt version. The image SHALL be pinned by digest in the Harness.

#### Scenario: Sub-agent model call
- **WHEN** a sub-agent compiled into an entry agent's runtime calls the model
- **THEN** its system prompt is its own Langfuse prompt, not the entry agent's

#### Scenario: Langfuse briefly unreachable
- **WHEN** Langfuse cannot be reached and the runtime has fetched the prompt before
- **THEN** the model call uses the last version fetched

### Requirement: Credentials injected at egress
Agent actors SHALL hold no credentials. Their model, MCP and Langfuse requests SHALL carry placeholders that Substrate's egress gateway replaces with keys from Secrets named in the agents' `ModelConfig`, `RemoteMCPServer` and `EgressPolicy`. The agent gateway SHALL admit the egress gateway by its mesh identity.

#### Scenario: Model call from an actor
- **WHEN** an agent calls the model or its MCP tool
- **THEN** the request reaches the gateway through Substrate's egress gateway with the agents' API key, and the actor never sees the key

### Requirement: Read-only fraud-analyst agent
The repository SHALL define `evidence-agent` as an `AgentTemplate` using the global model config and the knowledge tools. Its prompt seed SHALL make it read-only, forbid inventing payment facts it cannot retrieve, and keep it out of any payment decision.

#### Scenario: Analyst asks about a payment
- **WHEN** the agent is asked about a specific payment ID it has no evidence for
- **THEN** it states that it cannot retrieve that payment's evidence instead of fabricating details

### Requirement: Risk agent
The repository SHALL define `risk-agent` as an `AgentTemplate` using the global model config, with a read-only prompt seed for assessing the fraud risk of one payment, card, customer or merchant. Until its scoring and feature tools are attached, it SHALL say that it cannot retrieve a payment's features or score, explain which signals an analyst should check, and never invent identifiers, feature values, scores or outcomes.

#### Scenario: Risk question without tools
- **WHEN** the risk agent is asked for a specific payment's risk
- **THEN** it says it cannot retrieve that payment's features or score and lists the signals to check

### Requirement: Coordinator entry agent with sub-agents
The repository SHALL deploy `coordinator` as the analysts' only entry agent, behind the gateway's A2A route. `evidence-agent` and `risk-agent` SHALL be its kagent Shared sub-agents, compiled into its runtime through `subAgent` bindings, and SHALL NOT be deployed as agents of their own. The coordinator SHALL transfer each question to the specialist whose description matches it, and refuse requests to act.

#### Scenario: Knowledge question
- **WHEN** an analyst asks the coordinator what a decline code means
- **THEN** it transfers to `evidence_agent`, which answers from the knowledge tools with cited chunk ids, in the same trace

#### Scenario: Risk question
- **WHEN** an analyst asks the coordinator for the risk of a specific payment
- **THEN** it transfers to `risk_agent`, which answers

### Requirement: Self-hosted Langfuse
The application SHALL run self-hosted Langfuse v3 at a pinned chart and image, using the shared ClickHouse in `data` through its fixed Service contract, and app-owned Postgres and Redis instances. Its secrets SHALL come from Doppler through External Secrets. Public sign-up SHALL be disabled, and its UI SHALL be reachable only through the platform's authenticating proxy.

#### Scenario: First start
- **WHEN** Langfuse starts against an empty ClickHouse `langfuse` database and Postgres
- **THEN** it applies its migrations and creates the initial organisation, project and admin user from the provisioned keys

### Requirement: Prompts managed in Langfuse
Each agent's system prompt SHALL be the Langfuse prompt `<agent>-system-prompt`, and agents SHALL use its `production` version. A deployment-time job SHALL create the prompt from the agent's seed file in Git, and its A/B variant from the values, only when Langfuse does not have the prompt; it SHALL NOT change an existing prompt. A new production version published in Langfuse SHALL reach new conversations without a release.

#### Scenario: Prompt edited in Langfuse
- **WHEN** a new production version of an agent's prompt is published in Langfuse
- **THEN** the next conversations use it and their model calls are linked to the new version

#### Scenario: Release with a changed seed
- **WHEN** a release changes a prompt seed for a prompt Langfuse already has
- **THEN** Langfuse keeps its versions unchanged

### Requirement: Prompt A/B tests
An agent's prompt MAY run an A/B test, declared in its `production` version's config as the other label and the share of conversations it receives. The runtime SHALL assign each conversation to one side by a hash of the prompt name and the conversation's actor, so a conversation keeps its side for every model call, and each agent in a conversation SHALL be split on its own. Each model call SHALL be linked to the version it used, and the trace SHALL be tagged with each tested agent's side. Variants differ by prompt text; sampling settings come from the model config.

#### Scenario: Split
- **WHEN** many conversations are started with an agent whose prompt runs a test
- **THEN** both versions answer, in roughly the declared proportion, and Langfuse shows each version's calls, latency and tokens

### Requirement: Agent registry publication
The registry catalog SHALL live in Git and be published to the platform's agentregistry by a deployment-time job, never by CI. It SHALL record each entry agent's name, version and description, its sub-agents and the MCP servers it and they use; it SHALL NOT hold prompt copies or tool lists, which come from Langfuse and the MCP servers. A behaviour change of a published agent SHALL publish a new version. Agents SHALL be deployed by the application chart, not by the registry.

#### Scenario: Release publishes the catalog
- **WHEN** a release changes an agent or MCP server
- **THEN** one catalog job runs and the registry lists the entry agent at its new version with its sub-agents and MCP servers

### Requirement: Autoscaling mechanism per workload
The agents' Substrate `WorkerPool` SHALL be autoscaled by KEDA between a minimum and a maximum on Substrate's own metric of partly or fully booked workers, and Argo CD SHALL leave its replica count to KEDA. The knowledge API SHALL run as a Deployment, because its callers are admitted by mesh identity, which Knative's cluster-local gateway cannot carry, and SHALL be autoscaled by KEDA between two and four replicas on its combined REST and MCP request rate. The model servers SHALL NOT be autoscaled while each environment has one GPU. The documentation SHALL record this as the platform equivalent of the rubric's KEDA requirement, with the reason for each choice.

#### Scenario: Knowledge API under load
- **WHEN** the knowledge API's request rate exceeds the per-replica target during a load test
- **THEN** KEDA adds replicas up to four, and removes them after the stabilisation window once the load stops

#### Scenario: Conversations fill the worker pool
- **WHEN** concurrent conversations book the workers above the target share
- **THEN** KEDA adds workers up to the maximum, and removes them once the conversations are suspended

### Requirement: Agent dashboard
The chart SHALL ship a Grafana dashboard, picked up by the platform's dashboard sidecar, that shows the A2A calls and failures per entry agent, MCP tool calls and failures per tool, screening decisions and the decision model's latency, tokens, time to first token and generation latency.

#### Scenario: Agent called
- **WHEN** an entry agent is called through the gateway
- **THEN** the dashboard's calls-per-agent panel counts it under the agent's route
