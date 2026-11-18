## Purpose

Run read-only analyst agents with managed prompts, A/B tests, tracing and registry publication.

## Requirements

### Requirement: Global model config
A kagent `ModelConfig` named `default-model-config` in the agents' namespace SHALL reach the agent gateway's `/v1` route with the OpenAI provider, model `qwen35-4b` and the agents' gateway key. Every agent defined by this repository SHALL use it. No agent SHALL hold a model-server address or a model-provider key.

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
Every agent SHALL run one app-built runtime image: kagent's Go ADK entrypoint at the pinned kagent release with three project additions and no other change to kagent's behaviour:
- The Langfuse plugin. Before each model call it SHALL read the calling agent's prompt from Langfuse, choose the conversation's A/B variant, put the prompt first in the system instruction, and link the model call's span to the prompt version.
- The policy plugin, which enforces the agent's policy profile (see `agent-policy`).
- The native kagent remote A2A tool for remote agent skills, configured through a small composition step. It SHALL call only through the agent gateway and preserve the authenticated user bearer credential through native token propagation.

The policy plugin SHALL read the native authenticated principal and forwarded signed bearer credential. It SHALL ignore caller identity headers and refuse missing credentials. The image SHALL be pinned by digest in the Harness.

#### Scenario: Sub-agent model call
- **WHEN** a sub-agent compiled into an entry agent's runtime calls the model
- **THEN** its system prompt is its own Langfuse prompt, not the entry agent's

#### Scenario: Langfuse briefly unreachable
- **WHEN** Langfuse cannot be reached and the runtime has fetched the prompt before
- **THEN** the model call uses the last version fetched

#### Scenario: Remote skill call
- **WHEN** `merchant-assistant` calls the `review_decline` skill
- **THEN** the request goes to `decline-reviewer` through the gateway's internal A2A route, preserving the merchant's signed identity and `integration_id`, and both conversations' traces are linked

### Requirement: Credentials injected at egress
Agent actors SHALL hold no long-lived signing keys or gateway credentials. Native kagent propagation MAY carry a short-lived authenticated user bearer credential for delegation. Model and Langfuse requests SHALL carry placeholders replaced at egress from Secrets. Model calls SHALL use the shared agents' model key. MCP and internal A2A calls SHALL use native caller-bearer propagation on a credential-free origin dedicated to the entry agent. Substrate SHALL grant each actor only its configured origins, and the gateway SHALL expose tools only on the permitted entry agent's listener. The agent gateway SHALL admit the egress gateway by its mesh identity.

#### Scenario: Model call from an actor
- **WHEN** an agent calls the model
- **THEN** the request reaches the gateway through Substrate's egress gateway with the agents' model key, and the actor never sees the key

#### Scenario: MCP call from an actor
- **WHEN** `merchant-assistant` calls a tool on `/mcp/payments`
- **THEN** the request reaches the gateway through Substrate's egress gateway on the merchant listener with its native caller JWT; the analyst listener origin is not granted to that actor

### Requirement: Read-only fraud-analyst agent
The repository SHALL define `evidence-agent` once, in chart values, and render it as one `AgentTemplate` per audience: analyst and merchant. Every rendered template SHALL use the global model config and its audience's knowledge route. Its prompt seed SHALL make it read-only, forbid inventing payment facts it cannot retrieve, and keep it out of any payment decision.

#### Scenario: Analyst asks about a payment
- **WHEN** the agent is asked about a specific payment ID it has no evidence for
- **THEN** it states that it cannot retrieve that payment's evidence instead of fabricating details

#### Scenario: Merchant audience
- **WHEN** the merchant-audience `evidence_agent` searches the knowledge base
- **THEN** it receives only merchant and shared chunks

### Requirement: Self-hosted Langfuse
The application SHALL run self-hosted Langfuse v4 at a pinned chart and image, using the shared ClickHouse in `data` through its fixed Service contract, and app-owned Postgres and Redis instances. Its secrets SHALL come from Doppler through External Secrets. Public sign-up SHALL be disabled, and its UI SHALL be reachable only through the platform's authenticating proxy.

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
The registry catalog SHALL live in Git and be published to the platform's agentregistry by a deployment-time job, never by CI. It SHALL record each entry agent's name, version and description, its sub-agents, the MCP servers it and they use, and the remote agent skills it calls. It SHALL NOT hold prompt copies or tool lists, which come from Langfuse and the MCP servers. A behaviour change of a published agent SHALL publish a new version. Agents SHALL be deployed by the application chart, not by the registry.

#### Scenario: Release publishes the catalog
- **WHEN** a release changes an agent or MCP server
- **THEN** one catalog job runs and the registry lists both entry agents at their new versions with their sub-agents, MCP servers and remote skills

### Requirement: Autoscaling mechanism per workload
The agents' Substrate `WorkerPool` SHALL be autoscaled by KEDA between a minimum and a maximum on Substrate's own metric of partly or fully booked workers, and Argo CD SHALL leave its replica count to KEDA. The knowledge API and risk server SHALL be autoscaled by KEDA between two and four replicas, on the knowledge API's combined REST and MCP request rate and the risk server's tool-call rate respectively. KEDA SHALL target their Rollouts through the scale subresource, and Argo CD SHALL leave replica counts to KEDA. The model servers SHALL NOT be autoscaled while all three models share one claimed GPU.

#### Scenario: Knowledge API under load
- **WHEN** the knowledge API's request rate exceeds the per-replica target during a load test
- **THEN** KEDA adds replicas up to four, and removes them after the stabilisation window once the load stops

#### Scenario: Conversations fill the worker pool
- **WHEN** concurrent conversations book the workers above the target share
- **THEN** KEDA adds workers up to the maximum, and removes them once the conversations are suspended

#### Scenario: Risk tool demand rises
- **WHEN** the risk server's tool-call rate exceeds its configured per-replica target
- **THEN** KEDA increases its desired replica count up to four and returns toward two after demand falls and the stabilisation window expires

### Requirement: Agent dashboard
The chart SHALL ship a Grafana dashboard, picked up by the platform's dashboard sidecar and placed in the `LLM` folder like the other components' dashboards, that shows the A2A calls and failures per entry agent, MCP tool calls and failures per tool for the knowledge and risk servers, the risk tool's latency and feature view statuses, the replicas of both MCP servers, screening decisions and the decision model's latency, tokens, time to first token and generation latency.

#### Scenario: Agent called
- **WHEN** an entry agent is called through the gateway
- **THEN** the dashboard's calls-per-agent panel counts it under the agent's route

#### Scenario: Risk tool called
- **WHEN** an agent calls a risk tool
- **THEN** the dashboard counts it by tool and outcome and shows its latency and the status of each feature view it read

### Requirement: Risk agent scores with the risk tools
The repository SHALL define `risk-agent` as an `AgentTemplate` using the global model config and the `risk` tools, with a read-only prompt seed for assessing the fraud risk of one payment, card, customer or merchant. The prompt SHALL tell the agent to call the scoring tool with the identifiers and request fields the analyst gives, to ask for any it lacks instead of guessing, and to report the score, the model version, the decline threshold, any view that was not available, and the drivers. It SHALL state that drivers are not a per-prediction attribution, that the score is a re-score of the inputs given and not the recorded decision, and that a tool error means no score. It SHALL never invent identifiers, feature values, scores or outcomes.

#### Scenario: Risk question with identifiers
- **WHEN** the risk agent is asked for the risk of a payment and given the customer, the payment-method fingerprint and the attempt's fields
- **THEN** it calls the scoring tool and answers with the score, the model version, the decline threshold, the drivers and any view that was not available

#### Scenario: Missing identifiers
- **WHEN** the analyst gives no customer or payment-method fingerprint
- **THEN** the agent asks for them and does not call the tool with invented values

#### Scenario: Tool error
- **WHEN** the scoring tool returns an error
- **THEN** the agent says it could not score the payment and why, and gives no score

### Requirement: Blue-green releases for both MCP servers
In both dev and production configuration, the risk server and knowledge API SHALL use blue-green Rollouts with separate active and preview Services. The gateway SHALL keep addressing the active Services. A candidate SHALL become active only after it is ready at the desired replica count and a bounded pre-promotion analysis has successfully listed its MCP tools and called its scoring or retrieval tool against shared synthetic scoring inputs or a configured knowledge passage. Tool errors, missing request features and missing knowledge passages SHALL fail the analysis. Missing, expired or future history views SHALL be allowed under the scoring contract; invalid views and upstream failures SHALL fail analysis. Failed or timed-out analysis SHALL keep the previous revision active; no automatic promotion timer SHALL bypass analysis. A dedicated analysis identity SHALL be admitted only to the servers' tool ports. Both environments SHALL use the same workload strategy; live production deployment is outside this change.

#### Scenario: Candidate passes analysis
- **WHEN** either MCP server's candidate is ready and its preview tool call passes analysis
- **THEN** the active Service switches to that revision after it reaches the desired ready capacity, and the gateway's backend address stays the same

#### Scenario: Scoring candidate has no stored history
- **WHEN** the scoring candidate reads the shared synthetic identifiers and Feast reports missing history with available request features
- **THEN** analysis accepts a valid model score under the normal missing-feature contract without requiring a seeded dev customer

#### Scenario: Candidate fails analysis
- **WHEN** either server's preview tool call fails, returns a tool error or exceeds the analysis deadline
- **THEN** promotion is blocked and the active Service continues selecting the previous revision

#### Scenario: Reconcile during promotion
- **WHEN** Argo CD reconciles while KEDA scales or a Rollout changes Service revision selectors
- **THEN** replica counts and revision hash selectors remain controller-owned while base selectors and security configuration remain reconciled

#### Scenario: Candidate never becomes ready
- **WHEN** a candidate's pods fail their readiness probe, so its analysis never starts
- **THEN** the Rollout aborts at its progress deadline, the preview receives no traffic and the active Service keeps selecting the previous revision

#### Scenario: Adopt the knowledge workload
- **WHEN** the knowledge API migrates from its Deployment to a Rollout
- **THEN** ready active endpoints remain available during handover and the completed migration leaves one workload controller and no superseded Deployment

### Requirement: Analyst and merchant entry agents
The repository SHALL deploy its entry agents from one list in chart values, each on its own listener; the analyst and merchant assistants have external A2A routes and `decline-reviewer` has only the internal route:
- `analyst-assistant`, for analysts. It SHALL have `risk_agent` and the analyst-audience `evidence_agent` as kagent Shared sub-agents compiled into its runtime, and SHALL transfer each analyst question to the matching specialist.
- `decline-reviewer`, for `merchant-assistant` only. It SHALL call the payments, risk and analyst-audience knowledge tools itself, with no sub-agents, so the review's final answer always comes from the reviewer, and serve the `review_decline` skill. For that skill it SHALL:
- read the payment and the declined attempt's risk inputs for the merchant's integration;
- re-score a risk decline with `score_payment_risk`;
- look up the reason's merchant-facing wording;
- return only `{payment_id, reason_code, merchant_wording, review_outcome, retry_advice}`.

`review_outcome` SHALL be `needs_analyst_review` when a risk decline now re-scores below the decline threshold, and `consistent_with_policy` otherwise.
- `merchant-assistant`, for merchants. It SHALL have the merchant-audience `evidence_agent` as a Shared sub-agent and `get_payment_status` as its own tool. It SHALL call `decline-reviewer`'s `review_decline` skill over A2A when a merchant asks why a payment was declined or whether a decline should be reviewed.

Every agent SHALL refuse requests to act. Sub-agents SHALL NOT be deployed as agents of their own.

#### Scenario: Analyst risk question
- **WHEN** an analyst asks `analyst-assistant` for the risk of a payment with its identifiers
- **THEN** it transfers to `risk_agent`, which answers in the same trace

#### Scenario: Merchant asks about a decline
- **WHEN** a merchant asks `merchant-assistant` why one of its payments was declined
- **THEN** it reads the payment with `get_payment_status`, calls `review_decline`, and answers with the reason, the merchant-facing wording and the retry advice, without any score, feature or driver

#### Scenario: Merchant asks about another merchant's payment
- **WHEN** a merchant names a payment that belongs to another integration
- **THEN** `get_payment_status` reports it as not found and no review is requested
