## MODIFIED Requirements

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

### Requirement: Agent registry publication
The registry catalog SHALL live in Git and be published to the platform's agentregistry by a deployment-time job, never by CI. It SHALL record each entry agent's name, version and description, its sub-agents, the MCP servers it and they use, and the remote agent skills it calls. It SHALL NOT hold prompt copies or tool lists, which come from Langfuse and the MCP servers. A behaviour change of a published agent SHALL publish a new version. Agents SHALL be deployed by the application chart, not by the registry.

#### Scenario: Release publishes the catalog
- **WHEN** a release changes an agent or MCP server
- **THEN** one catalog job runs and the registry lists both entry agents at their new versions with their sub-agents, MCP servers and remote skills

## ADDED Requirements

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

## REMOVED Requirements

### Requirement: Coordinator entry agent with sub-agents
**Reason**: Replaced by the analyst and merchant entry agents. The analyst side keeps the same sub-agents and transfer behaviour under the name `analyst-assistant`.
**Migration**: Clients call `/a2a/analyst-assistant` instead of `/a2a/coordinator`. The `coordinator` Agent, its Langfuse prompt and its catalog entry are deleted.
