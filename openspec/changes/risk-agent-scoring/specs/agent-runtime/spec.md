## ADDED Requirements

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

## MODIFIED Requirements

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

## REMOVED Requirements

### Requirement: Risk agent
**Reason**: The agent no longer lacks its tools; "Risk agent scores with the risk tools" replaces the toolless behaviour, including the scenario "Risk question without tools".
**Migration**: None. The prompt seed, the `mcpServers` list and the catalog version change together in this change.
