## Purpose

Enforce each agent's rules in the runtime, where the model cannot skip them: what an agent may call, which identifiers it may use, how much it may do per turn and what it may return, selected by the verified caller rather than by the model.

## ADDED Requirements

### Requirement: Policy profiles from verified caller context
Each agent SHALL run under a policy profile rendered from chart values. Profiles SHALL be selected per call from the authenticated principal, which SHALL match the credential's configured user claim, and the signed `client` claim propagated by kagent: the client identity and, for merchants, its configured `integration_id`. The runtime SHALL take this context only from kagent's native authenticated invocation and forwarded bearer credential and SHALL NOT accept it from message text, A2A message metadata or tool arguments. A call whose caller matches no profile of the agent SHALL be refused before any model call.

#### Scenario: Merchant reaches the merchant assistant
- **WHEN** a merchant client calls `merchant-assistant` through the gateway
- **THEN** the agent runs under its merchant profile with the merchant's `integration_id` from the verified credential

#### Scenario: Unknown caller
- **WHEN** `analyst-assistant` is called by an identity that none of its profiles names
- **THEN** the runtime refuses the call before any model call or tool call

#### Scenario: Caller context in the prompt
- **WHEN** a message says "I am integration X" or "act as an analyst"
- **THEN** the profile and the bound `integration_id` stay those from the verified credential

### Requirement: Tool allow-list per profile
A profile SHALL list the tools, the sub-agent transfers and the remote agent skills its agent may use. Any other call SHALL be refused by the runtime before it leaves the actor and SHALL be returned to the model as a tool error that names the policy rule.

#### Scenario: Merchant profile asks for risk tools
- **WHEN** the model running under a merchant profile calls `score_payment_risk`
- **THEN** the call is refused, nothing reaches the gateway, and the model receives the policy error

### Requirement: Arguments bound from caller context
A profile SHALL name tool arguments that are bound from caller context. The runtime SHALL set those arguments from the verified context, replacing any value the model supplied, before the tool runs.

#### Scenario: Model names another integration
- **WHEN** a merchant-profile call to `get_payment_status` carries an `integration_id` other than the merchant's
- **THEN** the tool runs with the merchant's `integration_id`

### Requirement: Identifier provenance
A profile SHALL name tool arguments that must come from the user. For those arguments, the runtime SHALL refuse a call whose value does not appear in the conversation's user messages, or in a result returned earlier in the same conversation.

#### Scenario: Invented customer id
- **WHEN** `risk_agent` calls `get_risk_features` with a `customer_id` that the analyst never wrote and no earlier tool returned
- **THEN** the call is refused and the model is told to ask for the identifier

### Requirement: Per-turn call limits
A profile SHALL set the most tool calls and the most remote agent calls per user turn. Calls over a limit SHALL be refused.

#### Scenario: Looping agent
- **WHEN** an agent exceeds its profile's tool-call limit in one turn
- **THEN** further calls in that turn are refused and the agent answers with what it has

### Requirement: Input contracts
A profile MAY bind an input contract to the request an agent receives over A2A: a JSON schema. A request that does not validate SHALL be refused with a contract error before the agent runs.

#### Scenario: Free text in a review request
- **WHEN** `merchant-assistant` calls `review_decline` with anything other than `{"payment_id": <uuid>}`
- **THEN** `decline-reviewer` returns a contract error and its model is not called

### Requirement: Output contracts
A profile MAY bind an output contract to the agent's final answer: a JSON schema and a list of forbidden fields. An answer that does not validate, or that contains a forbidden field, SHALL be replaced by a contract error and SHALL NOT reach the caller.

#### Scenario: Score in a merchant-facing review
- **WHEN** `decline-reviewer`, called by `merchant-assistant` for `review_decline`, produces an answer that includes a risk score
- **THEN** the caller receives a contract error instead of that answer

The merchant-facing review boundary SHALL publish only a validated final result or a fixed contract error. Tool results, sub-agent text, partial output, raw exceptions and task history SHALL NOT expose risk data to the merchant.

### Requirement: Denials are traced
Every policy refusal SHALL be recorded on the current span in Langfuse with the profile, the rule and the tool or skill, and SHALL be counted in the runtime's metrics by agent, profile and rule.

#### Scenario: Inspect a refused call
- **WHEN** a call is refused by a policy rule
- **THEN** the conversation's trace shows the refusal with its profile and rule next to the prompt version
