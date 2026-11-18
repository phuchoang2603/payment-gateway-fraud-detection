## ADDED Requirements

### Requirement: Read-only payment tools for agents
The payments service SHALL serve read-only MCP tools on a port of its own, separate from its gRPC API, reachable only from the agent gateway:
- `get_payment_status(integration_id, payment_id)`: the same view as the payment status query (status, version and attempt summaries with status, decline reason and assessment status).
- `get_attempt_risk_inputs(integration_id, payment_id, attempt_id?)`: for the given attempt, or the latest one, the inputs the risk tools take. These come from the payment's checkout snapshot and the attempt's recorded `AttemptStarted` signals:
  - customer id;
  - payment-method fingerprint;
  - amount;
  - item count;
  - shipping and IP countries;
  - device presence;
  - the customer's and merchant's account creation times;
  - the decline source and reason.

Both tools SHALL answer only for the integration that owns the payment, and SHALL report another integration's payment as not found. Neither tool SHALL change state.

#### Scenario: Merchant reads its payment
- **WHEN** `get_payment_status` is called with integration A and one of A's payment ids
- **THEN** it returns the payment's status and attempt summaries

#### Scenario: Wrong integration
- **WHEN** either tool is called with integration B and integration A's payment id
- **THEN** it reports the payment as not found

#### Scenario: Risk inputs of a declined attempt
- **WHEN** `get_attempt_risk_inputs` is called for a payment whose latest attempt was declined by risk
- **THEN** it returns that attempt's recorded inputs with decline source `risk` and its reason code
