## MODIFIED Requirements

### Requirement: Payment lifecycle transitions

A payment SHALL begin with exactly one `PaymentCreated` event and be `OPEN` until `PaymentSucceeded` or `PaymentExpired`, after which no events SHALL follow. While open, at most one attempt SHALL be active. An attempt SHALL progress `AttemptStarted` → `RiskDecisionRecorded`, then either `AttemptDeclined` with source `RISK`, or `ProcessorOperationRequested` followed by `PaymentSucceeded`, `AttemptDeclined` with source `PROCESSOR`, or `ProcessorOutcomeUnknown`. An unknown outcome SHALL resolve only to `PaymentSucceeded` or `AttemptDeclined` with source `PROCESSOR`. Before a committed risk decision, assessment execution errors SHALL remain operational workflow state, not a risk outcome or a successful assessment event. Recovery SHALL retain the active attempt identity; normal expiry remains possible when no processor outcome is outstanding. A new attempt SHALL start only after the previous attempt is declined. `PaymentExpired` SHALL NOT occur while an attempt awaits a processor outcome. `RiskDecisionRecorded` SHALL carry outcome, reason codes and policy, model and feature versions.

#### Scenario: Retry after risk decline

- **WHEN** an attempt is declined by risk and the payment is still open
- **THEN** a new attempt may start under the same payment

#### Scenario: Processor call after risk decline

- **WHEN** `ProcessorOperationRequested` follows a risk decline for the same attempt
- **THEN** the history is invalid

#### Scenario: Expiry during unknown outcome

- **WHEN** a payment would expire while its attempt has an unknown processor outcome
- **THEN** expiry is not a legal transition until that attempt resolves

### Requirement: Durable attempt ownership before risk evaluation

Payments SHALL durably claim/start an attempt before invoking Fraud, record the resulting assessment before approved processor effects, and retain recoverable idempotency. The synchronous decision path SHALL NOT wait for Kafka publication or feature-stream processing. Observe-only velocity and shadow feature retrieval SHALL NOT change healthy rules-only outcomes or require a trained model. Operational errors SHALL remain distinct from declines. Durable assessment/evidence identity SHALL survive caller timeouts, while only Payments' committed risk event makes an assessment eligible for historical aggregates.

#### Scenario: Kafka outage during an attempt

- **WHEN** the broker or feature processor is unavailable
- **THEN** rules-only evaluation and durable payment processing remain independent of their recovery

#### Scenario: Crash after durable start

- **WHEN** Payments stops after committing a start but before recording the risk assessment
- **THEN** recovery retains the attempt identity and does not create a second conflicting attempt or duplicate processor effect

