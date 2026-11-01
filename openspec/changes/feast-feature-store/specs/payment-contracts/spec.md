## MODIFIED Requirements

### Requirement: Durable attempt ownership before risk evaluation

Payments SHALL durably claim/start an attempt before invoking Fraud, record the resulting assessment before approved processor effects, and retain recoverable idempotency. The synchronous decision path SHALL NOT wait for Kafka publication or feature-stream processing. Shadow feature retrieval SHALL NOT change healthy rules-only outcomes or require a trained model. Operational errors SHALL remain distinct from declines. Durable assessment/evidence identity SHALL survive caller timeouts, while only Payments' committed risk event makes an assessment eligible for historical aggregates.

#### Scenario: Kafka outage during an attempt

- **WHEN** the broker or feature processor is unavailable
- **THEN** rules-only evaluation and durable payment processing remain independent of their recovery

#### Scenario: Crash after durable start

- **WHEN** Payments stops after committing a start but before recording the risk assessment
- **THEN** recovery retains the attempt identity and does not create a second conflicting attempt or duplicate processor effect
