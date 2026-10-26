## ADDED Requirements

### Requirement: Self-contained attempt scoring context

New-contract AttemptStarted events SHALL contain immutable merchant/customer references, checkout snapshot and attempt signals sufficient to reconstruct the current Fraud request without PaymentCreated. Context SHALL match committed payment state and preserve optional signal absence. Lifecycle facts SHALL remain separate; labels, oracle fields, computed history features and raw credentials SHALL NOT enter this context.

#### Scenario: Attempt consumed without creation event

- **WHEN** a consumer receives a supported AttemptStarted before PaymentCreated
- **THEN** it can obtain all contracted current-attempt scoring inputs from that event without inventing missing customer, merchant or amount fields

#### Scenario: Snapshot consistency

- **WHEN** an attempt begins on an existing payment
- **THEN** its event context and Fraud request use the same immutable checkout values as committed payment state

### Requirement: Coordinated current event shape

The new attempt context SHALL use a fresh protobuf field number and an explicit new payload schema version with matching typed Parquet and data contracts. Existing field meanings SHALL remain unchanged. Current producers and consumers SHALL adopt the shape together. Unsupported shapes SHALL fail visibly; stored events and published datasets SHALL NOT be rewritten or supported through a new legacy adapter.

#### Scenario: Required context absent

- **WHEN** a record declares the new attempt schema but omits its scoring context
- **THEN** contract validation rejects it even if protobuf decoding succeeds

#### Scenario: Historical record retained

- **WHEN** a prior-contract ledger record or published source still exists after rollout
- **THEN** its original bytes and provenance remain unchanged and it is not silently enriched or selected as a new-contract record

### Requirement: Durable attempt ownership before risk evaluation

Payments SHALL durably claim/start an attempt before invoking Fraud, record the resulting assessment before approved processor effects, and retain recoverable idempotency. The synchronous decision path SHALL NOT wait for Kafka publication or feature-stream processing. This contract enrichment SHALL NOT change rules-only decision outcomes or introduce feature/model dependencies.

#### Scenario: Kafka outage during an attempt

- **WHEN** the broker or feature processor is unavailable
- **THEN** rules-only evaluation and durable payment processing remain independent of their recovery

#### Scenario: Crash after durable start

- **WHEN** Payments stops after committing a start but before recording the risk assessment
- **THEN** recovery retains the attempt identity and does not create a second conflicting attempt or duplicate processor effect
