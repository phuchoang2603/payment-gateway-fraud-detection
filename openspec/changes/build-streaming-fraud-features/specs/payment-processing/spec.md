## MODIFIED Requirements

### Requirement: Risk decision orchestration

Each attempt SHALL request a fraud decision within a configured deadline and durably record a valid assessment before any processor call. DECLINE SHALL end the attempt with a risk decline. Fraud unavailability, timeout or execution failure SHALL block processor authorization and retain a distinct recoverable assessment error, not create a FRAUD_UNAVAILABLE decline assessment. Payments SHALL reuse a completed durable assessment on retry, and SHALL atomically commit its authoritative RiskDecisionRecorded, decision evidence linkage and payment/attempt state using the existing transactional ledger/outbox publication path. Broker delivery SHALL remain at least once with downstream deduplication; a separate fraud.assessed payment event SHALL NOT be required.

#### Scenario: Fraud unavailable

- **WHEN** the fraud call times out
- **THEN** no processor operation is requested, the attempt reports operational assessment failure and recovery retains its identity without a fabricated risk decline

#### Scenario: Commit response lost

- **WHEN** Payments commits a risk decision but loses the caller response
- **THEN** retry returns recorded state without a second decision event or processor effect

### Requirement: Recovery and expiry workers

A recovery worker SHALL also resume stalled or retryable assessment execution under the same attempt identity with fenced ownership and bounded retries. It SHALL not rescore finalized assessments. An attempt with no processor intent MAY expire under the existing payment expiry policy; late assessment results SHALL NOT reopen expired payments or authorize effects. A recovery worker SHALL find attempts with unknown outcomes, and requested operations without a recorded outcome past a grace period. It SHALL query the processor by operation key and record the observed result. Where the processor has no record of the operation, it SHALL resubmit with the same key. An expiry worker SHALL record `PaymentExpired` for open payments past expiry with no attempt awaiting a processor outcome. Workers SHALL be safe to run concurrently and repeatedly.

#### Scenario: Crash after intent

- **WHEN** Payments stops after recording an operation request but before calling the processor
- **THEN** recovery submits that operation once with the same key and records its result

#### Scenario: Late success during pending expiry

- **WHEN** a payment is past expiry while its attempt outcome is unknown
- **THEN** it does not expire until recovery resolves the attempt, and a resolved success yields `PaymentSucceeded`

