## Purpose

Own authoritative payment state as versioned rows with an append-only event ledger, orchestrating fraud decisions and processor effects with durable idempotency and recovery, publishing outcomes through the ledger's Kafka feed and signed webhooks.

## Requirements

### Requirement: Versioned state with append-only ledger

Payments SHALL keep authoritative payment and attempt state in versioned rows and SHALL apply each transition only when the row still has the expected version, rejecting stale writers.

#### Scenario: Complete versioned state with append-only ledger contract

- **WHEN** the `Versioned state with append-only ledger` capability is configured or exercised
- **THEN** Payments SHALL keep authoritative payment and attempt state in versioned rows and SHALL apply each transition only when the row still has the expected version, rejecting stale writers. Each transition SHALL atomically commit the state change, exactly one `payments.v1` ledger event whose `aggregate_version` equals the payment's new version, and the command idempotency state. Ledger events SHALL NOT be updated or deleted, and the Payments runtime role SHALL be able only to insert and read them. Commands and workers SHALL read current state, not rehydrate it from the ledger. Each ledger event SHALL receive a unique global sequence position; positions SHALL NOT be treated as transaction commit order or used alone as a publication cursor.

#### Scenario: Concurrent writers

- **WHEN** two commands change the same payment from the same version
- **THEN** exactly one commits and the other retries against the new state or fails

#### Scenario: Ledger immutability

- **WHEN** any Payments database role attempts to update, delete or truncate ledger events
- **THEN** the statement is rejected and the ledger is unchanged

#### Scenario: State matches ledger

- **WHEN** a transition commits or rolls back
- **THEN** the payment's latest ledger `aggregate_version` equals its row version, with no gaps or duplicates

#### Scenario: Concurrent transaction commit order

- **WHEN** a transaction allocated a lower global position but commits after another transaction
- **THEN** both committed histories remain valid, and the sequence does not imply publication order

### Requirement: Idempotent payment creation

Creating a payment SHALL be idempotent per `(integration_id, Idempotency-Key)`, and the order reference SHALL be unique per integration.

#### Scenario: Complete idempotent payment creation contract

- **WHEN** the `Idempotent payment creation` capability is configured or exercised
- **THEN** Creating a payment SHALL be idempotent per `(integration_id, Idempotency-Key)`, and the order reference SHALL be unique per integration. A repeat with the same snapshot SHALL return the original payment, session, checkout token and expiry. A repeat with a different snapshot SHALL be a conflict. Payments SHALL derive `merchant_id` and `customer_id` as UUIDv5 values of the integration and external seller or buyer ID per the identity vectors, without a provisioning step. A new payment SHALL expire at a configured time after creation (initially 15 minutes).

#### Scenario: Changed snapshot under same key

- **WHEN** a repeat creation changes the amount
- **THEN** it is rejected as a conflict and the original payment is unchanged

#### Scenario: Identity vectors

- **WHEN** payments are created for each merchant and customer case in the shared identity vectors
- **THEN** they carry exactly the IDs the vectors list

### Requirement: Scoped checkout access

Each session SHALL have a checkout token that authorizes attempts only for that session until expiry. The token SHALL be verifiable without storing it in plaintext.

#### Scenario: Token for another session

- **WHEN** an attempt presents a valid token issued for a different session
- **THEN** it is rejected

### Requirement: Idempotent attempts

Starting an attempt SHALL require an idempotency key scoped to the session. A repeated key with the same content SHALL return the recorded result without new events. A repeated key with different content SHALL be a conflict. An attempt SHALL be refused when the payment is not open or another attempt is active.

#### Scenario: Retried submission

- **WHEN** the same attempt request is sent twice
- **THEN** one attempt and one set of events exist, and both calls return the same result

#### Scenario: Retry after process restart

- **WHEN** Payments restarts after recording a completed attempt and receives the same key and request
- **THEN** it returns the stored result without another processor effect

### Requirement: Risk decision orchestration

Each attempt SHALL request a fraud decision within a configured deadline and durably record a valid assessment before any processor call. DECLINE SHALL end the attempt with a risk decline. Fraud unavailability, timeout or execution failure SHALL block processor authorization and retain a distinct recoverable assessment error, not create a FRAUD_UNAVAILABLE decline assessment. Payments SHALL reuse a completed durable assessment on retry, and SHALL atomically commit its authoritative RiskDecisionRecorded, decision evidence linkage and payment/attempt state using the existing transactional ledger/outbox publication path. Broker delivery SHALL remain at least once with downstream deduplication; a separate fraud.assessed payment event SHALL NOT be required.

#### Scenario: Fraud unavailable

- **WHEN** the fraud call times out
- **THEN** no processor operation is requested, the attempt reports operational assessment failure and recovery retains its identity without a fabricated risk decline

#### Scenario: Commit response lost

- **WHEN** Payments commits a risk decision but loses the caller response
- **THEN** retry returns recorded state without a second decision event or processor effect

### Requirement: Processor effect intent and unknown outcomes

Payments SHALL record `ProcessorOperationRequested` with a stable operation key before calling the processor and SHALL reuse that key on every retry.

#### Scenario: Complete processor effect intent and unknown outcomes contract

- **WHEN** the `Processor effect intent and unknown outcomes` capability is configured or exercised
- **THEN** Payments SHALL record `ProcessorOperationRequested` with a stable operation key before calling the processor and SHALL reuse that key on every retry. In the same transaction it SHALL persist an encrypted opaque token and request binding in a private operation-request record for crash recovery, excluded from events and later CDC publication. It SHALL clear the encrypted token when the terminal result commits. A confirmed result SHALL be recorded as success or processor decline. A timeout or lost response SHALL be recorded as `ProcessorOutcomeUnknown` and SHALL NOT be treated as a decline. The whole attempt SHALL be bounded by a configured overall budget (initially 5 s); once the effect intent has committed, exhausting it SHALL return a pending status.

#### Scenario: Processor timeout

- **WHEN** the processor does not answer within the deadline
- **THEN** the attempt records an unknown outcome and the caller receives a pending status

### Requirement: Redis idempotency record

Payments SHALL keep idempotency records for payment creation and attempts in Redis as the first check.

#### Scenario: Complete redis idempotency record contract

- **WHEN** the `Redis idempotency record` capability is configured or exercised
- **THEN** Payments SHALL keep idempotency records for payment creation and attempts in Redis as the first check. A new key SHALL be claimed atomically (set-if-absent) with a request digest and an in-flight marker before any effect, and SHALL be completed with the final result after the database commit. A repeat SHALL return the in-flight or final result, and a digest mismatch SHALL be a conflict. Records SHALL expire after a configured retention window (initially 24 hours) and SHALL NOT be evicted by memory pressure. The Postgres unique constraint on the same key, committed with the first ledger event, SHALL remain the final arbiter: on a Redis miss, loss, outage or operation over 50 ms, Payments SHALL fall back to Postgres and SHALL NOT fail open. Records SHALL NOT contain payment-method tokens, checkout credentials or sensitive request bodies.

#### Scenario: Concurrent same-key requests

- **WHEN** two identical attempt requests arrive at once
- **THEN** one claims the key and runs, and the other returns the in-flight or final result without a second attempt

#### Scenario: Redis loses a key

- **WHEN** Redis is flushed, restarted or unavailable and a completed request is retried
- **THEN** Payments returns the database-recorded result without another event or processor effect

#### Scenario: Changed request on record hit

- **WHEN** a recorded key arrives with a different request digest
- **THEN** Payments rejects the conflict and does not invoke the processor

### Requirement: Recovery and expiry workers

A recovery worker SHALL also resume stalled or retryable assessment execution under the same attempt identity with fenced ownership and bounded retries. It SHALL not rescore finalized assessments. An attempt with no processor intent MAY expire under the existing payment expiry policy; late assessment results SHALL NOT reopen expired payments or authorize effects. A recovery worker SHALL find attempts with unknown outcomes, and requested operations without a recorded outcome past a grace period. It SHALL query the processor by operation key and record the observed result. Where the processor has no record of the operation, it SHALL resubmit with the same key. An expiry worker SHALL record `PaymentExpired` for open payments past expiry with no attempt awaiting a processor outcome. Workers SHALL be safe to run concurrently and repeatedly.

#### Scenario: Crash after intent

- **WHEN** Payments stops after recording an operation request but before calling the processor
- **THEN** recovery submits that operation once with the same key and records its result

#### Scenario: Late success during pending expiry

- **WHEN** a payment is past expiry while its attempt outcome is unknown
- **THEN** it does not expire until recovery resolves the attempt, and a resolved success yields `PaymentSucceeded`

### Requirement: Declines leave the payment open

`AttemptDeclined` SHALL end only the attempt; the payment SHALL stay open for another attempt until it succeeds or expires. `PaymentSucceeded` and `PaymentExpired` SHALL be the only terminal events, and no success SHALL follow an expiry. Payments SHALL offer no cancel command in this slice.

#### Scenario: Decline is not final

- **WHEN** an attempt is declined while the payment is open
- **THEN** the payment can still succeed through a new attempt or expire

### Requirement: Signed outcome webhooks

A notifier SHALL consume `payments.events.v1` in its own consumer group and, for each integration with a configured webhook URL, record one delivery per subscribed event keyed by `event_id`, so re-consumption creates no new delivery.

#### Scenario: Complete signed outcome webhooks contract

- **WHEN** the `Signed outcome webhooks` capability is configured or exercised
- **THEN** A notifier SHALL consume `payments.events.v1` in its own consumer group and, for each integration with a configured webhook URL, record one delivery per subscribed event keyed by `event_id`, so re-consumption creates no new delivery. It SHALL POST JSON carrying the event ID, type, payment ID, order reference, attempt ID, status, reason and occurrence time, with headers `Idempotency-Key: <event_id>` and `Gateway-Signature: t=<unix>,v1=<HMAC-SHA256 of t "." body>` under the integration's signing secret. Non-2xx responses and timeouts SHALL be retried with the same event ID and body, exponential backoff and jitter up to a configured limit, then marked failed and kept inspectable. Webhook URLs SHALL be HTTPS, except in-cluster service addresses (`*.svc.cluster.local`, `*.svc`), which MAY use plaintext HTTP because the mesh encrypts them. The `Gateway-Signature` SHALL authenticate the event payload regardless of transport. Delivery SHALL be at least once, SHALL NOT block or roll back committed outcomes, and SHALL NOT include tokens or personal data.

#### Scenario: Receiver outage

- **WHEN** the webhook URL returns errors for several retries and then recovers
- **THEN** the same event ID and body are delivered with fresh signatures, and the outcome stayed committed throughout

#### Scenario: Notifier restart

- **WHEN** the notifier re-consumes events it already recorded
- **THEN** no additional delivery is created and the receiver gets each event ID at least once

### Requirement: Payment status query

Payments SHALL return a payment's status, current version and attempt summaries only to its owning integration.

#### Scenario: Other integration reads status

- **WHEN** integration B requests integration A's payment
- **THEN** the payment is reported as not found
