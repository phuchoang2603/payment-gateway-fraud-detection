## Purpose

Specify the internal fraud decision RPC, its input validation, the stateless rules baseline and the versions reported with every decision.

## Requirements

### Requirement: Decision RPC

The fraud service SHALL expose `fraud.v1.FraudService/Decide` over internal gRPC. A request SHALL carry `integration_id`, `payment_id`, `attempt_id`, merchant and customer references, the checkout snapshot and the attempt signals. A response SHALL carry a `payments.v1.RiskAssessment` (an `APPROVE` or `DECLINE` outcome, zero or more reason codes and the policy version, plus model version, feature-set version and risk score only when a model or feature set contributed) and the evaluation time. A decline SHALL be a successful response, not an error status. The request identity and validated content digest SHALL identify one durable assessment. Responses SHALL expose assessment/evidence identity for Payments to commit; operational errors SHALL remain distinguishable from fraud outcomes.

#### Scenario: Rule decline

- **WHEN** a valid request matches a decline rule
- **THEN** the RPC returns OK with outcome `DECLINE` and the matching reason code

#### Scenario: Rules-only versions

- **WHEN** a decision is produced without a model
- **THEN** the response carries policy version `rules-v1` and omits model version, feature-set version and risk score

### Requirement: Decision input validation

The service SHALL reject with `INVALID_ARGUMENT` a request missing any identifier, the attempt time, the snapshot amount, shipping country, customer account creation time, or payment-method fingerprint, and a request whose snapshot is invalid. Absent optional signals SHALL NOT be replaced by defaults that could match a rule. Error messages SHALL NOT echo request values.

#### Scenario: Missing amount

- **WHEN** a request omits the snapshot amount
- **THEN** the service returns `INVALID_ARGUMENT` rather than treating the amount as zero

#### Scenario: Unsupported currency

- **WHEN** a request uses a currency the active policy does not define thresholds for
- **THEN** the service returns `INVALID_ARGUMENT`

### Requirement: Rules baseline policy

Policy `rules-v1` SHALL support USD and SHALL decline with `HIGH_AMOUNT` when the amount exceeds 2,000.00 USD. It SHALL decline with `NEW_ACCOUNT_GEO_MISMATCH` when the IP country is present and differs from the shipping country, the customer account is younger than 7 days at attempt time, and the amount exceeds 200.00 USD. Otherwise it SHALL approve. All matching reason codes SHALL be returned.

#### Scenario: High-amount boundary

- **WHEN** the amount is exactly 2,000.00 USD with no other rule matching
- **THEN** the outcome is `APPROVE`, and 2,000.01 USD yields `DECLINE` with `HIGH_AMOUNT`

#### Scenario: Account age boundary

- **WHEN** a geo-mismatched attempt above 200.00 USD comes from an account exactly 7 days old
- **THEN** `NEW_ACCOUNT_GEO_MISMATCH` does not match

#### Scenario: IP country absent

- **WHEN** the IP country is absent
- **THEN** `NEW_ACCOUNT_GEO_MISMATCH` does not match

### Requirement: Stateless deterministic decisions

The pure baseline rules SHALL remain deterministic for their input and policy version. Assessment execution SHALL use durable integration/payment/attempt identity, request-content binding, fenced ownership and states NEW, ASSESSING, COMPLETED or ASSESSMENT_ERROR. A completed result and its evidence SHALL be persisted before returning and reused across replicas, retries and response loss. Conflicting content SHALL be rejected. A retryable error SHALL resume under the same identity; an in-flight assessment SHALL NOT create concurrent unfenced work. Redis registration SHALL remain a separate per-entity fact, not a durable COMPLETED assessment. Existing APPROVE/DECLINE outcomes SHALL remain unchanged; REVIEW is outside this slice.

#### Scenario: Repeated request

- **WHEN** the same request is decided twice by different instances with the same policy version
- **THEN** both return the persisted completed result and evidence without another registration or processor authorization

#### Scenario: Worker takeover

- **WHEN** a worker resumes an expired assessment claim
- **THEN** a stale worker cannot overwrite its completed result

#### Scenario: Response lost

- **WHEN** an assessment commits internally but the caller loses the response
- **THEN** the same request returns that result even if online snapshots have since changed

### Requirement: Independent bounded dependency semantics

Velocity and historical-feature retrieval SHALL have separate deadlines, statuses and policy modes. Initial velocity checks SHALL be observe-only and Feast retrieval SHALL be shadow-only: their failure SHALL be recorded without changing the existing valid rules decision. A required dependency or assessment execution failure SHALL block authorization with an operational error, not fabricate a fraud decline or silently approve. Future model enforcement SHALL require an explicit compatible missing-feature policy. Counter observations and snapshot values SHALL remain separate populations.

#### Scenario: Feast unavailable during shadow rollout

- **WHEN** Feast times out while the rules evaluator remains healthy
- **THEN** the rules result remains valid and evidence records historical features as unavailable

#### Scenario: Assessment execution fails

- **WHEN** the assessment cannot complete durably
- **THEN** it records an operational error and returns no successful fraud decision
