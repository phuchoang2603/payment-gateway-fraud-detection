# authorization-velocity Specification

## Purpose

Detect immediate submission bursts independently of asynchronous assessed-history features while preserving attempt retry identity and explicit failure behavior.

## Requirements

### Requirement: Atomic per-entity submission registration

The guard SHALL atomically deduplicate and register a durable scoped attempt identity within each customer and payment-method entity and return 10s/60s submission counts including the current attempt. Windows SHALL use the guard's server clock and a documented boundary. Retry SHALL retain original registration time, and the same identity with conflicting entity binding SHALL fail. Deduplication retention SHALL cover the configured assessment retry horizon. Customer and method registration SHALL NOT claim cross-entity atomicity.

#### Scenario: Lost response after registration

- **WHEN** registration succeeds but its response is lost
- **THEN** retry does not increment again and can complete a missing registration for the other entity

#### Scenario: Simultaneous attempts

- **WHEN** distinct attempts register concurrently for one entity
- **THEN** each atomic operation returns a count reflecting its position in that entity's registration order

### Requirement: Submission and decision populations remain distinct

Registration SHALL NOT mean a fraud decision was completed. Counter observations SHALL remain separate from assessed-history features, SHALL NOT be added to their counts and SHALL NOT expand the ten-feature model vector. Finalized assessment retries SHALL reuse durable evidence instead of registering again. Operational failures SHALL NOT be labeled as fraud declines.

#### Scenario: Assessment fails after registration

- **WHEN** a registered attempt encounters an execution error
- **THEN** its submission remains counted for the defined window but no completed assessment is inferred

### Requirement: Isolated bounded guard and rollout

Authorization counters SHALL use a deployment and credentials separate from the historical online store, bounded retention, no silent memory eviction and explicit health/recovery status. The first rollout SHALL observe configured rule thresholds without enforcing them. Counter errors SHALL be recorded as unknown; enforcement SHALL require explicit thresholds and a failure policy that blocks authorization when required guard evaluation is unavailable.

#### Scenario: Counter restart loses history

- **WHEN** counter history is incomplete after recovery
- **THEN** the guard reports degraded coverage rather than presenting its counts as complete

#### Scenario: Observe-only threshold exceeded

- **WHEN** a submission exceeds a configured threshold in observe-only mode
- **THEN** the observation is retained without changing the existing rules outcome
