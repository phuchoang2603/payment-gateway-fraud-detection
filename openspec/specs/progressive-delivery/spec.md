# progressive-delivery Specification

## Purpose

Release public checkout changes progressively while preserving ingress security and restoring stable traffic when a candidate fails or cannot be evaluated safely.

## Requirements

### Requirement: Bounded checkout canary

The deployment SHALL support weighted stable/candidate traffic for the existing public Edge checkout route. The hostname, checkout-only path, trusted proxy header, timeout, readiness and mesh authorization boundaries SHALL remain enforced for both revisions. Internal Edge API traffic SHALL remain on the stable revision until promotion. Other services and background consumers SHALL retain their existing deployment behavior.

#### Scenario: Candidate traffic

- **WHEN** a ready Edge candidate enters a configured canary step
- **THEN** only the configured fraction of public checkout traffic reaches it and remaining traffic reaches stable
- **AND** the internal JSON API is not made public or routed to the candidate

#### Scenario: Candidate is not ready

- **WHEN** candidate pods cannot become ready within the rollout deadline
- **THEN** the release does not advance and ready stable capacity remains available

### Requirement: Measured promotion and traffic rollback

Promotion SHALL require fresh candidate-specific request count, error rate and latency measurements meeting explicit minimum traffic and configured thresholds over a defined observation window. Analysis SHALL exclude health probes and distinguish transport failures from valid business declines. Missing, stale, non-finite or erroneous measurements SHALL NOT permit promotion. Failed analysis SHALL restore public traffic to stable; insufficient traffic SHALL hold advancement and terminate analysis within a configured deadline without removing stable capacity.

#### Scenario: Healthy candidate

- **WHEN** every configured step has sufficient fresh candidate observations and passes all gates
- **THEN** the candidate becomes stable and the internal service selects the promoted revision

#### Scenario: Candidate regression

- **WHEN** a candidate exceeds the error or latency threshold
- **THEN** candidate traffic returns to zero and the failed release is visible to operators

#### Scenario: Quiet or unavailable telemetry

- **WHEN** candidate traffic is insufficient or the metrics query returns no data, stale data, NaN, infinity or an error
- **THEN** promotion is blocked and stable capacity is retained
- **AND** the configured analysis deadline prevents an unbounded wait

### Requirement: Coordinated GitOps ownership and adoption

GitOps SHALL own release configuration and security while the rollout controller owns active traffic weights and revision selectors. Reconciliation SHALL NOT reset a running traffic step. Migration from the Edge Deployment SHALL maintain ready stable endpoints and finish with one workload controller owning Edge. Traffic abort and source rollback SHALL be separately operable without changing payment persistence or event contracts.

#### Scenario: Reconcile during a canary

- **WHEN** Argo CD reconciles while candidate traffic is nonzero
- **THEN** active weights and revision selectors remain controller-owned while unauthorized hostname, path or security drift is corrected

#### Scenario: Adopt or revert Edge ownership

- **WHEN** the workload controller is migrated or the migration is reverted
- **THEN** the stable route retains ready endpoints throughout the handover
- **AND** the completed migration leaves no legacy Deployment controller
