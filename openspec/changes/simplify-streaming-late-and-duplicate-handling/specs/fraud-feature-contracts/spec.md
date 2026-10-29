## ADDED Requirements

### Requirement: Causal attempt contributions

History features SHALL count each unique attempt start once, using its attempt time in [s-window,s), with s the snapshot anchor, independently of the scored attempt's time t. Risk decisions SHALL NOT gate eligibility. Serving SHALL use only snapshots with anchor at or before t, so the current attempt never contributes to its own vector. Snapshots SHALL NOT be described as exact request-time windows.

#### Scenario: Current attempt arrives before feature retrieval

- **WHEN** a scored attempt's start event has already reached the stream processor
- **THEN** any snapshot including it has an anchor after t and is unusable for that attempt

#### Scenario: Duplicate and conflicting events

- **WHEN** the same event identity is delivered repeatedly
- **THEN** identical deliveries contribute once and conflicting content is rejected rather than counted again

## MODIFIED Requirements

### Requirement: Minimal baseline feature set

The baseline SHALL define request amount, customer account age and nullable IP/shipping mismatch. It SHALL define customer and method attempt snapshot counts over 5m, 1h and 24h, and customer distinct observed devices over 24h. The baseline SHALL contain exactly three current-request, four customer and three method quantities. Authorization submission counters SHALL NOT be model inputs. Keys SHALL include integration identity. Unknown device/IP SHALL remain absent; oracle and outcome fields SHALL NOT be model inputs.

#### Scenario: IP is unavailable

- **WHEN** an attempt lacks an observed IP country
- **THEN** its country-mismatch value is missing rather than inferred from shipping country

#### Scenario: No known historical snapshot

- **WHEN** the feature source is missing or stale
- **THEN** its values carry the defined unavailable status rather than representing unknown history as a known zero

### Requirement: Shared conformance examples

The feature contract SHALL include hand-checkable inputs, cutoffs and expected outputs for snapshot boundaries, expiration without new input, duplicate delivery, late input inside and beyond the 24-hour horizon, missing signals, integration separation, replay clocks, coverage and current-attempt exclusion. A framework-independent reference calculation SHALL establish those outputs. Batch and streaming implementations SHALL demonstrate conformance before claiming parity.

#### Scenario: Window lower boundary

- **WHEN** an attempt starts exactly at s-window
- **THEN** it is included, while an attempt starting at s is excluded

### Requirement: Availability and serving evidence

Feature handoffs SHALL distinguish attempt time, source recording time, observed availability and actual serving time. Each view SHALL expose snapshot anchor, source progress with its meaning, window coverage and semantic identity separately. A watermark alone SHALL NOT establish complete source coverage. Serving SHALL retain actual used values, both snapshot identities/revisions and anchors, read time, freshness/missingness and policy identities. Missing, stale, future-dated or incompatible snapshots SHALL NOT become known zeros. Historical reconstruction SHALL use observed serving availability, not just event time.

#### Scenario: Late-arriving fact

- **WHEN** an attempt becomes available after a decision's cutoff
- **THEN** it cannot retroactively alter the feature evidence used for that decision

## REMOVED Requirements

### Requirement: Causal historical contributions

**Reason**: Risk decisions no longer gate history eligibility.

**Migration**: Replaced by "Causal attempt contributions".
