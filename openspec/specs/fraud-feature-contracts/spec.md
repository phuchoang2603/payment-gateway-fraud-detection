# fraud-feature-contracts Specification

## Purpose

Define reproducible attempt-grain fraud features and their availability semantics so streaming, batch training and online serving use the same meanings without label leakage.

## Requirements

### Requirement: Versioned feature definitions

Every feature set SHALL declare a version and content hash, feature names/types/units, scoped entity keys, source fields, formulas, window boundaries, eligibility, availability and missingness behavior. Definition changes SHALL produce a new identity. Snapshot quantities SHALL have explicit snapshot semantic identities distinct from exact request-time features, even when their numeric types match. The definitions and conformance examples SHALL remain usable independently of any processing or serving framework.

#### Scenario: Different window definition

- **WHEN** a feature changes its boundary, eligible lifecycle facts or missing-value meaning
- **THEN** its definition identity changes rather than silently reusing the prior feature version

### Requirement: Minimal baseline feature set

The baseline SHALL define request amount, customer account age and nullable IP/shipping mismatch; customer/method assessed snapshot counts over 1h/24h; customer assessed amount sum over 24h, distinct observed devices over 7d and request-time elapsed seconds from the snapshot's latest eligible start within 7d (null if none or no longer in the request-time seven-day horizon). The baseline SHALL retain exactly three current-request, five customer and two method quantities; short-window submission counters SHALL NOT be model inputs. Keys SHALL include integration identity. Unknown device/IP SHALL remain absent; oracle and outcome fields SHALL NOT be model inputs.

#### Scenario: IP is unavailable

- **WHEN** an attempt lacks an observed IP country
- **THEN** its country-mismatch value is missing rather than inferred from shipping country

#### Scenario: No known historical snapshot

- **WHEN** the feature source is missing or stale
- **THEN** its values carry the defined unavailable status rather than representing unknown history as a known zero

### Requirement: Causal historical contributions

History features SHALL count each assessed attempt once only after its start context and committed risk decision are available at cutoff q. Windows SHALL use start time in [s-window,s), with s the snapshot anchor, independently of the scored attempt's time t. Snapshots SHALL NOT be described as exact request-time windows. Initial assessment SHALL precede its own committed contribution; repeated assessments SHALL reuse persisted evidence/results so the current attempt never contributes to its own vector. Declines SHALL contribute; undecided concurrent starts SHALL NOT. Observed transport/serving availability SHALL constrain eligibility when known.

#### Scenario: Current attempt arrives before feature retrieval

- **WHEN** a scored attempt's start event has already reached the stream processor
- **THEN** its unassessed start cannot contribute, and any completed retry reuses original evidence rather than a later snapshot

#### Scenario: Prior start has a late decision

- **WHEN** a prior start lies inside the event-time window but its risk decision is unavailable at q
- **THEN** it does not contribute to the snapshot at q

#### Scenario: Duplicate and conflicting events

- **WHEN** the same event identity is delivered repeatedly
- **THEN** identical deliveries contribute once and conflicting content is rejected rather than counted again

### Requirement: Shared conformance examples

The feature contract SHALL include hand-checkable inputs, cutoffs and expected outputs for snapshot boundaries, expiration without new input, duplicate delivery, reordered context/decision, missing signals, integration separation, replay clocks, coverage and current-attempt exclusion. A framework-independent reference calculation SHALL establish those outputs. Future batch and streaming implementations SHALL demonstrate conformance before claiming parity.

#### Scenario: Window lower boundary

- **WHEN** an eligible attempt starts exactly at s-window
- **THEN** it is included, while an attempt starting at s is excluded

### Requirement: Availability and serving evidence

Feature handoffs SHALL distinguish business time, source recording time, observed availability and actual serving time. Historical idealized replay SHALL NOT claim to reproduce observed live snapshots. Each view SHALL expose snapshot anchor, publication time, source progress with its meaning, window coverage and semantic identity separately. A watermark alone SHALL NOT establish complete source coverage. Serving SHALL retain actual used values, both snapshot identities/revisions and anchors, read time, freshness/missingness and model/policy identities. Missing, stale, future-dated or incompatible snapshots SHALL NOT become known zeros. A last-assessed timestamp later than the attempt time SHALL yield unavailable elapsed time rather than a negative value. Historical reconstruction SHALL use observed serving availability, not just event time.

#### Scenario: Late-arriving fact

- **WHEN** an old business event becomes available after a decision's cutoff
- **THEN** it cannot retroactively alter the feature evidence used for that decision

### Requirement: Leakage-safe training handoff

Training guidance SHALL use one scored attempt per example, chronological splits, feature warmup and label availability at the training cutoff. Truth/scenarios and postdecision outcomes SHALL stay outside features. The handoff SHALL report usable cohorts and label maturity, distinguish unreported labels from negatives, and document temporal/entity/campaign overlap and limitations.

#### Scenario: Immature chargeback target

- **WHEN** a successful attempt has no available reported chargeback at the training cutoff
- **THEN** absence alone is not interpreted as a confirmed negative

#### Scenario: Small evaluation cohort

- **WHEN** the development source has too few positives in a proposed split
- **THEN** the report records the limitation without claiming reliable model quality or fabricating extra examples
