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

### Requirement: Streaming attempt-history definition v2

`fraud-snapshot-v2` SHALL define the streaming attempt-history quantities: customer and payment-method attempt counts over 5m, 1h and 24h, and customer distinct observed devices over 24h. It SHALL count each unique attempt start once, using its attempt time in [s-window,s), with s the snapshot anchor; risk decisions SHALL NOT gate eligibility. Keys SHALL include integration identity, unknown devices SHALL remain absent, and unknown coverage SHALL be null rather than zero. Its wire messages SHALL live in the separate `features.v2` protobuf package with their own definition identity. `fraud-snapshot-v1` and the `features.v1` package SHALL remain unchanged for existing serving until the feature-store change replaces it.

#### Scenario: Window lower boundary

- **WHEN** an attempt starts exactly at s-window
- **THEN** it is included, while an attempt starting at s is excluded

#### Scenario: Duplicate and conflicting events

- **WHEN** the same event identity is delivered repeatedly
- **THEN** identical deliveries contribute once and conflicting content is rejected rather than counted again

### Requirement: Streaming definition v2 conformance examples

The v2 definition SHALL include hand-checkable inputs, cutoffs and expected outputs for window boundaries, duplicate delivery, late input inside and beyond the 24-hour horizon, unavailable inputs, integration separation, coverage and current-attempt exclusion. A framework-independent v2 reference calculation, kept alongside the v1 reference, SHALL establish those outputs, and the streaming job SHALL demonstrate conformance with it.

#### Scenario: Store-and-forward attempt inside the horizon

- **WHEN** an attempt with an attempt time ten minutes before the anchor becomes available before the cutoff
- **THEN** it counts in the 1h and 24h windows and not in the 5m window

### Requirement: Batch long-window definition v1
`fraud-history-batch-v1` SHALL define the daily-anchored batch history quantities for customers and payment methods over 7- and 30-day windows. Every quantity SHALL use the same anchor, eligibility and coverage rules:

- **Anchor:** `s` is a UTC midnight.
- **Attempt time:** the client attempt time when observed, otherwise the server start time.
- **Eligibility:** a lifecycle-valid attempt is eligible only when its attempt time is in `[s − window, s)` and its server start is before `s`.
- **Outcome-based quantities:** these count only outcomes that occurred before `s`.
- **Rates:** each rate SHALL name its numerator and denominator, and SHALL be null when the denominator is zero.
- **Coverage:** a window not fully covered by history SHALL be null.
- **Keys:** keys SHALL include integration identity, and unknown devices SHALL stay absent.

The definition SHALL include hand-checkable conformance examples for:

- window boundaries;
- expiry with no new input;
- client and server time disagreement;
- outcomes after the anchor;
- an empty denominator;
- coverage;
- integration separation.

The DP3 job SHALL reproduce those examples in CI. Labels, scenarios and the scored attempt's own decision and outcome SHALL NOT be inputs.

#### Scenario: Outcome after the anchor

- **WHEN** an attempt inside the 7-day window is declined after the anchor
- **THEN** it counts toward the attempt count but not toward the declined or decided counts at that anchor

#### Scenario: Window lower boundary

- **WHEN** an eligible attempt has attempt time exactly `s − 7 days`
- **THEN** it counts in the 7-day window at `s`, while an attempt at `s` does not

### Requirement: Feature-store serving parity
Offline retrieval and online serving of a feature service SHALL apply the same availability rules at the scored time `t`:

- the latest row with event time at or before `t`;
- unknown when that row is older than the view's TTL;
- unknown when the entity has no row;
- unknown where window coverage is unknown.

Unknown SHALL never become zero in either store. Online serving holds only the newest written row, and the documentation SHALL state three differences from offline retrieval:

- when that row is after `t`, serving SHALL report the view as unavailable;
- daily rows reach serving only after the batch definition's documented availability delay;
- stream values reach serving only after the streaming job's processing delay.

A TTL change SHALL change the feature service version.

#### Scenario: Same entity at the same time

- **WHEN** a training example and a live decision for one entity both have time `t`, and every input before `t` has reached the online store
- **THEN** both see the same values and the same unknowns for every feature of the service

#### Scenario: Newer row than the scored time ignored online

- **WHEN** the newest stored stream snapshot is after `t`
- **THEN** serving reports the view as unavailable

#### Scenario: Daily anchor not yet materialized

- **WHEN** a decision at 02:00 UTC precedes that day's DP3 run and materialization
- **THEN** serving returns the previous anchor's row, while offline retrieval at the same `t` returns the new anchor's row, as the availability delay documents

### Requirement: One implementation per streaming definition
Training history for a streaming definition SHALL be produced by the streaming job that serves it, replayed over recorded events, never by a second implementation. The streaming job SHALL pass the definition's conformance examples, which SHALL include attempts just inside and just past the lateness horizon. History rows SHALL carry a history epoch distinct from every live epoch, and their availability assumption (logical time, no processing delay) SHALL be documented. History and live rows SHALL NOT overlap in time for any entity, and training data SHALL keep the epoch of each stream value.

#### Scenario: Lateness boundary

- **WHEN** one attempt arrives just inside the 24-hour horizon and another just past it
- **THEN** the streaming job counts the first and drops the second, in both live and replay
