## ADDED Requirements

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
