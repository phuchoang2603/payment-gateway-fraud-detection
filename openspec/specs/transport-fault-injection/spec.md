# transport-fault-injection Specification

## Purpose

Apply deterministic delay, duplication and burst holds to already committed payment events within the live simulator workload, preserving authoritative history and supporting at-least-once recovery.

## Requirements

### Requirement: Redelivery onto the shared payment topic

The optional redelivery loop SHALL run within the live simulator workload and consume `payments.events.v1` in its own durable consumer group. For a seeded share selected by event identity, it SHALL publish a copy with the original key, value and headers byte-for-byte to the same partition, adding a redelivery marker header. It SHALL skip records that carry the marker. It SHALL commit offsets only after the copies before them are acknowledged. Restart MAY add further copies. It SHALL NOT alter payment storage, authoritative events or label delivery.

#### Scenario: Selected event is redelivered

- **WHEN** an event is selected for redelivery
- **THEN** consumers receive a second record with the same event identity and payload bytes on the same partition

#### Scenario: Own copy is consumed

- **WHEN** the loop reads a record carrying its marker header
- **THEN** it does not copy it again

#### Scenario: Repeated event selection

- **WHEN** the same event identities are processed with the same configuration
- **THEN** the same events are selected regardless of arrival order

### Requirement: Visible redelivery failures and evidence

A zero rate or disabled loop SHALL produce no copies. Malformed input and publication failure SHALL stop the workload visibly without committing past the affected record. The loop SHALL report its configuration and its consumed, selected and acknowledged counts, plus the observed rate.

#### Scenario: Malformed committed payload

- **WHEN** the loop cannot decode the required event identity
- **THEN** the workload reports failure without committing past that record
