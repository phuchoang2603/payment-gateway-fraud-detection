# transport-fault-injection Specification

## Purpose

Apply deterministic delay, duplication and burst holds to already committed payment events within the live simulator workload, preserving authoritative history and supporting at-least-once recovery.

## Requirements

### Requirement: Post-commit faults within the live workload

The optional delivery-fault loop SHALL run within the live Python workload and consume committed events from payments.events.v1 in its own durable consumer group. It SHALL publish only to payments.events.v1.faulty, preserving key, payload and existing headers byte-for-byte. It SHALL NOT alter payment storage, authoritative events or label delivery.

#### Scenario: Faulty delivery

- **WHEN** a committed event is delayed, duplicated or burst-held
- **THEN** only delivery timing/count changes; event identity, embedded timestamps, payload bytes and authoritative history remain unchanged

### Requirement: Independently configured delivery faults

The loop SHALL support seeded duplicate and late selection/delays by event ID, plus periodic wall-clock burst holds. A zero rate or disabled hold SHALL disable the corresponding fault. Faulty Kafka timestamps SHALL reflect publication time. Late delivery is permitted to reorder events; labels SHALL retain their independent scheduling path.

#### Scenario: Repeated event selection

- **WHEN** the same event IDs are processed with the same fault configuration
- **THEN** duplicate/late choices and selected delays are the same regardless of arrival order

#### Scenario: Burst hold

- **WHEN** records become due during a configured hold period
- **THEN** they are buffered until release without changing their embedded event time or payment content

### Requirement: Bounded backlog and acknowledged recovery

Held records SHALL have count and byte limits. The consumer SHALL pause admission at those limits while maintaining required group polling. A partition's committed next offset SHALL advance only through a contiguous prefix whose required publications are acknowledged. Restart SHALL replay unfinished work without loss within source retention; extra duplicates are permitted.

#### Scenario: Restart with held records

- **WHEN** the shared workload restarts while records are held or unacknowledged
- **THEN** the durable group position replays unfinished records and every source record is eventually forwarded at least once

#### Scenario: Backlog capacity reached

- **WHEN** the configured count or byte bound is reached
- **THEN** admission pauses rather than growing memory without bound or dropping queued records

### Requirement: Pass-through and visible failures

With faults disabled, an uninterrupted pass SHALL forward each consumed record once in partition order. Recovery SHALL remain at least once. Malformed input, fatal loop failure and unfinished shutdown work SHALL be reported visibly rather than silently discarded. The runner SHALL report configured settings, consumed/published/duplicate/delayed/held counts, observed rates and backlog.

#### Scenario: All faults disabled

- **WHEN** a pass completes without restart and all faults are disabled
- **THEN** forwarded keys, headers and values match the source sequence within each partition exactly once

#### Scenario: Malformed committed payload

- **WHEN** the loop cannot decode the required event identity
- **THEN** the workload reports failure without committing past or silently dropping that record
