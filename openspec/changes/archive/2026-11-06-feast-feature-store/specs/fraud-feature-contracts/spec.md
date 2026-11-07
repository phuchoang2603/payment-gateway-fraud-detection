## ADDED Requirements

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
