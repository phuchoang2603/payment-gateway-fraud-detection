## MODIFIED Requirements

### Requirement: Exact historical payment-method mapping

The gateway's synthetic card mapping SHALL cover the selected current-contract reference profile with the same method fingerprints and behavior assignments. Selection SHALL verify the manifest/profile digest and generator/calibration identity rather than require a fixed v8/v12 card count. The runner SHALL obtain opaque tokens through the public gateway API. Obsolete fixture compatibility SHALL NOT be retained, and source datasets SHALL NOT be rewritten to accommodate the mapping.

#### Scenario: Token reissuance

- **WHEN** the runner tokenizes the same reference card again and receives a new token
- **THEN** gateway events retain the reference method fingerprint without exposing the token or card number in logs, events or labels

#### Scenario: Unsupported reference

- **WHEN** a manifest cannot reproduce the current supported profiles or event contract
- **THEN** startup refuses it before submitting commands rather than silently using legacy identities

## ADDED Requirements

### Requirement: Current-contract live reference exercise

The new 90-day source SHALL support a bounded live exercise through gateway APIs at current machine time. Evidence SHALL pin source manifest/hash, profile, generator/calibration and contract identities, run and epoch. Live payment IDs SHALL remain distinct from historical payment IDs. Authoritative enriched events SHALL originate from committed Payments state, not direct simulator publication.

#### Scenario: Historical source precedes the live run

- **WHEN** the exercise selects the 90-day source months after its history ends
- **THEN** commands use current time without replaying history, filling the gap or requiring a maximum-gap exception

### Requirement: Schema cutover without shared-data deletion

New-contract landing SHALL use an explicit epoch and frozen start offsets where required to exclude incompatible retained records. Cutover SHALL quiesce affected owned writers before changing progress and SHALL retain old objects and shared Kafka assets. The new epoch SHALL NOT reuse stale checkpoints or select prior-shape batches as current-contract data.

#### Scenario: Old records retained on the topic

- **WHEN** the current-contract exercise starts while earlier-schema records remain retained
- **THEN** explicit epoch/offset selection isolates the new exercise without deleting topic history or rewriting landed files

### Requirement: Live transport and recovery evidence

The exercise SHALL demonstrate enriched-context/checkout consistency, protobuf-to-Parquet field preservation, provenance, delayed labels, configured delay/duplicate/burst behavior and restart after publication before offset acknowledgment. Completed objects SHALL remain immutable; retry overlap SHALL be reported. Successful source acceptance SHALL NOT claim feature-store parity, cross-system exactly-once or downstream quality acceptance.

#### Scenario: Archiver restarts after upload

- **WHEN** an archiver stops after publishing a completed batch but before Kafka progress is acknowledged
- **THEN** recovery preserves the published bytes and source offsets and reports any replay overlap without silently losing source records
