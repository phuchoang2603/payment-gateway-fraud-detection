## ADDED Requirements

### Requirement: Cross-language simulation contracts

CI SHALL reuse coverage and add only distinct wire/identity/retry checks: packaged generated protobuf contracts, historical-compatible Parquet decoding, profile mismatch, current-time starts, historical mapping and idempotency. It SHALL verify tokens/card numbers stay out of logs/Kafka. Dedicated config/default/getter tests, exhaustive per-card tests and exact wall-clock assertions SHALL NOT be required.

#### Scenario: Protobuf events and labels

- **WHEN** CI decodes representative contracted protobuf events/labels through the Python landing adapter
- **THEN** their Parquet business columns match the existing historical Arrow schemas while original wire bytes remain available without registry framing

### Requirement: Delivery-fault runtime verification

Verification SHALL reuse focused scenarios for deterministic selection, no-fault pass-through, bounded backlog and acknowledged-prefix recovery across shared-process restart. Broker-backed recovery tests SHALL remain opt-in against an explicitly supplied integration broker; default Python CI SHALL NOT start Kafka or require Testcontainers. It SHALL NOT add redundant logging-shape or render-only tests. Evidence SHALL distinguish deliberately injected duplicates from possible restart replay and SHALL NOT claim exactly-once recovery.

#### Scenario: Workload restarted with pending publications

- **WHEN** the integration recovery check restarts the workload while events are held or awaiting acknowledgments
- **THEN** every clean record is eventually forwarded and no committed position skips unfinished work

### Requirement: Immutable landing and DP1 interface evidence

Archiver verification SHALL demonstrate historical-compatible Parquet business schemas and byte/provenance preservation, duplicate conservation, low-traffic rotation, completed-object visibility and stable selected objects across retries. The written DP1 handoff SHALL specify late-file discovery, immutable batch retry/readback, contextual history and label maturity; implementation/evidence of DP1 multi-source ingestion is deferred to its next slice. A failed prerequisite SHALL block rollout.

#### Scenario: Archiver restart after publication

- **WHEN** archival restarts after writing a completed object selected by ingestion
- **THEN** that object remains unchanged and records/provenance remain recoverable with any replay duplicates identified

### Requirement: Historical and runtime evidence boundaries

Verification SHALL preserve source datasets, published contracts/bands and prior passed/failed results. Generator diagnostics SHALL remain distinct from enforced checks under the implemented DP1 policy. Historical acceptance and one-off publication SHALL NOT imply live or v12 DP1 acceptance. Tests/builds SHALL run in CI; local verification SHALL use non-test checks.

#### Scenario: Citing historical evidence

- **WHEN** an accepted v11 run or published v12 rewrite is cited during rollout
- **THEN** it remains separate from separately measured live recovery/landing and still-pending DP1 acceptance evidence
