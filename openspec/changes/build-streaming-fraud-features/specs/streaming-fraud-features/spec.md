## Purpose

Provide continuously updated assessed-history snapshots with explicit coverage, recoverable publication and supported online feature retrieval.

## ADDED Requirements

### Requirement: Independent scoped history aggregation

The processor SHALL maintain customer and payment-method histories independently using integration-scoped keys. Only unique attempts with available start context and committed risk decisions SHALL contribute. Identical event redelivery SHALL be idempotent; conflicting identity/content SHALL fail visibly. Unmatched context and decisions SHALL have bounded retention and an observable unresolved status rather than fabricated contributions. Kafka input SHALL remain continuous even though retained history is bounded.

#### Scenario: Reordered assessment

- **WHEN** a committed decision arrives before its start context
- **THEN** it contributes once after both become available and never before

#### Scenario: Shared method

- **WHEN** two customers in one integration use the same method fingerprint
- **THEN** their customer histories stay separate while the method history includes both

### Requirement: Hybrid snapshot emission

Snapshots SHALL update on eligible contributions and relevant window expirations, including expiration of distinct-device activity and last-assessed timestamps. Each window SHALL reflect the declared snapshot anchor and boundary. Exact device counts SHALL retain sufficient expiring activity to remove a device only after its last in-window contribution expires. Processing health SHALL be distinguishable from feature changes.

#### Scenario: Quiet customer

- **WHEN** the customer sends no new attempts while old contributions expire
- **THEN** snapshots reflect those expirations without requiring another payment

#### Scenario: Repeated device

- **WHEN** one device has two contributions and only the older one leaves the seven-day window
- **THEN** the device remains in the distinct count until its last eligible contribution expires

### Requirement: Recoverable ordered feature publication

Feature updates SHALL carry scoped entity/view identity, definition hash, run epoch, snapshot identity, anchor, monotonic revision, coverage and provenance. Updates SHALL be recoverable independently of the online store. Duplicate retries SHALL be harmless, and delayed older updates SHALL NOT replace newer online state. Publication and store writes SHALL NOT claim a transaction spanning Kafka, the processor and the online store.

#### Scenario: Writer restart after successful store write

- **WHEN** the writer stops after storing a snapshot but before committing its input offset
- **THEN** replay preserves that snapshot or a newer revision without regression

### Requirement: Separate online state and supported retrieval

Historical snapshots SHALL use a dedicated online-store deployment separate from authorization counters. Retrieval SHALL use the feature store's supported API with bounded deadlines, independent customer/method statuses and version checks. Independent views SHALL NOT imply a coordinated snapshot. Offline materialization SHALL NOT race the active online writer.

#### Scenario: One view lags

- **WHEN** customer history is fresh but method history exceeds its freshness limit
- **THEN** the result preserves customer availability and marks method history unavailable

### Requirement: Replay and live acceptance evidence

Acceptance for this shadow/observe-only slice SHALL reuse existing CI conformance/integration tests and recorded bounded live verification. Reports SHALL distinguish tested logical-clock conformance, duplicate/reorder/late-input handling, expiration, restart recovery and online readback from observed current-time freshness, coverage and resource/latency limitations. Full 90-day source replay, sustained-load testing and a broader deployed outage exercise SHALL NOT be prerequisites for closing this slice or be claimed as demonstrated. Any replay SHALL use a distinct output epoch and SHALL NOT seed old events as current live history. Historical source immutability and prior evidence SHALL be preserved.

#### Scenario: Old development source

- **WHEN** the 90-day source is replayed for conformance
- **THEN** windows use its historical clock and outputs cannot overwrite the current online epoch
