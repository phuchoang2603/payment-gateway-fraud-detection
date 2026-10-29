## MODIFIED Requirements

### Requirement: Replay and live acceptance evidence

Acceptance for this journal-only slice SHALL reuse the existing CI conformance and integration tests, the pinned replay benchmark and recorded bounded live verification. Reports SHALL distinguish tested logical-clock conformance, duplicate and late-input handling, expiration and restart recovery from observed current-time rates, coverage and resource/latency limitations. Full 90-day source replay, sustained-load testing and a broader deployed outage exercise SHALL NOT be prerequisites for closing this slice or be claimed as demonstrated. Any replay SHALL use a distinct output epoch and SHALL NOT seed old events as current live history. Historical source immutability and prior evidence SHALL be preserved.

#### Scenario: Old development source

- **WHEN** the 90-day source is replayed for conformance
- **THEN** windows use its historical clock and outputs cannot overwrite the current live epoch

### Requirement: Hybrid snapshot emission

Snapshots SHALL update on contributions and window expirations, including expiration of distinct-device activity. Each window SHALL reflect the declared snapshot anchor and boundary. Exact device counts SHALL retain expiring activity so a device leaves the count only after its last in-window attempt expires. Processing health SHALL be distinguishable from feature changes.

#### Scenario: Quiet customer

- **WHEN** the customer sends no new attempts while old contributions expire
- **THEN** snapshots reflect those expirations without requiring another payment

#### Scenario: Repeated device

- **WHEN** one device has two attempts and only the older one leaves the 24-hour window
- **THEN** the device remains in the distinct count until its last attempt expires

## ADDED Requirements

### Requirement: Recoverable feature journal publication

Feature updates SHALL carry scoped entity/view identity, definition hash, run epoch, snapshot identity, anchor, monotonic revision, coverage and provenance. The journal SHALL be written through a checkpointed exactly-once Kafka sink, so a restart neither loses nor repeats committed updates for read-committed consumers. Each entity's revisions SHALL increase monotonically, and a consumer SHALL be able to discard an update whose revision is not newer than the one it holds. No transaction spanning Kafka and any feature store SHALL be claimed.

#### Scenario: TaskManager restart during live traffic

- **WHEN** the TaskManager is lost after some updates are committed and the job restores from its last checkpoint
- **THEN** read-committed journal consumers see each `(key, revision)` at most once with one snapshot identity, and no entity's revision regresses

### Requirement: Independent scoped attempt aggregation

The processor SHALL maintain customer and payment-method histories independently using integration-scoped keys. Every unique attempt start SHALL contribute at its client attempt time, independently of risk decisions. Identical event redelivery SHALL be idempotent; conflicting content under one event or attempt identity SHALL be dead-lettered without contributing. Deduplication state SHALL expire by event time after the 24-hour horizon plus one hour. Kafka input SHALL remain continuous even though retained history is bounded.

#### Scenario: Redelivered start

- **WHEN** an attempt start is delivered twice with identical content
- **THEN** it contributes once to every window

#### Scenario: Shared method

- **WHEN** two customers in one integration use the same method fingerprint
- **THEN** their customer histories stay separate while the method history includes both

### Requirement: Event-time late-input handling

An attempt that arrives late but whose attempt time is within 24 hours of the processing anchor SHALL contribute to the windows it belongs to and produce a new snapshot revision. An attempt time earlier than the 24-hour horizon SHALL be dropped, counted as too late and logged with its event identity without failing the job. An attempt time after its observed availability SHALL fail visibly.

#### Scenario: Store-and-forward attempt

- **WHEN** an attempt with a client time ten minutes old arrives
- **THEN** it is counted in the 1h and 24h windows, not in the 5m window, and earlier snapshots remain unchanged

#### Scenario: Attempt beyond the horizon

- **WHEN** an attempt time is 25 hours before the processing anchor
- **THEN** the too-late count increases and no window changes

### Requirement: Journal-only streaming output

The streaming job SHALL publish `fraud-snapshot-v2` updates only to `fraud.feature-updates.v2.<epoch>`, registered with the schema registry under its own subject, and per-record rejects only to `fraud.feature-rejects.v1.<epoch>`. No online or offline feature store SHALL consume the v2 journal in this slice: the Feast writer and server and the fraud service's feature retrieval SHALL be disabled in every environment, and rules SHALL stay authoritative. Serving from the v2 journal belongs to the later feature-store change.

#### Scenario: Deployed streaming slice

- **WHEN** the charts are rendered for any environment
- **THEN** Flink targets the v2 journal topic, no Feast workload is rendered and the fraud service receives no feature-retrieval configuration

### Requirement: Dead-letter output for per-record data errors

The processor SHALL route per-record data errors to a dead-letter Kafka topic `fraud.feature-rejects.v1.<epoch>` while continuing to process later records. Data errors SHALL be undecodable or unsupported events, invalid attempt context, and conflicting content under one event or attempt identity. Each reject SHALL carry the original record bytes, the event identity as key when decodable and otherwise no key, and headers with the error class, a short sanitized message and the source topic, partition, offset and record timestamp; it SHALL carry no card data or tokens. Rejects SHALL commit through the same checkpoint as source offsets and feature updates, so a reject is neither lost nor re-emitted after recovery. Each reject SHALL increment a per-class `rejected` metric and be logged. Invariant and system errors, including entity state limits, clock or offset regression, partition-layout or bootstrap changes and attempt times after observed availability, SHALL still fail the job. Too-late attempts SHALL keep their `too_late` handling and SHALL NOT be dead-lettered. The dead-letter topic is a real-time triage path; durable raw retention remains with Bronze ingestion.

#### Scenario: Malformed record between good records

- **WHEN** an undecodable record arrives between two valid attempt starts
- **THEN** it is written to the dead-letter topic with its original bytes, no key and its source coordinates, and the following valid attempt still produces features

#### Scenario: Conflicting duplicate

- **WHEN** a record reuses an existing event identity with different content
- **THEN** it is dead-lettered with that event identity as key and class `conflicting_duplicate`, and the original contribution is unchanged

#### Scenario: Invariant violation

- **WHEN** a source offset regresses within checkpoint state
- **THEN** the job fails instead of dead-lettering the record

## REMOVED Requirements

### Requirement: Independent scoped history aggregation

**Reason**: Eligibility no longer requires matching attempt starts with risk decisions.

**Migration**: Replaced by "Independent scoped attempt aggregation".

### Requirement: Recoverable ordered feature publication

**Reason**: The online-store writer is disabled in this slice; journal recovery is specified independently of any store.

**Migration**: Replaced by "Recoverable feature journal publication". Online-store write semantics return with the feature-store change.

### Requirement: Separate online state and supported retrieval

**Reason**: The streaming output ends at the feature journal; the online store, its retrieval API and offline materialization move to the later feature-store change, and Feast and the fraud service's retrieval are disabled until then.

**Migration**: The feature-store change in the roadmap build order specifies online and offline stores and retrieval.
