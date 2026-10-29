## MODIFIED Requirements

### Requirement: Hybrid snapshot emission

Snapshots SHALL update on contributions and window expirations, including expiration of distinct-device activity. Each window SHALL reflect the declared snapshot anchor and boundary. Exact device counts SHALL retain expiring activity so a device leaves the count only after its last in-window attempt expires. Processing health SHALL be distinguishable from feature changes.

#### Scenario: Quiet customer

- **WHEN** the customer sends no new attempts while old contributions expire
- **THEN** snapshots reflect those expirations without requiring another payment

#### Scenario: Repeated device

- **WHEN** one device has two attempts and only the older one leaves the 24-hour window
- **THEN** the device remains in the distinct count until its last attempt expires

## ADDED Requirements

### Requirement: Independent scoped attempt aggregation

The processor SHALL maintain customer and payment-method histories independently using integration-scoped keys. Every unique attempt start SHALL contribute at its client attempt time, independently of risk decisions. Identical event redelivery SHALL be idempotent; conflicting identity/content SHALL fail visibly. Deduplication state SHALL expire by event time after the 24-hour horizon plus one hour. Kafka input SHALL remain continuous even though retained history is bounded.

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

## REMOVED Requirements

### Requirement: Independent scoped history aggregation

**Reason**: Eligibility no longer requires matching attempt starts with risk decisions.

**Migration**: Replaced by "Independent scoped attempt aggregation".
