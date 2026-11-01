# live-traffic-simulation Specification

## Purpose

Generate current-time payment traffic through gateway APIs using historical entities and behavior, publish independent labels, and define the immutable landing boundary for reusable DP1 ingestion.

## Requirements

### Requirement: Historical identities with current-time traffic

The live runner SHALL reuse historical profile/identity and behavior logic from the selected manifest and verify the profile digest before sending traffic. New commands SHALL use current machine time and gateway-owned event timestamps. A gap after historical history SHALL NOT require regeneration, catch-up, or a maximum-gap startup check.

#### Scenario: Historical history ends months before startup

- **WHEN** the selected history ends in July and the runner starts in October
- **THEN** it generates current-time commands using the historical entities without fabricating the intervening months or rejecting the calendar gap

#### Scenario: Profile mismatch

- **WHEN** rebuilt profiles do not match the manifest digest
- **THEN** startup fails before any gateway command is sent

### Requirement: Exact historical payment-method mapping

The gateway's synthetic card mapping SHALL cover the selected current-contract reference profile with the same method fingerprints and behavior assignments. Selection SHALL verify the manifest/profile digest and generator/calibration identity rather than require a fixed v8/v12 card count. The runner SHALL obtain opaque tokens through the public gateway API. Obsolete fixture compatibility SHALL NOT be retained, and source datasets SHALL NOT be rewritten to accommodate the mapping.

#### Scenario: Token reissuance

- **WHEN** the runner tokenizes the same reference card again and receives a new token
- **THEN** gateway events retain the reference method fingerprint without exposing the token or card number in logs, events or labels

#### Scenario: Unsupported reference

- **WHEN** a manifest cannot reproduce the current supported profiles or event contract
- **THEN** startup refuses it before submitting commands rather than silently using legacy identities

### Requirement: Idempotent gateway commands

Traffic generation SHALL use gateway HTTP/JSON APIs for tokenization, payment creation and attempts. It SHALL NOT call internal services or fabricate authoritative payment events. Each command SHALL use a run-scoped idempotency key; retries SHALL preserve key, token and request content. Concurrency SHALL be bounded and arrival lag reported.

#### Scenario: Attempt response timeout

- **WHEN** an attempt response times out and the runner retries
- **THEN** the same request and idempotency key are reused and at most one attempt is recorded

### Requirement: Shared generated protobuf contracts

Python components that encode labels or decode payment events SHALL use bindings generated from the same root protobuf contracts as Go. Those bindings and their runtime SHALL be included in the consumer/simulator build. Kafka payloads SHALL remain protobuf without a schema registry or Avro migration; the HTTP client SHALL continue using gateway JSON APIs.

#### Scenario: Cross-language event decoding

- **WHEN** Go emits a contracted payment event
- **THEN** the Python binding decodes its event identity and payload and the redelivery loop republishes the original bytes unchanged

### Requirement: Live labels with actual availability

The runner SHALL publish simulation-truth labels after attempt responses using the versioned FraudLabel contract. Chargebacks SHALL be delayed by the configured schedule/time scale and published only for succeeded attempts when due. Label availability SHALL equal actual publication time. Delayed labels SHALL remain independent of payment ingestion.

#### Scenario: Pending attempt when chargeback is due

- **WHEN** the scheduled attempt has not succeeded when its chargeback becomes due
- **THEN** the runner does not publish a chargeback label for that attempt

#### Scenario: Interrupted label schedule

- **WHEN** the bounded run ends or fails with pending in-memory chargebacks
- **THEN** evidence reports the unfinished schedule without claiming crash-safe label completeness

### Requirement: One shared current flow

The platform SHALL use one current environment, codebase, common runtime configuration and reusable topic-to-Bronze flow. Live payment events and labels SHALL reach Bronze only through their Kafka topics, read by DP1 in Kafka mode into the same tables as historical data. Historical and live synthetic truth SHALL be valid analytics and training inputs with source provenance. Actual-production isolation SHALL remain future scope.

#### Scenario: Historical and live inputs

- **WHEN** DP1 ingests a historical dataset and Kafka records
- **THEN** both land in the same Bronze tables with identifiable origins, without an archive stage or source-specific deployments

### Requirement: Training truth without leakage

Synthetic truth SHALL be a valid training target. Scenario/oracle-only fields SHALL remain excluded from features and payment decision inputs, and joins SHALL respect as-of label availability. Published manifests/config bands, source contracts and previous evidence SHALL remain unchanged; new policy SHALL NOT relabel prior failures.

#### Scenario: Label available after feature time

- **WHEN** a label becomes available after the feature's as-of time
- **THEN** it is not exposed at the earlier time and oracle/scenario fields remain excluded from features

#### Scenario: Prior failed evidence

- **WHEN** DP1 implements its revised diagnostic policy
- **THEN** previous failed and passed results keep their original status and published source contracts and bands remain unchanged

### Requirement: Current-contract live reference exercise

The new 90-day source SHALL support a bounded live exercise through gateway APIs at current machine time. Evidence SHALL pin source manifest/hash, profile, generator/calibration and contract identities, run and epoch. Live payment IDs SHALL remain distinct from historical payment IDs. Authoritative enriched events SHALL originate from committed Payments state, not direct simulator publication.

#### Scenario: Historical source precedes the live run

- **WHEN** the exercise selects the 90-day source months after its history ends
- **THEN** commands use current time without replaying history, filling the gap or requiring a maximum-gap exception

### Requirement: Store-and-forward attempts

The live runner SHALL optionally hold a seeded share of attempts for a configured delay range and then submit them with their original click time as the client attempt time. A zero rate SHALL send every attempt immediately with no client time. Held attempts SHALL be bounded in number. Held attempts SHALL keep their generated content and idempotency keys. Unsent held attempts at shutdown SHALL be reported. The run report SHALL record the configured and observed late counts and delays.

#### Scenario: Held attempt

- **WHEN** an attempt is selected with a seven-minute delay
- **THEN** it is submitted about seven minutes later carrying its original click time

### Requirement: Live late and duplicate evidence

The exercise SHALL demonstrate enriched-context/checkout consistency, provenance, delayed labels, store-and-forward lateness with original click times and redelivered duplicates on the shared topic, including restart after publication before offset commit. Reports SHALL distinguish injected duplicates from restart replay. Successful exercises SHALL NOT claim feature-store parity, cross-system exactly-once or downstream quality acceptance.

#### Scenario: Redelivery restarts before commit

- **WHEN** the redelivery loop stops after publishing a copy but before its offset is committed
- **THEN** restart may publish another copy, and consumers still count the event once by its identity
