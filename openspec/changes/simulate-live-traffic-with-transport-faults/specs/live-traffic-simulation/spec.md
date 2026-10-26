## Purpose

Generate current-time payment traffic through gateway APIs using historical entities and behavior, publish independent labels, and define the immutable landing boundary for reusable DP1 ingestion.

## ADDED Requirements

### Requirement: Historical identities with current-time traffic

The live runner SHALL reuse historical profile/identity and behavior logic from the selected manifest and verify the profile digest before sending traffic. New commands SHALL use current machine time and gateway-owned event timestamps. A gap after historical history SHALL NOT require regeneration, catch-up, or a maximum-gap startup check.

#### Scenario: Historical history ends months before startup

- **WHEN** the selected history ends in July and the runner starts in October
- **THEN** it generates current-time commands using the historical entities without fabricating the intervening months or rejecting the calendar gap

#### Scenario: Profile mismatch

- **WHEN** rebuilt profiles do not match the manifest digest
- **THEN** startup fails before any gateway command is sent

### Requirement: Exact historical payment-method mapping

The gateway's synthetic card mapping SHALL cover all 3.9 million v8 card identities preserved in v12 with the same method fingerprints and behavior assignments. The runner SHALL obtain opaque tokens through the public gateway API. Obsolete fixture compatibility SHALL NOT be retained, and source datasets SHALL NOT be rewritten to accommodate the mapping.

#### Scenario: Token reissuance

- **WHEN** the runner tokenizes the same historical card again and receives a new token
- **THEN** gateway events retain the historical method fingerprint without exposing the token or card number in logs, events or labels

### Requirement: Idempotent gateway commands

Traffic generation SHALL use gateway HTTP/JSON APIs for tokenization, payment creation and attempts. It SHALL NOT call internal services or fabricate authoritative payment events. Each command SHALL use a run-scoped idempotency key; retries SHALL preserve key, token and request content. Concurrency SHALL be bounded and arrival lag reported.

#### Scenario: Attempt response timeout

- **WHEN** an attempt response times out and the runner retries
- **THEN** the same request and idempotency key are reused and at most one attempt is recorded

### Requirement: Shared generated protobuf contracts

Python components that encode labels or decode payment events SHALL use bindings generated from the same root protobuf contracts as Go. Those bindings and their runtime SHALL be included in the consumer/simulator build. Kafka payloads SHALL remain protobuf without a schema registry or Avro migration; the HTTP client SHALL continue using gateway JSON APIs.

#### Scenario: Cross-language event decoding

- **WHEN** Go emits a contracted payment event
- **THEN** the Python binding decodes its event identity and payload and the fault loop forwards the original bytes unchanged

### Requirement: Live labels with actual availability

The runner SHALL publish simulation-truth labels after attempt responses using the versioned FraudLabel contract. Chargebacks SHALL be delayed by the configured schedule/time scale and published only for succeeded attempts when due. Label availability SHALL equal actual publication time. Delayed labels SHALL remain independent of payment ingestion.

#### Scenario: Pending attempt when chargeback is due

- **WHEN** the scheduled attempt has not succeeded when its chargeback becomes due
- **THEN** the runner does not publish a chargeback label for that attempt

#### Scenario: Interrupted label schedule

- **WHEN** the bounded run ends or fails with pending in-memory chargebacks
- **THEN** evidence reports the unfinished schedule without claiming crash-safe label completeness

### Requirement: One shared current flow

The platform SHALL use one current environment, codebase, common runtime configuration and reusable topic/landing/bronze flow. Historical and live synthetic truth SHALL be valid analytics/training inputs with source provenance. Actual-production isolation SHALL remain future scope. Historical exports and backfills SHALL remain another DP1 input mode.

#### Scenario: Historical and live inputs

- **WHEN** DP1 ingests historical files and completed live landing objects
- **THEN** both use the shared bronze ingestion architecture with identifiable origins, without duplicate source-specific deployments

### Requirement: Immutable raw landing

The Python archiver SHALL write decoded payment events and labels as historical-compatible Parquet under s3://synthetic-source/streaming/. It SHALL retain original payload/key bytes, ordered headers and Kafka positions in reserved transport metadata, preserve duplicate deliveries and leave completed objects unchanged across retries.

#### Scenario: Typed Parquet and exact replay

- **WHEN** a supported record is archived
- **THEN** Spark can read its historical-compatible business fields directly and original payload bytes and provenance remain recoverable without protobuf decoding in DP1

#### Scenario: Retry after object publication

- **WHEN** archival restarts after an object becomes visible to ingestion
- **THEN** the selected object's content remains unchanged and source records remain recoverable, with replay duplicates reported honestly

### Requirement: Completed-file batches and retry-safe DP1

The later DP1 integration slice SHALL select an immutable set of completed objects with durable discovery and batch identity, including late files in older date folders. It SHALL preserve source origin, IDs, schema versions and event/label-availability times, use retry-safe Iceberg commits and validate/read back each batch. Event-date maxima SHALL NOT define completeness; cross-system exactly-once SHALL NOT be claimed.

#### Scenario: Late file below an observed date maximum

- **WHEN** a new completed object arrives in an older date folder
- **THEN** durable discovery includes it in a subsequent batch rather than skipping it based on event date

#### Scenario: Batch retry after commit interruption

- **WHEN** a batch retries after its Iceberg commit or progress update was interrupted
- **THEN** recovery uses the same input identity and verifies the committed result without an unsafe duplicate commit

### Requirement: Contextual validation and generator diagnostics

Bronze SHALL enforce integrity/conservation/schema/contract/identity-content/lifecycle-context/linkage/leakage checks while preserving deliveries. Context SHALL include prior history and label maturity, without treating incomplete live batches as complete histories. Generator distribution/cardinality bands SHALL be informational diagnostics; curation/dedup SHALL remain in silver.

#### Scenario: Payment and labels span batches

- **WHEN** a batch contains part of a lifecycle and labels arrive later
- **THEN** validation consults prior history and maturity context rather than requiring a self-contained history or premature labels

#### Scenario: Generator band outside expectations

- **WHEN** a batch falls outside a generator distribution/cardinality band
- **THEN** the band is reported diagnostically while enforced checks determine acceptance under DP1's implemented runtime policy

### Requirement: Training truth without leakage

Synthetic truth SHALL be a valid training target. Scenario/oracle-only fields SHALL remain excluded from features and payment decision inputs, and joins SHALL respect as-of label availability. Published manifests/config bands, source contracts and previous evidence SHALL remain unchanged; new policy SHALL NOT relabel prior failures.

#### Scenario: Label available after feature time

- **WHEN** a label becomes available after the feature's as-of time
- **THEN** it is not exposed at the earlier time and oracle/scenario fields remain excluded from features

#### Scenario: Prior failed evidence

- **WHEN** DP1 implements its revised diagnostic policy
- **THEN** previous failed and passed results keep their original status and published source contracts and bands remain unchanged
