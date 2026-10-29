## ADDED Requirements

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

## MODIFIED Requirements

### Requirement: Shared generated protobuf contracts

Python components that encode labels or decode payment events SHALL use bindings generated from the same root protobuf contracts as Go. Those bindings and their runtime SHALL be included in the consumer/simulator build. Kafka payloads SHALL remain protobuf without a schema registry or Avro migration; the HTTP client SHALL continue using gateway JSON APIs.

#### Scenario: Cross-language event decoding

- **WHEN** Go emits a contracted payment event
- **THEN** the Python binding decodes its event identity and payload and the redelivery loop republishes the original bytes unchanged

## REMOVED Requirements

### Requirement: Live transport and recovery evidence

**Reason**: The Python landing archiver is removed; Parquet preservation and archiver restart no longer apply to the live exercise.

**Migration**: Replaced by "Live late and duplicate evidence"; Kafka-to-Bronze recovery belongs to the Bronze ingestion capability.
