## MODIFIED Requirements

### Requirement: Cross-language simulation contracts

CI SHALL reuse coverage and add only distinct wire, identity and retry checks: packaged generated protobuf contracts, protobuf decoding into the Bronze schema, current-time starts, historical mapping and idempotency. It SHALL verify that tokens and card numbers stay out of logs and Kafka. Dedicated config/default/getter tests, exhaustive per-card tests and exact wall-clock assertions SHALL NOT be required.

#### Scenario: Protobuf events and labels

- **WHEN** CI decodes representative contracted protobuf events and labels as DP1 Kafka mode does
- **THEN** their business columns have the same types as the same records ingested from generated historical Parquet

## ADDED Requirements

### Requirement: Rebuilt generator behavior tests

Simulator tests SHALL protect reproducibility for a small fixed configuration across worker counts and interruption, identity vectors, the entity world rebuilt from a recorded configuration, the schema version change boundary, identical duplicates, the per-day layout and manifest, refusal of a changed configuration or published version, and truth/chargeback separation. They SHALL use local outputs without object storage or network access.

#### Scenario: Offline test run

- **WHEN** simulator tests run in CI without storage credentials
- **THEN** they generate to a temporary directory and verify per-day files, the manifest and the schema change boundary

#### Scenario: Interrupted run

- **WHEN** a small run stops after some days and is rerun with a different worker count
- **THEN** its records and manifest equal those of an uninterrupted run

### Requirement: Bronze and Silver behavioral verification
Bounded CI fixtures SHALL protect the distinct behavioral risks: historical listing, per-file row checks and idempotent rerun, Kafka offset continuity, empty runs, Kafka and historical decoding parity, and atomic offset commits, lossless casting, Silver identical-duplicate collapse and conflict failure, attempt outcomes and lifecycle validity, every label linkage class and availability violation, rerun equality, and contract-to-native-test preparation. Broker-backed checks SHALL stay opt-in like existing broker tests. No configuration-only suites, per-option matrices, new CI jobs or duplicate layer assertions SHALL be added. Tests and image builds SHALL run only in CI; local checks SHALL be limited to lint, format, schema and OpenSpec validation and diff review. Ordinary CI SHALL NOT require live credentials.

#### Scenario: Verify changes without infrastructure access

- **WHEN** pipeline behavioral checks run in CI
- **THEN** local Spark and fixture data exercise the failure paths without reading live sources or contacting deployed services

#### Scenario: Label linkage cases

- **WHEN** Silver curation runs on fixtures with linked, pending and violating labels
- **THEN** each label receives its expected class and a violation fails the Silver checks

## REMOVED Requirements

### Requirement: Generator behavior tests
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Rebuilt generator behavior tests".

### Requirement: Data-pipeline behavioral verification
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Bronze and Silver behavioral verification".

### Requirement: Measured generator scale evidence
**Reason**: Full-scale publication is defined by the manifest byte total; pilot projections, resource peaks and full-data DP1/GX evidence are not part of the rebuilt generator.
**Migration**: See `historical-data-generation` "Full-scale source generation".

### Requirement: Raw inventory behavior tests
**Reason**: The task-fragment inventory is covered by per-day files.
**Migration**: See "Rebuilt generator behavior tests".

### Requirement: Historical evidence boundary
**Reason**: Refers to replaced layouts and product versions.
**Migration**: None.

### Requirement: Immutable landing and DP1 interface evidence
**Reason**: The landing archiver is removed.
**Migration**: See "Bronze and Silver behavioral verification".

### Requirement: Historical and runtime evidence boundaries
**Reason**: Refers to v11/v12 acceptance and the replaced DP1 policy.
**Migration**: None.

### Requirement: Deferred source gate verification
**Reason**: The deferred source gate declaration is removed.
**Migration**: None.

### Requirement: Recurring ingestion verification
**Reason**: Landing-batch recurring ingestion is removed.
**Migration**: See "Bronze and Silver behavioral verification".

### Requirement: Fresh integration acceptance
**Reason**: Refers to v16 acceptance and readback of the replaced integration.
**Migration**: None.
