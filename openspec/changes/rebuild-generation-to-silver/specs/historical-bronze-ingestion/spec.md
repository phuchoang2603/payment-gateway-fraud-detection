## ADDED Requirements

### Requirement: Delivery-preserving Bronze tables
DP1 SHALL write payment events and fraud labels from every input into one `bronze.payment_events` and one `bronze.fraud_labels` table. It SHALL retain every delivered record, including identical duplicates, preserve source times and physical schema version, cast to the Bronze contract schema without loss and attach provenance: source kind, run ID, ingestion time, and either dataset version and source file or Kafka topic, partition, offset and timestamp. It SHALL NOT deduplicate or apply business rules.

#### Scenario: Duplicate deliveries

- **WHEN** an input contains repeated identical events
- **THEN** Bronze contains every copy with its own provenance

#### Scenario: Lossy conversion

- **WHEN** a source value cannot be represented losslessly in the Bronze schema
- **THEN** the run fails without committing rather than narrowing, truncating or dropping the record

### Requirement: Historical source ingestion
In historical mode, DP1 SHALL ingest exactly the files listed in a selected dataset version's `manifest.json`, verifying each file's SHA-256 and row count before writing. Files of physical schema versions 3 and 4 SHALL be read together, with absent version-3 fields as null. The run SHALL replace only that dataset version's Bronze rows, so a rerun leaves the same content. After writing, Bronze row counts for the dataset version SHALL equal the manifest totals per table or the run SHALL fail. Source files and the manifest SHALL remain unchanged.

#### Scenario: Missing or altered source

- **WHEN** the manifest is absent or a listed file fails its checksum or row count
- **THEN** the run fails before writing and identifies the file

#### Scenario: Rerun the same version

- **WHEN** a dataset version is ingested twice
- **THEN** Bronze contains one copy of each delivered row from that version, and rows from other versions and from Kafka are unchanged

### Requirement: Kafka source ingestion
In Kafka mode, DP1 SHALL read `payments.events.v1` into the events table and `fraud.labels.v1` into the labels table directly from Kafka, decoding protobuf with the shared payments contracts. Each run SHALL read from the offsets recorded by that table's latest Bronze commit, or from configured starting offsets for the first run, up to the latest offsets at read time. It SHALL commit the rows and their ending offsets in the same table commit. A run that fails SHALL commit nothing, and a run with no new records SHALL commit nothing. If a required offset is no longer retained, the run SHALL fail rather than skip records.

#### Scenario: Consecutive runs

- **WHEN** two Kafka-mode runs execute with new records published between them
- **THEN** each record appears exactly once in Bronze across both runs, with its topic, partition and offset

#### Scenario: Failure after reading

- **WHEN** a run fails after reading records but before its commit
- **THEN** the next run reads the same records again and no offsets advance without their rows

#### Scenario: Retention exceeded

- **WHEN** the recorded start offset of a partition has been deleted by Kafka retention
- **THEN** the run fails and reports the affected topic and partition

### Requirement: Bronze contract checks
After each commit, the persisted Bronze tables SHALL pass the contract-derived StarRocks SQL checks for required and conditional nested fields, enumerations and supported physical schema versions. A failed check SHALL fail the run; Bronze checks SHALL NOT claim lifecycle, linkage, duplicate-consistency or label-availability validity, which belong to Silver.

#### Scenario: Required nested field missing

- **WHEN** a present lifecycle payload lacks one of its required fields
- **THEN** the Bronze check for that table fails with a positive violation count

## REMOVED Requirements

### Requirement: Content-preserving bronze output
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Delivery-preserving Bronze tables".

### Requirement: Completed source selection
**Reason**: Source-validation declarations, the v16 path and acceptance readback are replaced by direct manifest verification.
**Migration**: See "Historical source ingestion".

### Requirement: Recoverable ingestion identity
**Reason**: Replaced by idempotent dataset overwrite for files and atomic offset commits for Kafka.
**Migration**: See "Historical source ingestion" and "Kafka source ingestion".

### Requirement: Persisted snapshot validation
**Reason**: Bidirectional conservation receipts and per-day count comparisons are replaced by a manifest row-total check and SQL checks on the committed tables.
**Migration**: See "Historical source ingestion" and "Bronze contract checks".

### Requirement: Product acceptance boundary
**Reason**: Accepted-record pointers are removed; a run succeeds only when its checks pass, and consumers read the current tables.
**Migration**: None.

### Requirement: Bounded pilot execution
**Reason**: The v16 pilot no longer exists; scale evaluation is deferred.
**Migration**: None.

### Requirement: Shared recurring source boundary
**Reason**: Landing batches are removed; live data is read from Kafka.
**Migration**: See "Kafka source ingestion".

### Requirement: Context-aware recurring validation
**Reason**: History-dependent validation is owned by Silver.
**Migration**: See the `curated-payments` capability.

### Requirement: Dataset diagnostics are distinct from ingestion validity
**Reason**: Diagnostics are a separate on-demand job.
**Migration**: See "Generator diagnostics" in `historical-data-generation`.

### Requirement: Raw-ingestion boundary and recurring delivery acceptance
**Reason**: Landing manifests, ledgers, epochs and transport envelopes are removed.
**Migration**: See "Kafka source ingestion" and "Bronze contract checks".

### Requirement: Fresh DP1 state boundary
**Reason**: The one-time reset is a migration step of this change, not a lasting requirement.
**Migration**: See design.md, Migration Plan.
