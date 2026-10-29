## ADDED Requirements

### Requirement: Delivery-preserving Bronze tables
DP1 SHALL write payment events and fraud labels from every input into one `bronze.payment_events` and one `bronze.fraud_labels` table. It SHALL retain every delivered record, including identical duplicates, preserve source times and schema version, cast to the Bronze contract schema without loss and attach provenance: source kind, run ID, ingestion time, and either dataset version and source file or Kafka topic, partition, offset and timestamp. It SHALL NOT deduplicate or apply business rules.

#### Scenario: Duplicate deliveries

- **WHEN** an input contains repeated identical events
- **THEN** Bronze contains every copy with its own provenance

#### Scenario: Lossy conversion

- **WHEN** a source value cannot be represented losslessly in the Bronze schema
- **THEN** the run fails without committing rather than narrowing, truncating or dropping the record

### Requirement: Historical source ingestion
In historical mode, DP1 SHALL list the selected dataset version's root once, before reading any data, and SHALL compare the parquet objects found there with the files listed in `manifest.json`. A listed file that is missing, an unlisted parquet object, or an object whose size differs from the listed `bytes` SHALL fail the run before any write, naming the first mismatch. Files of schema versions 2 and 3 SHALL be read together in one pass, with fields absent from version 2 as null. The run SHALL replace only that dataset version's Bronze rows, so a rerun leaves the same content. After writing, DP1 SHALL group the written rows by source file and SHALL fail, naming the file, when a file's row count differs from the listed `rows`. Bronze SHALL NOT recompute the SHA-256 values the generator recorded; the source is read once.

#### Scenario: Missing, unlisted or resized source file

- **WHEN** a listed file is absent, an unlisted parquet object exists, or an object's size differs from the manifest
- **THEN** the run fails before writing and names the file

#### Scenario: Row count differs from the manifest

- **WHEN** a file's written rows differ from its manifest `rows`
- **THEN** the run fails and names the file

#### Scenario: Rerun the same version

- **WHEN** a dataset version is ingested twice
- **THEN** Bronze contains one copy of each delivered row from that version, and rows from other versions and from Kafka are unchanged

### Requirement: Kafka source ingestion
In Kafka mode, DP1 SHALL read `payments.events.v1` into the events table and `fraud.labels.v1` into the labels table directly from Kafka, decoding protobuf with the shared payments contracts. Each run SHALL read each partition from the offsets recorded by Bronze rows: the next offset after the highest `_kafka_offset` the table already holds for that topic (among the `_source = 'kafka'` rows for the events and labels tables, and among all rows of `bronze.rejected_records`), or from configured starting offsets for partitions without rows, up to the latest offsets at read time. No table property SHALL record progress. The good rows and their offsets SHALL commit together in one commit of the events or labels table. A record that fails protobuf decoding or a required-field check taken from the Bronze contract (an empty string counts as missing) SHALL be written to `bronze.rejected_records` with its raw key and value bytes, topic, partition, offset, Kafka timestamp, error naming the first failing check, run ID and ingestion time, in its own commit that runs before the accepted commit, so the rejected table never lags the accepted table. The run report SHALL record the start and end offset of each partition. A record that decodes but cannot be cast to the Bronze schema SHALL fail the run. A run that fails SHALL commit nothing, and a run with no new records SHALL commit nothing. If a required offset is no longer retained, the run SHALL fail rather than skip records. Historical mode SHALL keep failing on any bad file, because published sources are immutable.

#### Scenario: Consecutive runs

- **WHEN** two Kafka-mode runs execute with new records published between them
- **THEN** each record appears exactly once in Bronze or in the rejected records across both runs, with its topic, partition and offset

#### Scenario: Failure after reading

- **WHEN** a run fails after reading records but before its commit
- **THEN** the next run reads the same records again and no offsets advance without their rows

#### Scenario: Retention exceeded

- **WHEN** the recorded start offset of a partition has been deleted by Kafka retention
- **THEN** the run fails and reports the affected topic and partition

#### Scenario: Undecodable record

- **WHEN** a Kafka record fails protobuf decoding or a required-field check, such as an empty `event_id` or a label without `attempt_id`
- **THEN** it appears in `bronze.rejected_records` with its raw bytes and an error naming the failed check, the good rows of the same commit are written, and the run report and the rejected-records check show the count

### Requirement: Bronze contract checks
After each commit, the persisted Bronze tables SHALL pass the contract-derived StarRocks SQL checks for required and conditional nested fields, enumerations and supported schema versions. A failed check SHALL fail the run; Bronze checks SHALL NOT claim lifecycle, linkage, duplicate-consistency or label-availability validity, which belong to Silver.

#### Scenario: Required nested field missing

- **WHEN** a present lifecycle payload lacks one of its required fields
- **THEN** the Bronze check for that table fails with a positive violation count

## REMOVED Requirements

### Requirement: Content-preserving bronze output
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Delivery-preserving Bronze tables".

### Requirement: Completed source selection
**Reason**: Source-validation declarations, the v16 path and acceptance readback are covered by the listing and per-file row checks.
**Migration**: See "Historical source ingestion".

### Requirement: Recoverable ingestion identity
**Reason**: Covered by idempotent dataset overwrite for files and atomic offset commits for Kafka.
**Migration**: See "Historical source ingestion" and "Kafka source ingestion".

### Requirement: Persisted snapshot validation
**Reason**: Bidirectional conservation receipts and per-day count comparisons are covered by per-file row checks and SQL checks on the committed tables.
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
