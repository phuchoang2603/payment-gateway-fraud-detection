## MODIFIED Requirements

### Requirement: Generator behavior tests

Simulator tests SHALL protect reproducibility for a small fixed configuration, identity vectors, Arrow schema parity with ODCS, schema evolution, duplicates, format-3 raw task inventory, physical schema/date counts, truth separation and publication refusal on integrity or byte failure. Small tests SHALL also protect worker-independent records, verified resume, publication exclusivity and resource/size failure gates. They SHALL use local outputs and storage fault doubles without requiring object storage or network access.

#### Scenario: Offline test run

- **WHEN** simulator tests run in CI without storage credentials
- **THEN** they generate to a temporary directory and verify raw publication behaviors without native DuckDB validation

#### Scenario: Interrupted upload and changed execution settings

- **WHEN** a small run stops around upload/checkpoint acknowledgement and resumes with changed parallelism
- **THEN** its records, counts, physical schemas and duplicate behavior equal an uninterrupted run
- **AND** corrupt output, incompatible inputs or a competing writer cannot be silently accepted

#### Scenario: Bounded-resource failure

- **WHEN** an injected scratch or upload limit prevents completion
- **THEN** the test verifies diagnostic failure and absent completion manifest while valid checkpointed work remains resumable

#### Scenario: Normal command lifecycle

- **WHEN** the same command is run against prepared or completed output
- **THEN** it automatically resumes compatible preparation or finishes protection and cleanup without additional mode flags
- **AND** prepare-only leaves generated inventory ready without sorting, and completion declares skipped source checks with the DP1 gate

#### Scenario: Publication failure and retry

- **WHEN** accepted-object tagging loses its acknowledgement or cleanup partially fails
- **THEN** tests verify a visible failure, accurate successful-delete accounting and recovery by rerunning the same command without rewriting completed output
- **AND** cleanup removes only transient `_run/` objects and retains every listed raw source file

## ADDED Requirements

### Requirement: Measured generator scale evidence

Scale acceptance SHALL record the source/configuration/contract revisions, pilot and full-run commands, hardware, seed, elapsed time, throughput, peak process-tree memory and scratch, physical/uncompressed raw source bytes, additive distributions and separately identified downstream validation reports. CI SHALL NOT generate 100 GB. Estimates and slices SHALL be labelled and SHALL NOT substitute for a measured full-source run.

#### Scenario: Accept the large dataset

- **WHEN** an operator reports completion of the 100 GB dataset
- **THEN** the evidence identifies its immutable completion manifest and actual source-file byte total, separate full-data Spark/GX results for both persisted Bronze tables plus verified governance through DP1 and measured resource use
- **AND** it distinguishes pilot projections and subset checks from full-run observations

### Requirement: Raw inventory behavior tests

Small tests SHALL verify `manifest_version: 3`, `source_layout: task-fragments-v1`, exact `raw/{table}/{task.key}/v{schema_version}.parquet` keys and per-file checksum/bytes/rows/schema version/`partitions: {date: rows}`. They SHALL exercise multi-date fragments, delayed labels, physical schema cutover and duplicate preservation without a sorting or compaction assumption. Reports SHALL distinguish configured pools from observed additive counters and mark cardinality validation downstream.

#### Scenario: Exact DP1 consumption

- **WHEN** a source root has listed raw files and additional unlisted objects
- **THEN** DP1 validates and consumes exactly the listed files, derives Bronze dates from row timestamps and preserves duplicate deliveries and delayed labels
- **AND** mismatched integrity/schema/count metadata or failed Spark/GX quality prevents Bronze acceptance

#### Scenario: No permanent legacy migration path

- **WHEN** inspecting the maintained simulator command and runtime surface
- **THEN** no legacy migration command, fragment-copy adapter or retired-configuration normalization remains
- **AND** core tests retain coverage of compatible generation resume, manifest integrity and publication retries

### Requirement: Historical evidence boundary

Legacy v7, pilot, separate-process native-validation measurements and prior checked tasks SHALL remain explicitly historical. They SHALL NOT establish implementation, new raw-layout measurements or live `v8` validation. The product-version change to `5.0.0` SHALL preserve logical ODCS contracts.

#### Scenario: New scale evidence

- **WHEN** new raw-publication or full-run quality acceptance is reported
- **THEN** evidence distinguishes measured raw bytes and additive statistics from old compacted/native-validation measurements, projections and downstream cardinality checks
- **AND** no validation claim is inferred for existing live `v8`
