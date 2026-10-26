## ADDED Requirements

### Requirement: Current attempt context across source formats

New historical exports and decoded live landing SHALL expose the same typed attempt-context business fields and optionality as the current payment contract. Product/port/physical schema versions and hashes SHALL identify the new shape independently of dataset version. Generated schema references SHALL describe it. Live-only transport metadata SHALL remain separate from business fields.

#### Scenario: Historical and live attempts read by Spark

- **WHEN** Spark reads supported historical and live source files
- **THEN** their attempt context has matching business-column types without requiring protobuf decoding in ingestion

### Requirement: Feature contract references and source traceability

The development-source handoff SHALL identify its immutable manifest/hash, event and label contracts, generator/calibration revision, profile digest and baseline feature-definition identity. A source identity SHALL NOT imply that feature values were computed, online/offline parity passed, bronze ingestion was accepted or a model was trained.

#### Scenario: Source available before feature pipelines

- **WHEN** the new smoke source is published while Flink and Spark feature jobs are unimplemented
- **THEN** its handoff exposes the required inputs and definitions and reports feature/ingestion acceptance as pending

### Requirement: Development source evidence

The 90-day source SHALL report actual inventory rows/files/bytes and event/label time ranges, warmup, maturity, class/cohort counts and chronological split guidance. Reports SHALL distinguish measured values from estimates and source/contract checks from downstream acceptance. Generator distribution/cardinality bands SHALL remain diagnostics; integrity, identity/content, lifecycle, linkage and leakage checks SHALL remain enforced at their owning stage.

#### Scenario: Labels extend beyond purchase dates

- **WHEN** labels become available after the last configured purchase-start date
- **THEN** the inventory and report include those files/rows and their availability range instead of truncating them

#### Scenario: Previous failed evidence

- **WHEN** the new report uses informational distribution diagnostics
- **THEN** earlier failed/passed evidence and published contracts retain their original results and meanings
