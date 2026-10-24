## Purpose

Publish reproducible bronze payment and label snapshots from immutable historical source exports, preserving delivered content and making validation and recovery observable.

## ADDED Requirements

### Requirement: Completed source selection

DP1 SHALL ingest only files selected by a completed immutable source manifest and its embedded inventory; manifests without an embedded inventory are unsupported. It SHALL verify declared identities, supported schemas, file hashes, sizes and row counts before writing bronze. Source objects and the original manifest SHALL remain unchanged. Source validation SHALL either declare both native ports passed or explicitly declare `result: skipped`, `gate: dp1-spark-gx` and a nonblank reason with an embedded inventory and no conflicting port declarations. The latter permits ingestion, not quality acceptance. Run and validation evidence SHALL preserve this declaration; bronze acceptance SHALL require matching gate evidence, both persisted tables passing Spark/GX, both snapshot identities and verified governance readback.

#### Scenario: Accepted historical version

- **WHEN** an accepted version such as v8 or the v10 smoke export is selected with its completed manifest and matching source files
- **THEN** the run records the manifest digest, source version and exact input files without regenerating or modifying them

#### Scenario: Explicit deferred source validation

- **WHEN** a completed source manifest has an embedded inventory and the supported explicit deferred declaration
- **THEN** DP1 preflight permits ingestion and persists the declaration in run and validation reports
- **AND** missing, failed or incomplete table-validation evidence prevents acceptance
- **AND** unknown or missing downstream gate declarations fail before bronze writes

#### Scenario: Altered or incomplete source

- **WHEN** a manifest is absent, a file fails verification or a known dataset version has a different manifest digest
- **THEN** the run fails before bronze writes and identifies the input error

### Requirement: Content-preserving bronze output

DP1 SHALL retain all delivered business records and identical duplicates in separate event and label tables. It SHALL preserve source times, normalize supported physical schemas without loss and attach source/run provenance. It SHALL NOT perform DP2 curation or deduplication.

#### Scenario: Duplicate exports and schema cutover

- **WHEN** a historical export contains repeated identical events and both physical schema versions
- **THEN** normalized bronze content and delivered row counts match the source, with the absent v1 field represented as null and each source schema version retained

#### Scenario: Lossy conversion

- **WHEN** a source value cannot be represented losslessly in the declared bronze schema
- **THEN** the candidate fails rather than silently narrowing, truncating or dropping the record

### Requirement: Recoverable ingestion identity

DP1 SHALL serialize writes to its bronze product and persist run identity before writing. Retrying the same source SHALL NOT add deliveries or rewrite unrelated source versions. An accepted source version SHALL reuse its recorded snapshots; changed contracts SHALL require revalidation rather than re-ingestion.

#### Scenario: Commit outcome is uncertain

- **WHEN** the driver stops after an event-table commit but before the run records completion
- **THEN** recovery reconciles or safely replaces that candidate's output and completes without adding another copy of the delivered rows

#### Scenario: Replay an accepted version

- **WHEN** an accepted export with its manifest is submitted again
- **THEN** the run reuses the accepted table snapshots and reports a replay without adding rows or changing unrelated source-version output

### Requirement: Persisted snapshot validation

Validation SHALL read the recorded event and label snapshots for the selected source version. It SHALL cover complete histories and delayed labels, compare normalized business content and counts with source, and exclude ingestion metadata from business-content identity comparisons.

#### Scenario: History spans source dates

- **WHEN** a payment starts on one date and its lifecycle or label continues on later dates
- **THEN** validation evaluates the complete selected history and correct event-label linkage rather than rejecting an isolated daily slice

#### Scenario: A later writer changes a table

- **WHEN** current table state advances after the ingest stage records its snapshots
- **THEN** validation still evaluates the recorded snapshots and reports their identities

### Requirement: Product acceptance boundary

A completed DP1 run SHALL expose an accepted record pinning both validated snapshots, source digest, contract hashes and governance outcome. Partial writes, failed quality checks or incomplete governance SHALL NOT replace the previous accepted record. Referenced snapshots and reports SHALL remain available for reproduction.

#### Scenario: One table commits and another fails

- **WHEN** events commit successfully but label ingestion fails
- **THEN** the candidate remains recoverable without being advertised as an accepted product or changing the previous accepted record

#### Scenario: Governance publication fails

- **WHEN** data validation passes but OpenMetadata publication is incomplete
- **THEN** the run records validated data and a separate governance failure without claiming completed DP1 acceptance

### Requirement: Bounded pilot execution

The v10 smoke run SHALL use explicit compute and storage budgets, bounded failure samples and distributed data access. It SHALL report duration, input/output bytes, file counts, peak memory and available shuffle/spill evidence. It SHALL NOT claim 100 GB or large-scale optimization acceptance; the separate v8 acceptance reports its own measured resources.

#### Scenario: Small historical acceptance

- **WHEN** the complete v10 smoke dataset is processed
- **THEN** evidence identifies resource settings and measured results, preserves existing data problems and distinguishes correctness acceptance from later scale evaluation
