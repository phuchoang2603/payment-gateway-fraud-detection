## Purpose

Publish reproducible bronze payment and label snapshots from immutable historical exports and recurring completed landing batches, preserving delivered content and making input progress, validation and recovery observable.

## ADDED Requirements

### Requirement: Completed source selection

In historical mode, DP1 SHALL ingest only files selected by a completed immutable source manifest and its embedded inventory; manifests without an embedded inventory are unsupported. It SHALL verify declared identities, supported schemas, file hashes, sizes and row counts before writing bronze. Source objects and the original manifest SHALL remain unchanged. Source validation SHALL either declare both native ports passed or explicitly declare `result: skipped`, `gate: dp1-spark-gx` and a nonblank reason with an embedded inventory and no conflicting port declarations. The latter permits ingestion, not quality acceptance. Run and validation evidence SHALL preserve this declaration; bronze acceptance SHALL require matching gate evidence, both persisted tables passing Spark/GX, both snapshot identities and verified governance readback.

#### Scenario: Accepted historical version

- **WHEN** an accepted version such as v8 or the v11 smoke export is selected with its completed manifest and matching source files
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

For historical exports, validation SHALL read the recorded event and label snapshots for the selected source version. It SHALL cover complete histories and delayed labels, compare normalized business content and counts with source, and exclude ingestion metadata from business-content identity comparisons.

#### Scenario: History spans source dates

- **WHEN** a payment starts on one date and its lifecycle or label continues on later dates
- **THEN** validation evaluates the complete selected history and correct event-label linkage rather than rejecting an isolated daily slice

#### Scenario: A later writer changes a table

- **WHEN** current table state advances after the ingest stage records its snapshots
- **THEN** validation still evaluates the recorded snapshots and reports their identities

### Requirement: Product acceptance boundary

A completed DP1 run SHALL expose an accepted record pinning its complete output snapshot set, immutable input identities, contract hashes and governance outcome. Historical runs SHALL pin both validated snapshots and the source manifest digest. Recurring runs SHALL distinguish updated ports from unchanged accepted ports and retain input/progress identity. Partial writes, failed quality checks or incomplete governance SHALL NOT replace the previous accepted record. Referenced snapshots and reports SHALL remain available for reproduction.

#### Scenario: One table commits and another fails

- **WHEN** events commit successfully but label ingestion fails
- **THEN** the candidate remains recoverable without being advertised as an accepted product or changing the previous accepted record

#### Scenario: Governance publication fails

- **WHEN** data validation passes but OpenMetadata publication is incomplete
- **THEN** the run records validated data and a separate governance failure without claiming completed DP1 acceptance

### Requirement: Bounded pilot execution

The v11 smoke run SHALL use explicit compute and storage budgets, bounded failure samples and distributed data access. It SHALL report duration, input/output bytes, file counts, peak memory and available shuffle/spill evidence. It SHALL NOT claim 100 GB or large-scale optimization acceptance; the separate v8 acceptance reports its own measured resources.

#### Scenario: Small historical acceptance

- **WHEN** the complete v11 smoke dataset is processed
- **THEN** evidence identifies resource settings and measured results, preserves existing data problems and distinguishes correctness acceptance from later scale evaluation

### Requirement: Shared recurring source boundary

DP1 SHALL ingest completed immutable landing batches through the same versioned payment-event interface for real and simulator-driven service events. It SHALL retain source environment/origin, immutable file and batch identities, event identity/timestamps, schema versions and supplied Kafka topic/partition/offset provenance. Simulation inputs SHALL remain isolated from real inputs and SHALL NOT require rewriting historical sources or sharing their prefix. Ingestion retries SHALL NOT create extra deliveries; actual source-delivered duplicates SHALL remain preserved for silver curation.

#### Scenario: Same event interface across origins

- **WHEN** isolated real and simulator-driven inputs follow the supported event contract
- **THEN** the same ingestion logic processes each while evidence and output scopes keep their origins distinguishable and isolated

#### Scenario: Changed input under an existing identity

- **WHEN** previously selected file or batch identity is presented with changed content
- **THEN** ingestion rejects it rather than treating it as a retry or silently replacing accepted input

### Requirement: Context-aware recurring validation

Recurring validation SHALL reconcile the selected delivered content with recorded output snapshots and evaluate history-dependent rules using explicitly pinned prior context and declared completeness boundaries. Event and label inputs MAY arrive independently; missing same-batch labels or terminal lifecycle events SHALL NOT alone be a quality failure. Unavailable required context SHALL be reported as incomplete evidence and SHALL NOT satisfy acceptance. Historical full-history and synthetic-band acceptance requirements SHALL remain unchanged and SHALL NOT be applied indiscriminately to real or incomplete live batches.

#### Scenario: Delayed real label

- **WHEN** a label arrives in a later batch than its payment event
- **THEN** validation uses the referenced event history and label-availability time without requiring a synthetic label or duplicating the payment delivery

#### Scenario: Incomplete payment history

- **WHEN** a bounded batch ends before a payment has reached a terminal state
- **THEN** validation respects the declared history boundary and distinguishes incomplete context from a violated lifecycle rule
