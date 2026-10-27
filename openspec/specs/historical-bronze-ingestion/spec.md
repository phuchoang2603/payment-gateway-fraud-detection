# historical-bronze-ingestion Specification

## Purpose
Publish reproducible bronze payment and label snapshots from immutable historical exports and recurring completed landing batches, preserving delivered content and making input progress, validation and recovery observable.

## Requirements

### Requirement: Completed source selection
In historical mode, DP1 SHALL ingest only files selected by a completed immutable source manifest and its embedded inventory; manifests without an embedded inventory are unsupported. It SHALL verify declared identities, supported schemas, file hashes, sizes and row counts before writing bronze. Source objects and the original manifest SHALL remain unchanged. Source validation SHALL either declare both native ports passed or explicitly declare `result: skipped`, `gate: dp1-starrocks-openmetadata` and a nonblank reason with an embedded inventory and no conflicting port declarations. The latter permits ingestion, not quality acceptance. Current inputs SHALL match schema 3 events, schema 1 labels, current source contracts/product and the pinned feature definition. Run and validation evidence SHALL preserve these declarations unchanged. Bronze acceptance SHALL require both persisted tables passing native SQL checks, matching duplicate-aware conservation evidence and complete rule/run/contract/snapshot/governance readback. Retired GX gate/schema compatibility SHALL NOT be a current path; superseded acceptance SHALL NOT be imported into a fresh reset or relabeled as acceptance of the rewritten integration.

#### Scenario: Accepted historical version

- **WHEN** a published version such as the published 90-day v16 development export is selected with its completed manifest and matching source files
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

#### Scenario: Duplicate schema-3 deliveries

- **WHEN** a current-schema historical export contains repeated identical events and enriched attempt context
- **THEN** bronze content and delivered multiplicity match the source, with complete nested attempt context and source schema version retained

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
For historical exports, validation SHALL read the recorded event and label snapshots for the selected source version. It SHALL execute StarRocks SQL through native OpenMetadata against both explicit snapshot IDs, preserve complete selected deliveries and delayed labels, and compare persisted per-day row counts with the immutable manifest. Ingestion SHALL verify each source file's checksum and rows before writing and reconcile the selected source frames against recorded persisted snapshots bidirectionally with duplicate multiplicity preserved. A missing, failed or mismatched conservation receipt SHALL prevent acceptance; per-day counts alone SHALL NOT establish content equality.

#### Scenario: History spans source dates

- **WHEN** a payment starts on one date and its lifecycle or label continues on later dates
- **THEN** DP1 preserves every selected delivery and availability timestamp; DP2 validates full-history lifecycle and linkage

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
The v16 development run SHALL use explicit compute and storage budgets, bounded failure samples and distributed data access. It SHALL report duration, input/output bytes, file counts, peak memory and available shuffle/spill evidence. It SHALL NOT claim 100 GB or large-scale optimization acceptance; later >=100 GB current-contract coursework acceptance reports its own measured resources.

#### Scenario: Small historical acceptance

- **WHEN** the complete v16 development dataset is processed
- **THEN** evidence identifies resource settings and measured results, preserves existing data problems and distinguishes correctness acceptance from later scale evaluation

### Requirement: Shared recurring source boundary
DP1 SHALL ingest completed immutable landing batches through the same versioned payment-event interface for real and simulator-driven service events. It SHALL retain source environment/origin, immutable file and batch identities, event identity/timestamps, schema versions and supplied Kafka topic/partition/offset provenance. The current synthetic platform SHALL use one shared environment, codebase and common ingestion configuration for analytics and training without mandatory real/simulator-specific topics, storage or deployments. Origin provenance SHALL remain distinguishable; immutable historical exports SHALL remain separate from growing landing inputs. Ingestion retries SHALL NOT create extra deliveries; actual source-delivered duplicates SHALL remain preserved for silver curation.

#### Scenario: Same event interface across origins

- **WHEN** supported inputs follow the shared versioned event contract
- **THEN** common ingestion logic and configuration process them in the current environment while retaining their source origin in evidence

#### Scenario: Changed input under an existing identity

- **WHEN** previously selected file or batch identity is presented with changed content
- **THEN** ingestion rejects it rather than treating it as a retry or silently replacing accepted input

### Requirement: Context-aware recurring validation
Recurring DP1 validation SHALL reconcile the selected delivered content with recorded output snapshots. History-dependent rules using pinned prior context and declared completeness boundaries belong to DP2 silver. Event and label inputs MAY arrive independently; missing same-batch labels or terminal lifecycle events SHALL NOT alone be a quality failure. DP2 SHALL report unavailable context as incomplete evidence without blocking raw preservation. Raw file integrity, conservation and schema checks SHALL remain enforced; raw acceptance SHALL NOT imply business-quality acceptance. Generator cardinality/distribution/scenario bands SHALL be informational diagnostics rather than bronze acceptance gates for historical or recurring input.

#### Scenario: Delayed real label

- **WHEN** a label arrives in a later batch than its payment event
- **THEN** DP1 preserves the label independently without requiring a synthetic label or duplicating the payment delivery; DP2 owns referenced-history and availability checks

#### Scenario: Incomplete payment history

- **WHEN** a bounded batch ends before a payment has reached a terminal state
- **THEN** DP1 preserves the incomplete history; DP2 respects the declared history boundary and distinguishes incomplete context from a violated lifecycle rule

### Requirement: Dataset diagnostics are distinct from ingestion validity
Generator cardinality, duplicate-rate, skew, schema-share and scenario distribution bands SHALL NOT independently fail bronze acceptance. Reports SHALL distinguish available diagnostic measurements from blocking integrity, conservation, schema and contract outcomes. Published source configuration, contracts and prior execution evidence SHALL remain unchanged; a new policy SHALL require new versioned validation evidence rather than relabeling an old failed run.

#### Scenario: Generator band is exceeded

- **WHEN** a dataset passes required integrity, conservation and contract checks but a generator distribution band is exceeded
- **THEN** the report records the diagnostic finding without failing ingestion solely for that band, and acceptance still requires all other gates including governance readback

### Requirement: Raw-ingestion boundary and recurring delivery acceptance
DP1 SHALL preserve supported raw deliveries, immutable file/batch identities, epoch and Kafka bytes/key/ordered headers/positions. It SHALL verify file hashes/sizes/rows, lossless physical schema, independently pinned snapshots and exact duplicate-aware batch conservation. Native schema/required-field checks SHALL be bundled per table; recurring acceptance SHALL NOT require whole-history lifecycle reconstruction, duplicate business consistency, business amounts/risk/context validation, cross-table linkage or label maturity/as-of joins. These belong to DP2 silver and SHALL NOT be reported as passed by raw acceptance. Profiling SHALL follow on-demand profiling outside the daily ingestion path and remain distinct from raw SQL validity. The rewritten integration SHALL use a fresh explicit acceptance identity and namespace; old accepted records SHALL NOT short-circuit fresh governance readback.

Completed clean landing manifests are discovered across the configured epoch, independently of event dates. Immutable pending selections and an accepted manifest ledger SHALL prevent repeated delivery ingestion, reject changed identities and overlapping transport ranges, preserve delayed independent ports and resume uncertain commits before accepting new inputs. No-input runs SHALL avoid Spark and native quality work. A daily UTC CronWorkflow SHALL use the existing writer mutex and lifecycle cleanup; it SHALL NOT alter producer offsets or enable the simulator. Deliberate isolated failure/outage exercises are removed at the user’s request. Full-scale benchmarking and coursework acceptance follow completion of the whole pipeline.

#### Scenario: Raw recurring batch without complete history

- **WHEN** a completed clean landing batch includes independent labels or incomplete payment lifecycles
- **THEN** DP1 preserves and accounts for every delivery and transport envelope without claiming silver business/as-of validity
- **AND** an already accepted input is skipped and no-input execution launches no Spark or native tests

### Requirement: Fresh DP1 state boundary
A scoped fresh-start reset SHALL suspend DP1 scheduling and verify writer/query/Spark quiescence before removing inventoried DP1 bronze tables/data and run/acceptance/progress state. It SHALL preserve immutable source manifests/files, completed simulator landing sources, Kafka offsets and shared catalog/infrastructure. Recreated current assets SHALL require new acceptance without schema migration, legacy replay or imported old records. Ordinary replay after fresh acceptance SHALL remain duplicate-safe.

#### Scenario: Reingest after a scoped reset

- **WHEN** clean DP1 state is recreated from the unchanged published v16 source
- **THEN** the run verifies the complete manifest inventory and establishes new snapshots/results/governance evidence without selecting old accepted state or modifying the source
