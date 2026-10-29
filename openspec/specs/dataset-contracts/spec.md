## Purpose

Describe each data product with one ODPS descriptor and one ODCS contract per output port, kept together in one folder, so producers and consumers validate against the same rules with existing tools.

## Requirements

### Requirement: Data product folder

Each data product SHALL live in its own folder under `contracts/data-products/<product>/` containing `product.odps.yaml` and one `*.odcs.yaml` per output port. Every output port SHALL reference exactly one contract in the same folder by ID and version. Product, contract, physical schema and dataset versions SHALL be independent.

#### Scenario: Unresolved port contract

- **WHEN** an output port references a contract ID that no ODCS file in the product folder declares
- **THEN** the product lint fails

### Requirement: Historical payments product

The `historical-payments` product SHALL expose two output ports: `payment-events` and `fraud-labels`, each backed by its own ODCS contract with ownership, synthetic classification, field meanings, physical schema versions and SQL quality rules. The `fraud-labels` port SHALL declare the `payment-events` contract as an input contract. The descriptor SHALL record the storage location template, completion marker, access policy and consumption rules.

#### Scenario: Label leakage guidance

- **WHEN** a consumer reads the `fraud-labels` contract
- **THEN** it states that labels may be used only after `label_available_at` and that `scenario` is excluded from features

### Requirement: Declared data quality
The historical-source contracts SHALL declare executable SQL rules. Identical repeated event deliveries SHALL be valid; conflicting event identities, illegal lifecycle transitions, invalid amounts, invalid label sources, labels available before their occurrence and labels referencing missing or, for chargebacks, unsuccessful attempts SHALL fail. Data Contract CLI SHALL execute these source rules against a configured DuckDB data source without a repository-owned validation wrapper or a second source rule suite.

#### Scenario: Duplicate delivery

- **WHEN** identical copies of an event appear in a complete valid history
- **THEN** the payment-events rules pass

#### Scenario: Conflicting delivery

- **WHEN** the same `event_id` appears with different content
- **THEN** the delivery-consistency rule fails

#### Scenario: Invalid chargeback

- **WHEN** a chargeback label references an attempt that did not succeed
- **THEN** the fraud-labels linkage rule fails

### Requirement: Native lint and generated references

CI SHALL discover every product folder, lint each descriptor and contract with pinned Data Product CLI and Data Contract CLI, and reject drift between each contract and its generated Markdown reference under `docs/data-ml/data-products/<product>/`. CI lint SHALL NOT claim to validate produced datasets.

#### Scenario: Invalid contract

- **WHEN** a contract violates the official ODCS schema
- **THEN** CI fails

#### Scenario: Stale reference

- **WHEN** a field meaning changes without regenerating its reference
- **THEN** the documentation comparison fails

### Requirement: Execution boundaries
Documentation SHALL distinguish source DuckDB publication checks, bronze StarRocks SQL checks, contract lint and native catalog ingestion. Producing and ingesting jobs SHALL own data preparation and physical file safeguards. Official OpenMetadata ingestion workflows SHALL execute Git-authoritative bronze SQL quality rules through StarRocks and expose native results. Argo SHALL verify acceptance separately; no GX suite, custom result publisher or independent UI-authored rule source SHALL remain. Flink integration SHALL remain with its pipeline.

#### Scenario: Dataset validation

- **WHEN** a job exposes `payment_events` and `fraud_labels` in a DuckDB database
- **THEN** the documented `datacontract test` command checks each source contract and writes a report

#### Scenario: Bronze validation

- **WHEN** DP1 exposes its selected persisted bronze snapshots
- **THEN** native OpenMetadata tests execute contract-derived StarRocks SQL against the recorded snapshots and acceptance verifies their complete results separately

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

### Requirement: Bronze payments product
The `bronze-payments` folder SHALL contain one ODPS descriptor and ODCS contracts for payment-event and fraud-label Iceberg output ports. They SHALL declare distinct bronze identities, ownership, access, upstream source contracts and the label port's event dependency. Product, contract and source dataset versions SHALL remain independent.

#### Scenario: Resolve bronze product interfaces

- **WHEN** product descriptors and local output contracts are linted
- **THEN** both bronze ports resolve to contracts in their own folder and upstream source references remain distinguishable from bronze outputs

### Requirement: Bronze schema and consumption agreement

Bronze contracts SHALL define business fields, ingestion provenance, normalized physical types and the declared supported physical event and label schema versions. They SHALL permit identical duplicate deliveries, define nested required fields conditionally and retain label-availability and feature-leakage restrictions. Per-version normalization SHALL retain available values and fill absent optional fields with null, with original source schema identifiable independently of the normalized output contract version. Unsupported breaking changes SHALL require an explicit new contract or transformation rather than an implicit cast.

#### Scenario: Consume normalized historical data

- **WHEN** a consumer reads the Bronze contracts
- **THEN** it can distinguish source recording time from ingestion time, interpret enriched attempt context and version-dependent assessment reference availability, identify source/run provenance and determine when labels can be joined

### Requirement: Complete contract-derived bronze enforcement
Bronze validation SHALL derive native OpenMetadata test definitions and schema checks from Git ODCS with stable semantic rule IDs and explicit StarRocks SQL, without a handwritten parallel suite. Custom SQL SHALL return bounded scalar violation counts with zero as the passing threshold; validation SHALL NOT collect all invalid rows. Each quality rule and required schema constraint SHALL have a verified execution path. Unsupported or omitted checks SHALL fail preparation rather than produce a partial passing result.

#### Scenario: Native preparation skips a quality rule

- **WHEN** native test preparation omits a declared rule or cannot enforce a nested constraint
- **THEN** validation preparation fails with the uncovered rule or field identified

#### Scenario: Duplicate comparison includes provenance

- **WHEN** repeated business events carry different file or ingestion provenance
- **THEN** DP1 preserves both deliveries; DP2 duplicate-consistency compares business content independently of audit fields

### Requirement: Source and bronze semantic agreement
Source business checks and the planned silver SQL SHALL retain traceable semantic rule identities. Bronze SHALL enforce raw schema/integrity/conservation rather than duplicate the source business rule suite. Engine-specific SQL SHALL use its execution dialect; preparing test definitions SHALL NOT be treated as SQL translation or data validation.

#### Scenario: Scalar violation count

- **WHEN** a raw-schema count-of-violations rule is implemented as a native bronze custom SQL check
- **THEN** valid data returns zero, invalid data returns a positive violation count and both engines agree on pass/fail

#### Scenario: Conditional nested requirement

- **WHEN** a lifecycle payload is absent in a valid event but a required child is missing from a present payload in an invalid event
- **THEN** both execution paths accept the valid absence and reject the invalid child omission

### Requirement: Recurring input provenance and validation scope
Versioned bronze contracts SHALL distinguish historical-manifest provenance from recurring landing/batch and supplied Kafka provenance, retain source origin and define which rules require historical context or label maturity. Generator distribution bands SHALL remain informational diagnostics rather than bronze acceptance requirements. Simulation truth SHALL be usable as a training target while scenario/oracle-only fields remain excluded from features and label-availability restrictions remain enforced. Contract and consumer changes SHALL be coordinated without rewriting published historical contracts or source manifests.

#### Scenario: Real event without a synthetic manifest

- **WHEN** a landing batch without a historical manifest is selected
- **THEN** its provenance resolves to actual landing/batch identities rather than fabricated historical dataset versions or manifest hashes

Raw contracts SHALL retain optional epoch/batch and original Kafka envelope provenance. Native schema constraints SHALL be bundled per table without dropping nested requirements. Business-quality SQL transferred to `contracts/quality/silver-payments.yaml` remains planned DP2 work, not a raw acceptance assertion.

### Requirement: Inspectable catalog contract mapping
The Git-authoritative bronze contract SHALL map to the canonical table's native contract schema, active raw quality-test references, declared ownership and supported declared terms/semantics/security/SLA fields. Unsupported required mapping SHALL be reported as incomplete governance. Original ODCS identity/version/hash/YAML SHALL remain provenance rather than substitute for native fields. Product dependencies and pipeline executions SHALL carry flow inputs/outputs; the integration SHALL NOT invent native contract fields or guarantees absent from Git.

#### Scenario: Contract YAML is present but native fields are absent

- **WHEN** the catalog contains the contract hash and original YAML but lacks required native schema or active quality references
- **THEN** governance readback rejects completeness and identifies the missing mapping

### Requirement: Assessment evidence reference fidelity

Historical and decoded live contracts SHALL expose the current assessment reference fields with declared optionality and evidence origin. Supported legacy physical shapes SHALL normalize absent references to null without manufacturing identifiers, hashes or evidence documents. New live decoded records SHALL preserve references present in their payment event. Full assessment evidence SHALL remain Fraud-owned; references alone SHALL NOT establish feature-vector export or training-serving parity.

#### Scenario: Decode a committed assessment reference

- **WHEN** a supported payment event contains an assessment identifier and evidence hash
- **THEN** decoded landing, Bronze and curated outputs retain both values without requiring ingestion to decode transport bytes

#### Scenario: Historical reference is unavailable

- **WHEN** an older physical record lacks an assessment reference
- **THEN** its normalized value is null and its source schema remains identifiable

### Requirement: Coursework dataset evidence

Evidence SHALL include the generator configuration and revision, immutable manifest/hash, actual rows/files/physical bytes, format/compression/layout, separate event/label time ranges and measured distribution results. Results SHALL show merchant/category or supported region skew shares with denominators, observed identity cardinalities labelled exact or approximate, evolved-field nulls grouped by source version/date and eligible payload, and duplicate rates before and after curation. Approximate counts SHALL identify the method/error setting; configured pool sizes SHALL NOT substitute for observations. Screenshots of real query/output and corresponding executable code/query references SHALL accompany retained machine-readable results.

#### Scenario: Inspect schema null evidence

- **WHEN** an added assessment field is absent in older source partitions
- **THEN** the report identifies the old physical schema and eligible decision rows rather than counting unrelated event payloads as missing assessments

#### Scenario: Inspect duplicate evidence

- **WHEN** an export contains repeated identical events
- **THEN** evidence records extra deliveries divided by total deliveries, canonical event counts, conflicts and post-dedup duplicates, with the query and selected snapshots identified
