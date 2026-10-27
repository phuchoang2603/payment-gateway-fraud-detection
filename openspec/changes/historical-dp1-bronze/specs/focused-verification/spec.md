## ADDED Requirements

### Requirement: Deferred source gate verification

Focused checks SHALL cover explicit deferred-source preflight, preservation of its gate in persisted validation evidence and rejection of acceptance without passing results for both persisted tables and verified governance.

#### Scenario: Deferred source is ingested but validation fails

- **WHEN** a source declares the supported DP1 gate and persisted bronze violates a contract
- **THEN** the validation report retains the source declaration and acceptance remains refused


### Requirement: Data-pipeline behavioral verification

Bounded CI fixtures SHALL protect the distinct behavioral risks: source integrity/lossless normalization, duplicate-preserving ingestion, uncertain commits/replay, snapshot isolation, nested raw schema/transport semantics, native result completeness and metadata-only recovery. Source business-rule coverage SHALL remain separate from raw bronze acceptance; transferred silver SQL SHALL remain planned work. Existing CI entrypoints SHALL be reused; obsolete GX/custom publisher tests SHALL be removed. No configuration-only test suites, per-option/property matrices, new CI jobs or duplicate layer assertions SHALL be added. Tests and image builds SHALL run only in CI; local checks SHALL be limited to lint/format/schema/OpenSpec validation and diff review. Ordinary CI SHALL NOT require live credentials.

#### Scenario: Verify changes without infrastructure access

- **WHEN** DP1 behavioral checks run in CI
- **THEN** bounded fixtures and suitable local/fake boundaries exercise the relevant failure paths without reading live source exports or contacting production services

#### Scenario: Raw and business validation boundaries

- **WHEN** supported raw deliveries include incomplete lifecycles or independently arriving labels
- **THEN** bronze checks schema, transport and exact conservation without claiming business-rule or as-of acceptance
- **AND** source business checks and planned silver rules retain their distinct scope and semantic identities

### Requirement: Live historical and governance acceptance

Recorded Spark 4.1/Iceberg 1.12 operator compatibility checks SHALL remain historical evidence; production workflows SHALL NOT repeat disposable-table rollout probes. Live acceptance SHALL separately exercise current-contract native v16 development ingestion now, with later separate full-scale/coursework acceptance, persisted-snapshot validation, replay, partial-commit recovery and OpenMetadata readback. Deliberate isolated failure/outage exercises are not required; existing failed outcomes remain unchanged. Evidence SHALL identify revisions, images, contracts, manifests, snapshots, resource measurements and unverified prerequisites.

#### Scenario: Complete v16 development acceptance

- **WHEN** the configured live DP1 run over the 90-day v16 export succeeds
- **THEN** it verifies source-to-bronze content/count conservation, safe replay and visible products, schemas, imported contracts, lineage and validation results on canonical OpenMetadata assets

#### Scenario: Later full-scale source acceptance

- **WHEN** the live acceptance run ingests a completed current-contract >=100 GB source after the development pipeline is exercised
- **THEN** it verifies the exact manifest-listed inventory and byte/row totals, preserved duplicates and delayed labels, native OpenMetadata StarRocks SQL results on both persisted tables, separately reported available cardinality and distribution diagnostics, safe replay and OpenMetadata readback
- **THEN** a failed blocking contract/integrity check or resource exhaustion is reported as failure, while an exceeded generator band alone is informational

#### Scenario: Dependency unavailable

- **WHEN** cluster access, REST catalog compatibility, OpenMetadata readiness or capacity cannot be verified
- **THEN** planning/local checks are reported separately and no live deployment or end-to-end acceptance is claimed

### Requirement: Recurring ingestion verification

CI SHALL cover no-input runs, newly completed files in older date partitions, changed-content rejection, preserved source duplicates, retry/uncertain-commit recovery, metadata-only recovery, independent delayed labels, retained source origin, valid simulation-truth training targets and separation of informational bands from blocking checks using bounded fixtures. Separate live recurring acceptance SHALL demonstrate repeated runs, late input and recovery with exact input/output accounting and catalog readback; historical acceptance SHALL NOT establish recurring readiness.

#### Scenario: Advance across multiple batches

- **WHEN** two completed input batches, a late file and a retried committed batch are processed
- **THEN** all original deliveries remain accounted for without ingestion-generated duplicates and accepted snapshot/progress references are reproducible

### Requirement: Native StarRocks integration evidence

Implementation SHALL verify direct RustFS SigV4 access, external-catalog discovery, nested schemas, snapshot-specific SQL and native OpenMetadata passing/failing quality results with the selected versions. This SHALL be implementation acceptance rather than a per-batch disposable compatibility stage. Previous GX evidence SHALL remain historical and SHALL NOT establish StarRocks acceptance. SQL execution failure or unsupported discovery SHALL be reported without a hidden legacy fallback.

#### Scenario: Native external-catalog discovery is incomplete

- **WHEN** StarRocks can query a table but OpenMetadata cannot natively discover or profile its external-catalog identity correctly
- **THEN** integration readiness remains unverified and DP1 does not claim completed native acceptance
