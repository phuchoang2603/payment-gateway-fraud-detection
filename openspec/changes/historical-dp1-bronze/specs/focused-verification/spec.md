## ADDED Requirements

### Requirement: Deferred source gate verification

Focused checks SHALL cover explicit deferred-source preflight, preservation of its gate in persisted validation evidence and rejection of acceptance without passing results for both persisted tables and verified governance.

#### Scenario: Deferred source is ingested but validation fails

- **WHEN** a source declares the supported DP1 gate and persisted bronze violates a contract
- **THEN** the validation report retains the source declaration and acceptance remains refused


### Requirement: Data-pipeline behavioral verification

Small local fixtures SHALL protect source integrity, schema normalization, duplicate preservation, snapshot selection, uncertain-commit recovery and safe replay. They SHALL also protect SparkApplication terminal-state handling, bounded retries, cancellation/deadline cleanup, native admission rejection of active deletion/hard termination with allowed graceful stop/completed deletion and unaffected other namespaces, contract coverage/parity, failed-result publication, cancellation and canonical catalog identity. Catalog replacement checks SHALL cover ODCS identity preservation, custom GX success/failure/error results, idempotent metadata-only recovery and refusal of acceptance on incomplete OpenMetadata readback, reusing existing fixtures. Ordinary CI SHALL NOT require live cluster or storage credentials.

#### Scenario: Verify changes without infrastructure access

- **WHEN** DP1 behavioral checks run in CI
- **THEN** bounded fixtures and suitable local/fake boundaries exercise the relevant failure paths without reading live source exports or contacting production services

#### Scenario: Source and bronze rule parity

- **WHEN** shared valid/invalid fixtures are evaluated by source and bronze engines
- **THEN** equivalent business checks agree, including duplicates, nested payloads, schema cutover, lifecycle gaps, bad amounts and label linkage/availability

### Requirement: Live historical and governance acceptance

Recorded Spark 4.1/Iceberg 1.12 operator compatibility checks SHALL remain historical evidence; production workflows SHALL NOT repeat disposable-table rollout probes. Live acceptance SHALL separately exercise complete v11 smoke ingestion, then v8 ingestion, persisted-snapshot validation, replay, partial-commit recovery and OpenMetadata readback. It SHALL prove failed-quality visibility and metadata-outage recovery with isolated candidates. Evidence SHALL identify revisions, images, contracts, manifests, snapshots, resource measurements and unverified prerequisites.

#### Scenario: Complete v11 smoke acceptance

- **WHEN** the configured live DP1 run over the small v11 export succeeds
- **THEN** it verifies source-to-bronze content/count conservation, safe replay and visible products, schemas, imported contracts, lineage and validation results on canonical OpenMetadata assets

#### Scenario: Complete v8 source acceptance

- **WHEN** the live acceptance run ingests the published v8 manifest after the v11 smoke run
- **THEN** it verifies the exact manifest-listed inventory and byte/row totals, preserved duplicates and delayed labels, Spark/GX results on both persisted tables, separately reported available cardinality and distribution diagnostics, safe replay and OpenMetadata readback
- **THEN** a failed blocking contract/integrity check or resource exhaustion is reported as failure, while an exceeded generator band alone is informational

#### Scenario: Dependency unavailable

- **WHEN** cluster access, REST catalog compatibility, OpenMetadata readiness or capacity cannot be verified
- **THEN** planning/local checks are reported separately and no live deployment or end-to-end acceptance is claimed

### Requirement: Recurring ingestion verification

CI SHALL cover no-input runs, newly completed files in older date partitions, changed-content rejection, preserved source duplicates, retry/uncertain-commit recovery, metadata-only recovery, independent delayed labels, retained source origin, valid simulation-truth training targets and separation of informational bands from blocking checks using bounded fixtures. Separate live recurring acceptance SHALL demonstrate repeated runs, late input and recovery with exact input/output accounting and catalog readback; historical v11 acceptance SHALL NOT establish recurring readiness.

#### Scenario: Advance across multiple batches

- **WHEN** two completed input batches, a late file and a retried committed batch are processed
- **THEN** all original deliveries remain accounted for without ingestion-generated duplicates and accepted snapshot/progress references are reproducible
