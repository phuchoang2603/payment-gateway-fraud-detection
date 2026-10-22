## ADDED Requirements

### Requirement: Deferred source gate verification

Focused checks SHALL cover explicit deferred-source preflight, preservation of its gate in persisted validation evidence and rejection of acceptance without passing results for both persisted tables and verified governance.

#### Scenario: Deferred source is ingested but validation fails

- **WHEN** a source declares the supported DP1 gate and persisted bronze violates a contract
- **THEN** the validation report retains the source declaration and acceptance remains refused


### Requirement: Data-pipeline behavioral verification

Small local fixtures SHALL protect source integrity, schema normalization, duplicate preservation, snapshot selection, uncertain-commit recovery and safe replay. They SHALL also protect contract coverage/parity, failed-result publication, cancellation and canonical catalog identity. Ordinary CI SHALL NOT require live cluster or storage credentials.

#### Scenario: Verify changes without infrastructure access

- **WHEN** DP1 behavioral checks run in CI
- **THEN** bounded fixtures and suitable local/fake boundaries exercise the relevant failure paths without reading v7 or contacting production services

#### Scenario: Source and bronze rule parity

- **WHEN** shared valid/invalid fixtures are evaluated by source and bronze engines
- **THEN** equivalent business checks agree, including duplicates, nested payloads, schema cutover, lifecycle gaps, bad amounts and label linkage/availability

#### Scenario: Verify legacy inventory evidence

- **WHEN** focused fixtures exercise companion creation and selection for more than one dataset version
- **THEN** they prove bounded reads, source immutability, create-only evidence, explicit URI/digest pinning, original manifest/contract binding, rejection of altered evidence or inconsistent file/partition/row/file-byte totals, and rejection of cross-version reuse, while retaining embedded-inventory behavior

### Requirement: Live historical and governance acceptance

Live acceptance SHALL separately exercise complete v7 ingestion, persisted-snapshot validation, replay, partial-commit recovery and DataHub readback. It SHALL prove failed-quality visibility and metadata-outage recovery with isolated candidates. Evidence SHALL identify revisions, images, contracts, manifests, snapshots, resource measurements and unverified prerequisites.

#### Scenario: Complete v7 acceptance

- **WHEN** the configured live DP1 acceptance run succeeds
- **THEN** it verifies source-to-bronze content/count conservation, safe replay and visible products, schemas, lineage and validation results on canonical DataHub assets

#### Scenario: Dependency unavailable

- **WHEN** cluster access, REST catalog compatibility, DataHub readiness or capacity cannot be verified
- **THEN** planning/local checks are reported separately and no live deployment or end-to-end acceptance is claimed
