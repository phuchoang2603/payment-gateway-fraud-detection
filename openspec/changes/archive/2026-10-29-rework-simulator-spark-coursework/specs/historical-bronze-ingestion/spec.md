## MODIFIED Requirements

### Requirement: Completed source selection

In historical mode, DP1 SHALL ingest only files selected by a completed immutable source manifest and its embedded inventory; manifests without an embedded inventory are unsupported. It SHALL verify declared identities, supported physical schemas, file hashes, sizes and row counts before writing Bronze. Supported source product/contract hashes and physical schemas SHALL be an explicit declared set resolved through shared per-version definitions and lossless normalization rules, not hardcoded version-pair branches, a wildcard version check or a historical dataset-name allowlist. Source objects and the original manifest SHALL remain unchanged. Source validation SHALL either declare both native ports passed or explicitly declare `result: skipped`, `gate: dp1-starrocks-openmetadata` and a nonblank reason with an embedded inventory and no conflicting port declarations. The latter permits ingestion, not quality acceptance. Inputs SHALL identify the supported event/label shapes, source contracts/product and feature definition independently. Run and validation evidence SHALL preserve these declarations unchanged. Bronze acceptance SHALL require both persisted tables passing native SQL checks, matching duplicate-aware conservation evidence and complete rule/run/contract/snapshot/governance readback. Retired GX gate/schema compatibility SHALL NOT be a current path; superseded acceptance SHALL NOT be imported into a fresh reset or relabeled as acceptance of the rewritten integration.

#### Scenario: Accepted historical version

- **WHEN** a supported immutable export with matching manifest and files is selected
- **THEN** the run records its manifest digest, source version and exact input files without regenerating or modifying them

#### Scenario: Explicit deferred source validation

- **WHEN** a completed source manifest has an embedded inventory and the supported explicit deferred declaration
- **THEN** DP1 permits ingestion and persists the declaration in run and validation reports
- **AND** missing, failed or incomplete table-validation evidence prevents acceptance
- **AND** unknown or missing downstream gate declarations fail before Bronze writes

#### Scenario: Altered or incomplete source

- **WHEN** a manifest is absent, a file fails verification or a known dataset version has a different manifest digest
- **THEN** the run fails before Bronze writes and identifies the input error

#### Scenario: Mixed supported physical schemas

- **WHEN** an inventory contains multiple declared supported versions, including nonconsecutive versions
- **THEN** ingestion preserves all deliveries, source schema identity and available business fields, normalizing absent optional fields to null
- **AND** unsupported shapes, contract hashes or lossy conversion prevent acceptance
