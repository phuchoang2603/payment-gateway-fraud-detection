## MODIFIED Requirements

### Requirement: Ninety-day development population

The maintained development configuration SHALL span exactly 90 purchase-start dates for a modest US/USD electronics population with repeat behavior and supported fraud scenarios. It SHALL declare seed, calibration, entity pools, resource budgets, merchant/category skew, an identical-duplicate export rate and an explicit schedule of declared physical schema versions. It SHALL generate one problem-bearing dataset usable after curation for feature development, without requiring separate training/problem profiles or a scale-byte minimum. It SHALL remain a development source rather than evidence of 100 GB scale or real-world model quality. Published v16 SHALL remain unchanged.

#### Scenario: Default development range

- **WHEN** the maintained configuration uses 90 purchase-start dates
- **THEN** the manifest records the actual date range, schema shapes, duplicate deliveries, rows and bytes without promising a fixed output size
- **AND** lifecycle and label tails remain complete beyond the last purchase-start date

#### Scenario: Reuse for model preparation

- **WHEN** the same dataset is curated for later feature development
- **THEN** no separate training export is required and oracle/scenario fields remain excluded from features

## ADDED Requirements

### Requirement: Physical evolution preserves coherent histories

The generator SHALL select a declared physical schema by an ordered, unambiguous effective-time schedule using event occurrence time. Version identifiers SHALL resolve to supported schema definitions and per-version projection/normalization rules shared with ingestion. Selection and ingestion SHALL NOT require code branches for each version pair or require intermediate versions to appear in the source. Every selected version SHALL normalize losslessly to the declared output contract. Unknown versions, conflicting schedule boundaries and unsupported changes SHALL fail before publication or ingestion writes.

Schema 3 to 4 SHALL be the first development demonstration, retaining schema 3's physical shape and adding optional assessment references in schema 4. Later declared versions SHALL use the same path: 4 to 5 and 3 to 5 SHALL not require transition-specific orchestration or a synthetic schema-4 partition. Adding version 5 still requires an explicit shape and supported normalization rules; a number alone SHALL NOT establish compatibility. Existing identities, attempt context and lifecycle meanings SHALL remain valid. Identical duplicate deliveries SHALL retain their original version/content. Synthetic references SHALL identify synthetic evidence honestly, with unavailable references remaining null; they SHALL NOT imply a persisted Fraud-owned document. The new-version demonstration SHALL include populated references to prove value preservation.

#### Scenario: History crosses a scheduled version boundary

- **WHEN** a payment starts before a configured schema boundary and finishes afterward
- **THEN** its files use the declared effective versions without breaking linkage or lifecycle completion
- **AND** evidence distinguishes physical absence from unavailable or invalid new values

#### Scenario: Skip an intermediate version

- **WHEN** schema definitions and normalization rules support versions 3 and 5 and the schedule selects only those versions
- **THEN** generation and ingestion use the common version-selection path without producing version-4 data or executing a special 3-to-5 workflow

#### Scenario: Unsupported future version

- **WHEN** configuration selects version 5 without a supported schema definition or lossless normalization rule
- **THEN** validation fails before source publication or ingestion writes instead of interpreting it as version 4

### Requirement: Future drift continuation boundary

The new dataset SHALL retain reproducible entity-world identity and a declared historical end boundary for later live continuation through the existing gateway adapter. Configuration and evidence SHALL distinguish data-problem settings from later time-based distribution shifts. This change SHALL NOT claim drift detection or streaming-fault acceptance from historical diagnostics.

#### Scenario: Later shifted traffic

- **WHEN** a future live run applies a configured distribution shift using the historical entity world
- **THEN** the historical manifest remains immutable and phase/scenario metadata remains outside payment feature inputs

### Requirement: Domain-owned generator documentation

Maintained generator documentation SHALL live under `docs/data-ml/` in a dedicated generator guide covering configuration, seeds/entity identity, schema definitions and effective-time schedules, immutable source layout/publication/resume, data-problem controls, label availability and diagnostic interpretation. Existing product/pipeline pages SHALL link to the guide and retain their own product/pipeline responsibilities; moved instructions SHALL NOT remain duplicated. Generated field-level contract references SHALL remain contract-derived. Documentation SHALL identify the initial schema-3/4 demonstration without presenting it as the only supported transition.

#### Scenario: Configure a later declared schema

- **WHEN** a maintainer follows the guide to add a supported version or skip an intermediate version
- **THEN** they can identify the definition, normalization and schedule changes required without changing pair-specific workflow code
