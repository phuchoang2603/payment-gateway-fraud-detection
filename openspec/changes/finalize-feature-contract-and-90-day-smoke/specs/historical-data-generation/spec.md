## MODIFIED Requirements

### Requirement: Reproducible training source and report

The maintained configuration SHALL publish a new immutable 90-day smoke/development source using the current event contract, preserving published v8/v11/v12/v13, stopped incomplete v14 and all prior evidence. It SHALL retain reproducibility, bounded resources, resume, integrity, row accounting and manifest-last publication. Full-volume generation SHALL remain an operator-run Python job under a new version, deferred until the pipeline is built and exercised; a Spark generator SHALL NOT be required. A concise dataset report SHALL record lineage, seed/calibration/feature-definition identity, rows/bytes, date ranges, distributions, label maturity, explicit rate denominators and recommended temporal/entity/campaign evaluation boundaries. Distribution/shortcut reports SHALL remain diagnostics. Source publication SHALL NOT establish downstream DP1 quality acceptance.

#### Scenario: Published dataset already exists

- **WHEN** the improved logical plan encounters an existing published version
- **THEN** it refuses modification and uses an unused version
- **AND** no published source or stopped partial v14 is repurposed for the new contract

#### Scenario: Training handoff

- **WHEN** the dataset is handed to a training consumer
- **THEN** the handoff distinguishes truth from observed labels, excludes future/oracle features and identifies label maturity and quality acceptance status

### Requirement: Contract-gated immutable output

Output SHALL publish generation-only raw Parquet under `historical/<dataset_version>/raw/{table}/{task.key}/v{schema_version}.parquet`. The manifest SHALL use `manifest_version: 3` and `source_layout: task-fragments-v1`, with exact inventory entries containing key, checksum, bytes, rows, `schema_version` and `partitions: {date: rows}`. Date counts SHALL derive from event occurrence or label availability timestamps and sum to file rows; files MAY span multiple dates. Publication SHALL promise neither sorting nor compaction and SHALL NOT perform Python/DuckDB finalization or remote analytical scans.

Source completion SHALL require complete generation receipts, verified file integrity, row-count accounting and the configured physical-byte gate before exclusive manifest-last publication. The manifest SHALL record configuration/hashes, seed, original algorithm/revision, product/contract identities/hashes, statistics and an explicit `validation: {result: skipped, gate: dp1-spark-gx, reason: ...}` declaration. Product and logical ODCS versions/hashes SHALL advance with the new attempt context and identify its matching physical schema; dataset identity SHALL remain independent. Completed versions SHALL be immutable, with equivalent local/S3 guarantees.

DP1 SHALL validate exactly the listed raw files, preserve duplicate deliveries and delayed labels, and derive Bronze partitions from timestamp columns. Source publication SHALL NOT establish quality acceptance; both persisted Bronze tables SHALL pass Spark/GX checks and verified governance before acceptance.

#### Scenario: Validated bytes are published

- **WHEN** generation, integrity, row accounting and byte gates pass
- **THEN** the manifest lists the verified raw task files without sorting or compacting them
- **AND** its downstream gate does not claim contract checks have passed

#### Scenario: Contract violation

- **WHEN** published rows contain an illegal lifecycle transition or missing label linkage
- **THEN** DP1 quality acceptance fails even if source publication and integrity checks succeeded

#### Scenario: Existing dataset version

- **WHEN** a compatible completed format-3 dataset is rerun
- **THEN** the normal command verifies stored configuration, publication settings and raw inventory, then retries transient cleanup without regenerating rows or rewriting completed files or the manifest

#### Scenario: Interrupted run

- **WHEN** generation stops before manifest publication
- **THEN** consumers identify the dataset as incomplete and a compatible normal command resumes while refusing an active writer

#### Scenario: Multi-date raw file

- **WHEN** a task contains cross-midnight transitions and labels beyond history end
- **THEN** its file inventory includes all timestamp-derived date counts
- **AND** DP1 preserves all rows, including duplicate deliveries, and derives Bronze partitions from row timestamps rather than task paths

## ADDED Requirements

### Requirement: Ninety-day development population

The maintained smoke profile SHALL span exactly 90 purchase-start dates for a modest US/USD electronics population with repeat behavior and supported fraud scenarios. It SHALL declare seed, calibration, entity pools and resource budgets, use the current schema without forced cutover/duplicates, and have no scale-byte minimum. It SHALL remain a development source rather than evidence of 100 GB scale or real-world model quality.

#### Scenario: Default development range

- **WHEN** the proposed default profile starts on 2025-07-01
- **THEN** its last purchase-start date is 2025-09-28 and its manifest records actual output rather than a promised row/byte total

### Requirement: Complete development history tails

The 90-day limit SHALL apply to purchase starts, not lifecycle completion or label availability. Publication SHALL include complete histories and scheduled labels after the last start date, with separate observed timestamp ranges. Warmup and label-maturity guidance SHALL identify usable training/evaluation cohorts without classifying absent chargebacks as negative truth.

#### Scenario: Last-day purchase with delayed chargeback

- **WHEN** a purchase on day 90 succeeds and has a later available chargeback
- **THEN** its terminal events and scheduled label remain in the published inventory even when their timestamps lie outside the purchase-start range

### Requirement: Exercise before scale generation

The new source SHALL be exercised through contract/source checks and current-time live/landing acceptance before handoff. Evidence SHALL identify revisions, manifest/hash, run/epoch, actual outcomes and pending downstream checks. Full-scale generation SHALL remain deferred until ingestion, feature, training and serving paths are built and exercised; existing scale/coursework targets SHALL remain future acceptance requirements.

#### Scenario: Source checks pass but DP1 is pending

- **WHEN** publication and live acceptance succeed before persisted bronze quality checks exist
- **THEN** the source is reported ready for ingestion development without claiming DP1, model or scale acceptance

## REMOVED Requirements

### Requirement: Sorted and compacted Parquet layout

**Reason**: The current generation-only source publishes immutable task fragments without sorting or compaction; the retired requirement contradicts that maintained behavior.

**Migration**: Preserve all inventory-listed raw rows, duplicates and timestamp-derived date counts. Sorting/compaction and curated layout belong to downstream data pipelines, not source regeneration or Python finalization.
