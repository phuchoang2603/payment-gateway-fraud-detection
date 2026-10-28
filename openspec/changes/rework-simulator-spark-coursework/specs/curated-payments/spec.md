## Purpose

Produce reproducible curated payment histories from accepted raw deliveries, with temporal correctness, measured optimization and inspectable acceptance evidence.

## ADDED Requirements

### Requirement: Deterministic curated identities and delivery accounting

Silver SHALL declare scoped event, payment, attempt and label-observation grains and keys. Identical event deliveries SHALL collapse by business identity only after checking content consistency independently of provenance. Conflicting content SHALL fail acceptance rather than select an arbitrary winner. Every selected raw delivery SHALL map to a canonical record, identical duplicate, explicit pending-context record or bounded rejected/conflicting category, with exact accounting. Labels SHALL remain independent observations; distinct reports SHALL NOT collapse merely because they reference the same attempt.

#### Scenario: Duplicate has different ingestion metadata

- **WHEN** identical business content arrives through different raw files or transport positions
- **THEN** one canonical event is produced and all delivery provenance remains traceable without counting a business conflict

#### Scenario: Identity has conflicting content

- **WHEN** two selected deliveries share a scoped event identity but differ in business content
- **THEN** acceptance fails with bounded diagnostics and the previous accepted outputs remain available

### Requirement: Context and temporal quality

Curated validation SHALL enforce conditional schema, context, amounts, lifecycle, linkage and label availability using the declared completeness boundary. Complete historical input SHALL include terminal lifecycle tails. Recurring incomplete histories SHALL be reported as pending context rather than automatically invalid. Simulation truth, chargeback observations and risk decisions SHALL remain distinct. Missing chargebacks SHALL remain unknown. Future outcomes, scenario/oracle fields and assessment decisions SHALL NOT become predecision features. Accepted Silver SHALL provide enough source times, label availability and provenance for later point-in-time feature construction.

#### Scenario: Label arrives after its event batch

- **WHEN** an independently accepted label input references earlier accepted history
- **THEN** curation joins the pinned prior context and updates the affected attempt without inventing same-batch events or negative labels

### Requirement: Incremental recovery and backfill

Progress SHALL track accepted input receipts and immutable selected snapshots independently of event-date maxima. Reprocessing the same logical input SHALL produce equivalent canonical content without adding rows. Late accepted inputs SHALL recompute affected entities/time ranges, and backfills SHALL record their boundaries and transformation revision. Multi-table candidate writes SHALL not replace the accepted pointer until validation and governance succeed.

The development MVP SHALL publish complete Silver table snapshots using batch replacement and recover tagged commits on retry. Optimized incremental row writes are deferred; previous accepted snapshots SHALL remain readable.

#### Scenario: Newly accepted old event

- **WHEN** a new accepted input contains a record older than the latest processed event date
- **THEN** it is processed once and relevant curated history is corrected rather than skipped by a date watermark

### Requirement: Measured offline optimization

Coursework evidence SHALL retain an unoptimized baseline and each measured optimization step using the same selected data, declared compute budget and equivalent correct output. Reports SHALL explain observed skew, high-cardinality cost, schema normalization and duplicate handling using actual execution evidence. Runtime, task distributions, shuffle/spill, available memory metrics and physical plans SHALL identify collection limitations. Development results SHALL NOT establish deferred full-scale acceptance.

#### Scenario: Optimization has no measured benefit

- **WHEN** a candidate optimization is slower or introduces incorrect output
- **THEN** evidence retains that result and does not label the candidate an improvement

### Requirement: Reviewable data code and runtime evidence

An evidence index SHALL pair real output/UI screenshots with immutable code/query references and machine-readable receipts. Spark captures SHALL show the relevant baseline problem, changed execution plan/stages and resulting metrics. Argo captures SHALL show DP1/DP2 stage ordering; OpenMetadata captures SHALL show actual lineage, native validation and inspectable contracts.

Capture instructions SHALL be documentation for manual report preparation. Screenshot validation, upload schemas and screenshot-specific tests SHALL NOT be part of the pipeline or its acceptance.

#### Scenario: Missing screenshot or measurement

- **WHEN** a required artifact is absent or its run cannot be correlated
- **THEN** its evidence item remains pending and no screenshot or successful result is invented
