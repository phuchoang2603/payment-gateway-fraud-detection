# curated-payments Specification

## Purpose

Produce reproducible curated payment histories from accepted raw deliveries, with temporal correctness, measured optimization and inspectable acceptance evidence.

## Requirements

### Requirement: Deterministic curated identities and delivery accounting

Silver SHALL declare scoped event, payment, attempt and label-observation grains and keys. Identical event deliveries SHALL collapse by business identity only after checking content consistency independently of provenance. Conflicting content SHALL fail acceptance rather than select an arbitrary winner. Every selected raw delivery SHALL map to a canonical record, identical duplicate, explicit pending-context record or bounded rejected/conflicting category, with exact accounting. Labels SHALL remain independent observations; distinct reports SHALL NOT collapse merely because they reference the same attempt.

#### Scenario: Duplicate has different ingestion metadata

- **WHEN** identical business content arrives through different raw files or transport positions
- **THEN** one canonical event is produced and all delivery provenance remains traceable without counting a business conflict

#### Scenario: Identity has conflicting content

- **WHEN** two selected deliveries share a scoped event identity but differ in business content
- **THEN** acceptance fails with bounded diagnostics and the previous accepted outputs remain available

### Requirement: Context and temporal quality

Payment-context aggregation SHALL be reused across pending-context classification and attempt histories rather than recomputed independently. Curated validation SHALL enforce conditional schema, context, amounts, lifecycle, linkage and label availability using the declared completeness boundary. Complete historical input SHALL include terminal lifecycle tails. Recurring incomplete histories SHALL be reported as pending context rather than automatically invalid. Simulation truth, chargeback observations and risk decisions SHALL remain distinct. Missing chargebacks SHALL remain unknown. Future outcomes, scenario/oracle fields and assessment decisions SHALL NOT become predecision features. Accepted Silver SHALL provide enough source times, label availability and provenance for later point-in-time feature construction.

Silver attempt histories SHALL retain lifecycle `attempt_started_at` and separately expose client `attempted_at` from `AttemptStarted.signals.attempted_at`. Attempt-based offline feature construction SHALL use client attempt time consistently with the streaming feature contract, while lifecycle ordering remains based on envelope occurrence/aggregate version and availability is evaluated separately. Switching feature contracts SHALL NOT invalidate immutable raw manifests or require regeneration of unchanged source fields.

#### Scenario: Store-and-forward attempt

- **WHEN** an attempt reaches Payments after its client attempt time
- **THEN** Silver retains both timestamps without backdating lifecycle events or treating client time as availability

#### Scenario: Label arrives after its event batch

- **WHEN** an independently accepted label input references earlier accepted history
- **THEN** curation joins the pinned prior context and updates the affected attempt without inventing same-batch events or negative labels

### Requirement: Deterministic full-batch curation

Each development run SHALL explicitly select accepted Bronze receipts and their independent event/label snapshots, then recompute complete Silver outputs from that selection. Reprocessing identical logical inputs SHALL produce equivalent canonical content without adding rows. Newly accepted older events or labels SHALL be included when selected, without an event-date watermark. A run intended to retain prior history SHALL explicitly include its prior input receipts. Affected-entity incremental recomputation, automatic accepted-base merging and bounded-backfill modes are deferred.

The MVP SHALL replace complete Silver table snapshots and retain the selected input identities, transformation revision and resulting snapshot IDs. Failed validation or governance SHALL NOT promote candidate output as accepted. Previous accepted snapshots SHALL remain readable; no additional publication/recovery framework is required.

#### Scenario: Newly accepted old event

- **WHEN** the selected accepted inputs include a newly accepted record older than the previous maximum event date
- **THEN** full-batch curation includes it and corrects the resulting history without a special incremental path

### Requirement: Measured offline optimization

Coursework evidence SHALL retain an unoptimized baseline and each measured optimization step using the same selected data, declared compute budget and equivalent correct output. Reports SHALL explain observed skew, high-cardinality cost, schema normalization and duplicate handling using actual execution evidence. The benchmark SHALL run separately from normal Silver acceptance and target an observed skewed aggregation or relevant high-cardinality operation. Repartitioning event identities alone SHALL NOT establish merchant/category skew handling. Runtime, task distributions, shuffle/spill, available memory metrics and physical plans SHALL identify collection limitations. Development results SHALL NOT establish deferred full-scale acceptance.

#### Scenario: Optimization has no measured benefit

- **WHEN** a candidate optimization is slower or introduces incorrect output
- **THEN** evidence retains that result and does not label the candidate an improvement

### Requirement: Reviewable data code and runtime evidence

A verification page under `docs/data-ml` SHALL pair selected real output/UI screenshots with explanations, immutable code/query references and remote receipt locations. OpenSpec SHALL NOT store screenshot bundles or redundant JSON result copies. Spark captures SHALL show the relevant baseline problem, changed execution plan/stages and resulting metrics. Argo captures SHALL show DP1/DP2 stage ordering; OpenMetadata captures SHALL show actual lineage, native validation and inspectable contracts.

The existing generator notebook SHALL hold source configuration, diagnostics and plots; pipeline code SHALL retain useful data-validation results and run identities without a separate evidence framework. Screenshots and their explanations SHALL live in domain verification documentation. Screenshot validation, upload schemas and screenshot-specific tests SHALL NOT be part of the pipeline or its acceptance.

#### Scenario: Missing screenshot or measurement

- **WHEN** a required artifact is absent or its run cannot be correlated
- **THEN** its evidence item remains pending and no screenshot or successful result is invented
