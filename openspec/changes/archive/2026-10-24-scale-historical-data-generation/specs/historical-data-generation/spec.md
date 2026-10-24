## MODIFIED Requirements

### Requirement: Reproducible generation

The same logical generation configuration, seed and generator algorithm revision SHALL produce the same records. Worker count, completion order, destination, resource budgets and interruption/resume SHALL NOT change those records or inventory date row counts. Different seeds SHALL produce different histories with the same configured statistical shape. The manifest SHALL distinguish logical generation identity from execution settings and publication gates. Adding or changing acceptance bands SHALL NOT change the generation identity, so calibrated gates can be applied to already prepared output.

#### Scenario: Regenerate a dataset

- **WHEN** a dataset is generated twice to separate locations with the same logical configuration and algorithm revision
- **THEN** both outputs contain identical records and identical manifest row counts

#### Scenario: Change parallelism during resume

- **WHEN** an interrupted run resumes with a different worker count and sufficient resource budgets
- **THEN** its records and inventory date counts equal an uninterrupted run of the same generation plan

#### Scenario: Calibrate gates after preparation

- **WHEN** acceptance bands are added or changed after raw inventory was prepared
- **THEN** the prepared output keeps its generation identity and publishes under the gates in effect at publication

### Requirement: Continuable by live simulation

Profiles, and any single day's scenario schedule, SHALL be reproducible from the configuration and seed without generating earlier days. Merchant and customer IDs and address, IP-network and payment-method fingerprints SHALL match the shared identity vectors. The manifest SHALL record the history end time, a digest of the generated profiles, the synthetic fingerprint key version and the generation algorithm version. Continuation SHALL reject an unsupported algorithm or mismatched profile digest.

#### Scenario: Rebuild profiles from a manifest

- **WHEN** profiles are rebuilt from a manifest's configuration and seed with the same generator logic
- **THEN** their digest equals the manifest's profile digest

#### Scenario: Identity vectors

- **WHEN** the generator derives IDs and fingerprints for each case in the shared identity vectors
- **THEN** it produces exactly the listed values

#### Scenario: Incompatible generation algorithm

- **WHEN** continuation requests a dataset whose algorithm cannot reproduce its recorded profile digest
- **THEN** it fails explicitly instead of silently substituting new profiles

### Requirement: Offline data problems

The configuration SHALL control merchant and category skew, the number of distinct device, IP-network, address and payment-method fingerprints, a schema-evolution cutover date before which `user_agent_family` is physically absent and `schema_version` is 1, and a duplicate rate at which events are exported again with identical content. The manifest SHALL record configured values and cheap additive observed row/scenario/duplicate/schema totals and bounded skew counters. It SHALL keep configured identity pools separate from measurements and mark observed cardinality validation as downstream, without fabricated counts or remote analytical scans. DP1 cardinality reports SHALL identify exact counts versus estimates and each ratio's denominator.

#### Scenario: Schema evolution cutover

- **WHEN** a dataset spans the configured cutover date
- **THEN** raw files use the appropriate physical schema version for occurrence dates on each side, and DP1's merged read preserves the cutover

#### Scenario: Duplicate export

- **WHEN** the duplicate rate is 2%
- **THEN** about 2% of event rows repeat an earlier `event_id` with identical content, and the manifest reports the observed rate

#### Scenario: Scale profile data problems

- **WHEN** a scale dataset is accepted
- **THEN** source evidence reports additive merchant/category skew, duplicate and schema counts, while DP1 quality evidence supplies observed identity cardinalities and ratios
- **AND** physical size is not increased through padding, repeated copies of an accepted dataset or an inflated duplicate rate

### Requirement: Contract-gated immutable output

Output SHALL publish generation-only raw Parquet under `historical/<dataset_version>/raw/{table}/{task.key}/v{schema_version}.parquet`. The manifest SHALL use `manifest_version: 3` and `source_layout: task-fragments-v1`, with exact inventory entries containing key, checksum, bytes, rows, `schema_version` and `partitions: {date: rows}`. Date counts SHALL derive from event occurrence or label availability timestamps and sum to file rows; files MAY span multiple dates. Publication SHALL promise neither sorting nor compaction and SHALL NOT perform Python/DuckDB finalization or remote analytical scans.

Source completion SHALL require complete generation receipts, verified file integrity, row-count accounting and the configured physical-byte gate before exclusive manifest-last publication. The manifest SHALL record configuration/hashes, seed, original algorithm/revision, product/contract identities/hashes, statistics and an explicit `validation: {result: skipped, gate: dp1-spark-gx, reason: ...}` declaration. The physical product version SHALL be `5.0.0`; logical ODCS contracts SHALL remain unchanged. Completed versions SHALL be immutable, with equivalent local/S3 guarantees.

DP1 SHALL validate exactly the listed raw files, preserve duplicate deliveries and delayed labels, and derive Bronze partitions from timestamp columns. Source publication and accepted-object tags SHALL NOT establish quality acceptance; both persisted Bronze tables SHALL pass Spark/GX checks and verified governance before acceptance.

#### Scenario: Validated bytes are published

- **WHEN** generation, integrity, row accounting and byte gates pass
- **THEN** the manifest lists the verified raw task files without sorting or compacting them
- **AND** its downstream gate does not claim contract checks have passed

#### Scenario: Contract violation

- **WHEN** published rows contain an illegal lifecycle transition or missing label linkage
- **THEN** DP1 quality acceptance fails even if source publication and storage protection succeeded

#### Scenario: Existing dataset version

- **WHEN** a compatible completed format-3 dataset is rerun
- **THEN** the normal command verifies stored configuration, publication settings and raw inventory, then retries protection/cleanup without regenerating rows or rewriting completed files or the manifest

#### Scenario: Interrupted run

- **WHEN** generation stops before manifest publication
- **THEN** consumers identify the dataset as incomplete and a compatible normal command resumes while refusing an active writer

#### Scenario: Multi-date raw file

- **WHEN** a task contains cross-midnight transitions and labels beyond history end
- **THEN** its file inventory includes all timestamp-derived date counts
- **AND** DP1 preserves all rows, including duplicate deliveries, and derives Bronze partitions from row timestamps rather than task paths

### Requirement: Local execution

Generation SHALL run as a local CLI from the repository development environment with environment-only credentials and no cluster requirement. `--prepare-only` SHALL leave generated raw files and verified inventory ready without sorting or a completion manifest. A compatible normal command SHALL continue publication without regenerating rows.

#### Scenario: Workstation run

- **WHEN** an operator runs `historical` with storage credentials
- **THEN** it generates/uploads, verifies source gates, publishes manifest-last, then protects output and cleans only transient run data
- **AND** DP1 quality processing remains downstream

#### Scenario: Prepared inventory handoff

- **WHEN** generated inventory is prepared on one host and a compatible normal command continues on another
- **THEN** it verifies and publishes the same raw inventory without sorting, compaction, native validation or regenerated rows

## ADDED Requirements

### Requirement: Bounded generation resources

The operator SHALL configure generation workers, maximum in-flight work, scratch location and capacity, and transfer concurrency. Generation SHALL avoid requiring local storage for the entire remote dataset. Resource exhaustion SHALL stop the run with a diagnostic and no completion manifest. Resource reports SHALL include measured total worker memory and scratch peaks, including shared profile mappings.

#### Scenario: Remote dataset exceeds local scratch capacity

- **WHEN** cumulative remote output exceeds the configured local scratch capacity while active tasks fit within their budgets
- **THEN** completed data is uploaded and local scratch is reclaimed so generation continues

#### Scenario: Generation budget exhausted

- **WHEN** active generation or upload cannot fit its declared resource budget
- **THEN** the run preserves verified receipts, reports the failing phase and publishes no completion manifest

### Requirement: Verified resume

Automatic resume SHALL reuse completed work only when run-state format, logical generation identity, generator and contract revisions, and stored object integrity match. Partial or corrupt objects SHALL be regenerated without trusting file presence alone. One writer SHALL own a dataset version at a time, and completion SHALL use exclusive publication. Credentials SHALL NOT appear in run state, manifests or logs.

#### Scenario: Lost upload acknowledgement

- **WHEN** an upload completed remotely but the process stopped before checkpoint acknowledgement
- **THEN** resume verifies the object and either adopts it or regenerates it without duplicating logical rows

#### Scenario: Incompatible resume

- **WHEN** the normal command is rerun with changed generation inputs, contracts or an unsupported checkpoint format
- **THEN** it refuses without modifying completed work and identifies the mismatch

#### Scenario: Concurrent writer

- **WHEN** another process owns an unfinished dataset version
- **THEN** a second process cannot modify or complete that version

### Requirement: Bounded task fragments

Each generation task SHALL write at most one persistent raw object per table and physical schema version, appending purchase batches as row groups.

#### Scenario: Task fragments per table and schema version

- **WHEN** a task spans several purchase batches
- **THEN** its events and labels are written as one raw fragment per table and physical schema version

### Requirement: Accepted-version protection

When the configuration declares the accepted version, publication SHALL tag the completion manifest and every listed raw object as accepted, so a storage policy can deny writes and deletes for that version.

#### Scenario: Recover interrupted protection

- **WHEN** tagging fails or its acknowledgement is lost after the manifest is durable
- **THEN** the command exits nonzero and retains a manifest-bound publication receipt
- **AND** rerunning the same command verifies raw object integrity and the logical configuration, skips already protected tags and retries without rewriting completed objects or the manifest
- **AND** regeneration and overwriting of that completed version remain refused

#### Scenario: Accepted version is tagged

- **WHEN** the configuration declares the accepted version and the manifest is written
- **THEN** the manifest and every listed raw object carry the accepted tag
- **AND** candidate and pilot versions are published untagged and remain removable
- **AND** a policy denying writes and deletes on tagged objects cannot alter the accepted dataset

### Requirement: Post-publication cleanup

The normal command SHALL clean only transient `_run/` objects after durable publication and required protection, retaining raw files and a manifest-bound publication receipt. It SHALL count only successful deletions, report failures without withdrawing publication and automatically retry cleanup on a compatible completed version without regeneration.

#### Scenario: Transient objects removed after publication

- **WHEN** the normal command completes the manifest write and required protection
- **THEN** transient `_run/` fragments and receipts are removed
- **AND** the retained publication receipt records the removed objects and bytes while all listed raw files remain

#### Scenario: Partial cleanup failure

- **WHEN** a batch delete response contains successful removals and per-object errors
- **THEN** per-object S3 delete errors are inspected, the receipt counts only successful removals, and the command exits nonzero
- **AND** rerunning the same command removes remaining transient objects without rewriting the manifest or deleting the retained receipt

#### Scenario: Cleanup refused

- **WHEN** the storage credentials cannot delete intermediates
- **THEN** the published dataset remains intact and the run reports the cleanup failure without writing a second manifest

### Requirement: Probe-scale publication guard

Publication SHALL refuse a dataset smaller than one configured customer block unless the operator explicitly allows small datasets.

#### Scenario: Probe-scale publication refused

- **WHEN** the configured customer count is below one customer block and small datasets are not explicitly allowed
- **THEN** publication is refused with no completion manifest
- **AND** prepared output remains available for local or explicitly allowed probes

### Requirement: Scale calibration

A pilot SHALL use a separate immutable dataset with the intended algorithm and raw layout. Its report SHALL record actual rows, physical/uncompressed bytes, throughput, resource peaks and projected full-run time/storage. The full run SHALL use a fixed evidence-based plan and declared acceptance bounds, never a timing-dependent byte threshold. Legacy compacted/native-validation measurements SHALL remain historical and SHALL NOT establish new-layout acceptance.

#### Scenario: Representative pilot

- **WHEN** a pilot is used to size the full run
- **THEN** it exercises the intended entity/profile scale, both schema versions and complete delayed-label output
- **AND** projections are labelled estimates and recalibrated if entity diversity or compression changes materially

#### Scenario: Compromise-window warmup

- **WHEN** the full history outlasts a short pilot's compromise-window warmup
- **THEN** full-run scenario bands account for steady-state attempt proportions rather than copying early-history bands unchanged
- **AND** bounded calibration samples remain labelled estimates rather than full-run observations

#### Scenario: Absolute scenario rate at larger volume

- **WHEN** the scale configuration increases ordinary purchase traffic
- **THEN** its report explicitly calibrates card-testing bursts per day and reports both scenario counts and attempt-level proportions
- **AND** per-customer compromise probabilities and label delays are not multiplied automatically with volume

### Requirement: Offline volume accounting

The scale configuration SHALL declare a minimum physical source Parquet byte target. Completion SHALL require measured inventory-listed raw event-plus-label bytes to meet it, counted once and excluding checkpoints, reports, streaming data, replicas and downstream copies. Reports SHALL also identify uncompressed row-group bytes and GB/GiB units. Coursework acceptance SHALL name its source and distinguish the project's physical-byte target from unspecified rubric compression accounting.

#### Scenario: Coursework scale target

- **WHEN** a run is claimed to satisfy this change's 100 GB acceptance target
- **THEN** inventory-listed raw source Parquet files total at least 100,000,000,000 bytes
- **AND** the evidence cites `EDAI K11 - DE.xlsx`, sheet `edai-1 (50%)`, cell B5 as the offline-volume requirement

#### Scenario: Undersized fixed plan

- **WHEN** a completed generation plan produces fewer physical bytes than its configured minimum
- **THEN** the size gate fails with observed and required bytes, preserves its diagnostic state and writes no completion manifest
- **AND** changing the logical plan requires a new dataset version

### Requirement: Generation-only simulator boundary

The maintained simulator SHALL generate and publish raw source files without a legacy migration command, server-side fragment-copy support or retired-configuration compatibility. Compatible generation resume and publication retries SHALL remain supported. Existing legacy output MAY receive a one-time operational handoff outside the maintained simulator, only after its writer stops and complete receipts, original provenance, checksums and byte/row gates are verified.

#### Scenario: Legacy v8 handoff

- **WHEN** the old finalizer is stopped after generation completes
- **THEN** the one-time handoff copies existing receipted fragments to persistent raw keys without regenerating rows or running finalization
- **AND** it retains original fragments and producer provenance and publishes a completion manifest only after integrity and byte/row gates pass
- **AND** source publication does not establish DP1 quality acceptance
