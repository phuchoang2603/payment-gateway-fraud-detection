## MODIFIED Requirements

### Requirement: Reproducible generation

The same logical generation configuration, seed and generator algorithm revision SHALL produce the same records. Worker count, completion order, destination, resource budgets and interruption/resume SHALL NOT change those records or completed partition row counts. Different seeds SHALL produce different histories with the same configured statistical shape. The manifest SHALL distinguish logical generation identity from execution settings and publication gates. Adding or changing acceptance bands SHALL NOT change the generation identity, so calibrated gates can be applied to already prepared output.

#### Scenario: Regenerate a dataset

- **WHEN** a dataset is generated twice to separate locations with the same logical configuration and algorithm revision
- **THEN** both outputs contain identical records and identical manifest row counts

#### Scenario: Change parallelism during resume

- **WHEN** an interrupted run resumes with a different worker count and sufficient resource budgets
- **THEN** its final records and partition counts equal an uninterrupted run of the same generation plan

#### Scenario: Calibrate gates after preparation

- **WHEN** acceptance bands are added or changed after final files were prepared
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

The configuration SHALL control merchant and category skew, the number of distinct device, IP-network, address and payment-method fingerprints, a schema-evolution cutover date before which `user_agent_family` is physically absent and `schema_version` is 1, and a duplicate rate at which events are exported again with identical content. The manifest SHALL record configured and observed values for each. Cardinality reports SHALL identify exact counts versus estimates and record distinct-to-row ratios and their denominators.

#### Scenario: Schema evolution cutover

- **WHEN** a dataset spans the configured cutover date
- **THEN** partitions before it lack the column, partitions after it contain it, and a merged read shows nulls only before the cutover

#### Scenario: Duplicate export

- **WHEN** the duplicate rate is 2%
- **THEN** about 2% of event rows repeat an earlier `event_id` with identical content, and the manifest reports the observed rate

#### Scenario: Scale profile data problems

- **WHEN** a scale dataset is accepted
- **THEN** its evidence reports observed merchant and category skew, relevant identity cardinalities and ratios, duplicate rate and rows on both sides of the schema cutover
- **AND** physical size is not increased through padding, repeated copies of an accepted dataset or an inflated duplicate rate

### Requirement: Contract-gated immutable output

Output SHALL be written under `historical/<dataset_version>/` as Parquet partitioned by day (events by occurrence date, labels by availability date) for `payment_events` and `fraud_labels`, following the historical-payments contracts. Unpublished files SHALL remain incomplete until both native contract checks pass over the exact final files, unless the run explicitly declares downstream validation as the contract gate; such a run SHALL record `result: skipped`, `gate: dp1-spark-gx` and a nonblank reason in the manifest, SHALL still enforce the checksum, partition row-count and byte gates, and SHALL require an explicit configured downstream gate and accepted-version setting before tagging its output accepted. The manifest SHALL be written last and record configuration and hashes, seed, algorithm/revision, product/contract identities and hashes, partition counts, statistics and reports. Completed versions SHALL be immutable. Local and S3 output SHALL share these guarantees.

#### Scenario: Validated bytes are published

- **WHEN** validation passes
- **THEN** the completed manifest identifies the same final Parquet objects read by the contract checks
- **AND** no data file is rewritten between successful validation and completion

#### Scenario: Deferred contract validation

- **WHEN** an operator publishes with generation-time contract validation skipped
- **THEN** the manifest records the explicit `dp1-spark-gx` gate, skipped result and reason; integrity and size gates still decide publication
- **AND** tagging the output accepted requires both the configured `dp1-spark-gx` gate and accepted-version setting

#### Scenario: Contract violation

- **WHEN** a generation defect yields an illegal lifecycle transition
- **THEN** the payment-events lifecycle check fails, the generator exits nonzero with the native report and no manifest is written

#### Scenario: Existing dataset version

- **WHEN** the output already contains a manifest for the configured dataset version
- **THEN** the normal command verifies its logical configuration, validation gate and file integrity, then retries protection and cleanup without regenerating histories or rewriting completed files or the manifest

#### Scenario: Interrupted run

- **WHEN** generation stops before the manifest is written
- **THEN** consumers identify the dataset version as incomplete by the missing manifest
- **AND** rerunning the normal command automatically resumes compatible unfinished work while refusing an active writer

#### Scenario: Histories and labels cross partition dates

- **WHEN** a payment completes after its creation date or its chargeback becomes available on a later date
- **THEN** publication validation includes its complete history and linked labels rather than treating each date as an independent complete dataset

### Requirement: Local execution

Generation SHALL run as a local CLI from the repository's development environment, with credentials taken only from the environment. It SHALL NOT require a container image, Kubernetes resource or workflow engine. An operator SHALL be able to prepare unpublished final files on one host and continue validation and completion by running the normal command on another host using the same generation identity and compatible run state.

#### Scenario: Workstation run

- **WHEN** an operator runs the `historical` command with the native validation gate and storage credentials in the environment
- **THEN** it generates, validates and publishes the dataset without any cluster access

#### Scenario: Separate preparation and validation

- **WHEN** an operator stops after preparing final files and runs the normal command with the native gate on a host with a larger validation budget
- **THEN** the native contract checks read the prepared objects without regenerating histories
- **AND** the completion manifest is absent until those checks pass

## ADDED Requirements

### Requirement: Bounded generation resources

The operator SHALL configure generation workers, maximum in-flight work, scratch location and capacity, and validation/sort memory, threads and spill budgets. Generation SHALL avoid requiring local storage for the entire remote dataset. Resource exhaustion SHALL stop the run with a diagnostic and no completion manifest. Resource reports SHALL include measured total worker memory and scratch peaks, beyond database memory settings alone.

#### Scenario: Remote dataset exceeds local scratch capacity

- **WHEN** cumulative remote output exceeds the configured local scratch capacity while active tasks and spill fit within their budgets
- **THEN** completed data is uploaded and local scratch is reclaimed so generation continues

#### Scenario: Validation budget exhausted

- **WHEN** a native validation query cannot complete within the configured memory or spill capacity
- **THEN** the run preserves resumable output, reports the failing phase and publishes no completion manifest
- **AND** it does not substitute sampled validation for complete validation

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

Each generation task SHALL write at most one intermediate object per table and physical schema version, appending purchase batches as row groups.

#### Scenario: Task fragments per table and schema version

- **WHEN** a task spans several purchase batches
- **THEN** its events and labels are written as one fragment per table and physical schema version

### Requirement: Accepted-version protection

When the configuration declares the accepted version, publication SHALL tag the completion manifest and every final object as accepted, so a storage policy can deny writes and deletes for that version.

#### Scenario: Recover interrupted protection

- **WHEN** tagging fails or its acknowledgement is lost after the manifest is durable
- **THEN** the command exits nonzero and retains a manifest-bound publication receipt
- **AND** rerunning the same command verifies final object integrity and the logical configuration, skips already protected tags and retries without rewriting completed objects or the manifest
- **AND** regeneration and overwriting of that completed version remain refused

#### Scenario: Accepted version is tagged

- **WHEN** the configuration declares the accepted version and the manifest is written
- **THEN** the manifest and every final object carry the accepted tag
- **AND** candidate and pilot versions are published untagged and remain removable
- **AND** a policy denying writes and deletes on tagged objects cannot alter the accepted dataset

### Requirement: Post-publication cleanup

The normal command SHALL automatically remove `_run/` intermediates only after the manifest is durable, record the removed object and byte counts in a retained publication receipt, and exit nonzero on a refused delete without withdrawing the published version. Cleanup SHALL follow successful protection when accepted tagging is required, preserve the publication receipt, inspect per-object S3 delete errors and count only successful deletes. The normal command SHALL automatically retry cleanup on a completed version without regeneration.

#### Scenario: Intermediates removed after publication

- **WHEN** the normal command completes the manifest write and required protection
- **THEN** `_run/` fragments and task and partition receipts are removed
- **AND** the retained publication receipt records the removed objects and bytes

#### Scenario: Partial cleanup failure

- **WHEN** a batch delete response contains successful removals and per-object errors
- **THEN** the receipt counts only successful removals, the command exits nonzero and rerunning the same command removes the remaining intermediates without rewriting the manifest

#### Scenario: Cleanup refused

- **WHEN** the storage credentials cannot delete intermediates
- **THEN** the published dataset remains intact and the run reports the cleanup failure without writing a second manifest

### Requirement: Probe-scale publication guard

Publication SHALL refuse a dataset smaller than one configured customer block unless the operator explicitly allows small datasets.

#### Scenario: Probe-scale publication refused

- **WHEN** the configured customer count is below one customer block and small datasets are not explicitly allowed
- **THEN** generation stops before validation with no completion manifest
- **AND** prepared output remains available for local or explicitly allowed probes

### Requirement: Scale calibration

A pilot SHALL generate a separate immutable dataset using the same algorithm and physical layout as the scale run. Its report SHALL record actual rows, physical and uncompressed Parquet bytes, throughput, resource peaks and projected full-run time/storage. The full run SHALL use a fixed generation plan selected from that evidence, with declared scenario and data-problem acceptance bounds; it SHALL NOT stop at a timing-dependent byte threshold.

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

The scale configuration SHALL declare a minimum physical source Parquet byte target. Completion SHALL require measured final event-plus-label bytes to meet it, counted once and excluding checkpoints, reports, streaming data, replicas and downstream copies. Reports SHALL also identify uncompressed row-group bytes and GB/GiB units. Coursework acceptance SHALL name its source and distinguish the project's physical-byte target from unspecified rubric compression accounting.

#### Scenario: Coursework scale target

- **WHEN** a run is claimed to satisfy this change's 100 GB acceptance target
- **THEN** final source Parquet files total at least 100,000,000,000 bytes
- **AND** the evidence cites `EDAI K11 - DE.xlsx`, sheet `edai-1 (50%)`, cell B5 as the offline-volume requirement

#### Scenario: Undersized fixed plan

- **WHEN** a completed generation plan produces fewer physical bytes than its configured minimum
- **THEN** the size gate fails with observed and required bytes, preserves its diagnostic state and writes no completion manifest
- **AND** changing the logical plan requires a new dataset version
