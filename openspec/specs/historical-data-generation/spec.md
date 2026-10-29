## Purpose

Generate reproducible ecommerce payment histories with known fraud scenarios, configurable offline data problems and delayed labels, published as immutable versioned datasets that satisfy the historical-payments data product contracts.

## Requirements

### Requirement: Validated generator configuration

The generator SHALL read one configuration file declaring dataset version, seed, synthetic integration identity, start date and duration (default 90 days), entity counts, purchase behavior, per-scenario rates and parameters, data-problem settings, label delays and output location. Invalid or inconsistent configuration SHALL fail before any output is written. Credentials SHALL NOT be accepted in the configuration file.

#### Scenario: Unknown scenario name

- **WHEN** the configuration enables a scenario that does not exist
- **THEN** the generator exits nonzero naming the invalid field and writes nothing

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

### Requirement: Ecommerce behavior and fraud scenarios

Legitimate customers SHALL purchase mostly from familiar merchants, categories, devices and shipping addresses, with amounts drawn from category/customer behavior. The generator SHALL support individually enabled and parameterized `obvious_high_value`, `stolen_card_new_device`, `card_testing`, `account_takeover` and `friendly_fraud` experiments. Baseline realism SHALL include overlapping legitimate/fraud amounts, device changes and network sharing, with temporal/campaign patterns across attempts. `obvious_high_value` SHALL be an explicit stress scenario rather than establishing realism. Scenario names SHALL remain outside payment features.

#### Scenario: Card testing burst

- **WHEN** `card_testing` is enabled
- **THEN** low-value attempts arrive in campaign bursts across multiple payment methods with configurable context reuse and elevated processor declines

#### Scenario: Scenario disabled

- **WHEN** a scenario rate is zero
- **THEN** no label carries that scenario

#### Scenario: Legitimate unusual purchase

- **WHEN** a legitimate customer spends heavily or changes devices
- **THEN** the generated observation does not automatically acquire fraudulent intent

### Requirement: Lifecycle histories

Every generated payment SHALL be a complete history covering risk declines, processor declines, unknown processor outcomes that later resolve, retries and expiries. Historical risk decisions SHALL match the gateway's existing versioned policy semantics for the same contracted inputs and record the applicable policy version. Fraud intent, risk decisions and processor outcomes SHALL remain separate. Historical generation SHALL NOT call gateway APIs or write to payment event storage.

#### Scenario: Retried payment

- **WHEN** an attempt is declined and the customer retries
- **THEN** the next attempt starts on the same payment after the decline and the history ends in success or expiry

### Requirement: Offline data problems

The configuration SHALL control merchant/category skew, identity pools and explicit data-problem experiments. Baseline realism SHALL use the current collection schema and a declared duplicate-export setting without automatically inheriting an artificial historical cutover or fixed duplicate rate. Schema-evolution experiments SHALL declare cutover dates and physical schema shapes. Duplicate exports SHALL repeat original event identity/content. The manifest SHALL record configured values and cheap additive observed row/scenario/duplicate/schema totals and bounded skew counters, distinguish identity pools from measurements and mark observed cardinality validation as downstream without fabricated counts or remote analytical scans. DP1 cardinality reports SHALL identify exact counts versus estimates and each ratio's denominator.

#### Scenario: Schema evolution cutover

- **WHEN** an explicit experiment spans its declared schema cutover
- **THEN** raw files use the declared physical shapes and downstream merged reads preserve the evolution

#### Scenario: Duplicate export

- **WHEN** an experiment requests duplicate exports
- **THEN** repeated events retain their identity/content and the manifest reports configured and observed duplication

#### Scenario: Scale profile data problems

- **WHEN** a scale dataset is accepted
- **THEN** source evidence reports additive merchant/category skew, duplicate and schema counts, while DP1 quality evidence supplies observed identity cardinalities and ratios
- **AND** physical size is not increased through padding, repeated copies of an accepted dataset or an inflated duplicate rate

### Requirement: Delayed labels and truth separation

The generator SHALL emit simulation-truth labels with their scenario for every attempt in the separate `fraud_labels` dataset. Chargeback reporting probability and delay SHALL be configurable independently of hidden fraud intent. Chargebacks SHALL reference only succeeded attempts; `friendly_fraud` SHALL permit a legitimate succeeded attempt to receive a chargeback without changing its underlying truth intent. Label occurrence SHALL use the generated attempt's business timestamp consistently, reporting delay SHALL begin after success, and availability SHALL remain distinct from occurrence. Scenario/oracle information and future outcomes SHALL NOT become predecision features. Evidence SHALL distinguish oracle truth from reported chargebacks and immature/unreported labels; absence of a chargeback SHALL NOT establish a negative. Consumer joins SHALL respect label availability.

#### Scenario: Declined fraudulent attempt

- **WHEN** a fraudulent attempt is declined
- **THEN** it has a simulation-truth label and no chargeback label

#### Scenario: Recent or unreported fraud

- **WHEN** a successful fraudulent attempt has not produced an available chargeback
- **THEN** observed-label evidence does not present it as a confirmed negative

### Requirement: Local execution

Generation SHALL run as a local CLI from the repository development environment with environment-only credentials and no cluster requirement. `--prepare-only` SHALL leave generated raw files and verified inventory ready without sorting or a completion manifest. A compatible normal command SHALL continue publication without regenerating rows.

#### Scenario: Workstation run

- **WHEN** an operator runs `historical` with storage credentials
- **THEN** it generates/uploads, verifies source gates, publishes manifest-last, then protects output and cleans only transient run data
- **AND** DP1 quality processing remains downstream

#### Scenario: Prepared inventory handoff

- **WHEN** generated inventory is prepared on one host and a compatible normal command continues on another
- **THEN** it verifies and publishes the same raw inventory without sorting, compaction, native validation or regenerated rows

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

### Requirement: Completed-source immutability

Publication SHALL use no object tags or accepted-version configuration. A completed manifest and its inventory SHALL remain immutable: incompatible configuration or corrupt source content SHALL fail recovery without rewriting published files. Cleanup SHALL remain restricted to transient run metadata.

#### Scenario: Completed source recovery

- **WHEN** a completed dataset is opened for publication recovery
- **THEN** the command verifies its logical configuration and every inventory checksum before retrying cleanup
- **AND** it does not regenerate, overwrite or tag completed source files or the manifest

### Requirement: Post-publication cleanup

The normal command SHALL clean only transient `_run/` objects after durable publication, retaining raw files and a manifest-bound publication receipt. It SHALL count only successful deletions, report failures without withdrawing publication and automatically retry cleanup on a compatible completed version without regeneration.

#### Scenario: Transient objects removed after publication

- **WHEN** the normal command completes the manifest write
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

### Requirement: Source-informed US training population

The generator SHALL support a US electronics ecommerce population in USD with varied customer purchase frequencies, repeat merchants/categories, plausible baskets and local-hour/weekend/seasonal variation. Account creation timestamps SHALL remain fixed per entity and SHALL NOT follow purchase dates. Internal IANA timezone rules SHALL determine local purchase scheduling while stored timestamps remain UTC. Hidden generation context SHALL NOT become observable model features. Calibration inputs SHALL be versioned, available offline and identified in generation provenance, with source references, reference periods, reuse terms, checksums and explicit assumptions.

#### Scenario: Repeat customer across dates

- **WHEN** a customer purchases on different days
- **THEN** its identity and account creation timestamp stay fixed while purchase context can vary

#### Scenario: Daylight saving transition

- **WHEN** regional purchase scheduling crosses a DST transition
- **THEN** local-hour patterns use the applicable IANA rules and output preserves valid distinct UTC instants

#### Scenario: Calibration changes

- **WHEN** logical calibration inputs change
- **THEN** generation identity changes and an incompatible existing run cannot be resumed

### Requirement: Realistic existing observations

Generated device/network/browser/address fields SHALL follow existing collection and normalization semantics with legitimate sharing, changes and optional signal absence. Scenario identity SHALL NOT be exposed through reserved visible fingerprint/subnet formats. Shipping country SHALL NOT substitute for unavailable IP country. The dataset SHALL NOT claim observations absent from the platform's existing collection contracts.

#### Scenario: Shared device or missing signal

- **WHEN** legitimate customers share a device/network or a signal is unavailable
- **THEN** observed values can coincide or be absent without automatically implying fraudulent intent

### Requirement: Reproducible training source and report

The maintained configuration SHALL publish a new immutable 90-day smoke/development source using the current event contract, preserving published v8/v11/v12/v13, stopped incomplete v14 and all prior evidence. It SHALL retain reproducibility, bounded resources, resume, integrity, row accounting and manifest-last publication. Full-volume generation SHALL remain an operator-run Python job under a new version, deferred until the pipeline is built and exercised; a Spark generator SHALL NOT be required. A concise dataset report SHALL record lineage, seed/calibration/feature-definition identity, rows/bytes, date ranges, distributions, label maturity, explicit rate denominators and recommended temporal/entity/campaign evaluation boundaries. Distribution/shortcut reports SHALL remain diagnostics. Source publication SHALL NOT establish downstream DP1 quality acceptance.

#### Scenario: Published dataset already exists

- **WHEN** the improved logical plan encounters an existing published version
- **THEN** it refuses modification and uses an unused version
- **AND** no published source or stopped partial v14 is repurposed for the new contract

#### Scenario: Training handoff

- **WHEN** the dataset is handed to a training consumer
- **THEN** the handoff distinguishes truth from observed labels, excludes future/oracle features and identifies label maturity and quality acceptance status

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
