## MODIFIED Requirements

### Requirement: Validated generator configuration

The generator SHALL read one configuration file declaring dataset version, seed, synthetic integration identity, start date and duration (default 90 days), entity counts, purchase behavior including merchant and category skew and the foreign-IP probability, per-scenario rates and parameters, a duplicate-export rate, ordered schema changes, label delays, an optional minimum source size and output root. Run settings (mode, shard, worker count and contract directory) SHALL come from the environment, not the configuration file. Invalid or inconsistent configuration, including a schema change to a version below 3 or unordered schema changes, SHALL fail before any output is written. Credentials SHALL NOT be accepted in the configuration file.

#### Scenario: Unknown scenario name

- **WHEN** the configuration enables a scenario that does not exist
- **THEN** the generator exits nonzero naming the invalid field and writes nothing

#### Scenario: Unsupported schema version

- **WHEN** a schema change declares a version below 3, or changes are not in ascending time order, or a version the payment-events contract does not declare
- **THEN** the generator exits nonzero before writing output

### Requirement: Ninety-day development population

The maintained development configuration SHALL publish dataset v18: 90 purchase-start dates from 2026-07-31 through 2026-10-28 for a modest US/USD electronics population with repeat behavior, supported fraud scenarios, a 2% republished-export rate, a hot merchant share of 0.5, and a change to schema version 3 on 2026-09-15. It SHALL remain a development source rather than evidence of scale or real-world model quality. Published v8–v17 datasets SHALL remain untouched and are not readable by the rebuilt pipeline.

#### Scenario: Default development range

- **WHEN** the development configuration is generated
- **THEN** its last purchase-start date is 2026-10-28, both schema versions (2 and 3) are present and its manifest records actual rows rather than promised totals

## ADDED Requirements

### Requirement: Deterministic per-day generation

The same logical configuration and generator digest SHALL produce the same records. Purchases SHALL be keyed by purchase-start day, customer and that customer's purchase number that day; card-testing bursts SHALL be keyed by day and burst. Worker count, the division of a day into parts, day completion order and interruption SHALL NOT change records. Each purchase-start day SHALL be generated independently of other days. Different seeds SHALL produce different histories with the same configured statistical shape.

#### Scenario: Regenerate a dataset

- **WHEN** a dataset is generated twice to separate locations with the same configuration and digest
- **THEN** both outputs contain identical records and identical manifest row counts

#### Scenario: Change parallelism or split after interruption

- **WHEN** an interrupted run is rerun with a different worker count or a different part size
- **THEN** its records equal those of an uninterrupted run

### Requirement: Shared entity world

The entity world (entity pools, identities, fingerprints, skew weights, purchase behavior, customer activity and compromise schedules) SHALL be built from the generator configuration and seed alone, by one module shared with the live simulator. Merchant and customer IDs and address, IP-network and payment-method fingerprints SHALL match the shared identity vectors. The manifest SHALL record the complete logical configuration, so a consumer can rebuild the same world without reading generated files. The live simulator SHALL reject a manifest whose recorded world digest differs from the digest of the world it builds from that configuration.

#### Scenario: Rebuild the world from a manifest

- **WHEN** the live simulator builds the entity world from a v18 manifest's recorded configuration
- **THEN** it obtains the same entities and payment-method fingerprints the generator used

#### Scenario: World digest mismatch

- **WHEN** a manifest's world digest does not match the world built from its recorded configuration
- **THEN** the live simulator refuses the manifest

#### Scenario: Identity vectors

- **WHEN** the generator derives IDs and fingerprints for each case in the shared identity vectors
- **THEN** it produces exactly the listed values

### Requirement: Gateway-aligned fraud policy

Each simulated attempt SHALL receive the gateway's policy decision: `HIGH_AMOUNT` for amounts above 200000 minor units, and `NEW_ACCOUNT_GEO_MISMATCH` when the IP country differs from the shipping country, the customer account is younger than seven days and the amount exceeds 20000 minor units. An attempt SHALL be declined when any reason code applies, and every reason code SHALL be recorded with the gateway's policy version. IP countries SHALL follow the configured foreign-IP probability and the compromise scenarios.

#### Scenario: Geo mismatch on a new account

- **WHEN** an attempt for a new account with an IP country different from the shipping country has an amount above 20000 minor units
- **THEN** the attempt is declined with the `NEW_ACCOUNT_GEO_MISMATCH` reason code

### Requirement: Configured data problems

The configuration SHALL control merchant/category Zipf skew, a hot merchant share (the share of ordinary purchases that go to the first merchant key before the Zipf tail), entity pool sizes, a republished-export rate and dated schema changes. Schema versions SHALL be declared by the historical payment-events contract: a field declared with `sinceSchemaVersion` N SHALL be absent from every event whose `schema_version` is below N. Each event's `schema_version` SHALL be chosen by its occurrence time: 2 before the configured change and the change's version from it. Every event row SHALL carry its `schema_version`. A republished export SHALL repeat the original event's identity, business content and schema version, with a `recorded_at` 1 to 300 seconds later. A version the contract does not declare SHALL be refused before any output is written.

#### Scenario: Schema change boundary

- **WHEN** a payment's risk decisions occur before and after a configured change to version 3
- **THEN** the earlier events have schema version 2 with the assessment identity fields absent, and the later events have schema version 3 with both fields present

#### Scenario: Schema version parts

- **WHEN** a version-2 part and a version-3 part are opened
- **THEN** the version-2 Parquet schema lacks the fields declared with `sinceSchemaVersion` 3, and the version-3 Parquet schema has them

#### Scenario: Republished export

- **WHEN** the republished-export rate is greater than zero
- **THEN** a repeated event has the original's event identity, business content and schema version, and a later `recorded_at`

#### Scenario: Hot merchant share

- **WHEN** the hot merchant share is 0.9 on the small configuration
- **THEN** the first merchant receives roughly that share of ordinary purchases, plus its share of the Zipf tail

### Requirement: Per-day immutable dataset layout

Output SHALL be written under `historical/<dataset_version>/` as `payment_events/date=<purchase-start date>/part-NNNNN.v<schema_version>.parquet` and `fraud_labels/date=<purchase-start date>/part-NNNNN.parquet`, keeping each payment's full lifecycle and labels in its purchase-start day. The generator SHALL write the logical configuration, the generator digest and their hash before any data file, and SHALL refuse to continue a version whose stored configuration or digest differs. A day MAY consist of several parts with deterministic names; a day SHALL be complete only when its completion marker lists every part with rows, bytes and SHA-256. A rerun SHALL skip complete days and regenerate incomplete ones. Each data file SHALL become visible only when complete. Memory and local scratch SHALL be bounded by the internal part size and worker count, independent of dataset size. Publication SHALL list every data file with rows, bytes and SHA-256, the total physical bytes, the logical configuration, the generator digest and contract identities in `manifest.json`, written last and only if absent. If the configuration declares a minimum source size and the total is smaller, publication SHALL refuse and report both values. A version with a manifest SHALL be immutable. Output SHALL conform to the `historical-payments` contracts; source publication SHALL NOT claim Bronze or Silver quality acceptance.

#### Scenario: Rerun after interruption

- **WHEN** generation stops after some days and is rerun with the same configuration
- **THEN** completed days are skipped, missing days are generated and the manifest is published once all days exist

#### Scenario: Changed configuration or contract under an existing version

- **WHEN** a rerun uses a configuration or generator digest that differs from the stored one
- **THEN** the generator refuses and modifies nothing

#### Scenario: Published version

- **WHEN** the generator targets a version that already has a manifest
- **THEN** it refuses and modifies nothing

#### Scenario: Undersized source

- **WHEN** all days are complete but the total physical bytes are below the configured minimum
- **THEN** no manifest is written and the run reports observed and required bytes

### Requirement: Generator execution

Environment datasets SHALL be generated by an on-demand cluster workflow running the simulator image: shard pods generate disjoint sets of purchase-start days in parallel, and a publish pod writes the manifest once every day has a marker. Each pod SHALL take its configuration from `SIMULATOR_CONFIG` and its mode, shard, worker count and contract directory from the environment, with environment-supplied object credentials, explicit resources, a deadline and retries that resume from completed days. The same code SHALL run locally against a directory for tests and development. Local and S3-compatible roots SHALL have equivalent guarantees.

#### Scenario: Pod replaced during generation

- **WHEN** a shard pod is lost after some days completed and the workflow retries
- **THEN** the retry skips completed days and the published dataset equals an uninterrupted run

### Requirement: Generator diagnostics

The repository SHALL provide an on-demand diagnostics job over ingested Bronze data that reports, each with its denominator: per-version row counts and null counts for fields declared with `sinceSchemaVersion`; the top merchant share, the top-10 merchant share and the top category share of purchases; the near-duplicate rate, the share of distinct events that have a republished export; and entity cardinality, with exact values distinguished from approximate estimates. Diagnostics SHALL be informational and SHALL NOT gate generation, Bronze or Silver.

#### Scenario: Diagnostics after ingestion

- **WHEN** the diagnostics job runs against Bronze containing v18
- **THEN** its report lists each measure with its denominator, shows the skew levels and marks approximate cardinalities as estimates

### Requirement: Full-scale source generation

A maintained full-scale configuration SHALL publish dataset v19 to the prod bucket: 365 purchase-start days from 2025-10-29 through 2026-10-28 for about 3 million customers, with the same behavior, scenarios, republished-export rate and hot merchant share, a change to schema version 3 on 2026-04-30, and a declared minimum of 100,000,000,000 physical Parquet bytes across listed event and label files. It SHALL use the same generator, layout and manifest as v18. Publication establishes only the source; Bronze and Silver at full scale and coursework evidence remain later work.

#### Scenario: Full-scale publication

- **WHEN** the full-scale workflow completes in prod
- **THEN** `historical/v19/manifest.json` exists with a total of at least 100,000,000,000 physical bytes and both schema versions (2 and 3) present

#### Scenario: Scale claim from the development source

- **WHEN** the v18 development configuration completes
- **THEN** no scale or coursework-volume acceptance is claimed

### Requirement: Dataset end and label availability

Purchase-start days SHALL end on the last full day before the dataset is generated, so the history reaches the present and rolling windows at its end are full when live traffic continues the same world. Lifecycle tails and delayed chargeback labels SHALL keep their real timestamps, even when they fall after the generation date, so a label can become available later than the last purchase-start day. Documentation SHALL state that such labels are not yet available at generation time, and consumers SHALL apply an as-of cutoff to `label_available_at` and `occurred_at` for any training or feature use.

#### Scenario: Recent window

- **WHEN** a consumer computes a 30-day lookback as of 2026-10-29
- **THEN** the lookback covers purchase-start days from 2026-09-29 through 2026-10-28 in full

#### Scenario: Label not yet available

- **WHEN** a chargeback label has a `label_available_at` after the generation date
- **THEN** it keeps its real timestamp and an as-of cutoff at the generation date excludes it

## REMOVED Requirements

### Requirement: Reproducible generation
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Deterministic per-day generation".

### Requirement: Continuable by live simulation
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Shared entity world".

### Requirement: Offline data problems
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Configured data problems".

### Requirement: Contract-gated immutable output
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Per-day immutable dataset layout".

### Requirement: Local execution
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Generator execution".

### Requirement: Bounded generation resources
**Reason**: Scratch and in-flight budgeting are covered by bounded parts.
**Migration**: See "Per-day immutable dataset layout".

### Requirement: Verified resume
**Reason**: Run-state checkpoints, writer ownership and receipt adoption are covered by per-day atomic files and a stored configuration hash.
**Migration**: Rerun the same command; completed days are skipped.

### Requirement: Bounded task fragments
**Reason**: Task fragments are covered by per-day files.
**Migration**: See "Per-day immutable dataset layout".

### Requirement: Completed-source immutability
**Reason**: Folded into "Per-day immutable dataset layout".
**Migration**: None.

### Requirement: Post-publication cleanup
**Reason**: No transient `_run/` state exists anymore.
**Migration**: None.

### Requirement: Probe-scale publication guard
**Reason**: Small datasets are valid development and test inputs.
**Migration**: None.

### Requirement: Scale calibration
**Reason**: Pilot projections are covered by measuring the dev run before the full-scale run.
**Migration**: See "Full-scale source generation".

### Requirement: Offline volume accounting
**Reason**: Covered by the configured minimum source size in the manifest-publication rule.
**Migration**: See "Full-scale source generation".

### Requirement: Generation-only simulator boundary
**Reason**: Legacy handoff and retired-configuration rules no longer apply to a fresh generator.
**Migration**: None; old datasets are untouched.

### Requirement: Reproducible training source and report
**Reason**: Covered by the v18 development configuration and the diagnostics job; the feature-definition identity is no longer part of source provenance.
**Migration**: See "Ninety-day development population" and "Generator diagnostics".

### Requirement: Exercise before scale generation
**Reason**: Live/landing acceptance before scale generation no longer applies; evidence capture is out of scope.
**Migration**: See "Full-scale source generation".
