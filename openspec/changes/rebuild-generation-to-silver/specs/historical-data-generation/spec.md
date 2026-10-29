## MODIFIED Requirements

### Requirement: Validated generator configuration

The generator SHALL read one configuration file declaring dataset version, seed, synthetic integration identity, start date and duration (default 90 days), entity counts, purchase behavior including merchant and category skew, per-scenario rates and parameters, a duplicate-export rate, ordered physical schema changes, label delays and output root. Invalid or inconsistent configuration, including an unsupported physical schema version or unordered schema changes, SHALL fail before any output is written. Credentials SHALL NOT be accepted in the configuration file.

#### Scenario: Unknown scenario name

- **WHEN** the configuration enables a scenario that does not exist
- **THEN** the generator exits nonzero naming the invalid field and writes nothing

#### Scenario: Unsupported schema version

- **WHEN** a schema change declares a physical version other than 3 or 4, or changes are not in ascending time order
- **THEN** the generator exits nonzero before writing output

### Requirement: Ninety-day development population

The maintained development configuration SHALL publish dataset v18: 90 purchase-start dates from 2025-07-01 for a modest US/USD electronics population with repeat behavior and supported fraud scenarios, a 2% duplicate-export rate and a change from physical schema 3 to 4 on 2025-08-15. It SHALL remain a development source rather than evidence of scale or real-world model quality. Published v8–v17 datasets SHALL remain untouched and are not readable by the rebuilt pipeline.

#### Scenario: Default development range

- **WHEN** the development configuration is generated
- **THEN** its last purchase-start date is 2025-09-28, both physical schema versions are present and its manifest records actual rows rather than promised totals

## ADDED Requirements

### Requirement: Deterministic per-day generation

The same logical configuration and generator revision SHALL produce the same records. Worker count, day completion order and interruption SHALL NOT change records. Each purchase-start day SHALL be generated independently of other days. Different seeds SHALL produce different histories with the same configured statistical shape.

#### Scenario: Regenerate a dataset

- **WHEN** a dataset is generated twice to separate locations with the same configuration and revision
- **THEN** both outputs contain identical records and identical manifest row counts

#### Scenario: Change parallelism after interruption

- **WHEN** an interrupted run is rerun with a different worker count
- **THEN** its records equal those of an uninterrupted run

### Requirement: Shared entity world

The entity world (entity pools, identities, fingerprints, skew weights, purchase behavior and compromise schedules) SHALL be built from the generator configuration and seed alone, by one module shared with the live simulator. Merchant and customer IDs and address, IP-network and payment-method fingerprints SHALL match the shared identity vectors. The manifest SHALL record the complete logical configuration, so a consumer can rebuild the same world without reading generated files.

#### Scenario: Rebuild the world from a manifest

- **WHEN** the live simulator builds the entity world from a v18 manifest's recorded configuration
- **THEN** it obtains the same entities and payment-method fingerprints the generator used

#### Scenario: Identity vectors

- **WHEN** the generator derives IDs and fingerprints for each case in the shared identity vectors
- **THEN** it produces exactly the listed values

### Requirement: Configured data problems

The configuration SHALL control merchant/category Zipf skew, entity pool sizes, an identical duplicate-export rate and dated physical schema changes. Each event's physical schema version SHALL be chosen by its occurrence time; version 3 SHALL omit the assessment `assessment_id` and `evidence_sha256` fields and version 4 SHALL include them. Every event row SHALL carry its `physical_schema_version`. Duplicate exports SHALL repeat the original event identity, content and physical version.

#### Scenario: Schema change boundary

- **WHEN** a payment's risk decisions occur before and after a configured change to version 4
- **THEN** the earlier events have version 3 with the two fields absent and the later events have version 4 with both fields present

#### Scenario: Duplicate export

- **WHEN** the duplicate-export rate is greater than zero
- **THEN** repeated events are byte-identical in business content to their originals, including physical schema version

### Requirement: Per-day immutable dataset layout

Output SHALL be written under `historical/<dataset_version>/` as `payment_events/date=<purchase-start date>/part-*.parquet` and `fraud_labels/date=<purchase-start date>/part-*.parquet`, keeping each payment's full lifecycle and labels in its purchase-start day. The generator SHALL write the logical configuration and its hash before any data file, and SHALL refuse to continue a version whose stored configuration differs. A day MAY consist of several parts with deterministic names; a day SHALL be complete only when its completion marker lists every part with rows, bytes and SHA-256. A rerun SHALL skip complete days and regenerate incomplete ones. Each data file SHALL become visible only when complete. Memory and local scratch SHALL be bounded by the configured chunk size and worker count, independent of dataset size. Publication SHALL list every data file with rows, bytes and SHA-256, the total physical bytes, the seed, configuration, generator revision and contract identities in `manifest.json`, written last and only if absent. If the configuration declares a minimum source size and the total is smaller, publication SHALL refuse and report both values. A version with a manifest SHALL be immutable. Output SHALL conform to the `historical-payments` contracts; source publication SHALL NOT claim Bronze or Silver quality acceptance.

#### Scenario: Rerun after interruption

- **WHEN** generation stops after some days and is rerun with the same configuration
- **THEN** completed days are skipped, missing days are generated and the manifest is published once all days exist

#### Scenario: Changed configuration under an existing version

- **WHEN** a rerun uses a configuration whose hash differs from the stored one
- **THEN** the generator refuses and modifies nothing

#### Scenario: Published version

- **WHEN** the generator targets a version that already has a manifest
- **THEN** it refuses and modifies nothing

#### Scenario: Undersized source

- **WHEN** all days are complete but the total physical bytes are below the configured minimum
- **THEN** no manifest is written and the run reports observed and required bytes

### Requirement: Generator execution

Environment datasets SHALL be generated by an on-demand cluster workflow running the simulator image as one pod, with the configuration selected by `SIMULATOR_CONFIG`, environment-supplied object credentials, explicit resources, a deadline and retries that resume from completed days. The same code SHALL run locally against a directory for tests and development. Local and S3-compatible roots SHALL have equivalent guarantees.

#### Scenario: Pod replaced during generation

- **WHEN** the generator pod is lost after some days completed and the workflow retries
- **THEN** the retry skips completed days and the published dataset equals an uninterrupted run

### Requirement: Generator diagnostics

The repository SHALL provide an on-demand diagnostics job over ingested Bronze data that reports per-version row counts and v4-field null counts, merchant and category top-share and skew, entity cardinality with exact values distinguished from approximate estimates, and the identical-duplicate rate with its denominator. Diagnostics SHALL be informational and SHALL NOT gate generation, Bronze or Silver.

#### Scenario: Diagnostics after ingestion

- **WHEN** the diagnostics job runs against Bronze containing v18
- **THEN** its report lists each measure with its denominator and marks approximate cardinalities as estimates

### Requirement: Full-scale source generation

A maintained full-scale configuration SHALL publish dataset v19 to the prod bucket: 365 purchase-start days for about 3 million customers, with the same behavior, scenarios, 2% duplicate-export rate and a mid-period change from physical schema 3 to 4, and a declared minimum of 100,000,000,000 physical Parquet bytes across listed event and label files. It SHALL use the same generator, layout and manifest as v18. Publication establishes only the source; Bronze and Silver at full scale and coursework evidence remain later work.

#### Scenario: Full-scale publication

- **WHEN** the full-scale workflow completes in prod
- **THEN** `historical/v19/manifest.json` exists with a total of at least 100,000,000,000 physical bytes and both physical schema versions present

#### Scenario: Scale claim from the development source

- **WHEN** the v18 development configuration completes
- **THEN** no scale or coursework-volume acceptance is claimed

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
**Reason**: Scratch and in-flight budgeting are replaced by chunk-bounded parts.
**Migration**: See "Per-day immutable dataset layout".

### Requirement: Verified resume
**Reason**: Run-state checkpoints, writer ownership and receipt adoption are replaced by per-day atomic files and a stored configuration hash.
**Migration**: Rerun the same command; completed days are skipped.

### Requirement: Bounded task fragments
**Reason**: Task fragments are replaced by per-day files.
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
**Reason**: Pilot projections are replaced by measuring the dev run before the full-scale run.
**Migration**: See "Full-scale source generation".

### Requirement: Offline volume accounting
**Reason**: Replaced by the configured minimum source size in the manifest-publication rule.
**Migration**: See "Full-scale source generation".

### Requirement: Generation-only simulator boundary
**Reason**: Legacy handoff and retired-configuration rules no longer apply to a fresh generator.
**Migration**: None; old datasets are untouched.

### Requirement: Reproducible training source and report
**Reason**: Replaced by the v18 development configuration and the diagnostics job; the feature-definition identity is no longer part of source provenance.
**Migration**: See "Ninety-day development population" and "Generator diagnostics".

### Requirement: Exercise before scale generation
**Reason**: Live/landing acceptance before scale generation no longer applies; evidence capture is out of scope.
**Migration**: See "Full-scale source generation".
