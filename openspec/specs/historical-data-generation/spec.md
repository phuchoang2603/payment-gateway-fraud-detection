## Purpose

Generate reproducible ecommerce payment histories with known fraud scenarios, configurable offline data problems and delayed labels, published as immutable versioned datasets that satisfy the historical-payments data product contracts.

## Requirements

### Requirement: Validated generator configuration

The generator SHALL read one configuration file declaring dataset version, seed, synthetic integration identity, start date and duration (default 90 days), entity counts, purchase behavior including merchant and category skew and the foreign-IP probability, per-scenario rates and parameters, a duplicate-export rate, ordered schema changes, label delays, an optional minimum source size and output root. Run settings (mode, shard, worker count and contract directory) SHALL come from the environment, not the configuration file. Invalid or inconsistent configuration, including a schema change to a version below 3 or unordered schema changes, SHALL fail before any output is written. Credentials SHALL NOT be accepted in the configuration file.

#### Scenario: Unknown scenario name

- **WHEN** the configuration enables a scenario that does not exist
- **THEN** the generator exits nonzero naming the invalid field and writes nothing

#### Scenario: Unsupported schema version

- **WHEN** a schema change declares a version below 3, or changes are not in ascending time order, or a version the payment-events contract does not declare
- **THEN** the generator exits nonzero before writing output

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

### Requirement: Delayed labels and truth separation

The generator SHALL emit simulation-truth labels with their scenario for every attempt in the separate `fraud_labels` dataset. Chargeback reporting probability and delay SHALL be configurable independently of hidden fraud intent. Chargebacks SHALL reference only succeeded attempts; `friendly_fraud` SHALL permit a legitimate succeeded attempt to receive a chargeback without changing its underlying truth intent. Label occurrence SHALL use the generated attempt's business timestamp consistently, reporting delay SHALL begin after success, and availability SHALL remain distinct from occurrence. Scenario/oracle information and future outcomes SHALL NOT become predecision features. Evidence SHALL distinguish oracle truth from reported chargebacks and immature/unreported labels; absence of a chargeback SHALL NOT establish a negative. Consumer joins SHALL respect label availability.

#### Scenario: Declined fraudulent attempt

- **WHEN** a fraudulent attempt is declined
- **THEN** it has a simulation-truth label and no chargeback label

#### Scenario: Recent or unreported fraud

- **WHEN** a successful fraudulent attempt has not produced an available chargeback
- **THEN** observed-label evidence does not present it as a confirmed negative

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

### Requirement: Ninety-day development population

The maintained development configuration SHALL publish dataset v18: 90 purchase-start dates from 2026-07-31 through 2026-10-28 for a modest US/USD electronics population with repeat behavior, supported fraud scenarios, a 2% republished-export rate, a hot merchant share of 0.5, and a change to schema version 3 on 2026-09-15. It SHALL remain a development source rather than evidence of scale or real-world model quality. Published v8–v17 datasets SHALL remain untouched and are not readable by the rebuilt pipeline.

#### Scenario: Default development range

- **WHEN** the development configuration is generated
- **THEN** its last purchase-start date is 2026-10-28, both schema versions (2 and 3) are present and its manifest records actual rows rather than promised totals

### Requirement: Complete development history tails

The 90-day limit SHALL apply to purchase starts, not lifecycle completion or label availability. Publication SHALL include complete histories and scheduled labels after the last start date, with separate observed timestamp ranges. Warmup and label-maturity guidance SHALL identify usable training/evaluation cohorts without classifying absent chargebacks as negative truth.

#### Scenario: Last-day purchase with delayed chargeback

- **WHEN** a purchase on day 90 succeeds and has a later available chargeback
- **THEN** its terminal events and scheduled label remain in the published inventory even when their timestamps lie outside the purchase-start range

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
