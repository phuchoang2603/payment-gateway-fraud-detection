## ADDED Requirements

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

The improved algorithm SHALL publish v13 as a small smoke/reference dataset after the stopped unpublished full-scale v13 is entirely removed, preserving v8/v11/v12 and all prior evidence. Existing reproducibility, bounded resource, resume, integrity, row accounting and manifest-last publication requirements SHALL remain enforced. Measured machine/throughput evidence SHALL inform bounded smoke resource settings. Full-volume distributed Spark generation and its physical-size target SHALL remain a separate follow-up. A concise dataset report SHALL record lineage, seed/calibration identity, rows/bytes, date ranges, distributions, label maturity, explicit rate denominators and recommended temporal/entity/campaign evaluation boundaries. Distribution/shortcut reports SHALL remain diagnostics. Source publication SHALL NOT establish downstream DP1 quality acceptance.

#### Scenario: Published dataset already exists

- **WHEN** the improved logical plan encounters an existing published version
- **THEN** it refuses modification and uses an unused version
- **AND** only the explicitly user-authorized deletion of unpublished partial v13 permits v13 smoke reuse

#### Scenario: Training handoff

- **WHEN** the dataset is handed to a training consumer
- **THEN** the handoff distinguishes truth from observed labels, excludes future/oracle features and identifies label maturity and quality acceptance status

## MODIFIED Requirements

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
