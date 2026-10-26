## ADDED Requirements

### Requirement: Source-informed US calibration

The regenerated population SHALL represent US customers and merchants using USD, with regional time zones and domestic activity variation. Calibration inputs SHALL be versioned and available offline. Each external input SHALL record its publisher, URL, reference period, retrieval date, checksum, reuse terms, extraction method and population limitations. Measured parameters, transferred approximations and unsupported assumptions SHALL be distinguished. Calibration identity SHALL participate in generation identity and manifest provenance. Generation SHALL NOT depend on live internet requests or reproduce identifiable source customers.

#### Scenario: Reference population differs from checkout population

- **WHEN** a source describes all consumer payments or a non-US retailer
- **THEN** the calibration records that limitation and does not present its frequencies as directly measured US electronics checkout behavior

#### Scenario: Calibration changes

- **WHEN** a source snapshot or logical calibration parameter changes
- **THEN** logical generation identity changes and the changed plan cannot resume into the previous dataset version

### Requirement: Stable entities and shared temporal behavior

Historical generation and current-time live simulation SHALL use the same versioned entity and behavior model. Customer and merchant account creation timestamps SHALL remain fixed across purchases and continuation, and purchases SHALL NOT predate the relevant accounts. The model SHALL support heterogeneous customer activity/spending, sessions, local-time seasonality, shared households/devices/networks, and dated device/network changes. A day's schedule SHALL remain reproducible without generating earlier purchase records and independently of worker/task order. Live commands SHALL use machine time and gateway-owned event timestamps without requiring catch-up or a maximum calendar gap.

#### Scenario: Repeat purchases across history and live continuation

- **WHEN** the same customer purchases on different historical days and later through the live runner
- **THEN** the customer's identity and account creation timestamp remain the same while observed device/network context can change according to the dated model

#### Scenario: Rebuild a campaign day

- **WHEN** a day inside a session or campaign window is reconstructed directly
- **THEN** its scheduled entities and contexts match reconstruction under the same seed, model, calibration and logical arrival settings during the full run

### Requirement: Checkout-observable simulation signals

Historical and live simulation SHALL use consistent checkout observation semantics for browser families, device fingerprints, network fingerprints and missingness. Latent device/network identities SHALL NOT be substituted for observed fingerprints where checkout cannot collect them. Shipping country SHALL NOT be used as IP country; IP country SHALL remain absent in the collection mode without GeoIP enrichment. Hidden scenario/oracle attributes SHALL NOT enter payment decision inputs or feature fields.

#### Scenario: Collector has no IP-country enrichment

- **WHEN** a purchase has a US shipping address but the selected collector cannot determine IP country
- **THEN** its IP-country signal is absent in both historical and live observations

#### Scenario: Shared device or reset fingerprint

- **WHEN** customers share a browser or a customer's observable fingerprint resets
- **THEN** observations can share or change the fingerprint without changing the customer identity or exposing a latent identity field as a feature

### Requirement: IANA timezone observations and local-calendar semantics

The shared historical/live observation contract SHALL retain optional `browser_timezone` as an IANA identifier and `browser_utc_offset_minutes` using local-minus-UTC sign convention. Hosted checkout SHALL collect these browser-reported observations and the attempt API SHALL support the same fields. They SHALL NOT be treated as verified physical location. Latent home/current zones SHALL remain separate and SHALL NOT fill missing observations. Unknown/invalid zones SHALL produce unavailable local-calendar features without rejecting payment or defaulting to a shipping/server timezone. Valid conflicting zone/offset observations SHALL remain distinguishable. Calibration/collector provenance SHALL identify the tzdb rules revision and normalization semantics used for derivation. All event, label and analytical timestamps SHALL remain UTC instants.

#### Scenario: DST fall-back repeats a local hour

- **WHEN** two UTC events map to the repeated local hour under the observed IANA zone
- **THEN** they remain distinct instants with their correct effective offsets, and calendar derivation does not reorder or merge them

#### Scenario: Missing browser zone

- **WHEN** checkout cannot provide a valid browser zone
- **THEN** payment processing continues and local-calendar features stay unavailable rather than using the simulator's hidden home zone

#### Scenario: Local day versus rolling window

- **WHEN** a downstream consumer derives a local-day feature across a DST transition
- **THEN** the declared semantics distinguish that 23/25-hour day from a rolling elapsed 24-hour window and retain UTC storage partition semantics

### Requirement: Canonical event and knowledge times

Gateway attempt responses SHALL expose the canonical persisted attempt-start timestamp used to anchor simulation-truth occurrence and attempt-linked chargeback occurrence. Reporting delays SHALL become eligible after confirmed success. Event occurrence, source recording, label release and downstream observation/ingestion times SHALL remain distinct. Live `label_available_at` SHALL represent the UTC publication-attempt time, never a scheduled due date or the simulator's pre-request timestamp; evidence SHALL count delivered labels only after broker acknowledgement and SHALL NOT claim the embedded field measures exact broker commit/consumer visibility. Downstream handoff SHALL describe both event-time and knowledge-time eligibility, microsecond analytical precision, clock-sync evidence/limitations and late-arrival handling without implementing a feature pipeline. Cross-payment global order SHALL NOT be inferred from clocks or UUIDs.

#### Scenario: Tokenization delays an attempt

- **WHEN** tokenization or payment creation delays the live attempt
- **THEN** labels use its canonical gateway attempt time rather than a timestamp captured before those requests

#### Scenario: Late feature or label observation

- **WHEN** a record's business timestamp precedes its availability to a consumer
- **THEN** handoff semantics do not permit an earlier prediction to use information it could not yet observe

### Requirement: Realistic collection and lifecycle parity

Historical and live observations SHALL share the declared hosted/API collection semantics, including address normalization, browser-family/fingerprint derivation, network-prefix hashing and optional signal loss. The population SHALL support correlated baskets, tenure, replacement, domestic travel/shipping variation and legitimate sharing without exposing hidden household/person IDs as observed customer identities. Live generation SHALL support same-payment business retries, method changes, delayed retries, abandonment/expiry and pending reconciliation consistent with the historical lifecycle model. Transport retries SHALL preserve idempotency key and request; a new business attempt SHALL have a distinct command identity. Synthetic processor controls SHALL preserve intended method identity and SHALL NOT be exposed as fraud features. Evidence SHALL distinguish supported mechanisms from actual observed processor calibration.

#### Scenario: Same payment retried after decline

- **WHEN** the simulated customer retries a declined attempt using another method
- **THEN** the new attempt belongs to the same payment with its own command identity and method fingerprint, while replay of either command cannot create another attempt

#### Scenario: Request timeout before known outcome

- **WHEN** a live request times out or returns a pending outcome
- **THEN** bounded reconciliation determines its authoritative status or reports unresolved work without inventing a decline, success or chargeback

#### Scenario: Collection changes an observed fingerprint

- **WHEN** a timezone offset or browser attribute used by checkout fingerprinting changes
- **THEN** simulated observations follow that collection behavior without changing the latent person identity or requiring a different customer ID

### Requirement: Diagnostic realism evidence and history coverage

Calibration evidence SHALL map modeled features to available collection paths, units, null semantics, time basis and provenance. It SHALL identify measured versus assumed joint relationships and explicit rate denominators. Realism runs SHALL be distinguished from load multipliers, label-time compression and transport/scenario stress. Shortcut audits, sensitivity comparisons and generator distribution/cardinality comparisons SHALL remain diagnostics rather than new ingestion/model-score gates. Training handoff SHALL identify predecision feature exclusions, the applicable prefilter population, forward-time/grouped evaluation needs, mature labels and observation coverage. Allowed historical/live calendar gaps SHALL remain explicitly unobserved periods, not fabricated purchases or confirmed inactivity. Public calibration SHALL NOT be represented as proof of matching a real checkout's joint distribution without representative target observations.

#### Scenario: Live starts after a historical gap

- **WHEN** history ends months before a current-time run
- **THEN** startup remains permitted while handoff records the gap and relevant warmup/coverage limits rather than assuming zero prior activity

#### Scenario: Fault or load experiment

- **WHEN** a run scales arrivals, compresses label delays or introduces transport duplicates
- **THEN** evidence identifies those settings and does not present resulting velocities/delays/delivery counts as natural customer calibration

#### Scenario: Fraud-rate comparison

- **WHEN** calibration compares intent, approved fraud, chargeback or monetary-loss rates
- **THEN** it names each denominator and observation/reporting window instead of treating them as interchangeable prevalence estimates

### Requirement: Immutable realism regeneration handoff

Regeneration SHALL use a new algorithm/profile identity and an unused dataset version. Published v8/v11/v12 objects, manifests, configuration bands and previous failed/passed evidence SHALL remain unchanged. Before full generation, operational calibration SHALL record current machine resources, measured generation/upload throughput, aggregate worker memory and scratch peaks, projected time/storage and bounded execution settings. The existing physical-volume requirement SHALL remain in force. Publication SHALL retain manifest-last source integrity and row accounting without claiming downstream quality acceptance. Switching live selection SHALL require a published manifest/hash and matching vault method identities/behavior assignments; no old-algorithm runtime compatibility path SHALL be required.

#### Scenario: Existing version encountered

- **WHEN** the new logical plan encounters an already published dataset version
- **THEN** it refuses modification and uses an unused version instead

#### Scenario: Live mapping does not match

- **WHEN** the new manifest's profile or method mapping differs from the deployed vault/model
- **THEN** live startup fails before sending payment commands rather than silently substituting fixtures

#### Scenario: Source publication completes

- **WHEN** the new source passes its publication gates
- **THEN** the handoff records lineage, calibration assumptions, rows/bytes and event/label date ranges, and reports DP1 Spark/GX acceptance separately

## MODIFIED Requirements

### Requirement: Ecommerce behavior and fraud scenarios

Legitimate customers SHALL purchase mostly from familiar merchants, categories, devices and shipping addresses, with amounts drawn from calibrated category/customer behavior. The generator SHALL support individually enabled and parameterized `obvious_high_value`, `stolen_card_new_device`, `card_testing`, `account_takeover` and `friendly_fraud` experiments. Realism mode SHALL give legitimate and fraudulent purchases overlapping amount, device and network observations; scenario identity SHALL NOT be encoded in reserved visible fingerprints, forced foreign countries or a field value unique to fraud. Campaign-level temporal/linkage patterns SHALL remain possible. `obvious_high_value` SHALL be an explicitly enabled stress experiment, identified separately in evidence rather than establishing realism.

#### Scenario: Card testing burst

- **WHEN** `card_testing` is enabled
- **THEN** its attempts arrive in low-value campaign bursts across multiple payment methods, with configurable device/network reuse and elevated processor declines

#### Scenario: Scenario disabled

- **WHEN** a scenario rate is zero
- **THEN** no label carries that scenario

#### Scenario: Legitimate unusual purchase

- **WHEN** realism mode generates legitimate high-value spending or a new-device purchase
- **THEN** the observation is permitted without automatically assigning fraudulent intent

### Requirement: Lifecycle histories

Every generated payment SHALL be a complete history covering risk declines, processor declines, unknown processor outcomes that later resolve, retries and expiries. Historical risk decisions SHALL match the gateway's versioned policy semantics for the same contracted inputs and SHALL record the applicable policy version instead of an unrelated generator-only rule policy. Fraud intent, risk decisions and processor outcomes SHALL remain distinct. Historical generation SHALL NOT call gateway APIs or write to payment event storage; live authoritative events SHALL remain gateway-produced.

#### Scenario: Retried payment

- **WHEN** an attempt is declined and the customer retries
- **THEN** the next attempt starts on the same payment after the decline and the history ends in success or expiry

#### Scenario: Matching policy inputs

- **WHEN** the same amount, account creation timestamp, attempt timestamp and available location signals are evaluated under the same policy revision
- **THEN** historical and gateway risk outcomes/reason codes agree independently of hidden fraud intent

### Requirement: Offline data problems

The configuration SHALL control merchant/category skew, identity pools and explicit data-problem experiments. Realism baseline generation SHALL use the current collection schema and an explicitly declared duplicate-export setting without automatically inheriting a historical forced cutover or fixed duplicate rate. A schema-evolution experiment SHALL declare its cutover and physical schema shapes, including when `user_agent_family` is absent; an injected duplicate export SHALL repeat the original event identity and content. The manifest SHALL record mode/settings and cheap additive observed row/scenario/duplicate/schema totals and bounded skew counters. It SHALL keep configured identity pools separate from measurements and mark observed cardinality validation as downstream, without fabricated counts or remote analytical scans. DP1 cardinality reports SHALL identify exact counts versus estimates and each ratio's denominator. Delivery preservation SHALL apply independently of experiment mode.

#### Scenario: Schema evolution cutover

- **WHEN** an experiment declares a cutover between different physical collection schemas
- **THEN** raw files use the declared shape for each occurrence date and merged downstream reads preserve the evolution without changing older source files

#### Scenario: Duplicate export

- **WHEN** an experiment requests a duplicate-export rate
- **THEN** exported repeats retain event identity/content and the manifest records the setting and observed duplicate count without calling it measured production behavior

#### Scenario: Scale profile data problems

- **WHEN** a scale dataset is accepted
- **THEN** source evidence reports additive merchant/category skew, duplicate and schema counts, while DP1 quality evidence supplies observed identity cardinalities and ratios
- **AND** physical size is not increased through padding, repeated copies of an accepted dataset or an inflated duplicate rate

### Requirement: Delayed labels and truth separation

The generator SHALL emit simulation-truth labels with their scenario for every attempt in the separate `fraud_labels` dataset. Chargeback reporting probability and delay SHALL be configurable independently of hidden fraud intent and processor outcomes. Chargebacks SHALL reference only succeeded attempts; `friendly_fraud` SHALL permit a legitimate succeeded attempt to receive a chargeback without changing its simulation-truth intent. Scenario names SHALL NOT appear in payment events or features. Label occurrence SHALL use canonical attempt-start time consistently across historical/live output; reporting delay SHALL begin only after success. Live availability SHALL use publication-attempt time with acknowledgement-based delivery accounting and separate downstream visibility semantics. Evidence SHALL distinguish simulation truth, observed chargebacks, unreported cases and right-censored/pending cases; absence of a chargeback SHALL NOT establish a negative label. Consumer joins SHALL respect label availability and declare their selected target source.

#### Scenario: Declined fraudulent attempt

- **WHEN** a fraudulent attempt is declined
- **THEN** it has a simulation-truth label and no chargeback label

#### Scenario: Unreported or immature fraud

- **WHEN** a fraudulent succeeded attempt is unreported or its chargeback is not yet available
- **THEN** observed-label evidence does not turn it into a confirmed negative and simulation truth remains distinguishable

#### Scenario: Friendly dispute

- **WHEN** a legitimate succeeded attempt receives a friendly-fraud chargeback
- **THEN** its chargeback label is positive while its simulation-truth intent remains legitimate

#### Scenario: Interrupted live label schedule

- **WHEN** a live run ends with scheduled chargebacks still in memory
- **THEN** evidence reports the unfinished schedule without claiming durable recovery or complete observed labels
