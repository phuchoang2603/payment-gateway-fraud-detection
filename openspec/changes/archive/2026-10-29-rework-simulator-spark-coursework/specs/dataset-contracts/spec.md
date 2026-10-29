## MODIFIED Requirements

### Requirement: Bronze schema and consumption agreement

Bronze contracts SHALL define business fields, ingestion provenance, normalized physical types and the declared supported physical event and label schema versions. They SHALL permit identical duplicate deliveries, define nested required fields conditionally and retain label-availability and feature-leakage restrictions. Per-version normalization SHALL retain available values and fill absent optional fields with null, with original source schema identifiable independently of the normalized output contract version. Unsupported breaking changes SHALL require an explicit new contract or transformation rather than an implicit cast.

#### Scenario: Consume normalized historical data

- **WHEN** a consumer reads the Bronze contracts
- **THEN** it can distinguish source recording time from ingestion time, interpret enriched attempt context and version-dependent assessment reference availability, identify source/run provenance and determine when labels can be joined

## ADDED Requirements

### Requirement: Assessment evidence reference fidelity

Historical and decoded live contracts SHALL expose the current assessment reference fields with declared optionality and evidence origin. Supported legacy physical shapes SHALL normalize absent references to null without manufacturing identifiers, hashes or evidence documents. New live decoded records SHALL preserve references present in their payment event. Full assessment evidence SHALL remain Fraud-owned; references alone SHALL NOT establish feature-vector export or training-serving parity.

#### Scenario: Decode a committed assessment reference

- **WHEN** a supported payment event contains an assessment identifier and evidence hash
- **THEN** decoded landing, Bronze and curated outputs retain both values without requiring ingestion to decode transport bytes

#### Scenario: Historical reference is unavailable

- **WHEN** an older physical record lacks an assessment reference
- **THEN** its normalized value is null and its source schema remains identifiable

### Requirement: Coursework dataset evidence

Evidence SHALL include the generator configuration and revision, immutable manifest/hash, actual rows/files/physical bytes, format/compression/layout, separate event/label time ranges and measured distribution results. Results SHALL show merchant/category or supported region skew shares with denominators, observed identity cardinalities labelled exact or approximate, evolved-field nulls grouped by source version/date and eligible payload, and duplicate rates before and after curation. Approximate counts SHALL identify the method/error setting; configured pool sizes SHALL NOT substitute for observations. Screenshots of real query/output and corresponding executable code/query references SHALL accompany retained machine-readable results.

#### Scenario: Inspect schema null evidence

- **WHEN** an added assessment field is absent in older source partitions
- **THEN** the report identifies the old physical schema and eligible decision rows rather than counting unrelated event payloads as missing assessments

#### Scenario: Inspect duplicate evidence

- **WHEN** an export contains repeated identical events
- **THEN** evidence records extra deliveries divided by total deliveries, canonical event counts, conflicts and post-dedup duplicates, with the query and selected snapshots identified
