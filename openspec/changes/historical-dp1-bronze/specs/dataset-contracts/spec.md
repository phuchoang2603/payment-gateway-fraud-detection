## MODIFIED Requirements

### Requirement: Declared data quality

The historical-source contracts SHALL declare executable SQL rules. Identical repeated event deliveries SHALL be valid; conflicting event identities, illegal lifecycle transitions, invalid amounts, invalid label sources, labels available before their occurrence and labels referencing missing or, for chargebacks, unsuccessful attempts SHALL fail. Data Contract CLI SHALL execute these source rules against a configured DuckDB data source without a repository-owned validation wrapper or a second source rule suite.

#### Scenario: Duplicate delivery

- **WHEN** identical copies of an event appear in a complete valid history
- **THEN** the payment-events rules pass

#### Scenario: Conflicting delivery

- **WHEN** the same `event_id` appears with different content
- **THEN** the delivery-consistency rule fails

#### Scenario: Invalid chargeback

- **WHEN** a chargeback label references an attempt that did not succeed
- **THEN** the fraud-labels linkage rule fails

### Requirement: Execution boundaries

Documentation SHALL distinguish source DuckDB checks, bronze Spark/GX checks, contract lint and catalog ingestion. Producing and ingesting jobs SHALL own data preparation, physical file checks and publication gating. DataHub SHALL consume contract metadata and execution evidence without replacing validation. Flink integration SHALL remain with its pipeline.

#### Scenario: Dataset validation

- **WHEN** a job exposes `payment_events` and `fraud_labels` in a DuckDB database
- **THEN** the documented `datacontract test` command checks each source contract and writes a report

#### Scenario: Bronze validation

- **WHEN** DP1 exposes its selected persisted bronze snapshots
- **THEN** contract-derived Spark/GX checks validate those outputs and catalog publication reports the outcome separately

## ADDED Requirements

### Requirement: Bronze payments product

The `bronze-payments` folder SHALL contain one ODPS descriptor and ODCS contracts for payment-event and fraud-label Iceberg output ports. They SHALL declare distinct bronze identities, ownership, access, upstream source contracts and the label port's event dependency. Product, contract and source dataset versions SHALL remain independent.

#### Scenario: Resolve bronze product interfaces

- **WHEN** product descriptors and local output contracts are linted
- **THEN** both bronze ports resolve to contracts in their own folder and upstream source references remain distinguishable from bronze outputs

### Requirement: Bronze schema and consumption agreement

Bronze contracts SHALL define business fields, ingestion provenance, normalized physical types and supported source schema versions. They SHALL permit identical duplicate deliveries, define nested required fields conditionally and retain label-availability and feature-leakage restrictions.

#### Scenario: Consume normalized historical data

- **WHEN** a consumer reads the bronze contracts
- **THEN** it can distinguish source recording time from ingestion time, interpret v1's absent field, identify source/run provenance and determine when labels can be joined

### Requirement: Complete contract-derived bronze enforcement

Bronze validation SHALL derive executable suites and schema checks from ODCS without a handwritten parallel suite. Each quality rule and required schema constraint SHALL have a verified execution path. Unsupported or omitted checks SHALL fail preparation rather than produce a partial passing result.

#### Scenario: Exporter skips a quality rule

- **WHEN** suite generation omits a declared rule or cannot enforce a nested constraint
- **THEN** validation preparation fails with the uncovered rule or field identified

#### Scenario: Duplicate comparison includes provenance

- **WHEN** repeated business events carry different file or ingestion provenance
- **THEN** their duplicate-consistency check compares business content and does not fail solely because audit fields differ

### Requirement: Source and bronze semantic agreement

Equivalent source and bronze business checks SHALL retain traceable semantic rule identities and agree on shared valid/invalid fixtures after declared schema normalization. Engine-specific SQL SHALL use its execution dialect; exporting a suite SHALL NOT be treated as SQL translation or data validation.

#### Scenario: Scalar aggregate differs from unexpected-row semantics

- **WHEN** a source count-of-violations rule is represented as a bronze unexpected-row check
- **THEN** valid data returns no violating rows, invalid data returns violations and both engines agree on pass/fail

#### Scenario: Conditional nested requirement

- **WHEN** a lifecycle payload is absent in a valid event but a required child is missing from a present payload in an invalid event
- **THEN** both execution paths accept the valid absence and reject the invalid child omission
