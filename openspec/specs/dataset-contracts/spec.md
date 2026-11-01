## Purpose

Describe each data product with one ODPS descriptor and one ODCS contract per output port, kept together in one folder, so producers and consumers validate against the same rules with existing tools.

## Requirements

### Requirement: Data product folder

Each data product SHALL live in its own folder under `contracts/data-products/<product>/` containing `product.odps.yaml` and one `*.odcs.yaml` per output port. Every output port SHALL reference exactly one contract in the same folder by ID and version. Product and contract versions SHALL be independent of dataset versions. The `historical-payments`, `bronze-payments` and `silver-payments` products SHALL start at version 1.0.0 with no archived version copies or separate physical-schema files.

#### Scenario: Unresolved port contract

- **WHEN** an output port references a contract ID that no ODCS file in the product folder declares
- **THEN** the product lint fails

### Requirement: Historical payments product

The `historical-payments` product SHALL expose two output ports: `payment-events` and `fraud-labels`, each backed by its own ODCS contract with ownership, synthetic classification, field meanings and the supported schema versions, including which fields are absent in schema version 2 (fields declared with `sinceSchemaVersion` 3). The `fraud-labels` port SHALL declare the `payment-events` contract as an input contract. The descriptor SHALL record the storage layout, the manifest as completion marker, access policy and consumption rules.

#### Scenario: Label leakage guidance

- **WHEN** a consumer reads the `fraud-labels` contract
- **THEN** it states that labels may be used only after `label_available_at` and that `scenario` is excluded from features

### Requirement: Native lint and generated references

CI SHALL discover every product folder, lint each descriptor and contract with pinned Data Product CLI and Data Contract CLI, and reject drift between each contract and its generated Markdown reference under `docs/data/products/<product>/`. CI lint SHALL NOT claim to validate produced datasets.

#### Scenario: Invalid contract

- **WHEN** a contract violates the official ODCS schema
- **THEN** CI fails

#### Scenario: Stale reference

- **WHEN** a field meaning changes without regenerating its reference
- **THEN** the documentation comparison fails

### Requirement: Current attempt context across source formats

Historical Parquet and Kafka-decoded payment events SHALL expose the same typed business fields and optionality as the current payment protobuf contract, apart from fields declared absent in schema version 2. Generated schema references SHALL describe them. Kafka and file provenance SHALL remain separate from business fields.

#### Scenario: Historical and live attempts read by Spark

- **WHEN** Bronze ingests a historical file and Kafka records of the same event type
- **THEN** their business columns have identical types after ingestion

### Requirement: Bronze payments product
The `bronze-payments` folder SHALL contain one ODPS descriptor and ODCS contracts for payment-event and fraud-label Iceberg output ports. They SHALL declare distinct bronze identities, ownership, access, upstream source contracts and the label port's event dependency. Product, contract and source dataset versions SHALL remain independent.

#### Scenario: Resolve bronze product interfaces

- **WHEN** product descriptors and local output contracts are linted
- **THEN** both bronze ports resolve to contracts in their own folder and upstream source references remain distinguishable from bronze outputs

### Requirement: Inspectable catalog contract mapping
The Git-authoritative bronze contract SHALL map to the canonical table's native contract schema, active raw quality-test references, declared ownership and supported declared terms/semantics/security/SLA fields. Unsupported required mapping SHALL be reported as incomplete governance. Original ODCS identity/version/hash/YAML SHALL remain provenance rather than substitute for native fields. Product dependencies and pipeline executions SHALL carry flow inputs/outputs; the integration SHALL NOT invent native contract fields or guarantees absent from Git.

#### Scenario: Contract YAML is present but native fields are absent

- **WHEN** the catalog contains the contract hash and original YAML but lacks required native schema or active quality references
- **THEN** governance readback rejects completeness and identifies the missing mapping

### Requirement: Contract quality rules
Bronze and Silver contracts SHALL declare executable quality rules as scalar violation-count ClickHouse SQL with a zero threshold and stable rule IDs. Bronze rules SHALL cover raw schema validity only. Silver rules SHALL cover business validity: unique keys, lifecycle validity, label linkage and availability, amounts and label-source semantics. Historical source contracts SHALL describe schema and meanings without a separate executable rule suite.

#### Scenario: Conflicting delivery

- **WHEN** the same `event_id` appears with different content
- **THEN** Silver curation fails rather than a source or Bronze rule passing it on

#### Scenario: Invalid chargeback

- **WHEN** a chargeback label references an attempt that did not succeed on a finished payment
- **THEN** the Silver label-linkage rule fails

### Requirement: Validation ownership
Spark jobs SHALL own source integrity, row conservation, Kafka offset bookkeeping and duplicate conflicts. Native OpenMetadata test suites SHALL execute the Git-authoritative contract SQL through ClickHouse against the committed Bronze and Silver tables. Contract lint SHALL NOT claim to validate data. No source DuckDB rule suite, GX suite, custom result publisher or UI-authored rule source SHALL remain.

#### Scenario: Silver validation

- **WHEN** DP2 commits its tables
- **THEN** native OpenMetadata tests execute the Silver contract SQL against them and the run fails on any violation

### Requirement: Bronze schema and provenance
Bronze contracts SHALL define business fields, schema version, provenance for both historical files and Kafka records, and normalized physical types. They SHALL permit identical duplicate deliveries, define nested required fields conditionally and retain label-availability and feature-leakage restrictions.

#### Scenario: Consume Bronze data

- **WHEN** a consumer reads the Bronze contracts
- **THEN** it can distinguish historical from Kafka rows, source time from ingestion time and version-2 from version-3 events, and determine when labels can be joined

### Requirement: Contract-derived native checks
Bronze and Silver validation SHALL derive native OpenMetadata test definitions from Git ODCS rules with stable semantic IDs and explicit ClickHouse SQL, without a handwritten parallel suite. Each rule SHALL return a bounded scalar violation count with zero as the passing threshold. A declared rule that cannot be prepared SHALL fail preparation rather than produce a partial passing result.

#### Scenario: Native preparation skips a quality rule

- **WHEN** native test preparation omits a declared rule
- **THEN** validation preparation fails with the uncovered rule identified

### Requirement: Silver payments product
The `silver-payments` folder SHALL contain one ODPS descriptor and ODCS contracts for the `payment-events`, `attempts` and `labels` Iceberg ports and the `rejected-events` port, declaring ownership, keys, field meanings including outcome, payment-status and linkage classes, the Bronze input contracts, and the Silver quality rules. The `rejected-events` contract SHALL declare the rule that zero rejected rows come from the historical source.

#### Scenario: Resolve Silver product interfaces

- **WHEN** product descriptors and contracts are linted
- **THEN** all four Silver ports resolve to contracts in their folder and reference the Bronze contracts as inputs

### Requirement: Gold data products
`contracts/data-products/gold-features` SHALL describe the Iceberg gold tables. `contracts/data-products/payment-marts` SHALL describe the ClickHouse marts. Each SHALL have an ODPS descriptor, with Silver input ports and one output port per table, and one v1 ODCS contract per table. Each contract SHALL carry:

- the schema, keys and partitioning;
- point-in-time join semantics;
- the rate denominators;
- the `created` and `event_timestamp` meanings;
- executable ClickHouse-dialect quality rules, each a scalar violation count that must be zero.

Generated references SHALL be regenerated under `docs/data/products/`.

#### Scenario: Lint the gold contracts

- **WHEN** CI lints the data products
- **THEN** both gold products and their contracts pass the pinned native linters, and their generated references match

### Requirement: Contract-scoped check objects
Each ODCS contract's ClickHouse server SHALL name the database its rules run in: `lakehouse` for Iceberg tables and `marts` for native marts. The `{object}` placeholder in a rule SHALL resolve to that database and the contract's physical table name. The catalog test SHALL attach to that table's canonical entity. A rule MAY reference other tables only through fully qualified names.

#### Scenario: Mart rule placement

- **WHEN** a `payment-marts` rule is prepared as a native test
- **THEN** its SQL reads `marts.fact_payment_attempt` and the test attaches to that mart's catalog entity, not to a `lakehouse` table
