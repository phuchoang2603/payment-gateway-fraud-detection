## ADDED Requirements

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
