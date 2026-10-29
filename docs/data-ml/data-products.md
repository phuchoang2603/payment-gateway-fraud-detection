# Data products

Each data product lives in one folder under [`contracts/data-products/`](../../contracts/data-products): an [ODPS](https://github.com/bitol-io/open-data-product-standard) descriptor `product.odps.yaml` and one [ODCS](https://github.com/bitol-io/open-data-contract-standard) contract per output port. The descriptor identifies the product, owner, access and ports; each contract defines its port's field meanings and quality rules. Product, contract, physical schema and dataset versions are independent, and dataset manifests record each identity, version and file hash.

Each contract has a generated schema reference under [`docs/data-ml/data-products/<product>/`](data-products). CI discovers every product folder, lints descriptor and contracts with pinned native tools, and diffs each reference against a fresh export (`bash scripts/ci-checks.sh contracts`). After changing a contract, regenerate its reference with the export command in [`scripts/ci-checks.sh`](../../scripts/ci-checks.sh).

Contract lint does not validate data. Producing and ingesting jobs own physical file checks, data preparation and publication gating. Once a job exposes a port's tables or views in DuckDB, run the contract's quality rules against them:

```bash
DATACONTRACT_DUCKDB_DATABASE=/path/to/staging.duckdb \
  uvx --from 'datacontract-cli[duckdb]==1.2.7' datacontract test \
  contracts/data-products/<product>/<port>.odcs.yaml --server validation \
  --output validation.json --output-format json
```

The SQL rules use DuckDB syntax. Source DuckDB rules are distinct from downstream validation. DP1 and DP2 use Spark ingestion into Iceberg and snapshot-pinned StarRocks SQL with native OpenMetadata quality/governance.

## Historical payments

[`historical-payments`](../../contracts/data-products/historical-payments) is the reproducible synthetic source for DP1 ingestion and downstream feature and training pipelines described in [data pipeline architecture](data-pipeline.md). It stays outside bronze tables and production payment storage.

| Port | Contract | Reference |
|---|---|---|
| `payment-events` | [`payment-events.odcs.yaml`](../../contracts/data-products/historical-payments/payment-events.odcs.yaml) | [schema](data-products/historical-payments/payment-events.md) |
| `fraud-labels` | [`fraud-labels.odcs.yaml`](../../contracts/data-products/historical-payments/fraud-labels.odcs.yaml) | [schema](data-products/historical-payments/fraud-labels.md) |

The fraud-labels linkage rule reads `payment_events`, so validate labels in a database that exposes both tables.

### Development source

Fresh dev v17, independent Bronze and Silver acceptance are documented in [verification](verification.md). Configuration and schema evolution are in the [generator guide](data-generator.md). Full-scale and prod acceptance remain deferred.

## Bronze payments

[`bronze-payments`](../../contracts/data-products/bronze-payments) exposes `bronze.raw_payment_events` and `bronze.raw_fraud_labels`. It preserves identical deliveries, source identity, physical schema version and available transport provenance. Spark verifies source integrity and duplicate-aware conservation; StarRocks/OpenMetadata validate persisted snapshots and governance before acceptance.

## Silver payments

[`silver-payments`](../../contracts/data-products/silver-payments) exposes canonical events, event delivery provenance, attempt histories, independent label observations and label delivery provenance. Its [generated references](data-products/silver-payments) define grain, keys and temporal meaning. Separate [DP2 execution](data-pipeline.md#silver-dp2) accepts the five snapshots only after quality and governance succeed.
