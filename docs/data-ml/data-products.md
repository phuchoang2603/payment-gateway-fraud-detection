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

The SQL rules use DuckDB syntax. Source DuckDB rules are distinct from downstream validation. The owning DP1 plan retains Spark ingestion into Iceberg and adopts snapshot-pinned StarRocks SQL with native OpenMetadata quality/governance; Flink feature processing is a later stage.

## Historical payments

[`historical-payments`](../../contracts/data-products/historical-payments) is the reproducible synthetic source for DP1 ingestion and downstream feature and training pipelines described in [data pipeline architecture](data-pipeline.md). It stays outside bronze tables and production payment storage.

| Port | Contract | Reference |
|---|---|---|
| `payment-events` | [`payment-events.odcs.yaml`](../../contracts/data-products/historical-payments/payment-events.odcs.yaml) | [schema](data-products/historical-payments/payment-events.md) |
| `fraud-labels` | [`fraud-labels.odcs.yaml`](../../contracts/data-products/historical-payments/fraud-labels.odcs.yaml) | [schema](data-products/historical-payments/fraud-labels.md) |

The fraud-labels linkage rule reads `payment_events`, so validate labels in a database that exposes both tables.

### Development source

Fresh dev v17 is published at `s3://platform-dev/datasets/payment-gateway/historical/v17/manifest.json`. Its actual digest and measured characteristics are recorded in the [evidence index](../../openspec/changes/rework-simulator-spark-coursework/evidence/index.md) and executed [generator notebook](../../src/data-pipeline/simulator/notebooks/historical-sanity.ipynb); previous-environment sources are not current acceptance.

For configuration, schema schedules, immutable publication, label semantics and diagnostic interpretation, see the [data generator guide](data-generator.md). Source publication does not accept Bronze or Silver. Full-scale generation remains deferred until the whole pipeline is ready.

## Bronze payments

`bronze-payments` version `4.0.0` defines `bronze.raw_payment_events` and `bronze.raw_fraud_labels` Iceberg ports. Physical event schemas 3 and 4 preserve enriched AttemptStarted context; schema 4 adds optional assessment references. Label schema 1 retains availability and feature-leakage restrictions. Ingestion preserves source and transport provenance, including physical schema version separately from the event envelope, and preserves identical deliveries. The revised contract is pending development acceptance.

Git bronze contracts define StarRocks COUNT-of-violations SQL and conditional nested requirements. The official version-matched OpenMetadata ingestion image discovers tables and executes native quality rules; Argo performs explicit product/contract/lineage writes and verifies complete results. Spark performs source-integrity checks, ingestion and bidirectional duplicate-aware source/output reconciliation against recorded snapshots. Native per-day counts alone do not establish content equality.

Fresh dev v17 is published; persisted validation remains pending before Bronze acceptance. See the [DP1 operation instructions](data-pipeline.md#historical-dp1). Prod pipeline acceptance remains deferred.

## Silver payments

[`silver-payments`](../../contracts/data-products/silver-payments) defines canonical events, exact event and label delivery provenance, attempt histories and independent label observations. Its [generated contract references](data-products/silver-payments) describe the ports. Candidate snapshots are not accepted until data quality and native governance succeed; the new product remains pending development acceptance.
