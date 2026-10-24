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

The SQL rules use DuckDB syntax. Spark and Flink execution, and DataHub catalog ingestion, are verified with those pipelines when they exist.

## Historical payments

[`historical-payments`](../../contracts/data-products/historical-payments) is the reproducible synthetic source for DP1 ingestion and downstream feature and training pipelines described in [data pipeline architecture](data-pipeline.md). It stays outside bronze tables and production payment storage.

| Port | Contract | Reference |
|---|---|---|
| `payment-events` | [`payment-events.odcs.yaml`](../../contracts/data-products/historical-payments/payment-events.odcs.yaml) | [schema](data-products/historical-payments/payment-events.md) |
| `fraud-labels` | [`fraud-labels.odcs.yaml`](../../contracts/data-products/historical-payments/fraud-labels.odcs.yaml) | [schema](data-products/historical-payments/fraud-labels.md) |

The fraud-labels linkage rule reads `payment_events`, so validate labels in a database that exposes both tables.

### Published version

`historical/v8` is the published full-scale source, generated from [`historical-scale.yaml`](../../src/data-pipeline/simulator/configs/historical-scale.yaml) (seed 2603): 24,982 raw files, 123,286,939,620 physical and 243,759,611,982 uncompressed Parquet bytes, with validation deferred to the `dp1-spark-gx` gate. `historical/v10` is a small smoke export from [`historical-smoke.yaml`](../../src/data-pipeline/simulator/configs/historical-smoke.yaml) (100,000 customers, six days) using the same manifest format. Each `manifest.json` records the configuration hash, generator revision, contract hashes, per-file inventory, per-partition row counts and storage metrics. [`historical-sanity.ipynb`](../../src/data-pipeline/simulator/notebooks/historical-sanity.ipynb) reads a published manifest for distribution, skew, cutover and duplicate checks.

The command runs from the devenv shell. The generation-only publication path below does not establish a completed migration or validation of `v8`:

```bash
cd src/data-pipeline/simulator
doppler run -p payment-gateway-fraud-detection -c prd -- sh -c \
  'AWS_ACCESS_KEY_ID="$HISTORICAL_RUSTFS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$HISTORICAL_RUSTFS_SECRET_KEY" \
   uv run --locked simulator historical --config configs/historical-scale.yaml'
```

Bump `dataset_version` in the config for every new dataset. Pass `--output <dir>` to generate locally without credentials.

The approved new `historical --config <path>` path generates and uploads raw task Parquet, verifies checksums, row-count accounting and the physical-byte gate, and writes the completion manifest last. It then tags configured accepted output and cleans only `_run/`, retaining persistent `raw/` files. It resumes compatible unfinished work or repairs protection/cleanup on a completed inventory without rewriting files or the manifest. Completed versions cannot be regenerated or overwritten; recovery verifies the stored logical configuration and publication settings.

The manifest uses `manifest_version: 3`, `source_layout: task-fragments-v1` and an exact inventory of `raw/{table}/{task.key}/v{schema_version}.parquet` files beneath the dataset root. Each file records `schema_version`, checksum, bytes, rows and `partitions: {date: rows}`; those dates summarize occurrence dates for events and availability dates for labels. A task file can cover several dates. Source files carry no sorting or compaction guarantee. This physical publication change versions the historical-payments product to `5.0.0`; the logical ODCS contracts remain unchanged.

Generation-time contract checks are recorded as `result: skipped`, gate `dp1-spark-gx` and a reason. Python/DuckDB finalization and remote analytical scans are removed from the new path. DP1 validates exactly the listed raw files, preserves duplicate deliveries and delayed labels, derives Bronze partitions from timestamp columns and requires Spark/GX checks on both persisted tables plus verified governance before Bronze quality acceptance. Source publication and `accepted=true` storage protection alone do not establish quality acceptance. Reports use cheap additive row/scenario/duplicate/schema and bounded skew counters; observed cardinality validation is marked downstream, with configured pools kept separate from measurements.

After durable publication and successful protection, cleanup retains `_run/publication.json` with successful delete counts and never deletes inventory-listed raw files. Tagging or cleanup failures exit nonzero; rerunning verifies file integrity and retries. Source credentials need `GetObjectTagging` and `PutObjectTagging`; bucket policy protects accepted objects. Delete scope covers `_run/` and abandoned probe/pilot prefixes.

`--prepare-only` leaves generated raw inventory ready, without sorting or a completion manifest. The normal command continues publication under the same generation identity. Normal shutdown releases ownership. After a crash, stop the former writer first and pass `--takeover-stopped-writer`; elapsed time never authorizes takeover. Small remote publication still requires `--allow-small-dataset` below one customer block; probes normally use local `--output`.

The simulator is generation-only: it has no migration command or old-configuration compatibility path.

The historical 2026-10-22 RustFS probe confirmed create-if-absent, ETag-conditional update and streamed transfer; HEAD returned no SHA-256 checksum, so integrity verification streams each object. Unsupported conditional operations fail rather than falling back to unsafe writes. New execution controls cover workers, in-flight reservations, scratch and transfers, without DuckDB sort/validation budgets. Logical customer blocks, burst groups, layout, scenarios, contracts and minimum source bytes form the generation identity.

The full configuration gates the stolen-card attempt share at 2.0–3.2% to cover the warmup of seven-day compromise windows (a bounded days 30–35 sample estimated 2.67%); the smoke configuration keeps the shorter-history band. Both gate duplicate, skew, schema and scenario shares and scale card-testing bursts/day to their customer counts (60 and 2). Full-year distribution acceptance is a DP1 result.

The coursework source is `docs/coursework/rubic/EDAI K11 - DE.xlsx`, sheet `edai-1 (50%)`, cell B5: offline simulation of at least 100 GB. Compression accounting is unspecified there. This project conservatively requires at least **100,000,000,000 physical bytes** across inventory-listed raw event and label Parquet files for the new path, counted once, excluding `_run`, reports, streaming data, replicas and downstream copies. The manifest separately records uncompressed Parquet column-chunk bytes and decimal GB/GiB. A fixed plan below the byte gate cannot publish; changing logical inputs requires a new dataset version.