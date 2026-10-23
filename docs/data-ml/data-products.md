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

`historical/v7` is the current accepted dataset, generated from [`src/simulator/configs/historical.yaml`](../../src/simulator/configs/historical.yaml) (seed 2603) and validated against both contracts before publication. Its `manifest.json` records the configuration hash, generator revision, contract hashes, per-partition row counts and storage metrics; [`historical-sanity.ipynb`](../../src/simulator/notebooks/historical-sanity.ipynb) holds the executed distribution, skew, cutover, duplicate and layout checks.

| Measure | Value |
|---|---|
| History | 2026-07-01 to 2026-09-28 (90 days); schema cutover 2026-08-15 |
| Catalogue | Refurbished electronics: phone, smartwatch, laptop, accessory, tablet, headphones, game console, camera; condition excellent, good or fair |
| Payments / unique events / exported event rows | 420,392 / 2,171,314 / 2,215,309 |
| Duplicate export rate | 1.99% (configured 2%) |
| Fraud attempts by scenario | stolen card 12,157; account takeover 3,360; card testing 1,225; obvious high value 510 |
| Labels / chargebacks / friendly-fraud payments | 442,817 / 12,309 / 1,175 |
| Distinct customers / devices / IP networks / addresses / payment methods | 30,000 / 53,845 / 9,202 / 20,123 / 26,724 |
| Layout | 91 event files (median 2.5 MiB, 237 MiB total) and 111 label files (median 0.24 MiB), one zstd file per day |

Generate a new version from the devenv shell; completed inventory-backed versions are verified and recovered without rewriting their data:

```bash
cd src/simulator
doppler run -p payment-gateway-fraud-detection -c prd -- sh -c \
  'AWS_ACCESS_KEY_ID="$HISTORICAL_RUSTFS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$HISTORICAL_RUSTFS_SECRET_KEY" \
   uv run --locked simulator historical --config configs/historical.yaml'
```

Bump `dataset_version` in the config for every new dataset. Pass `--output <dir>` to generate locally without credentials.

`historical --config <path>` generates new output, automatically resumes compatible unfinished output, or verifies a completed inventory and finishes any pending protection/cleanup without rewriting the final files or manifest. Run the same command again after a normal interruption or a publication failure. Integrity, row-count, physical-byte and observed-distribution gates still apply. Completed versions cannot be regenerated or overwritten; publication recovery also requires the same logical configuration, accepted-version setting and validation gate.

Historical publication defers both generation-time contracts to DP1 and records `result: skipped`, the `dp1-spark-gx` gate and a reason in the manifest. With `accepted_version: true`, publication tags the manifest and final objects `accepted=true`; protected storage alone is not quality acceptance. DP1 requires passing checks on both persisted tables and verified governance before bronze acceptance.

After durable publication and successful protection, the command automatically removes `_run/` fragments and receipts, retaining `_run/publication.json` with successful delete counts. Tagging or cleanup failures exit nonzero; rerunning the same command retries them after verifying final file integrity. Source credentials need `GetObjectTagging` as well as `PutObjectTagging` so retries can skip objects already protected by policy. Source credentials may delete `_run/` and abandoned probe or pilot prefixes; bucket policy denies writes and deletes on objects tagged `accepted=true`.

`--prepare-only` remains available for a separate preparation/validation host: it uploads and verifies final files without a completion manifest. Run the normal command on the validation host with the same generation identity, changing only `execution` budgets as needed. Normal shutdown releases ownership. After a crash, stop the former writer first and rerun with `--takeover-stopped-writer`; elapsed time never authorizes takeover. The command never takes ownership from another writer automatically. Publication below one configured customer block still requires `--allow-small-dataset`; probes normally use a local `--output`.

The 2026-10-22 RustFS probe confirmed create-if-absent, ETag-conditional update and streamed transfer; HEAD returned no SHA-256 checksum, so integrity verification streams each object. Unsupported conditional operations fail rather than falling back to unsafe writes. `execution` controls workers, in-flight reservations, scratch capacity, DuckDB memory/threads/spill and transfers; `chunk_purchases` controls batch memory. Logical customer blocks, burst groups, layout, scenarios, contracts and minimum source bytes form the generation identity.

Without explicit deferral, both native contracts run over views of the exact final inventory, including transitions across midnight and all delayed labels. DuckDB and its bundled HTTPFS extension are pinned together. Data Contract CLI 1.2.7 ignores its `duckdb_connection` argument for DuckDB servers; the simulator uses a scoped connection adapter to keep the unchanged native checks on the caller's resource-controlled connection. Failed validation, capacity, integrity or size gates retain prepared data without publishing. Resource reports distinguish sampled process-tree RSS, scratch/spill peaks and remote intermediate bytes; DuckDB's memory limit is not a process-wide cap.

[`historical-scale.yaml`](../../src/simulator/configs/historical-scale.yaml) (dataset version `v8`, the intended scale target) and [`historical-pilot.yaml`](../../src/simulator/configs/historical-pilot.yaml) (`v8-pilot`) share three million customer profiles, layout, scenarios and one year or six days of history. The measured pilot produced 3,124,545 payments, 1,985,818,453 physical final Parquet bytes and 4,843,810,164 uncompressed column-chunk bytes. Generation/finalization took 1,440.4/952.5 seconds and native validation took 1,942.6 seconds over two attempts. Process-tree RSS, scratch and spill peaks were 5,112,930,304, 8,686,332,811 and 8,421,998,592 bytes. These measurements project roughly 120.8 GB of final Parquet and 40.4 hours of generation/finalization for the fixed full plan; they do not establish full-run acceptance.

The six-day pilot includes the warmup of seven-day stolen-card compromise windows. A bounded calibration sample using the intended profiles and the first 100,000-customer block on days 30–35 estimated a 2.67% stolen-card attempt share. The full config therefore gates that share at 2.0–3.2%; the pilot retains its observed 0.8–1.8% band. Both configurations retain the measured duplicate/skew/schema and other scenario bands and 60 card-testing bursts/day. Full-year distribution and byte acceptance remain unmeasured. Native full-history validation projects about 512 GB of spill from the pilot, exceeding this workstation's capacity. The explicit DP1 handoff defers those source checks to Spark/GX; publication and protected storage alone do not establish data-quality acceptance. DP1 persists the declaration and requires both validated bronze snapshots and verified governance before acceptance.

The coursework source is `docs/coursework/rubic/EDAI K11 - DE.xlsx`, sheet `edai-1 (50%)`, cell B5: offline simulation of at least 100 GB. Compression accounting is unspecified there. This project conservatively requires at least **100,000,000,000 physical bytes** across final event and label Parquet files, counted once, excluding `_run`, reports, streaming data, replicas and downstream copies. The manifest separately records uncompressed Parquet column-chunk bytes and decimal GB/GiB. A fixed plan below the byte gate cannot publish; changing logical inputs requires a new dataset version. v7 remains the accepted correctness dataset until a separately measured full run passes every gate.

A small remote integration check on 2026-10-22 published `s3://synthetic-source/historical/runtime-process-check-2800fb7138e74bd699970de8f4ccfdc8/manifest.json` and removed it after measurement, so that object no longer resolves: 707 payments, 3,957 exported event rows, 882 labels, 582,399 physical and 894,981 uncompressed Parquet bytes. Both native reports passed. Separate CLI processes on the same workstation prepared then resumed the files. Preparation took 45.39 seconds (8.50 generation/upload; 35.46 finalization), with sampled RSS/scratch peaks of 253,603,840/246,483 bytes. Resume used 768 MiB/two DuckDB threads instead of 512 MiB/one thread, regenerated and uploaded nothing, and took 52.76 seconds (46.86 validation), with RSS/scratch peaks of 325,181,440/40,939 bytes and no sampled spill. Remote receipted intermediates totaled 997,221 bytes. Preparation uploaded 1,579,620 file bytes in 4.10 transfer-seconds; checksum read bytes and per-attempt budgets are in the manifest. This fixture used 300 customers, test scenario rates and 5% duplicates; its compression and throughput are **not scale calibration**. Its generator revision is `b2d88d5304ea53d3b87084522763e81496047fc2e6a1e946829a7e4d85cc7ec5`, resume configuration SHA-256 `86de17c995ba8688f96077137edb979e36a0a2473db5105e5907e627e6b62e8a`; the manifest retains profile and unchanged source-contract hashes. A separate physical validation host and 100 GB full-run acceptance remain unverified.

Profile setup alone was also measured at the candidate entity scale: three million customers and 20,000 merchants built and hashed in 8.45 seconds, with 264,321,152 bytes of read-only mappings and a sampled process RSS peak of 321,253,376 bytes. This generated no payment data. The workstation has 11 GiB RAM; 38 GiB local disk was free at pilot launch. The storage owner authorized 3 TB for this work on 2026-10-22, and authenticated RustFS status reported 3.2 TiB available on one healthy server/disk. Remote intermediates are removed automatically after publication or by the storage owner's lifecycle rules. The representative pilot and its calibrated plan are recorded above; full-run acceptance remains pending. `acceptance_bands` gate observed duplicate/skew shares, scenario attempt shares and per-schema event shares before publication. Pilot identity-ratio bands are omitted from the full config because their row denominators grow with history length.
