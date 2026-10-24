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

`historical/v7` is the current accepted dataset, generated from [`src/data-pipeline/simulator/configs/historical.yaml`](../../src/data-pipeline/simulator/configs/historical.yaml) (seed 2603) and validated against both contracts before publication. Its `manifest.json` records the configuration hash, generator revision, contract hashes, per-partition row counts and storage metrics; [`historical-sanity.ipynb`](../../src/data-pipeline/simulator/notebooks/historical-sanity.ipynb) holds the executed distribution, skew, cutover, duplicate and layout checks.

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

The command runs from the devenv shell. The generation-only publication path below does not establish a completed migration or validation of `v8`:

```bash
cd src/data-pipeline/simulator
doppler run -p payment-gateway-fraud-detection -c prd -- sh -c \
  'AWS_ACCESS_KEY_ID="$HISTORICAL_RUSTFS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$HISTORICAL_RUSTFS_SECRET_KEY" \
   uv run --locked simulator historical --config configs/historical.yaml'
```

Bump `dataset_version` in the config for every new dataset. Pass `--output <dir>` to generate locally without credentials.

The approved new `historical --config <path>` path generates and uploads raw task Parquet, verifies checksums, row-count accounting and the physical-byte gate, and writes the completion manifest last. It then tags configured accepted output and cleans only `_run/`, retaining persistent `raw/` files. It resumes compatible unfinished work or repairs protection/cleanup on a completed inventory without rewriting files or the manifest. Completed versions cannot be regenerated or overwritten; recovery verifies the stored logical configuration and publication settings.

The manifest uses `manifest_version: 3`, `source_layout: task-fragments-v1` and an exact inventory of `raw/{table}/{task.key}/v{schema_version}.parquet` files beneath the dataset root. Each file records `schema_version`, checksum, bytes, rows and `partitions: {date: rows}`; those dates summarize occurrence dates for events and availability dates for labels. A task file can cover several dates. Source files carry no sorting or compaction guarantee. This physical publication change versions the historical-payments product to `5.0.0`; the logical ODCS contracts remain unchanged.

Generation-time contract checks are recorded as `result: skipped`, gate `dp1-spark-gx` and a reason. Python/DuckDB finalization and remote analytical scans are removed from the new path. DP1 validates exactly the listed raw files, preserves duplicate deliveries and delayed labels, derives Bronze partitions from timestamp columns and requires Spark/GX checks on both persisted tables plus verified governance before Bronze quality acceptance. Source publication and `accepted=true` storage protection alone do not establish quality acceptance. Reports use cheap additive row/scenario/duplicate/schema and bounded skew counters; observed cardinality validation is marked downstream, with configured pools kept separate from measurements.

After durable publication and successful protection, cleanup retains `_run/publication.json` with successful delete counts and never deletes inventory-listed raw files. Tagging or cleanup failures exit nonzero; rerunning verifies file integrity and retries. Source credentials need `GetObjectTagging` and `PutObjectTagging`; bucket policy protects accepted objects. Delete scope covers `_run/` and abandoned probe/pilot prefixes.

`--prepare-only` leaves generated raw inventory ready, without sorting or a completion manifest. The normal command continues publication under the same generation identity. Normal shutdown releases ownership. After a crash, stop the former writer first and pass `--takeover-stopped-writer`; elapsed time never authorizes takeover. Small remote publication still requires `--allow-small-dataset` below one customer block; probes normally use local `--output`.

The simulator has no legacy migration command or old-configuration compatibility path. The old `v8` finalizer was stopped after generation completed. Its existing fragments require a one-time operational handoff outside the maintained simulator: verify complete receipts and original provenance, copy to persistent raw keys, verify checksums and byte/row gates, then publish a manifest last. Retain the original fragments until the handoff is verified. This neither regenerates rows nor runs finalization, and source publication does not establish DP1 quality acceptance.

The historical 2026-10-22 RustFS probe confirmed create-if-absent, ETag-conditional update and streamed transfer; HEAD returned no SHA-256 checksum, so integrity verification streams each object. Unsupported conditional operations fail rather than falling back to unsafe writes. New execution controls cover workers, in-flight reservations, scratch and transfers, without DuckDB sort/validation budgets. Logical customer blocks, burst groups, layout, scenarios, contracts and minimum source bytes form the generation identity.

Historical legacy sizing evidence: [`historical-scale.yaml`](../../src/data-pipeline/simulator/configs/historical-scale.yaml) (dataset version `v8`) and [`historical-pilot.yaml`](../../src/data-pipeline/simulator/configs/historical-pilot.yaml) (`v8-pilot`) share three million customer profiles, layout, scenarios and one year or six days of history. The measured pilot produced 3,124,545 payments, 1,985,818,453 physical final Parquet bytes and 4,843,810,164 uncompressed column-chunk bytes. Generation/finalization took 1,440.4/952.5 seconds and native validation took 1,942.6 seconds over two attempts. Process-tree RSS, scratch and spill peaks were 5,112,930,304, 8,686,332,811 and 8,421,998,592 bytes. These legacy measurements projected roughly 120.8 GB of compacted final Parquet and 40.4 hours of generation/finalization for the fixed full plan; they do not establish full-run acceptance or measurements of the new raw publication path. The pilot's generated fragments totaled 2,006,142,136 bytes; that historical observation is distinct from its compacted final bytes.

The six-day pilot includes the warmup of seven-day stolen-card compromise windows. A bounded calibration sample using the intended profiles and the first 100,000-customer block on days 30–35 estimated a 2.67% stolen-card attempt share. The full config therefore gates that share at 2.0–3.2%; the pilot retains its observed 0.8–1.8% band. Both configurations retain the measured duplicate/skew/schema and other scenario bands and 60 card-testing bursts/day. Full-year distribution and byte acceptance remain unmeasured. Legacy native full-history validation projected about 512 GB of spill from the pilot, exceeding this workstation's capacity. The approved new path assigns quality processing to DP1 Spark/GX; these estimates establish no validation result for live `v8`.

The coursework source is `docs/coursework/rubic/EDAI K11 - DE.xlsx`, sheet `edai-1 (50%)`, cell B5: offline simulation of at least 100 GB. Compression accounting is unspecified there. This project conservatively requires at least **100,000,000,000 physical bytes** across inventory-listed raw event and label Parquet files for the new path, counted once, excluding `_run`, reports, streaming data, replicas and downstream copies. The manifest separately records uncompressed Parquet column-chunk bytes and decimal GB/GiB. A fixed plan below the byte gate cannot publish; changing logical inputs requires a new dataset version. v7 remains the accepted correctness dataset until a separately measured full run passes every gate.

Historical legacy integration evidence: a small remote integration check on 2026-10-22 published `s3://synthetic-source/historical/runtime-process-check-2800fb7138e74bd699970de8f4ccfdc8/manifest.json` and removed it after measurement, so that object no longer resolves: 707 payments, 3,957 exported event rows, 882 labels, 582,399 physical and 894,981 uncompressed Parquet bytes. Both native reports passed. Separate CLI processes on the same workstation prepared then resumed the files. Preparation took 45.39 seconds (8.50 generation/upload; 35.46 finalization), with sampled RSS/scratch peaks of 253,603,840/246,483 bytes. Resume used 768 MiB/two DuckDB threads instead of 512 MiB/one thread, regenerated and uploaded nothing, and took 52.76 seconds (46.86 validation), with RSS/scratch peaks of 325,181,440/40,939 bytes and no sampled spill. Remote receipted intermediates totaled 997,221 bytes. Preparation uploaded 1,579,620 file bytes in 4.10 transfer-seconds; checksum read bytes and per-attempt budgets are in the manifest. This fixture used 300 customers, test scenario rates and 5% duplicates; its compression and throughput are **not scale calibration**. Its generator revision is `b2d88d5304ea53d3b87084522763e81496047fc2e6a1e946829a7e4d85cc7ec5`, resume configuration SHA-256 `86de17c995ba8688f96077137edb979e36a0a2473db5105e5907e627e6b62e8a`; the manifest retains profile and unchanged source-contract hashes. This measured legacy native-validation handoff is not a requirement or validation result for the new raw path; 100 GB full-run quality acceptance remains unverified.

Profile setup alone was also measured at the candidate entity scale: three million customers and 20,000 merchants built and hashed in 8.45 seconds, with 264,321,152 bytes of read-only mappings and a sampled process RSS peak of 321,253,376 bytes. This generated no payment data. The workstation has 11 GiB RAM; 38 GiB local disk was free at pilot launch. The storage owner authorized 3 TB for this work on 2026-10-22, and authenticated RustFS status reported 3.2 TiB available on one healthy server/disk. In the new path only transient `_run/` data is cleaned after publication; persistent raw inventory remains. The representative pilot and its calibrated plan are recorded above; full-run acceptance remains pending. Legacy `acceptance_bands` gated observed duplicate/skew shares, scenario attempt shares and per-schema event shares before publication. Pilot identity-ratio bands were omitted from the full config because their row denominators grow with history length. New reports retain additive observations and defer observed cardinality validation to DP1.
