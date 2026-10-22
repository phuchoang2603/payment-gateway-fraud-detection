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

### Published version

`historical/v8` is the original published full-scale source, generated with seed 2603 (its exact configuration is retained in its manifest): 24,982 raw files, 123,286,939,620 physical and 243,759,611,982 uncompressed Parquet bytes, with validation deferred to the `dp1-spark-gx` gate. `historical/v10` is a small smoke export (100,000 customers, six days) using the same manifest format. Each `manifest.json` records the configuration hash, generator revision, contract hashes, per-file inventory, per-partition row counts and storage metrics. [`historical-sanity.ipynb`](../../src/data-pipeline/simulator/notebooks/historical-sanity.ipynb) reads a published manifest for distribution, skew, attempt context and duplicate checks.

`historical/v12` is an immutable one-off -365-day calendar shift of v8, with the same entities/content and no regeneration. Its manifest SHA-256 is `66aa2af7fecb1177e0d3891ea86054ff9c076ff6e8743b35b6bec0bd351511ce`; 2,851 files received full row/remote readback checks and 22,131 received upload-integrity/HEAD checks. Spark/GX acceptance remains pending. Live decoded Parquet lands separately under `streaming/`; see the [landing interface](data-pipeline.md#live-simulation-and-landing).

The maintained smoke profile now targets a new 90-day development source, `historical/v16`: 10,000 customers, 2,000 merchants, July 1 through September 28, 2025 purchase starts, seed 2603 and current payload schema 3. Lifecycle completion and scheduled label availability may extend beyond that range. It has no 100 GB size gate. Publication and source checks passed: [manifest](https://minio.home.phuchoang.sbs/synthetic-source/historical/v16/manifest.json) SHA-256 `719815e3f0a90e27bbc8303feaacdddb4858c604325656982fd443e49b05882e`. The CI-image Kubernetes run verified all 1,954 inventory files and all nine source SQL checks. It contains 795,718 events and 161,967 labels in 139,597,476 physical Parquet bytes; this is development-source evidence, not DP1 acceptance. Previous source versions are historical evidence, not supported current-runtime inputs; the operator removed the old sources and stopped v14.

The first schema-3 candidate v15 failed its declared risk-assessment rule because that rule incorrectly required reason codes on clean approvals. Its manifest and failed evidence remain unchanged. Contract 4.0.1 corrects the rule to require reasons on declines; v16 passes the corrected rule.

The enriched attempt context matches gateway scoring inputs. The manifest pins [baseline feature definitions](../../contracts/features/fraud-baseline-v1.yaml); it does not contain computed feature snapshots or certify downstream ingestion. Use a seven-day history warmup, chronological splits and label maturity at each training cutoff. Absent chargebacks are not confirmed negatives. Report class/cohort counts and temporal/entity/campaign overlap before interpreting model results.

The [manifest-bound development report](../../openspec/changes/archive/2026-10-26-finalize-feature-contract-and-90-day-smoke/evidence/v16-development-report.json) records events through September 29 00:58:24 UTC and label availability through October 19 22:17:57.008 UTC. It measures 9,118 positive and 148,084 negative simulation-truth attempts. Friendly-dispute simulation truth remains negative while its chargeback label is positive; choose a label-source/target policy rather than merging those targets implicitly.

After seven days of warmup, one conservative evaluation schedule is:

| Anchors (end exclusive) | Role | Positive / negative attempts | Labels available before |
|---|---|---|---|
| July 8–August 1 | Training | 2,848 / 38,809 | August 22 |
| August 22–September 1 | Validation | 914 / 16,218 | September 22 |
| September 22–29 | Test | 679 / 11,956 | October 20 |

Fit after training maturity, select after validation maturity, then evaluate the untouched test interval. The excluded intervals allow the configured 21-day chargeback tail; always enforce actual availability. Customer/method overlap is substantial (4,874 customers and 5,346 methods shared between training and test), so this is temporal evaluation, not an entity holdout. Stable campaign IDs are not exported; campaign overlap is unmeasured and customer/method overlap is only a proxy. These counts support pipeline experiments, not a model-quality claim.

Generate only the development profile now. Full-volume Python generation is deferred until ingestion, features, training and serving have been exercised; no Spark simulator is required. Run generation from the devenv shell:

```bash
cd src/data-pipeline/simulator
doppler run -p payment-gateway-fraud-detection -c prd -- sh -c \
  'AWS_ACCESS_KEY_ID="$HISTORICAL_RUSTFS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$HISTORICAL_RUSTFS_SECRET_KEY" \
   uv run --locked simulator historical --config configs/historical-smoke.yaml'
```

Bump `dataset_version` in the config for every new dataset. Pass `--output <dir>` to generate locally without credentials.

The approved new `historical --config <path>` path generates and uploads raw task Parquet, verifies checksums, row-count accounting and the physical-byte gate, and writes the completion manifest last. It then cleans only `_run/`, retaining persistent `raw/` files. It resumes compatible unfinished work or repairs cleanup on a completed inventory without rewriting files or the manifest. Completed versions cannot be regenerated or overwritten; recovery verifies the stored logical configuration.

The manifest uses `manifest_version: 3`, `source_layout: task-fragments-v1` and an exact inventory of `raw/{table}/{task.key}/v{schema_version}.parquet` files beneath the dataset root. Each file records `schema_version`, checksum, bytes, rows and `partitions: {date: rows}`; those dates summarize occurrence dates for events and availability dates for labels. A task file can cover several dates. Source files carry no sorting or compaction guarantee. The current context contract versions the historical-payments product to `6.0.1` and the payment-events contract to `4.0.1`; current files use event schema 3.

New generation-time contract checks are recorded as `result: skipped`, gate `dp1-starrocks-openmetadata` and a reason. Older manifest gate names remain historical provenance. Python/DuckDB finalization and remote analytical scans are removed from the new path. DP1 validates exactly the listed raw files, preserves duplicate deliveries and delayed labels, derives Bronze partitions from timestamp columns and requires the owning DP1 snapshot-pinned SQL checks on both persisted tables plus verified native OpenMetadata governance before Bronze quality acceptance. Source publication alone does not establish quality acceptance. Reports use cheap additive row/scenario/duplicate/schema and bounded skew counters; observed cardinality validation is marked downstream, with configured pools kept separate from measurements.

After durable publication, cleanup retains `_run/publication.json` with successful delete counts and never deletes inventory-listed raw files. Cleanup failures exit nonzero; rerunning verifies file integrity and retries. Publication uses no object tags. Source immutability is enforced by completed-manifest/configuration/inventory checks; runtime credentials cannot delete streaming sources, and historical delete scope is limited to `historical/*/_run/*`.

`--prepare-only` leaves generated raw inventory ready, without sorting or a completion manifest. The normal command continues publication under the same generation identity. Normal shutdown releases ownership. After a crash, stop the former writer first and pass `--takeover-stopped-writer`; elapsed time never authorizes takeover. Small remote publication still requires `--allow-small-dataset` below one customer block; probes normally use local `--output`.

The historical command is generation-only: it has no migration command or old-configuration compatibility path. The separate `live` entry point drives current-time gateway commands.

The historical 2026-10-22 RustFS probe confirmed create-if-absent, ETag-conditional update and streamed transfer; HEAD returned no SHA-256 checksum, so integrity verification streams each object. Unsupported conditional operations fail rather than falling back to unsafe writes. New execution controls cover workers, in-flight reservations, scratch and transfers, without DuckDB sort/validation budgets. Logical customer blocks, burst groups, layout, scenarios, contracts and minimum source bytes form the generation identity.

The maintained generator starts fresh with a US-only electronics calibration profile, fixed entity birth dates, regional IANA/DST scheduling and UTC storage. Calibration references and explicit behavioral assumptions are hashed into generation identity. The candidate configurations use current event schema three, no injected duplicates and overlapping fraud/legitimate contexts. Schema-cutover branches are removed; duplicate-delivery experiments require `data_problems.experiment: true`. Prior-version evidence is historical; old source versions are not supported inputs to the current algorithm. Live source selection is separate.

Distribution/scenario/skew counters are informational diagnostics, not source publication gates. Amount/context/time/activity counters use payments, missingness/risk counters use attempts, and purchase-weighted activity is not a population census. Chargeback reporting probability and availability beyond the configured history end are explicit; absent chargebacks are not confirmed negatives. Oracle scenario/truth and postdecision outcomes must be excluded from features. Use forward-time evaluation with mature labels plus entity/campaign holdouts; public payment context does not establish a representative checkout fraud distribution.

The coursework source is `docs/coursework/rubic/EDAI K11 - DE.xlsx`, sheet `edai-1 (50%)`, cell B5: offline simulation of at least 100 GB. Compression accounting is unspecified there. Full-scale coursework evidence requires at least **100,000,000,000 physical bytes** across inventory-listed raw event and label Parquet files, counted once, excluding `_run`, reports, streaming data, replicas and downstream copies. The v13 smoke export does not satisfy this target. The manifest separately records uncompressed Parquet column-chunk bytes and decimal GB/GiB. A configured full-scale byte gate still prevents undersized publication. Changing logical inputs requires a new dataset version after publication.

## Bronze payments

`bronze-payments` is a separate source-aligned product with `bronze.raw_payment_events` and `bronze.raw_fraud_labels` Iceberg ports. Its contracts preserve business fields and duplicate deliveries, normalize the absent v1 browser field to null, and append `_source_dataset_version`, `_source_manifest_sha256`, `_source_file`, `_ingestion_run_id` and `_ingested_at`. Contract/product/source versions remain independent. Labels retain their availability cutoff and feature-leakage restrictions.

Source publication still uses the unchanged Data Contract CLI/DuckDB commands above. DP1 exports the bronze ODCS rules with Data Contract CLI 1.2.7 and imports them into GX 1.24 on Spark 3.5.8; exporting a suite does not validate data or translate DuckDB SQL. Each bronze quality block contains Spark unexpected-row SQL with its source semantic rule ID. Preparation rejects missing/unsupported rules; generated conditional checks cover nested required fields and array items, and recursive physical checks enforce the exact normalized schema. The native top-level column-set expectation permits GX's nested Spark column listing because the recursive check separately rejects extra columns.

After integrating the generator inventory change, run the locked fixtures and contract lint from the repository devenv. An isolated checkout can set `DP1_GENERATOR_ROOT` to the integrated `src/simulator` dependency:

```bash
bash scripts/check-data-platform.sh
bash scripts/ci-checks.sh contracts
```

Run `dp1 validate` in the Spark image against a recorded snapshot pair; `dp1 publish` is the separate metadata command. See the [DP1 operation instructions](data-pipeline.md#historical-dp1) for configuration and replay. DataHub consumes metadata/results and does not replace either validation engine.
