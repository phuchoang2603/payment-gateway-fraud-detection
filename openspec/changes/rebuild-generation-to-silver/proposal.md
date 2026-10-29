## Why

The path from historical generation to curated data has grown into receipts, resume/takeover machinery, byte gates, a separate Python landing archiver, five Silver tables, two Silver modes and candidate/accept pointers. An earlier Silver attempt still left correctness gaps in label linkage. This change rebuilds generation → Bronze → Silver from scratch with one clear implementation per stage, so the data path is small enough to reason about before Flink, Feast and training build on it.

## What Changes

- Rewrite the historical generator around one entity-world module, `simulator/world.py`, built from configuration and seed and used by both the generator and the live simulator. One configuration declares seed, dates, entity counts, merchant/category skew, a republished-export rate, the foreign-IP and hot-merchant shares and dated schema changes. Event schema versions 2 and 3 are declared by the payment-events contract with `sinceSchemaVersion`, so version-2 files physically lack the fields added in version 3. Version 2 is the historical v18 contract's earlier shape; version 3 is the gateway's current shape. Records are keyed by day, customer and purchase number, so they do not depend on how a day is split or how many workers run. Generation is sharded across pods; a rerun skips completed days.
- Publish dataset v18 as `historical/v18/{payment_events,fraud_labels}/date=YYYY-MM-DD/part-*.parquet` with `manifest.json` written last (the logical configuration, a generator digest over the generator sources, calibration and contracts, contract identities, totals and the file list). Events are written to one part file per schema version (`.v2.parquet`, `.v3.parquet`).
- **BREAKING**: remove writer takeover, scratch budgeting, task receipts, `_run/` cleanup, byte and probe-scale gates, prepare-only mode, the manifest feature-definition reference and the old task-fragment layout. Published v8–v17 datasets remain untouched and unsupported.
- **BREAKING**: remove `src/data-pipeline/bronze-ingestion` and add two projects: `batch` for Spark jobs and `openmetadata_jobs` for catalog jobs. Bronze (DP1) ingests two inputs into the same Iceberg tables: manifest-listed v18 files and Kafka topics read directly by Spark over explicit offset ranges, with ending offsets committed in the same Iceberg snapshot. Bronze keeps every delivery, including duplicates. Kafka records that fail decoding or a required-field check are quarantined in `bronze.rejected_records` in the same commit as the good rows, so the run does not fail for them.
- **BREAKING**: remove the Python landing archiver (`simulator/landing/*`), `contracts/landing/` and its deployment. Kafka retention bounds how long Bronze may go without running.
- Add Silver (DP2): a full recompute from pinned Bronze snapshots into `payment_events` (deduplicated), `attempts` (one row per attempt) and `labels` (deduplicated observations). Conflicting payments move to `silver.rejected_events`, with every row of the payment, so the run succeeds and the report counts them. Duplicate, conflict and unlinked-label counts go to a run report instead of provenance tables. Label linkage and availability are enforced against actual attempts.
- **BREAKING**: reset data-product contracts to v1, with one ODCS contract per table for historical source, Bronze and Silver, and no archived version copies.
- Run checks as StarRocks SQL derived from the ODCS contracts, executed by native OpenMetadata test suites. Bronze and Silver are two Argo workflows of three stages each: Spark job → SQL checks → OpenMetadata publication, with one retry level. Recurring and profiling side paths, metadata-only replay and full catalog readback acceptance are removed.
- Run historical generation as an on-demand Argo workflow in the cluster for both environments, using the simulator image. Run settings come from the environment, and the workflow runs shard pods in parallel and then a publish pod. Dev generates the 90-day v18 into `platform-dev`. Prod generates the full-scale v19 (365 days, about 3M customers, at least 100 GB of physical Parquet) into the prod AWS bucket on spot burst capacity, with the prod data-pipelines application enabled for the generator workflow only. Generation is no longer run from a workstation.
- Keep code-only diagnostics (skew, cardinality, duplicate rate, schema split) and a separate Spark benchmark (baseline vs. optimized, median of 3 runs, equal-output assertion) as on-demand jobs. No evidence capture is part of this change, and Bronze/Silver are not run at full scale.

## Capabilities

### New Capabilities

- `curated-payments`: Silver curation of Bronze payment events and labels into deduplicated events, attempts and label observations, with enforced linkage and availability, a run report, diagnostics and an optimization benchmark.

### Modified Capabilities

- `historical-data-generation`: shared entity world, per-day deterministic output, the v18 layout and manifest, a dated schema change and duplicate rate in one configuration, cluster execution, and a full-scale v19 source; resource, resume-receipt, cleanup and scale-calibration requirements are removed.
- `historical-bronze-ingestion`: rewritten for two inputs (historical manifest files and Kafka offset ranges) into the same Bronze tables, idempotent writes and contract-derived SQL checks; landing batches, acceptance records and recurring landing selection are removed.
- `dataset-contracts`: v1 contracts per table for source, Bronze and Silver products; business rules move to Silver contracts; source DuckDB checks, feature-definition traceability and evidence requirements are removed.
- `data-pipeline-orchestration`: two three-stage workflows, one retry level and a disabled-by-default Kafka schedule; replaces versioned-export selection, recurring landing ingestion, metadata replay and the multi-stage graph.
- `data-catalog-governance`: Silver products and Kafka-to-Bronze-to-Silver lineage; acceptance reduced to this run's native results; profiling becomes on demand.
- `live-traffic-simulation`: Kafka topics become the live input to Bronze; landing archive, landing batch and landing epoch requirements are removed.
- `talos-gitops-deployment`: the Python Parquet archiver workload and landing permissions are removed; data-pipeline deployment selects v18 and both workflows; prod enables only the generator template; the schedule reads Kafka.
- `focused-verification`: generator and pipeline tests match the rebuilt behavior; raw inventory, landing, recurring and v16 acceptance verification requirements are removed.

## Impact

- **Code:** `src/data-pipeline/simulator/simulator/` is rewritten (except `live/`). `simulator/landing/` is removed. `src/data-pipeline/bronze-ingestion/` is removed in favour of `src/data-pipeline/batch/` (Spark) and `src/data-pipeline/openmetadata_jobs/` (catalog). The `simulator/notebooks/historical-sanity.ipynb` notebook is removed in favour of the diagnostics job.
- **Contracts:** `contracts/data-products/{historical-payments,bronze-payments}` are reset, `silver-payments` is added, `contracts/landing/` is removed, and `contracts/quality/silver-payments.yaml` moves into the Silver ODCS contracts.
- **Infra:**
  - `infra/charts/data-pipelines` (Bronze and Silver workflows, on-demand generator, diagnostics and benchmark), and `infra/argocd/app-of-apps/values-prod.yaml`, which enables the chart in prod for the generator only.
  - The archiver block in `infra/charts/live-traffic`.
  - `infra/docker/data-platform-*.Dockerfile`, which gain the Kafka and protobuf Spark connectors and a payments descriptor set.
  - `.github/service-images.json`, which gets complete rebuild paths, including every contracts directory the images copy.
  - Python CI paths.
- **Docs:** `docs/data/offline.md`, `storage.md` and `orchestration-governance.md` (layout from the `restructure-docs-by-coursework` and `merge-data-docs-drop-requirements` changes), with regenerated product references under `docs/data/products/`.
- **Parallel work:** the live simulator change (thread `explore live simulator and data drift rework`) owns `simulator/live/`, streaming, Feast, features, `contracts/features` and the Go Edge/Fraud changes. `simulator/world.py` lands on `main` with the generator PR. The live change then moves its imports to it, and this change deletes the old generator modules only after nothing in `live/` imports them. It touches no live behavior.
- **Out of scope:** running Bronze/Silver at full scale, evidence capture, gold/feature marts, Flink/Feast changes, model training and drift detection. Long-window feature materialization (a daily Spark batch into Feast, with short windows kept in Flink) is handed to the feature change; see design.md, Handoff.
- **Cost:** the prod run stores about 100+ GB in the prod bucket and runs one bounded generator pod on prod nodes for several hours.
