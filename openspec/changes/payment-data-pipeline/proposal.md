## Why

The offline path from historical generation to analytics had grown into separate changes, workflows and modes: receipts and takeover in the generator, a landing archiver, full and incremental Silver, a separate DP3 workflow, and whole-table checks that grew with history. This change replaces all of it with one data pipeline: one generator, one Bronze ingest for two sources, and one `job(start, end)` model for Silver, Gold and the ClickHouse marts, audited and published layer by layer. A daily run and a year-long backfill are the same code, and every stage's cost follows its range, not the history.

## What Changes

- **Generator.** One entity-world module, `simulator/world.py`, shared with the live simulator. Generation is per purchase-start day, deterministic per key, sharded across pods, and resumable from day markers. Dated schema versions 2 and 3 are declared by the payment-events contract. Datasets are v18 (dev, 90 days) and v19 (prod, 365 days, 3M customers, 984M events, 133.5 GB).
- **BREAKING**: remove writer takeover, scratch budgeting, receipts, byte gates, the landing archiver (`simulator/landing/*`, `contracts/landing/`) and `src/data-pipeline/bronze-ingestion`. Add the `batch` (Spark and ClickHouse jobs) and `openmetadata` (catalog jobs) projects.
- **DP1 Bronze.** Manifest-listed historical files and Kafka topics land in the same Iceberg tables, keeping every delivery. Kafka progress is the Bronze rows themselves, and undecodable records go to `bronze.rejected_records`.
- **`dp-pipeline` over a UTC day range.** Silver, Gold and the marts run as one workflow, `silver → check-silver → publish-silver → gold → marts → check-gold → publish-gold → publish-marts → profile`, under one writer mutex. `dp-daily` (suspended) runs Bronze from Kafka and then yesterday plus two lookback days. `dp-backfill` runs the pipeline one month at a time, in order, and stops at the first failed month.
- **Silver.** Payment-scoped: a payment belongs to the UTC day of its first event, its `payment_day`. A run reads Bronze days `start − 1` to `end + 1` and overwrites only its payment days. Deduplication collapses republished copies, conflicting payments are quarantined whole in `silver.rejected_events`, and labels link to the attempts of the 45 days before their availability day.
- **Gold (Iceberg `gold`).** `dim_customer` is an SCD2 over the observed profile, continued from the version open at the range start. `feat_customer_daily` and `feat_payment_method_daily` hold 7- and 30-day windows at daily anchors, with rows only where a value changes. `label_attempt` holds point-in-time labels. Flink owns the 5m/1h/24h windows; DP3 builds no short windows.
- **Marts (ClickHouse `marts`).** `fact_payment_attempt` and `fact_merchant_day` keep a rolling 90-day window, with time-first sort keys and skip indexes chosen for fixed queries. They are built a day at a time into staging tables and published by partition replace.
- **Validation.** Contract-derived ClickHouse SQL rules run as native OpenMetadata tests, each scoped to the range the audited commit records. Lineage is published on every run, and the published marts are profiled.
- **Contracts.** v1 ODPS and ODCS for `historical-payments`, `bronze-payments`, `silver-payments`, `gold-features` and `payment-marts`, plus the `fraud-history-batch-v1` feature definition.
- **Platform pieces owned here.** The ClickHouse instance and its users (`admin`, `langfuse`, `openmetadata`, `marts`, `grafana`), the Polaris catalog (`lakehouse` chart), catalog registration, and the Grafana dashboards as code.
- **Lakehouse maintenance.** Nightly snapshot expiry, dropping of stale audit branches and compaction of recent Bronze files. On demand: a one-month Z-order of `silver.attempts`.
- **Evidence.** Optimization evidence comes from the production backfill: per-month stage times, failures and their fixes, and Argo, OpenMetadata and Grafana screenshots. The on-demand benchmarks stay as tools, but the evidence does not depend on running them.

## Capabilities

### New Capabilities

- `curated-payments`: day-range Silver curation with deduplication, conflict quarantine, attempt histories, label linkage, run reports, range-scoped checks, storage layout and maintenance.
- `offline-feature-tables`: Gold Iceberg tables: SCD2 customer dimension, daily-anchored history features, point-in-time labels, naming and relationships, day-range runs.
- `analytics-marts`: ClickHouse marts with query-driven keys and indexes, staged builds joined in hash buckets, least-privilege users and indexing evidence.
- `analytics-dashboards`: Grafana dashboards as code over the marts, and the platform datasource they need.

### Modified Capabilities

- `historical-data-generation`: shared entity world, per-day deterministic output, dated schema change, cluster execution and the full-scale v19 source.
- `historical-bronze-ingestion`: two inputs into shared Bronze tables, idempotent writes and contract checks.
- `dataset-contracts`: v1 contracts for every product from source to marts, and contract-scoped check objects.
- `data-pipeline-orchestration`: write-audit-publish workflows, `dp-pipeline`, `dp-daily` and `dp-backfill`, one retry level and writer serialization.
- `data-catalog-governance`: lineage from source to marts, native test evidence, range-scoped validation and scoped mart profiling.
- `fraud-feature-contracts`: the batch long-window definition `fraud-history-batch-v1`.
- `live-traffic-simulation`: Kafka topics are the live input to Bronze; landing requirements are removed.
- `talos-gitops-deployment`: data-pipeline deployment in both environments, ClickHouse, Polaris, driver placement on on-demand nodes.
- `focused-verification`: generator and pipeline behavior tests match the rebuilt code.

## Impact

- **Code:** `src/data-pipeline/simulator` (rewritten outside `live/`), `src/data-pipeline/batch` (`days`, `lakehouse`, `bronze`, `silver`, `gold`, `maintenance`), `src/data-pipeline/openmetadata` (`checks`, `wap`, `lineage`, `publish`, `profile`).
- **Contracts:** `contracts/data-products/*`, `contracts/features/fraud-history-batch-v1.*`, mart DDL under `batch/gold/sql/marts`.
- **Infra:** `infra/charts/{data-pipelines,clickhouse,lakehouse}`, the Spark and metadata Dockerfiles, and `.github/service-images.json`.
- **Docs:** `docs/data/offline.md`, `storage.md`, `orchestration-governance.md` and the generated references under `docs/data/products/`.
- **Supersedes:** the `rebuild-generation-to-silver` and `dp3-features-and-marts` changes, merged into this one.
- **Kept data:** `gold.feat_attempt_rt` (2025-10-29 to 2026-01-31) stays in prod for a training dry run, although no code writes it. Drop it only after the user confirms the Flink history exists.
- **Out of scope:** Feast wiring, Flink's live and replayed windows, training, drift, and the LLM platform's analytics tool.
