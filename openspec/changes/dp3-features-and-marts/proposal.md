## Why

PR #102 ends the offline path at Silver. Nothing yet produces the long-window features that Feast's offline store and training need. There is also no star-schema layer, no warehouse indexing evidence, and nothing to look at analytically. Those gaps leave several rubric rows open:

- DP3 ingest and validate, and DP3 lineage and validation;
- SCD2 dimensions, `feat_` tables, relationships and naming;
- data-warehouse indexing.

This change plans DP3 on top of Silver, so that Feast, training, Grafana and the later analytics MCP tool all read governed tables with one meaning each.

## What Changes

- **DP3 offline features (Spark → Iceberg `gold` namespace).** A full recompute from pinned Silver snapshots writes four tables:
  - `gold.dim_customer`: SCD2 over the observed customer profile, with `valid_from_ts`, `valid_to_ts` and `is_current`.
  - `gold.feat_customer_daily` and `gold.feat_payment_method_daily`: 7- and 30-day history at daily anchors, keyed by integration-scoped customer or payment method, with `event_timestamp` and `created`. A row is written whenever a value changes, so a point-in-time "latest row at or before t" lookup is exact.
  - `gold.label_attempt`: one row per attempt and label source, carrying the entity keys and attempt time, so labels join features point-in-time without going through Silver.
- **Feature definition.** A new versioned batch definition, `fraud-history-batch-v1`, sits under `contracts/features/`. It records windows, eligibility, denominators, coverage and conformance examples, and the Spark job must reproduce those examples in CI.
- **ClickHouse marts.** A native `marts` database holds:
  - `marts.fact_payment_attempt`, built by ClickHouse `INSERT … SELECT` from the `lakehouse` Silver tables;
  - `marts.fact_merchant_day`, aggregated from that fact.

  The marts keep a rolling 180 days through a table `TTL` that drops whole days, ready for the live simulator. A `build_info` row records each build's window and Silver snapshots. Both use time-first `ORDER BY` keys, partitions and skip indexes chosen for fixed dashboard and investigation queries. Each run fully rebuilds the marts into staging tables and swaps them in atomically, so reruns are idempotent and readers never see a partial build. There is no Spark gold mart.
- **Warehouse indexing evidence.** An on-demand `warehouse-benchmark` workflow runs fixed queries three ways: against raw Iceberg through `lakehouse`, against an unsorted MergeTree copy, and against the indexed marts. It records rows, bytes, marks, memory, durations and `EXPLAIN indexes = 1` into a report.
- **Two modes.** `dp3-gold` takes `mode=historical|incremental`. Incremental mode reads the `silver.changed_payments` feed from DP2's incremental mode, which the Bootstrap prod thread implements. It recomputes only the affected customers, cards, labels and mart days, and must equal a historical run apart from `created`. The daily schedule runs incremental, and the v19 evidence uses historical.
- **DP3 orchestration.** A single `dp3-gold` Argo workflow has two ingest stages, Spark features and the ClickHouse marts build, then a validate stage. The validate stage runs the ODCS ClickHouse SQL rules as native OpenMetadata test suites and profiles the marts. The exit handler publishes pipeline status and Silver → gold lineage, as DP1 and DP2 do. The suspended Kafka schedule gains DP3 after DP2.
- **Contracts.** Two new data products: `gold-features`, for the Iceberg tables, and `payment-marts`, for the ClickHouse tables. Each has ODPS and ODCS contracts with ClickHouse-dialect quality rules. The check runner resolves each contract's own database instead of assuming `lakehouse`.
- **ClickHouse users.**
  - `grafana`: read-only, with a constrained profile and `SELECT` on `marts.*` only.
  - `marts`: the pipeline writer, limited to `marts.*`, the benchmark scratch database, and reads of `lakehouse.*`.
  - The setup job creates the `marts` and `marts_benchmark` databases.
  - `openmetadata` gains `SELECT` on `marts.*`.
- **Catalog.** The OpenMetadata ClickHouse connector discovers both the `lakehouse` and `marts` schemas. Grafana dashboards are not catalogued.
- **Grafana dashboards as code.** Three dashboards, Payments overview, Merchant drill-down and Fraud outcomes, ship as ConfigMaps labelled `grafana_dashboard: "1"`. They point at a fixed datasource UID that the platform provisions.
- **Platform requests, not made here:**
  - the `grafana-clickhouse-datasource` plugin and the `clickhouse-marts` datasource;
  - Doppler keys `CLICKHOUSE_GRAFANA_PASSWORD` and `CLICKHOUSE_MARTS_PASSWORD`, and their ExternalSecret in `observability`;
  - sidecar namespace coverage;
  - a network path from Grafana to ClickHouse.

- **Docs.** Update the existing pages: offline data (schemas, naming, ERD), storage (the warehouse section), orchestration and governance (DP3), feature store (long windows), and the roadmap rows. Generated product references are added under `docs/data/products/`.

No breaking changes. Nothing downstream consumes gold today. The feature-store page's sentence "DP3 computes the same definitions … as of each attempt time" is corrected to daily anchors (design decision 3).

## Capabilities

### New Capabilities

- `offline-feature-tables`: DP3 gold tables in Iceberg. Covers the SCD2 customer dimension, the daily-anchored customer and payment-method history features with coverage and denominators, the point-in-time label table, the naming conventions and table relationships, and recompute idempotency.
- `analytics-marts`: ClickHouse native fact marts built from the lakehouse. Covers their sort keys, partitions and skip indexes, atomic full rebuild, the warehouse users and grants, and the before/after indexing evidence.
- `analytics-dashboards`: Grafana dashboards as code over the marts. Covers the read-only datasource contract and the platform prerequisites.

### Modified Capabilities

- `data-pipeline-orchestration`: adds the DP3 workflow (two ingest stages, then validate), its writer serialization with DP2, and DP3 in the suspended Kafka schedule.
- `data-catalog-governance`: adds DP3 Silver → gold lineage, gold and mart validation evidence, and scoped profiling of the marts.
- `dataset-contracts`: adds the `gold-features` and `payment-marts` products, and contract rules that name their own ClickHouse database.
- `fraud-feature-contracts`: adds the versioned batch long-window definition and its conformance examples.

## Impact

- **Code:**
  - `src/data-pipeline/batch`: a new `gold` Spark job, a `marts` ClickHouse build job and a `warehouse_benchmark` job, using an HTTP ClickHouse client. Tests are added.
  - `src/data-pipeline/openmetadata_jobs`: contract-aware table resolution, profiling of the marts, and DP3 pipelines and lineage edges.
- **Contracts:**
  - `contracts/data-products/{gold-features,payment-marts}`;
  - `contracts/features/fraud-history-batch-v1.{yaml,examples.json}`;
  - mart DDL under `src/data-pipeline/batch/batch/sql/`.
- **Infra:**
  - `infra/charts/data-pipelines`: the `dp3-gold` and `warehouse-benchmark` templates, the schedule step, values, and dashboard ConfigMaps;
  - `infra/charts/clickhouse`: the `grafana` and `marts` users and profiles, the new databases, and their ExternalSecrets;
  - `infra/charts/data-pipelines` `catalog.connectors` (which owns OpenMetadata registration since #102): ClickHouse schemas for `lakehouse` and `marts`.
- **Images:** the Spark image ships the SQL files and the ClickHouse client library. Rebuild paths are unchanged, because the batch package is already covered.
- **Docs:** `docs/data/offline.md`, `storage.md`, `orchestration-governance.md`, `docs/ml/feature-store.md`, `docs/overview/roadmap.md`, and the generated product references.
- **Sequencing:** this branch is rebased onto `main` after #102 merged. Implementation waits until the prod v19 Bronze and Silver evidence run finishes. The PR's dev deployment is then a v18 smoke run, and DP3 is tested and evidenced on prod v19 after merge. The platform requests block only the Grafana evidence, not DP3.
- **Out of scope, as follow-ups:** Feast wiring, streaming features into the offline store, materialization and TTLs (roadmap steps 2–7); training; the LLM platform (#103), including the read-only ClickHouse analytics MCP tool, which will reuse the `grafana` user's model over `marts.*`; and incremental gold.
