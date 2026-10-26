# Proposal

## Why

DP1 must preserve historical and recurring deliveries in Iceberg while making their contracts, quality and lineage visible. The current Spark/GX validator and custom OpenMetadata publisher duplicate capabilities available through a shared SQL engine and native ingestion. Adopt StarRocks for distributed SQL validation and analytics, with native OpenMetadata workflows scheduled by Argo.

The same engine will support future gold data marts and materialized views. This removes the proposed separate Trino and analytical ClickHouse services. Existing observability ClickHouse remains unrelated. This is a clean replacement, without migration or legacy compatibility paths.

## What Changes

- Retain Spark ingestion through Apache Spark Operator, immutable source verification, current schema-3 context preservation, duplicate-aware source/output reconciliation, duplicate preservation, recoverable commits and accepted snapshot records. Historical exports and completed recurring landing batches share DP1.
- Deploy shared StarRocks in the platform repository through Argo CD and the official StarRocks operator/Helm chart. Use the existing RustFS Iceberg REST catalog with native SigV4 and S3 access; do not replace the catalog, add a signing proxy or copy the lake merely to query it.
- **BREAKING:** Replace GX suites and the Spark validation application with contract-derived StarRocks SQL executed by version-matched official OpenMetadata ingestion workflows. Remove GX dependencies, exporters, custom result projection, custom schema discovery, publisher code and the dedicated metadata image, along with obsolete configuration, tests and image wiring.
- Native OpenMetadata StarRocks discovery owns canonical table identities, profiling and quality results. Argo HTTP templates handle the remaining explicit product/contract/provenance/lineage writes and acceptance readback. Git contracts remain authoritative; no independently authored UI rule suite.
- **BREAKING:** Delete obsolete DP1-owned OpenMetadata artifacts, including `dp1-compatibility`, old custom services/table registrations, GX test definitions/results and stale DP1 products/contracts/pipelines. Create the native artifacts cleanly; no migration, aliases or dual publication. Remove old DP1 lake/run-evidence artifacts for a fresh current-contract run; preserve unrelated shared catalog assets and immutable v16 source data.
- Pin both Iceberg snapshots in validation SQL. Enforce raw schema, file integrity and exact delivery conservation; DP2 owns lifecycle, business consistency, linkage and as-of/label-availability validation; generator distribution bands are informational. Keep ingestion safeguards distinct from native SQL quality results.
- Preserve immutable source declarations and feature-definition hashes as provenance. Current v16 declares `dp1-starrocks-openmetadata`; historical GX and earlier native outcomes remain historical evidence and are not relabeled or reused as current-contract acceptance.
- Update the roadmap to use StarRocks for analytical queries and gold fact/dimension marts, including automated materialized-view refresh. External Iceberg views use scheduled or explicit refresh; materialized views store derived data and are not a zero-storage query path. SCD2 and point-in-time training semantics require explicit modeling.
- Reuse existing CI entrypoints and focused behavioral coverage. Remove obsolete tests; do not add configuration-only suites, option matrices, new CI jobs or per-run compatibility probes.
- Establish current-contract persisted-bronze and OpenMetadata acceptance now using the published 90-day v16 development source, without waiting for scale generation. Exercise replay/recovery and retain source/live acceptance as separate upstream evidence. Full-scale/coursework acceptance is deferred until the whole pipeline is finished and is not a completion gate for this change; no source generation belongs to DP1. Recurring DP1 consumes the separately owned completed-file interface.

## Capabilities

### New Capabilities

- `historical-bronze-ingestion`: Recoverable content-preserving historical and recurring ingestion with snapshot-pinned acceptance.
- `data-pipeline-orchestration`: Argo-managed Spark ingestion and native OpenMetadata SQL stages, bounded cleanup and independently recoverable evidence.
- `data-catalog-governance`: Native StarRocks discovery, quality/profiling, canonical assets, contract provenance and lineage.

### Modified Capabilities

- `dataset-contracts`: Git-authoritative bronze schema and StarRocks SQL checks, distinct from source DuckDB publication checks.
- `talos-gitops-deployment`: Platform-owned StarRocks/operator and OpenMetadata; application-owned pipeline SQL, workflow configuration and workload credentials.
- `focused-verification`: Proportionate verification of ingestion, snapshot SQL, native discovery and complete quality-result acceptance without redundant config tests.

## Impact

Implementation affects `src/data-pipeline/bronze-ingestion/`, bronze ODPS/ODCS contracts, the data-pipelines chart, metadata image/dependency/CI wiring and their existing behavioral tests. Platform changes belong in `talos-proxmox`; this proposal does not deploy them. Only the affected roadmap and existing pipeline/product/deployment guidance change.

Select `s3://synthetic-source/historical/v16/manifest.json`, exact byte SHA-256 `719815e3f0a90e27bbc8303feaacdddb4858c604325656982fd443e49b05882e`: 1,954 files, 139,597,476 physical bytes, 795,718 events and 161,967 labels. Event schema 3, source events ODCS 4.0.1, labels 1.0.0 and product 6.0.1 are current; `fraud-baseline-v1` is pinned at SHA-256 `2ee2a5153a248bdc98df3b2643e10a65bc5693ea111129b6c65a67352aa4ec0b`. Publication/source checks do not establish persisted-bronze/governance acceptance. Do not mutate or regenerate sources or reinterpret old passed/failed evidence. A later current-contract immutable source must meet the separate 100,000,000,000 physical-byte coursework gate; v16 does not.

Recurring raw ingestion uses immutable clean landing manifests and a durable acceptance ledger, independently ingests delayed ports and retains the complete Kafka envelope. A daily UTC CronWorkflow skips Spark on no input. Bundle native schema/required-field checks; profiling is optional. Move historical/business-context, duplicate-consistency, amount/risk, linkage and maturity/as-of checks to DP2 silver, preserving their SQL definitions and previous v16 outcomes. New raw acceptance uses `bronze-raw-v3`; deliberate isolated failure exercises are removed and scale acceptance follows whole-pipeline completion.
