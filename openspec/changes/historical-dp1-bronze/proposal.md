# Proposal

## Why

DP1 must preserve historical and recurring deliveries in Iceberg while making their contracts, quality and lineage visible. The current Spark/GX validator and custom OpenMetadata publisher duplicate capabilities available through a shared SQL engine and native ingestion. Adopt StarRocks for distributed SQL validation and analytics, with native OpenMetadata workflows scheduled by Argo.

The same engine will support future gold data marts and materialized views. This removes the proposed separate Trino and analytical ClickHouse services. Existing observability ClickHouse remains unrelated. This is a clean replacement, without migration or legacy compatibility paths.

## What Changes

- Retain Spark ingestion through Apache Spark Operator, immutable source verification, lossless normalization, duplicate preservation, recoverable commits and accepted snapshot records. Historical exports and completed recurring landing batches share DP1.
- Deploy shared StarRocks in the platform repository through Argo CD and the official StarRocks operator/Helm chart. Use the existing RustFS Iceberg REST catalog with native SigV4 and S3 access; do not replace the catalog, add a signing proxy or copy the lake merely to query it.
- **BREAKING:** Replace GX suites and the Spark validation application with contract-derived StarRocks SQL executed by version-matched official OpenMetadata ingestion workflows. Remove GX dependencies, exporters, custom result projection, custom schema discovery, publisher code and the dedicated metadata image, along with obsolete configuration, tests and image wiring.
- Native OpenMetadata StarRocks discovery owns canonical table identities, profiling and quality results. Argo HTTP templates handle the remaining explicit product/contract/provenance/lineage writes and acceptance readback. Git contracts remain authoritative; no independently authored UI rule suite.
- **BREAKING:** Delete obsolete DP1-owned OpenMetadata artifacts, including `dp1-compatibility`, old custom services/table registrations, GX test definitions/results and stale DP1 products/contracts/pipelines. Create the native artifacts cleanly; no migration, aliases or dual publication. Preserve unrelated shared catalog assets, source data, lake snapshots and durable evidence.
- Pin both Iceberg snapshots in validation SQL. Enforce schema, integrity, per-day row conservation against the manifest, lifecycle context, linkage and label availability; generator distribution bands are informational. Keep ingestion safeguards distinct from native SQL quality results.
- Preserve immutable source declarations as provenance, explicitly recording the replacement StarRocks validation policy for sources such as v12 whose published gate names `dp1-spark-gx`. Old GX evidence is not native-engine acceptance and old outcomes are not relabeled.
- Update the roadmap to use StarRocks for analytical queries and gold fact/dimension marts, including automated materialized-view refresh. External Iceberg views use scheduled or explicit refresh; materialized views store derived data and are not a zero-storage query path. SCD2 and point-in-time training semantics require explicit modeling.
- Reuse existing CI entrypoints and focused behavioral coverage. Remove obsolete tests; do not add configuration-only suites, option matrices, new CI jobs or per-run compatibility probes.
- Establish fresh native v11 acceptance before running v12. Gold transformations, simulator/Kafka landing implementation and actual deployment are outside this planning update; recurring DP1 consumes the separately agreed completed-file interface.

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

Published v8/v11/v12 sources remain unchanged. V12 selects manifest SHA-256 `66aa2af7fecb1177e0d3891ea86054ff9c076ff6e8743b35b6bec0bd351511ce`; publication does not establish DP1 acceptance. Coursework tool substitutions require equivalent evidence, not assumed instructor approval.
