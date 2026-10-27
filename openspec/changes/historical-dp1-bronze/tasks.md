## 1. Retained ingestion and platform baseline

- [x] 1.1 Implement embedded-inventory source verification, bounded normalization and duplicate-preserving historical Iceberg ingestion; current-schema CI and v16 exercise establish fresh conservation evidence.
- [x] 1.2 Persist source/run identity, recover uncertain table commits and reuse accepted snapshots without ingestion-generated duplicates; existing recovery/replay coverage verifies these boundaries.
- [x] 1.3 Deploy scoped Apache Spark Operator and verify Spark 4.1.3/Iceberg 1.12 native RustFS snapshot access and cleanup; retain recorded platform/operator evidence.
- [x] 1.4 Deploy persistent authenticated OpenMetadata with private Dex access and verify anonymous API rejection; retain existing platform evidence.
- [x] 1.5 Install native Workflow deletion/termination guard and verify graceful cleanup and failed-cleanup recovery before writer handoff.

## 2. Shared StarRocks foundation

- [x] 2.1 Add platform-owned StarRocks through Argo CD and the official operator/Helm chart in `talos-proxmox`; pin versions, scope RBAC, size FE/BE/PVCs and verify reconciliation/persistence without installing an app-owned engine.
- [x] 2.2 Configure native RustFS REST SigV4 signing service/region and separate scoped S3 access; verify catalog listing, nested schemas and exact event/label snapshot SQL without proxy/catalog replacement or full dataset import.
- [x] 2.3 Verify native OpenMetadata metadata/profiler/test connections select the external Iceberg catalog using supported configuration; prove canonical identities, schema fidelity and one passing/failing SQL test. Resolve upstream support gaps explicitly, without custom discovery or a copied internal table fallback.
- [ ] 2.4 Record bounded recurring-ingestion resources, deadlines and observed usage; preserve the recovered historical OOM limitation. Full resource/scale benchmarking follows whole-pipeline completion.

## 3. Native contracts, SQL and acceptance

- [x] 3.1 Rewrite bronze ODCS execution rules as StarRocks SQL/native OpenMetadata tests with stable semantic IDs and complete nested schema coverage; verify contract lint and focused valid/invalid cases through existing CI.
- [x] 3.2 Implement snapshot-specific COUNT-of-violations SQL for schema, duplicate consistency, lifecycle, amounts, label linkage/availability and context; verify pinned event/label snapshots remain isolated from later commits.
- [x] 3.3 Preserve native per-day counts and add bidirectional duplicate-aware persisted/source reconciliation inside ingestion; require matching manifest/snapshot evidence before acceptance.
- [x] 3.4 Pin the current native gate, source contracts/product and feature-definition hashes in fresh v16 evidence; reject legacy schemas/gates and keep distribution bands diagnostic.
- [x] 3.5 Read back the complete expected native result set with run/source/contract/snapshot correlation; verify Failed, Aborted, stale, missing or incomplete results refuse acceptance even when the ingestion container exits zero.
- [x] 3.6 Persist bounded validation/governance evidence before failure and implement metadata-only recovery; verify no ingestion/revalidation, changed outcomes or duplicate catalog identities on replay.

## 4. Argo native ingestion and catalog cleanup

- [x] 4.1 Replace the second Spark validation stage and custom metadata image with version-matched official OpenMetadata discovery/profiler/test-suite steps; verify distinct observable stages and bounded retries through existing workflow checks.
- [x] 4.2 Use scoped Argo HTTP templates for native product/contract provenance, explicit Argo lineage and readback; configure Agent/WorkflowTaskSet RBAC and verify original ODCS IDs/hashes/artifact references and discovered table FQNs.
- [x] 4.3 Bound StarRocks validation queries with a per-user query timeout; verify no active Spark work before writer handoff and failed cleanup blocks subsequent ingestion.
- [x] 4.4 Remove GX suites/exporters/dependencies, custom discovery/HTTP/result publisher, metadata image/build job, validation application and obsolete config/tests; inspect imports, lockfiles, charts and CI to verify no compatibility or fallback path remains.
- [x] 4.5 Delete obsolete DP1-owned OpenMetadata artifacts including `dp1-compatibility`, custom table/services, GX tests/results and stale products/contracts/pipelines; verify clean native discovery and unaffected unrelated assets, source/lake data and durable reports.

## 5. Focused verification and native acceptance

- [x] 5.1 Adapt existing CI behavioral coverage for SQL semantics, snapshot isolation, result completeness, replay and cleanup; remove obsolete tests and verify existing independent CI jobs pass without new config suites, matrices or credentialed CI checks.
- [x] 5.2 Verify current schema-3 contracts and source/feature identity checks with existing behavioral CI fixtures and a CI-built image.
Deliberate isolated SQL/data-failure and metadata-outage exercises are removed at the user’s request. Existing genuine failures and CI behavioral coverage remain evidence.

- [x] 5.4 Adapt current schema-3 bronze contracts/runtime and run published v16 development acceptance now: manifest `719815e3f0a90e27bbc8303feaacdddb4858c604325656982fd443e49b05882e`, all 1,954 files / 139,597,476 bytes / 795,718 events / 161,967 labels including complete tails. Verify context, conservation, native SQL, canonical discovery and governance, then replay/recovery; source checks alone cannot complete this task.
- [x] 5.5 Record v16 persisted-bronze/governance evidence separately from source/live and historical GX/native evidence; update affected pipeline/product guidance with pinned contracts/features/snapshots and measured limits.

Scale/coursework acceptance (>=100,000,000,000 physical Parquet bytes) is deferred until the whole pipeline is finished; it is not a completion gate for this change. No scale generation is authorized.

## 6. Recurring completed-file ingestion

Producer source/live smoke and landing/recovery prerequisites are recorded in the archived v16 change, including the clean epoch `v16-20261026` and group `payments-parquet-landing-v16-20261026`. Review those exact receipts before recurring DP1 selection. Historical v16 persisted-bronze acceptance passed; it does not establish recurring readiness. Do not reset producer offsets, combine faulty deliveries as independent business events, duplicate simulator work or assume in-memory delayed labels survive shutdown.

- [x] 6.1 Agree immutable landing completion/inventory and durable progress interfaces with the live-streaming change; verify late files in older date folders are discoverable without an event-date cutoff.
- [x] 6.2 Implement common input selection, independent event/label batches and content-preserving incremental commits with pinned changed/unchanged ports; verify no-input, changed-content rejection and uncertain-commit recovery using existing behavioral CI coverage.
- [ ] 6.3 Establish the raw/silver validation boundary: retain lossless schema, file integrity, transport provenance and exact batch conservation in DP1; defer lifecycle reconstruction, duplicate consistency, business amounts/risk/context, cross-table linkage and as-of/maturity checks to DP2. Bundle native schema checks to avoid a scan per field; no same-batch labels or terminal events are required.
- [ ] 6.4 Add opt-in CronWorkflow/progress/landing permissions and demonstrate repeated live batches plus metadata-only recovery with exact accounting; historical acceptance does not establish recurring readiness.

## 7. Analytical roadmap alignment

- [x] 7.1 Update the existing roadmap to select StarRocks for analytics/gold fact-dimension marts and automated materialized views, replacing planned Trino/analytical ClickHouse; review affected sections for explicit refresh, storage, SCD2 and training boundaries.
- [x] 7.2 Record the DP2 planning handoff: mart grain/keys, SCD2/as-of joins, accepted upstream versions, refresh freshness and late-data recovery; require a future fixed-query correctness/performance baseline before selecting materialization or native serving loads. This handoff does not implement or accept gold marts.

## Fresh-start acceptance status

Code validation passed on `1da4a5c` in the existing CI jobs. V16 persisted-bronze, all 146 native quality results, profiling, snapshot reuse and governance-only recovery passed. The accepted record is `s3://fraud-lakehouse-prod/dp1-evidence/starrocks-v2/accepted/v16.json`; bounded receipts are in `evidence/`. The first SQL abort and subsequent stale-provenance rejection remain recorded. One recovered executor OOM means scale resource sizing remains open; deliberate failure exercises are no longer requested.

V16 is the current development selector and was ingested into an empty bronze namespace without schema migration or imported acceptance records. The immutable source and unrelated shared services/assets remain unchanged. Its persisted-bronze/native/governance evidence is distinct from upstream source/live smoke. Full-scale/coursework acceptance, recurring ingestion and downstream feature/training/serving acceptance remain separate later gates.
