## 1. Retained ingestion and platform baseline

- [x] 1.1 Implement embedded-inventory source verification, bounded normalization and duplicate-preserving historical Iceberg ingestion; prior v11 evidence verifies row/content conservation.
- [x] 1.2 Persist source/run identity, recover uncertain table commits and reuse accepted snapshots without ingestion-generated duplicates; existing recovery/replay coverage verifies these boundaries.
- [x] 1.3 Deploy scoped Apache Spark Operator and verify Spark 4.1.3/Iceberg 1.12 native RustFS snapshot access and cleanup; retain recorded platform/operator evidence.
- [x] 1.4 Deploy persistent authenticated OpenMetadata with private Dex access and verify anonymous API rejection; retain existing platform evidence.
- [x] 1.5 Install native Workflow deletion/termination guard and verify graceful cleanup and failed-cleanup recovery before writer handoff.
- [x] 1.6 Publish v11 and remove retired v7 companion support; retain immutable source/evidence references.

## 2. Shared StarRocks foundation

- [x] 2.1 Add platform-owned StarRocks through Argo CD and the official operator/Helm chart in `talos-proxmox`; pin versions, scope RBAC, size FE/BE/PVCs and verify reconciliation/persistence without installing an app-owned engine.
- [x] 2.2 Configure native RustFS REST SigV4 signing service/region and separate scoped S3 access; verify catalog listing, nested schemas and exact event/label snapshot SQL without proxy/catalog replacement or full dataset import.
- [x] 2.3 Verify native OpenMetadata metadata/profiler/test connections select the external Iceberg catalog using supported configuration; prove canonical identities, schema fidelity and one passing/failing SQL test. Resolve upstream support gaps explicitly, without custom discovery or a copied internal table fallback.
- [ ] 2.4 Measure combined Spark, StarRocks and OpenMetadata scheduling/memory/storage; record bounded resource and deadline settings before scale submission.

## 3. Native contracts, SQL and acceptance

- [ ] 3.1 Rewrite bronze ODCS execution rules as StarRocks SQL/native OpenMetadata tests with stable semantic IDs and complete nested schema coverage; verify contract lint and focused valid/invalid cases through existing CI.
- [ ] 3.2 Implement snapshot-specific COUNT-of-violations SQL for schema, duplicate consistency, lifecycle, amounts, label linkage/availability and context; verify pinned event/label snapshots remain isolated from later commits.
- [ ] 3.3 Compare persisted per-day row counts for the pinned snapshots with the immutable manifest in native SQL; content equality rests on ingestion's per-file SHA-256 verification and lossless normalization and is not re-verified after the write.
- [ ] 3.4 Version validation policy and explicitly supersede the original v12 `dp1-spark-gx` declaration with StarRocks execution while retaining original provenance; verify old GX records cannot satisfy new acceptance and generator bands are diagnostic only.
- [ ] 3.5 Read back the complete expected native result set with run/source/contract/snapshot correlation; verify Failed, Aborted, stale, missing or incomplete results refuse acceptance even when the ingestion container exits zero.
- [ ] 3.6 Persist bounded validation/governance evidence before failure and implement metadata-only recovery; verify no ingestion/revalidation, changed outcomes or duplicate catalog identities on replay.

## 4. Argo native ingestion and catalog cleanup

- [ ] 4.1 Replace the second Spark validation stage and custom metadata image with version-matched official OpenMetadata discovery/profiler/test-suite steps; verify distinct observable stages and bounded retries through existing workflow checks.
- [ ] 4.2 Use scoped Argo HTTP templates for native product/contract provenance, explicit Argo lineage and readback; configure Agent/WorkflowTaskSet RBAC and verify original ODCS IDs/hashes/artifact references and discovered table FQNs.
- [ ] 4.3 Bound StarRocks validation queries with a per-user query timeout; verify no active Spark work before writer handoff and failed cleanup blocks subsequent ingestion.
- [ ] 4.4 Remove GX suites/exporters/dependencies, custom discovery/HTTP/result publisher, metadata image/build job, validation application and obsolete config/tests; inspect imports, lockfiles, charts and CI to verify no compatibility or fallback path remains.
- [x] 4.5 Delete obsolete DP1-owned OpenMetadata artifacts including `dp1-compatibility`, custom table/services, GX tests/results and stale products/contracts/pipelines; verify clean native discovery and unaffected unrelated assets, source/lake data and durable reports.

## 5. Focused verification and native acceptance

- [ ] 5.1 Adapt existing CI behavioral coverage for SQL semantics, snapshot isolation, result completeness, replay and cleanup; remove obsolete tests and verify existing independent CI jobs pass without new config suites, matrices or credentialed CI checks.
- [ ] 5.2 Run fresh complete v11 native acceptance using CI-built images; verify 547,166 events, 107,937 labels, content conservation, canonical discovery, SQL results, lineage and governance readback with measured resources.
- [ ] 5.3 Verify deliberate SQL failure and metadata outage/recovery on isolated candidates; demonstrate failure visibility, unchanged previous acceptance and metadata-only replay.
- [ ] 5.4 After native v11 readiness, ingest published v12 with manifest SHA-256 `66aa2af7fecb1177e0d3891ea86054ff9c076ff6e8743b35b6bec0bd351511ce`; verify all 24,982 files, 123,289,031,513 physical bytes, 1,015,047,213 events and 202,362,326 labels, snapshot SQL/conservation and governance. Report capacity/deadline failure honestly.
- [ ] 5.5 Record native v11/v12 evidence separately from old GX acceptance and update only affected existing pipeline/product/deployment guidance; verify source immutability and reproducible accepted references.

## 6. Recurring completed-file ingestion (deferred)

Deferred until the simulator owner demonstrates bounded current-time gateway traffic, identities, independently arriving labels and delivery faults with the archiver disabled, and then landing/recovery, and that evidence is reviewed. The landing schema, path and manifest handoff is informational only. Do not start multi-source configuration, completed-manifest discovery or recurring incremental ingestion from these tasks before that review; the historical `SOURCE_ROOT` remains single-source.

- [ ] 6.1 Agree immutable landing completion/inventory and durable progress interfaces with the live-streaming change; verify late files in older date folders are discoverable without an event-date cutoff.
- [ ] 6.2 Implement common input selection, independent event/label batches and content-preserving incremental commits with pinned changed/unchanged ports; verify no-input, changed-content rejection and uncertain-commit recovery using existing behavioral CI coverage.
- [ ] 6.3 Apply contextual native SQL checks with pinned history and label maturity, retaining origin/offset/batch provenance and informational generator diagnostics; verify incomplete context never becomes an invented pass or a requirement for same-batch labels.
- [ ] 6.4 Add opt-in CronWorkflow/progress/landing permissions and demonstrate repeated live batches plus metadata-only recovery with exact accounting; historical v11 acceptance does not establish recurring readiness.

## 7. Analytical roadmap alignment

- [x] 7.1 Update the existing roadmap to select StarRocks for analytics/gold fact-dimension marts and automated materialized views, replacing planned Trino/analytical ClickHouse; review affected sections for explicit refresh, storage, SCD2 and training boundaries.
- [ ] 7.2 During later DP2 planning, define mart grain/keys, SCD2/as-of joins, accepted upstream versions, refresh freshness and late-data recovery; verify a fixed-query correctness/performance baseline before choosing materialization or native serving loads. This task is a planning handoff, not implementation of gold in DP1.

## Evidence status

Native foundation verified on prod on 2026-10-26: StarRocks 4.1.6 (platform commits `a3535cd`..`85944be`) reads the RustFS Iceberg REST catalog with native SigV4 and exact snapshot SQL; the official OpenMetadata 2.0.6 ingestion image discovers `starrocks.lakehouse.bronze.raw_*` (nested struct children are not extracted by the 2.0.6 connector; the full type text is kept and checked by generated SQL rules) and all 117 generated native rules returned Success against the v11 snapshots left by the earlier run. This is a manual trial, not native v11 acceptance. Obsolete DP1 OpenMetadata artifacts were deleted; lake data, source files and durable evidence were not touched.

The checked baseline tasks describe retained work, not native StarRocks readiness. Old v11 workflow `historical-dp1-bronze-rflqv` passed 117 GX checks and governance for 547,166 events/107,937 labels in 25m43s (validation 332.33s), under `dp1-evidence/reruns/v11-memory-cache-20261025/`. Preserve these original outcomes and durable records; they do not accept the replacement path. V12 is published but unsubmitted for this native design. No new engine, pipeline execution or catalog deletion occurs during this planning update.
