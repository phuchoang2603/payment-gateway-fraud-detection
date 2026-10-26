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
- [ ] 2.4 Measure combined Spark, StarRocks and OpenMetadata scheduling/memory/storage; record bounded resource and deadline settings before scale submission.

## 3. Native contracts, SQL and acceptance

- [x] 3.1 Rewrite bronze ODCS execution rules as StarRocks SQL/native OpenMetadata tests with stable semantic IDs and complete nested schema coverage; verify contract lint and focused valid/invalid cases through existing CI.
- [x] 3.2 Implement snapshot-specific COUNT-of-violations SQL for schema, duplicate consistency, lifecycle, amounts, label linkage/availability and context; verify pinned event/label snapshots remain isolated from later commits.
- [x] 3.3 Preserve native per-day counts and add bidirectional duplicate-aware persisted/source reconciliation inside ingestion; require matching manifest/snapshot evidence before acceptance.
- [ ] 3.4 Pin the current native gate, source contracts/product and feature-definition hashes in fresh v16 evidence; reject legacy schemas/gates and keep distribution bands diagnostic.
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
- [ ] 5.3 Verify deliberate SQL failure and metadata outage/recovery on isolated candidates; demonstrate failure visibility, unchanged previous acceptance and metadata-only replay.
- [ ] 5.4 Adapt current schema-3 bronze contracts/runtime and run published v16 development acceptance now: manifest `719815e3f0a90e27bbc8303feaacdddb4858c604325656982fd443e49b05882e`, all 1,954 files / 139,597,476 bytes / 795,718 events / 161,967 labels including complete tails. Verify context, conservation, native SQL, canonical discovery and governance, then replay/recovery; source checks alone cannot complete this task.
- [ ] 5.5 Record v16 persisted-bronze/governance evidence separately from source/live and historical GX/native evidence; update affected pipeline/product guidance with pinned contracts/features/snapshots and measured limits.
- [ ] 5.6 Later, after the end-to-end development path is exercised and an immutable current-contract full source is published, measure >=100,000,000,000 physical Parquet bytes and separate scale/coursework pipeline acceptance. Do not generate or wait for scale input during v16 implementation.

## 6. Recurring completed-file ingestion (deferred)

Producer source/live smoke and landing/recovery prerequisites are recorded in the archived v16 change, including the clean epoch `v16-20261026` and group `payments-parquet-landing-v16-20261026`. Review those exact receipts before recurring DP1 selection. Complete historical v16 persisted-bronze acceptance first; it does not establish recurring readiness. Do not reset producer offsets, combine faulty deliveries as independent business events, duplicate simulator work or assume in-memory delayed labels survive shutdown.

- [ ] 6.1 Agree immutable landing completion/inventory and durable progress interfaces with the live-streaming change; verify late files in older date folders are discoverable without an event-date cutoff.
- [ ] 6.2 Implement common input selection, independent event/label batches and content-preserving incremental commits with pinned changed/unchanged ports; verify no-input, changed-content rejection and uncertain-commit recovery using existing behavioral CI coverage.
- [ ] 6.3 Apply contextual native SQL checks with pinned history and label maturity, retaining origin/offset/batch provenance and informational generator diagnostics; verify incomplete context never becomes an invented pass or a requirement for same-batch labels.
- [ ] 6.4 Add opt-in CronWorkflow/progress/landing permissions and demonstrate repeated live batches plus metadata-only recovery with exact accounting; historical acceptance does not establish recurring readiness.

## 7. Analytical roadmap alignment

- [x] 7.1 Update the existing roadmap to select StarRocks for analytics/gold fact-dimension marts and automated materialized views, replacing planned Trino/analytical ClickHouse; review affected sections for explicit refresh, storage, SCD2 and training boundaries.
- [ ] 7.2 During later DP2 planning, define mart grain/keys, SCD2/as-of joins, accepted upstream versions, refresh freshness and late-data recovery; verify a fixed-query correctness/performance baseline before choosing materialization or native serving loads. This task is a planning handoff, not implementation of gold in DP1.

## Fresh-start acceptance status

Code validation passed on `41ad675`: Python behavioral tests, contract checks, Helm checks and the Spark image build succeeded in the existing CI jobs. Deployment and live testing are deferred at the user’s request; this does not establish v16 persisted-bronze acceptance.

V16 is the only current development selector. Remove legacy DP1 tables/catalog registrations/run-evidence state before fresh ingestion; do not migrate old schemas, import earlier acceptance records or relabel failed attempts. The immutable v16 source and unrelated shared services/assets remain unchanged. Source/live smoke evidence is upstream only. Persisted-bronze, native quality, replay/recovery and governance acceptance remain pending until executed by this change. Full-scale/coursework acceptance follows later and cannot be claimed from v16.
