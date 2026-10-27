## 1. Retained ingestion and platform baseline

These checked items describe retained implementation, not acceptance of the rewritten integration or survival of old DP1 records after reset.

- [x] 1.1 Retain bounded immutable inventory verification, current-schema normalization and duplicate-preserving Iceberg writes; existing behavioral CI and prior v16 observations establish the implementation baseline.
- [x] 1.2 Retain persisted input identity, uncertain-commit recovery, independent snapshots and exact source/output reconciliation; existing behavioral coverage exercises replay and conservation.
- [x] 1.3 Retain platform-owned Spark Operator, StarRocks native RustFS access and shared authenticated OpenMetadata; deployed ownership and native connection behavior have been verified.
- [x] 1.4 Retain writer serialization, native Workflow deletion/termination guard, Spark cleanup and bounded Argo retry authority; existing lifecycle coverage verifies the baseline.
- [x] 1.5 Retain six bundled Git-derived raw assertions and the DP1/DP2 boundary; existing CI verifies native schema constraints without requiring complete lifecycles or same-batch labels.
- [x] 1.6 Retain completed clean-landing selection, immutable pending/accepted ledgers, independent delayed ports and no-input skipping; existing CI and prior recurring observations verify the baseline.
- [x] 1.7 Retain the StarRocks gold/MV roadmap and silver SQL handoff; review confirms grain/keys/SCD2/as-of/refresh/storage remain later DP2 work.

## 2. Metadata runtime rewrite

- [x] 2.1 Build a thin server-matched official OpenMetadata ingestion image with the official SDK, isolated from Spark/JVM imports, with shared SDK/runner plumbing and product-specific publication mapping; verify its native workflow and synchronization entrypoints through the existing CI/image workflow.
- [x] 2.2 Replace HTTP request-generation/templates with one idempotent SDK synchronization path; remove obsolete publisher glue, unused RBAC/config/dependencies and tests, and verify no parallel publisher or custom discovery/results remains.
- [x] 2.3 Map native nested contract schema, active raw test references, declared terms/semantics/security/SLA fields and resolvable owners while retaining Git identity/hash/YAML; existing behavioral CI must reject unsupported required mapping or unresolved owners.
- [ ] 2.4 Synchronize product/domain/assets and entity-side membership, owners and pipeline membership; verify complete forward/reverse API readback without treating a UI drawer as proof of API absence.
- [ ] 2.5 Resolve actual historical/clean-landing storage assets and publish independent source-to-pipeline-to-output lineage; behavioral coverage must reject fabricated events-to-labels ingestion dependencies.
- [ ] 2.6 Publish real pipeline tasks/dependencies, current execution/task I/O/status, counts/snapshots and Argo links; verify idempotent metadata-only replay preserves execution identity and original native results.

## 3. Argo and profiling

- [x] 3.1 Replace nested request-level workflow wrappers with a readable stage DAG with isolated parallel historical validation/profiling and recurring profiling skipped; verify rendered dependencies, replay/no-input short circuits and existing lifecycle behavior in CI.
- [ ] 3.2 Enable meaningful supported scalar-column profiling with bounded sample/row scope, excluded binary fields, timeout/resources and on-demand execution outside the daily ingestion path; verify profile coverage and truthful latest-state versus snapshot labeling.
- [x] 3.3 Identify/bound run-owned StarRocks queries and verify quiescence along with Spark cleanup before writer handoff; behavioral CI must keep unresolved cleanup blocking the next writer.
- [ ] 3.4 Strengthen readback for native contract fields, owners, both product directions, source edges, current execution I/O/status and declared profile coverage; stale/incomplete metadata must refuse acceptance despite successful native process exit.

## 4. Scoped fresh start

- [x] 4.1 Confirm the requested deletion boundary and inventory exact DP1 catalog IDs, warehouse/table locations, evidence/progress prefixes and repository receipts; verify immutable v16, simulator sources, Kafka offsets and unrelated assets are excluded.
- [x] 4.2 Suspend DP1 scheduling, stop active writers gracefully and verify Spark/query quiescence under writer serialization before deletion; preserve shared services and their storage.
- [x] 4.3 Remove inventoried obsolete DP1 metadata, old suites/cases/results, incorrect lineage/stale executions, bronze data and run/accepted/progress records plus superseded repository receipts/references; verify scoped absence without deleting sources or shared assets.
- [ ] 4.4 Recreate canonical current artifacts with native discovery and the SDK path using a fresh explicit acceptance policy/namespace; verify no imported accepted state, compatibility aliases or legacy replay path.

## 5. Fresh development acceptance

- [x] 5.1 Adapt existing behavioral CI for the distinct rewrite risks and inspect CI-built immutable images/check results; remove obsolete coverage without adding configuration-only suites, matrices or independent check jobs.
- [ ] 5.2 Run fresh historical v16 acceptance using manifest SHA-256 `719815e3f0a90e27bbc8303feaacdddb4858c604325656982fd443e49b05882e`, all 1,954 files / 139,597,476 bytes / 795,718 events / 161,967 labels including full tails; verify exact conservation, native raw tests and complete new metadata readback.
- [ ] 5.3 Run fresh bounded recurring clean-landing acceptance, repeated/event-only inputs, accepted replay, metadata-only recovery and no-input behavior; verify exact delivery/transport accounting and unchanged producer offsets/source objects.
- [ ] 5.4 Inspect Argo stage graph and OpenMetadata native contract, ownership/product, lineage, execution I/O and meaningful profile views; record API/UI discrepancies rather than claiming unverified integration.
- [ ] 5.5 Record fresh receipts and minimal affected guidance with revisions/images/contracts/snapshots, profile scope and measured resource limits; enable the declared daily schedule only after fresh readiness and ensure superseded DP1 records are not active acceptance.

## Acceptance boundaries

Previous v16 data checks and raw recurring checks do not accept this rewrite: the inspected catalog lacked native contract fields and current pipeline tasks/execution metadata. Fresh metadata and overall acceptance are pending. The authorized scoped reset is complete: both bronze tables, old DP1 catalog entities, 234 lake/evidence object versions and seven terminal workflow resources were removed. Empty table/entity/prefix readback and the unchanged v16 manifest hash were verified. The daily CronWorkflow is suspended. Fresh run `historical-dp1-bronze-dcdpv` verified source files and committed both snapshots, then failed exact reconciliation on RustFS DNS resolution; native metadata stages were not reached. Graceful stop verified Spark and query cleanup. See `evidence/v16-catalog-v4-attempt1.json`. Recreating metadata assets and fresh acceptance remain pending; cleanup and snapshot commits do not accept the rewrite.

V16 remains the immutable 90-day development source, not scale/coursework evidence. Preserve its source contracts/product/feature hashes, manifest inventory and complete event/label tails. Source/live acceptance in the archived simulator change remains separately owned upstream evidence. DP2 business/as-of/features/training/serving and later >=100,000,000,000-byte acceptance remain separate. No generator, producer reset or deliberate live failure/outage exercise is requested. Tests and image builds run only in CI; local checks are lint/format/schema/OpenSpec validation and diff review.
