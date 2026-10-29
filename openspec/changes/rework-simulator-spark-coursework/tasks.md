All tasks are verified against the fresh dev baseline. Previous-environment results are not carried forward as acceptance. Tests and image builds run in CI only.

Implementation update: fresh dev v17 generation/notebook, independent Bronze and Silver acceptance, and the standalone equivalent-output benchmark are complete. Screenshots and explanations are in [development verification](../../../docs/data-ml/verification.md); operational receipts remain in object storage. Prod/full-scale acceptance remains deferred.

## 1. Declared schemas and normalization

- [x] 1.1 Define versioned physical schemas and direct per-version normalization to the output contract, with schema 3 unchanged and schema 4 adding assessment references; verify existing CI contract checks cover optionality, source-origin/hash semantics and generated references without Kafka framing changes.
- [x] 1.2 Replace fixed version/pair assumptions in generator, manifest/inventory/path validation and decoded readers with declared definitions and an ordered effective-time schedule; verify focused CI behavior supports consecutive/nonconsecutive declared versions, rejects unknown/ambiguous schedules before writes and preserves each duplicate's original version.
- [x] 1.3 Implement lossless mixed-version normalization and contract identity checks; verify CI behavior preserves new values, fills absent optional fields with null, distinguishes physical/envelope versions and rejects unsupported type/meaning changes without silent fallback.
- [x] 1.4 Coordinate source/product/Bronze/landing contract versions and generated references, removing stale enumerations and assumptions; verify environment-scoped identities and leave remote sources and archived history untouched.

## 2. One generator configuration and new source

- [x] 2.1 Extend the maintained development config with version scheduling and controlled duplicates alongside existing skew/entity controls; verify CI generation coverage protects deterministic identity, cross-version lifecycle completion and complete delayed labels without creating separate training profiles.
- [x] 2.2 Add bounded source counters and Spark diagnostics for skew shares, approximate cardinality/error settings, evolved-field nulls/values by eligible payload/version/date and exact duplicate rates; verify outputs distinguish configured pools from observations and declare all denominators.
- [x] 2.3 Configure fresh dev v17 at `s3://platform-dev/datasets/payment-gateway/historical/v17`, dates, seed, initial 3-to-4 schedule and resource budgets, then publish the immutable development source after required CI checks; verify manifest-last integrity, complete inventory/tails and both real schema shapes without fabricating a version-5 dataset or running full scale.
- [x] 2.4 Use the existing notebook to capture actual generator configuration, schema definitions/schedule and diagnostic query/code/output with rows/files/bytes/format/compression/time ranges; verify captures and machine-readable results correlate to the selected manifest/hash and code revision.

## 3. DP1 reuse and Spark Silver

- [x] 3.1 Exercise DP1 on the mixed-version source using normal CI-built images; verify independently pinned event/label snapshots, native raw checks and bidirectional duplicate-aware conservation before accepting raw outputs.
- [x] 3.2 Implement Silver scoped grains/keys, canonical events, attempt histories, label observations and delivery provenance in the existing Spark package; verify focused CI behavior rejects content conflicts and retains exact delivery accounting while collapsing identical business duplicates.
- [x] 3.3 Adapt Silver quality contracts for conditional schema, lifecycle/context/amount/linkage and complete versus recurring pending history; verify native results enforce label availability and keep truth/chargeback/decision meanings separate without recreating retired DP1 fault probes.
- [x] 3.4 Replace affected-entity incremental processing with full-batch recomputation over explicitly selected accepted Bronze receipts; remove automatic accepted-base merging and bounded-backfill controls and their redundant tests. Retain simple input/output identities and separate acceptance; verify idempotent replay, independent late labels and newly accepted older records in focused CI checks.
- [x] 3.5 Reuse one payment-context aggregation for pending classification and attempt histories; preserve scoped joins, conflict handling, exact delivery accounting and temporal rules.

## 4. Data pipeline integration and relevant evidence

- [x] 4.1 Provide thin independent DP1 and DP2 Argo entrypoints with separate acceptance and direct-from-accepted-Bronze execution; verify CI workflow checks and deployed stage graph show readable ordering, bounded retries and the same graph for supported version schedules.
- [x] 4.2 Reuse shared OpenMetadata connections/runtime/SDK for curated contracts, lineage and native SQL checks; verify readback pins actual accepted inputs/outputs and complete current rule results.
- [x] 4.3 Move the optional benchmark out of normal Silver acceptance; preserve a correct unoptimized baseline and measure an observed skewed aggregation or relevant high-cardinality operation using fixed input/resources; verify equal canonical content and retain plans, task durations, shuffle/spill/runtime and honest collection limits with code captures and Spark UI screenshots.
- [x] 4.4 Capture Argo DP1/DP2 ingest/validate ordering and OpenMetadata lineage/validation/contracts alongside data diagnostics; verify the domain verification page resolves actual run/application IDs, selected snapshots, code revisions and results. Exclude Docker, gold/warehouse tuning, Flink and other unrelated evidence tasks.

## 5. Domain documentation and cleanup

- [x] 5.1 Create `docs/data-ml/data-generator.md` covering implemented configuration, schema definitions/schedules, supported/breaking changes, seeds/entity continuity, immutable publication/resume, label tails and diagnostics; verify examples match implemented YAML/config and document how a declared later version or skipped intermediate version uses the same path.
- [x] 5.2 Move maintained generator instructions from `data-products.md` into the guide, remove stale publication receipts and retain current product descriptions, and minimally link from `data-pipeline.md` and existing navigation; verify links resolve and no instructions or generated field references are duplicated.
- [x] 5.3 Remove superseded changed-area branches/references and redundant coverage, run local non-test lint/schema/diff checks and inspect required CI tests/builds; verify all changes stay within data scope and no local tests/builds or unrelated optimization work is performed.
- [x] 5.4 Simplify the development writer to recoverable batch snapshot replacement, remove screenshot validation and its tests, and retain screenshots and explanations in the domain verification page; verify lint/spec checks locally and writer behavior in CI.

- [x] 5.5 Use parametrized equivalence partitions and cutover boundary values, Hypothesis duplicate-replay idempotency, and CI-only mutmut on changed functions within each project’s declared scope with a mutation score above 80%; retain the exact mutation scope and results. Remove the Silver test-only interruption hook and avoid new publication-failure simulations.

## 6. Config-only service execution

- [x] 6.1 Move mutation selection/scoring/reporting to one reusable script driven by project configuration and changed functions; remove the helper-only test suite and its test dependencies, retaining useful boundary checks in Bronze.
- [x] 6.2 Remove runtime CLI overrides and mode arguments from simulator, Bronze/Silver, metadata and Feast entrypoints while preserving main’s optional `SIMULATOR_CONFIG` override. Supply runtime settings through YAML/environment configuration, update deployed manifests and affected tests/docs together, and verify CI without changing Kafka contracts or rerunning sources.

## 7. One batch project

- [x] 7.1 Consolidate Bronze, Silver and shared modules into one `batch` project and lockfile; retain separate pipeline entrypoints/workflows, share test fixtures and the Spark image environment, and update imports, build inputs, workflow paths and domain references. Verify static checks locally and tests/builds in CI; leave Go services and the other data workloads unchanged.

## 8. Shared-platform integration and fresh dev acceptance

- [x] 8.1 Adapt Silver and Spark utility templates to the current image helper and `global.imageTags`; remove equal-revision assumptions and reconcile Bronze/Silver run and benchmark identities with immutable release tags.
- [x] 8.2 Replace legacy Silver storage secrets with `gateway-storage` and `gateway-catalog-client`, verify Polaris auth and environment-selected S3 settings, and replace stale governance namespace defaults with `data`.
- [x] 8.3 Configure environment-specific DP2 evidence roots under `runs/payment-gateway/dp2` without prod writes to dev; keep schedules disabled pending source/catalog verification.
- [x] 8.4 After CI succeeds, generate fresh dev v17, configure its actual digest, execute the cleared notebook, and accept independent Bronze and Silver runs. Record only new outputs and actual captures; defer prod/full-scale acceptance.
