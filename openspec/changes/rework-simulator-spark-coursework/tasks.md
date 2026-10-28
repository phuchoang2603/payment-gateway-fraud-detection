## 1. Declared schemas and normalization

- [ ] 1.1 Define versioned physical schemas and direct per-version normalization to the output contract, with schema 3 unchanged and schema 4 adding assessment references; verify existing CI contract checks cover optionality, source-origin/hash semantics and generated references without Kafka framing changes.
- [ ] 1.2 Replace fixed version/pair assumptions in generator, manifest/inventory/path validation and decoded readers with declared definitions and an ordered effective-time schedule; verify focused CI behavior supports consecutive/nonconsecutive declared versions, rejects unknown/ambiguous schedules before writes and preserves each duplicate's original version.
- [ ] 1.3 Implement lossless mixed-version normalization and contract identity checks; verify CI behavior preserves new values, fills absent optional fields with null, distinguishes physical/envelope versions and rejects unsupported type/meaning changes without silent fallback.
- [ ] 1.4 Coordinate source/product/Bronze/landing contract versions and generated references, removing stale enumerations and assumptions; verify original v16 manifests, source files and historical receipts remain unchanged.

## 2. One generator configuration and new source

- [ ] 2.1 Extend the maintained development config with version scheduling and controlled duplicates alongside existing skew/entity controls; verify CI generation coverage protects deterministic identity, cross-version lifecycle completion and complete delayed labels without creating separate training profiles.
- [ ] 2.2 Add bounded source counters and Spark diagnostics for skew shares, approximate cardinality/error settings, evolved-field nulls/values by eligible payload/version/date and exact duplicate rates; verify outputs distinguish configured pools from observations and declare all denominators.
- [ ] 2.3 Record an unused dataset version, dates, seed, initial 3-to-4 schedule and resource budgets, then publish the immutable development source after required CI checks; verify manifest-last integrity, complete inventory/tails and both real schema shapes without fabricating a version-5 dataset or running full scale.
- [ ] 2.4 Capture actual generator configuration, schema definitions/schedule and diagnostic query/code/output with rows/files/bytes/format/compression/time ranges; verify captures and machine-readable results correlate to the selected manifest/hash and code revision.

## 3. DP1 reuse and Spark Silver

- [ ] 3.1 Exercise DP1 on the mixed-version source using normal CI-built images; verify independently pinned event/label snapshots, native raw checks and bidirectional duplicate-aware conservation before accepting raw outputs.
- [ ] 3.2 Implement Silver scoped grains/keys, canonical events, attempt histories, label observations and delivery provenance in the existing Spark package; verify focused CI behavior rejects content conflicts and retains exact delivery accounting while collapsing identical business duplicates.
- [ ] 3.3 Adapt Silver quality contracts for conditional schema, lifecycle/context/amount/linkage and complete versus recurring pending history; verify native results enforce label availability and keep truth/chargeback/decision meanings separate without recreating retired DP1 fault probes.
- [ ] 3.4 Add accepted-input progress, affected-entity correction, bounded backfill and candidate/accepted receipts; verify CI covers idempotent replay, independent labels and newly accepted older records without duplicate outputs or promotion of failed candidates.

## 4. Data pipeline integration and relevant evidence

- [ ] 4.1 Provide independent DP1 and DP2 Argo entrypoints with separate acceptance and direct-from-accepted-Bronze execution; verify CI workflow checks and deployed stage graph show readable ordering, bounded retries and the same graph for supported version schedules.
- [ ] 4.2 Reuse shared OpenMetadata connections/runtime/SDK for curated contracts, lineage and native SQL checks; verify readback pins actual accepted inputs/outputs and complete current rule results.
- [ ] 4.3 Preserve a correct unoptimized Spark baseline and measure targeted skew/high-cardinality optimizations using fixed input/resources; verify equal canonical content and retain plans, task durations, shuffle/spill/runtime and honest collection limits with code captures and Spark UI screenshots.
- [ ] 4.4 Capture Argo DP1/DP2 ingest/validate ordering and OpenMetadata lineage/validation/contracts alongside data diagnostics; verify one concise evidence index resolves actual run/application IDs, selected snapshots, code revisions and results. Exclude Docker, gold/warehouse tuning, Flink and other unrelated evidence tasks.

## 5. Domain documentation and cleanup

- [x] 5.1 Create `docs/data-ml/data-generator.md` covering implemented configuration, schema definitions/schedules, supported/breaking changes, seeds/entity continuity, immutable publication/resume, label tails and diagnostics; verify examples match implemented CLI/config and document how a declared later version or skipped intermediate version uses the same path.
- [x] 5.2 Move maintained generator instructions from `data-products.md` into the guide, retain dataset receipts/product descriptions, and minimally link from `data-pipeline.md` and existing navigation; verify links resolve and no instructions or generated field references are duplicated.
- [ ] 5.3 Remove superseded changed-area branches/references and redundant coverage, run local non-test lint/schema/diff checks and inspect required CI tests/builds; verify all changes stay within data scope and no local tests/builds or unrelated optimization work is performed.
- [ ] 5.4 Simplify the development writer to recoverable batch snapshot replacement, remove screenshot validation and its tests, and provide manual capture guidance; verify lint/spec checks locally and writer behavior in CI.

- [ ] 5.5 Use parametrized equivalence partitions and cutover boundary values, Hypothesis duplicate-replay idempotency, and CI-only mutmut on the changed `SchemaSchedule.version_at` function with a mutation score above 80%; retain the exact mutation scope and results. Remove the Silver test-only interruption hook and avoid new publication-failure simulations.
