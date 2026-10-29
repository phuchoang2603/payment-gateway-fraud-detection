## Context

See proposal.md for motivation. The rebased baseline is main `08c6ac0` (shared dev/prod platform migration). Existing generator and Silver implementation is retained, but previous publication, CI and runtime receipts do not establish acceptance in the new environment. Fresh development verification is pending.

Reference repositories were inspected read-only:

- `RecSys-MLops`, `https://github.com/itsmekhoathekid/RecSys-MLops.git`, revision `6bd6e99f481f2a8d5886d6f0e8356540dc20e135`: its generator drift controller uses baseline/post-drift dates and abrupt/gradual purchase multipliers; its DP2 separates transform/validate/publication. It does not require the three-profile abstraction previously suggested here. Its direct PostgreSQL source writer is not appropriate for our existing authoritative payment service. Do not copy arbitrary duplicate winners or full overwrites as incremental recovery.
- `card-fraud-platform`, `https://github.com/cvinhnguyen/card-fraud-platform.git`, revision `cc34515304815774a5dad6f796683b46c27f93ac`: `generator/generate_drift.py` reuses the same customers/cards/merchants for a later configured live period, separately emitting labels and nightly exports. Its fact/dimension and reconciliation patterns support our roadmap. Do not copy export-date-only progress, whole-input replay buffering or unreported-dispute negatives.

The coursework rows supplied in the conversation and `docs/overview/roadmap.md` require configured problems plus code, output/UI captures and before/after explanation. The instructor permits equivalent orchestration/catalog tools. The original project workbook is not available in this checkout; the inspected reference workbook is contextual evidence, not a replacement authority.

## Goals / Non-Goals

**Goals:** make one source useful for both offline-problem demonstration and subsequent model preparation; preserve raw evidence while making curated outputs safe and reproducible; collect the evidence while executions are still inspectable.

**Non-Goals:** a new simulator service, new source writer, generic experiment framework, mandatory separate training dataset, live bootstrap, Flink changes, feature computation/training, durable live label scheduling, drift detection, Docker optimization, gold marts or lake/warehouse tuning. No source writes, generation, tests, image builds or pipeline runs occur during planning.

## Decisions

### One generator, one new historical dataset

Extend the maintained 90-day development configuration with an explicit schema schedule and controlled duplicates (proposed default: 2% extra deliveries relative to total exported event deliveries; report the actual achieved rate), retaining the existing skew controls and entity pools. Do not inflate entities or bytes merely to hit a demonstration metric. Use merchant/category shares and supported geographic regions; do not invent a city field absent from the payment contract. Compute cardinalities downstream with Spark `approx_count_distinct` and record relative error and denominators.

Generate fresh environment-scoped v17 at `s3://platform-dev/datasets/payment-gateway/historical/v17/manifest.json`; do not copy the old source or reuse its digest. Publish one immutable manifest containing the selected schema versions and complete lifecycle/label tails. Split training chronologically later from accepted curated data; no dedicated training-profile machinery. A later drift schedule continues the same entity world and records changed dates/distributions outside model inputs. Actual drift generation/detection follows the first usable features/model rather than blocking this slice.

### Declared schemas and effective-time schedules

Separate three concerns: versioned physical schema definitions, the generation schedule, and normalization into the selected output contract. Keep the supported definitions and per-version projections in the existing contract/schema boundary, with the minimum code needed by generator and Spark readers. Do not introduce a registry service or a generic migration framework. Manifest/path validation derives allowed versions from the declared definitions rather than a hardcoded `{3,4}` list or per-transition branches.

Configuration declares an initial version followed by ordered effective-time version changes. Select using event occurrence time; reject unknown versions, duplicate/ambiguous boundaries and unsupported conversions before writing. A lifecycle may span versions. A duplicate keeps its original record/version. A schedule can select one version, multiple versions, or skip intermediate versions. Read each physical version through its own declared schema, then normalize directly to the target output shape and combine; do not require sequential 3 → 4 → 5 data rewrites.

Adding version 5 requires its actual schema definition, permitted differences, projection/normalization and contract identity. After that, 4 → 5 and 3 → 5 use the same generator/ingestion/workflow path; they need no special pairwise adapter or intermediate version-4 files. Additive optional fields normalize old absence to null. Only explicitly supported lossless type conversions are allowed. Removed required fields, incompatible types and changed meanings require an explicit contract decision/transformation; schema merging alone cannot make them safe. Do not fabricate a version-5 business field solely to demonstrate extensibility; verify scheduling/normalization with focused CI behavioral fixtures.

The first real dataset still demonstrates schema 3 → 4. Keep schema 3 physically unchanged. Schema 4 adds optional `risk_decision_recorded.assessment.assessment_id` and `evidence_sha256`; retain existing types, enriched context and lifecycle meanings. Label schema remains unchanged. For historical new-version decisions, define deterministic synthetic references to a versioned canonical synthetic evidence representation reconstructible from retained attempt inputs/assessment values. Specify hash serialization in the source contract before generation. This is synthetic evidence, not a live Fraud document or proof of an online served vector. Unavailable references remain null; include populated new-version references to prove value preservation without deploying another evidence service.

Update Arrow generation/decoding, manifest/path checks, source/product/Bronze contracts, landing schema and generated references together. Preserve the distinction between manifest/file physical schema and the original event envelope's `schema_version`; do not relabel service-produced bytes. Preserve original bytes/key/ordered headers/positions. This is a decoded business-schema alignment, not a Kafka framing/registry change. Coordinate the completed-landing interface with the separate Kafka Connect thread without changing its producer contracts or deployments.

Report nulls and preserved values by eligible payload, source version and date, distinguishing physical absence from unavailability/invalid new values. Leave remote sources and archived history untouched; clear stale results from this change and active guides. New ingestion resolves declared contract identities without unrestricted legacy fallback or rewriting prior acceptance.

### Persist Bronze, run independent orchestration

Use independent Argo entrypoints for existing DP1 and new DP2 stages. Keep reusable stage templates and separate raw/curated acceptance. DP2 can start from an accepted DP1 receipt without rerunning ingestion. Historical and completed landing inputs share the implementation and tables, with explicit source selection/progress.

DP2 pins both independent Bronze snapshots, accepted source filters and the accepted landing ledger version. Never select latest table state or use a maximum event/export date as input progress. A future landing implementation may change; the DP2 consumer boundary is the accepted DP1 receipt with source provenance and supported business schema, not archiver internals.

### Spark Silver curation

Extend the existing data-platform Python package and Spark image rather than creating a parallel Spark platform. Output canonical event and label-observation tables, attempt histories and delivery-to-canonical provenance. Keys include integration scope. For each event identity compare normalized business content separately from source/transport metadata before deduplication. Include schema identity when checking transport-equivalent copies; any cross-version representation of the same business identity needs explicit equivalence rules, not arbitrary ordering.

Record raw selected deliveries = canonical representative deliveries + identical duplicates + pending/rejected/conflicting deliveries under a mutually exclusive accounting policy. Keep distinct label observations; without an authoritative report ID use a documented hash of canonical observation content, collapsing exact copies only. Reconcile amount/count totals after joins to catch multiplication.

Adapt `contracts/quality/silver-payments.yaml` for scoped identity and complete historical versus recurring prefix context. Enforce lifecycle order, attempt linkage, conditional fields, amount consistency and label timing. Pending context is explicit and revisited when accepted inputs arrive. Missing chargebacks are unknown; simulation truth is a target only; no scenario, future outcome or decision covariates enter predecision features.

Use a reproducible full selected-source transformation at development size, over explicitly selected accepted Bronze receipts, including prior history when required. Publish complete table snapshots using batch replacement; defer optimized incremental writes until measurements justify them. Retain input identities, transformation revision, output snapshots and the previous accepted result; reuse existing commit identification without adding a recovery framework. Promote a product acceptance receipt only after complete data/governance checks; readers resolve that receipt rather than assuming multi-table atomic commits.

Compute the shared payment-context aggregate once for pending classification and attempt reconstruction. Defer affected-entity incremental recomputation, automatic accepted-base merging and bounded-backfill controls. Newly accepted older events and late labels are handled by recomputing the complete explicit selection.

Gold marts, SCD2 analytics and selected MVs remain subsequent roadmap work. This slice ends at accepted Silver, which DP3 can consume directly.

### Measured optimization, not configuration theatre

Run benchmarks separately from normal Silver acceptance. Target an observed merchant/category aggregation or relevant high-cardinality operation; repartitioning event IDs alone is not evidence of resolving merchant/category skew. Preserve baseline code/configuration at an immutable revision. Both baseline and optimized runs must normalize schemas and deduplicate correctly. Change one measured factor per step, keep fixed inputs and resource budgets, disclose cache/warmup and repeat conditions. Inspect Spark UI/physical plans to choose targeted optimization: hot-key aggregation partials/salting only when supported by imbalance, broadcast only for bounded small dimensions, partition/shuffle strategy for high-cardinality aggregation/dedup. Capture task duration spread, shuffle, spill, runtime and available memory; label missing peak/RSS measurements honestly. Compare output content bidirectionally including multiplicity where relevant, not just row counts.

Full 100 GB generation/acceptance remains after the whole pipeline. No container-size, lake-maintenance or warehouse-tuning experiment is required in this change.

### Evidence collected alongside each stage

Keep source configuration, distributions, cardinality, schema null/value results and duplicate plots in the existing generator notebook. Retain useful validation results and run identities in the pipelines without an evidence framework. Keep selected screenshots and explanations in `docs/data-ml/verification.md`, with operational receipts remaining in object storage rather than copied into OpenSpec. Screenshots support the coursework report; do not implement screenshot validation, upload protocols or tests. Captions link actual output to the code revision, manifest/hash, run/application IDs and selected snapshots. Keep useful data diagnostics and Spark event logs. Capture real UI/code and redact credentials; missing captures remain pending without blocking data acceptance.

| Requirement | Capture and retained evidence | Owner / timing |
| --- | --- | --- |
| Generator configuration and data characteristics | Config code, rows/files/bytes/format/compression/date summary output and manifest | This slice, new source |
| Skew | Merchant/category or supported region percentages and query/code | This slice, diagnostics |
| Cardinality | `approx_count_distinct` by scoped ID, error settings, pool sizes separately | This slice, diagnostics |
| Configured schema evolution (first dataset: 3 → 4) | Definitions/schedule/normalization code; eligible-field null/value results by date/version; mixed-read plan | This slice, DP1/DP2 |
| Duplicates | Injection/dedup code; exact before/after rates, conflicts and accounting output | This slice, DP2 |
| Spark optimization | Baseline and each step's code, Spark SQL/stage/task UI, metrics and equal-output comparison | This slice, DP2 |
| DP1 and DP2 stages | Argo UI showing ingest/validate ordering and run outcomes; workflow code | This slice, fresh dev runs |
| DP1 and DP2 governance | OpenMetadata actual lineage, validation results and native contracts; publisher/contract code | This slice, fresh dev runs |

The instructor accepts Argo/OpenMetadata as equivalent tools. Do not install Airflow/DataHub to match screenshots. Do not infer a waiver of streaming requirements from that equivalence. Preserve genuine historical failures; do not recreate retired DP1 bad-data/SQL/outage exercises.

### Documentation in the data domain

Create `docs/data-ml/data-generator.md` as the maintained guide for the historical generator and its live continuation boundary. Move the maintained generation/configuration/layout/resume instructions from `data-products.md` into that guide, replacing them with a concise link; retain current product summaries in `data-products.md`, removing stale publication receipts. Keep orchestration/acceptance operations in `data-pipeline.md`, linking the guide for source configuration. Update the existing docs navigation where needed. Do not add source-directory READMEs or duplicate generated field references.

The guide covers config examples and denominators, seeds/entity continuity, schema definitions and schedules (including nonconsecutive supported versions), physical versus envelope versions, supported versus breaking changes, immutable publication/resume, complete label tails and diagnostics/evidence queries. Minimal affected sections of other domain docs are updated when behavior changes. Produce these docs during implementation alongside the behavior they describe; planning does not describe unimplemented commands as available.

## Risks / Trade-offs

- Unknown versions could silently lose fields → require explicit definitions, contract identities and lossless normalization; reject unsupported inputs before writes.
- Pairwise adapters grow quadratically → normalize each declared source version directly to the output contract; schedule and orchestration do not encode version pairs.
- Small inputs may not expose shuffle/skew bottlenecks → retain measured results honestly and preserve the baseline for later scale.
- Synthetic evidence could be mistaken for live served vectors → document origin/hash semantics and keep actual online evidence export separate.
- Future landing replacement → depend on accepted DP1 receipts/business schema and retain transport provenance without archiver-specific logic.

## Migration Plan

1. Adapt existing schema/contracts and workflows to main’s environment storage, Polaris credentials and release image identities; check focused behavior in CI.
2. Build normal runtime images in existing CI, without image-size optimization. Execute generation and deployed development acceptance through CI-controlled jobs; no local tests/builds or full-scale generation.
3. Publish fresh dev v17 with the initial 3 → 4 schedule and accept DP1 raw snapshots before DP2; capture diagnostics and schema evidence.
4. Exercise Spark Silver baseline and measured optimizations against pinned inputs; collect relevant code/UI captures and native governance readback.
5. Promote only complete accepted outputs; retain prior accepted pointers and candidate diagnostics on failure. Publish the generator guide and minimal domain cross-links with the implemented behavior.

## Open Questions

- Record the fresh v17 manifest digest, approximation error and bounded benchmark resources before execution; these do not change the architecture or acceptance obligations.
- Actual version-5 business requirements are future work. Supporting a declared version schedule does not pre-approve unknown fields or breaking semantics.

### Environment and deployment boundary

Trusted PR releases deploy to dev through `deploy/dev`; merged main releases use `deploy/prod`. CI builds images and writes deployment branches without contacting Kubernetes. Per-service immutable commit tags resolve through `global.imageTags` in the release configuration; Spark and metadata images may have different source revisions. Update workflow image helpers and run identity validation to that model, rather than requiring equal revisions or digest-only references.

Dev uses MinIO bucket `platform-dev`; prod uses AWS bucket `platform-prod-142737101504-us-east-1`. Sources live under `datasets/payment-gateway/`, receipts and Spark event logs under `runs/payment-gateway/`, and managed Iceberg under `lakehouse/`. Workloads run in namespace `data`. Use environment-selected S3 endpoint, region and path style, `gateway-storage`, and `gateway-catalog-client`. Polaris REST is `http://iceberg-catalog.data.svc.cluster.local:8181/api/catalog`, warehouse `lakehouse`, OAuth client `platform`, token path `/api/catalog/v1/oauth/tokens`. The `lakehouse-integration` chart owns application storage/catalog registrations; the platform owns servers and operators.

Keep schedules disabled until fresh source/catalog verification. This change accepts generator → Bronze → Silver in dev only; prod remains service scope until the whole pipeline and full-scale dataset are ready. Do not migrate old offsets, checkpoints or feature epochs as part of this reset. Configure the actual fresh manifest digest after publication. The generator notebook starts with cleared outputs and a dev-scoped cache; it cannot serve as evidence until re-executed against that digest.

### Config-only execution

Python data-service runtime settings come from their existing configuration channels: simulator YAML with main’s optional `SIMULATOR_CONFIG` path override and dedicated historical/live entrypoints, and environment configuration for pipeline stages and Feast. Entry points do not accept CLI overrides. Helm supplies these values with the matching CI-built images. Python mutation selection and scoring live in a reusable repository script; each project declares its scope in `pyproject.toml`, and CI evaluates changed functions only. Shared batch code lives in the internal `fraud_batch.common` package and is covered through Bronze consumer tests. Bronze and Silver share one batch project, lockfile, runtime environment and test fixtures while retaining separate workflows.

### Course example review

Read-only FSDS examples at `/home/felix/repos/fsds` informed the simplification: `data-lake-with-minio/utils/generate_fake_data.py` uses configured Parquet generation; `lake-house-with-minio/utils/schema_evolution.py` demonstrates old/new shapes; `batch-processing-with-pyspark/scripts/spark_sql_from_df.py` uses direct transformations; `data-validation/notebooks/full_flow.ipynb` exposes diagnostic results; and `pipeline-orchestration-with-airflow/run_env/dags/demo.py` declares explicit task dependencies. Adopt that direct structure while retaining our scoped identities, conflict handling, exact conservation and temporal semantics. These examples do not supply measured optimization evidence. The Flink example uses no watermarks and does not establish late-arrival acceptance.
