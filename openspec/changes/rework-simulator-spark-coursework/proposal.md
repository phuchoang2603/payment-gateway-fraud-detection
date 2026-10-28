## Why

The generator and accepted DP1 preserve raw payment histories, but the current schema-3-only path cannot demonstrate physical schema evolution, Spark curation or measured offline optimization. Align the contracts, publish one reproducible problem-bearing development dataset and build Spark DP2 with reviewable coursework evidence before further Flink work.

## What Changes

- Keep one historical generator and the existing live gateway adapter, sharing entity identities and business contracts. Do not introduce separate training/problem/drift profiles or a direct operational-database writer.
- Configure and measure merchant/category skew, identity cardinality, identical duplicate exports and a configurable physical-schema version schedule (initial demonstration: event schema 3 to 4) within one new immutable historical dataset. Preserve complete lifecycle and delayed-label tails; publish fresh dev v17 under the environment bucket prefix.
- **BREAKING**: advance affected source/product/physical/Bronze contracts together, including assessment evidence references. Replace schema-3-only assumptions with version declarations and normalization rules shared by generation and ingestion, without transition-specific branches; unsupported shapes fail rather than silently losing fields.
- Reuse DP1 and implement Spark Silver curation with conflict-aware deduplication, delivery accounting, lifecycle/linkage/context checks and label-availability semantics.
- Run DP1 and DP2 as independent Argo workflows, with persisted Bronze and separate acceptance boundaries.
- Preserve reproducible unoptimized/optimized Spark executions for the generated offline problems. Require actual diagnostic output, Spark UI, code, Argo and OpenMetadata captures with immutable references.
- Document generator configuration, schema schedules, source layout and diagnostic interpretation in `docs/data-ml/data-generator.md`; move relevant maintained instructions from existing data docs and link them without duplicating generated contract references. Preserve the source boundary for later drift continuation, without implementing drift detection.

## Capabilities

### New Capabilities

- `curated-payments`: Spark Silver curation, reproducible offline optimization and evidence-backed acceptance.

### Modified Capabilities

- `historical-data-generation`: one coursework development configuration with measured offline problems, declared version schedules and domain-owned generator documentation.
- `dataset-contracts`: assessment reference fidelity, supported schema semantics and reproducible diagnostic/evidence requirements.
- `historical-bronze-ingestion`: bounded supported-schema/contract selection for the new source while preserving raw multiplicity.
- `data-pipeline-orchestration`: composable DP1/DP2 stages with distinct accepted outputs and readable stage dependencies.

## Impact

Affected implementation areas are `src/data-pipeline/simulator`, `src/data-pipeline/batch`, `contracts/data-products`, `contracts/landing`, `contracts/quality/silver-payments.yaml`, `infra/charts/data-pipelines`, and domain documentation under `docs/data-ml/`. Reuse environment-selected MinIO/AWS S3 and Polaris Iceberg REST, Apache Spark Operator, Argo, StarRocks, and shared OpenMetadata 2.0.6 runtime/SDK integration; add no orchestrator, catalog or feature engine.

Existing implementation is retained after rebasing onto main `59bc194`; deployment integration and fresh development verification remain pending. Generate a fresh source at `s3://platform-dev/datasets/payment-gateway/historical/v17/manifest.json`, then accept separate DP1 and DP2 runs in dev. Prod pipeline acceptance follows completion of the whole pipeline and full-scale generation. Superseded execution evidence is removed from this change and active guides; archived history remains historical. Docker/image-size optimization, lake/warehouse tuning, gold marts, Flink/Feast changes, HybridSource, Kafka Connect/registry rework, full-scale acceptance, model training and drift detection are outside implementation scope. The instructor permits equivalent tools: use Argo for orchestration screenshots and OpenMetadata for lineage, validation and contract screenshots. Streaming experiments remain deferred, without claiming an exemption.
