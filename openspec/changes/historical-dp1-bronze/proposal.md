# Proposal

## Why

DP1 preserves raw deliveries, but its catalog integration is incomplete: contracts mainly contain embedded YAML, the pipeline has no task definitions and still exposes an old execution, and lineage incorrectly makes events an input to labels. The Argo graph expands individual API operations while profiling exposes only row and column counts. Rewrite this integration and start with clean DP1-owned state.

## What Changes

- Retain Spark ingestion through Apache Spark Operator, immutable source verification, duplicate-aware source/output conservation, independent Iceberg snapshots and recoverable historical/recurring commits. DP1 remains raw preservation; business consistency, lifecycle, linkage and as-of validation belong to DP2 silver.
- Keep platform-owned StarRocks, the existing RustFS Iceberg REST catalog, native SigV4 and separate storage credentials. Shared infrastructure and simulator-owned sources remain outside the DP1 reset.
- **BREAKING:** Replace the HTTP-heavy governance implementation with one thin metadata image based on the exact server-matched official OpenMetadata ingestion image. Its official SDK maps Git contracts/products, ownership, lineage and pipeline executions; native workflows still own table discovery, profiling, SQL tests and results. No custom discovery, custom result projection, GX or parallel publisher.
- Publish inspectable native contract schema and active test references, declared ownership, semantics and applicable security/SLA fields. Preserve original ODCS identities, versions, hashes and YAML as provenance. Model inputs/outputs on products and executions rather than inventing contract fields.
- Represent actual historical/clean-landing source assets and independent event/label output paths. Publish current pipeline tasks, run status, execution inputs/outputs, counts, snapshots and Argo links. Verify both product membership and entity-side relationships.
- Express a compact Argo DAG: select, ingest, discover, parallel validate/profile, publish/readback/accept. Keep writer serialization, bounded retries, metadata-only recovery, no-input skipping and verified cleanup before writer handoff.
- Enable meaningful bounded scalar-column profiling for v16 development and a declared bounded recurring cadence. Record selection, sampling and time; latest-state profiles are descriptive and cannot prove snapshot-specific acceptance.
- **BREAKING:** Remove superseded DP1 governance code/config/tests and old DP1 catalog registrations, test cases/results, bronze data and run/progress/acceptance evidence within an explicitly inventoried DP1 cleanup boundary. Recreate current artifacts without migration, aliases, compatibility branches or importing old acceptance. Pause writers and verify query/Spark cleanup before deleting state. Preserve immutable v16, simulator historical/landing sources, producer offsets and unrelated shared assets.
- Reuse existing independent CI checks and the existing image workflow. Add only distinct behavioral coverage; no configuration-only suites, matrices or redundant CI gates. Tests and builds run in CI only.
- Establish fresh v16 historical and recurring development acceptance after the rewrite; previous checks do not accept the missing metadata integration. Deliberate failure/outage exercises remain excluded. Scale/coursework evidence follows completion of the whole pipeline.

## Capabilities

### New Capabilities

- `historical-bronze-ingestion`: Recoverable raw historical/recurring ingestion, snapshot-pinned acceptance and scoped fresh-start semantics.
- `data-pipeline-orchestration`: Readable Argo dependencies, finite native metadata stages and safe writer lifecycle.
- `data-catalog-governance`: Complete native contracts, products, ownership, execution metadata, source lineage and meaningful profiles.

### Modified Capabilities

- `dataset-contracts`: Git-authoritative schema and SQL rules mapped into inspectable native contract fields.
- `talos-gitops-deployment`: Platform/application ownership, generic secret references and immutable metadata runtime.
- `focused-verification`: Behavioral CI and fresh catalog/readback acceptance without redundant configuration checks.

## Impact

Affected implementation is the bronze ingestion governance/readback code, metadata runtime/image wiring, data-pipelines chart, existing behavioral tests and affected pipeline documentation. Platform infrastructure remains owned by `talos-proxmox`. This change implements no generator, simulator, silver transformations or gold marts.

Use `s3://synthetic-source/historical/v16/manifest.json`, byte SHA-256 `719815e3f0a90e27bbc8303feaacdddb4858c604325656982fd443e49b05882e`: 1,954 Parquet files, 139,597,476 physical bytes, 795,718 events and 161,967 labels, including complete tails. Current identities are event schema 3, source events ODCS 4.0.1, labels 1.0.0 and product 6.0.1; `fraud-baseline-v1` SHA-256 is `2ee2a5153a248bdc98df3b2643e10a65bc5693ea111129b6c65a67352aa4ec0b`. Never mutate or regenerate this source. V16 is development evidence, not the later 100,000,000,000-byte coursework gate. Gold star-schema/MV roadmap work remains later DP2 scope.
