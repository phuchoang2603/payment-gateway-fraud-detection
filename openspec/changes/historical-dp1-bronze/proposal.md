# Proposal

## Why

A small historical export can exercise DP1 while payment-gateway work continues, but this repository has no source-to-bronze pipeline, bronze contracts or verified catalog governance. Ingesting a small `v11` smoke export through the existing RustFS Iceberg catalog gives us a correctness baseline before accepting a full-scale export. The selected scale input is now the published v12 calendar transformation of v8, preserving v8 unchanged.

DP1 must also ingest ongoing completed batches from immutable live-event landing files. Historical exports are one input mode of the same source-to-bronze pipeline. Real and simulator-driven payment events share versioned service event contracts in one current environment with shared configuration and retained origin provenance. Synthetic events and simulation truth are valid inputs to analytics and model training; separate real/simulator deployments, topics and storage are not required for this synthetic platform.

The published v10 smoke export failed its IP-network cardinality band. Publish v11 with a 1,024-network pool and the unchanged acceptance bands; preserve v8/v10 source objects, manifests and contracts.

## What Changes

- Add a historical DP1 that reads a completed source manifest, verifies its files and ingests payment events and fraud labels into isolated bronze Iceberg tables. Read `s3://synthetic-source/historical/v11/` (smoke) and `historical/v12/` at `https://minio.home.phuchoang.sbs`; do not regenerate or modify accepted source objects. The retired `v7` export is deleted and not supported.
- Add recurring bounded DP1 execution over newly completed landing files, with durable input progress, late-file discovery, retry-safe Iceberg commits and per-batch validation/governance evidence. Kafka-to-landing and simulator implementation belong to the live-streaming change; use a separate landing location from historical synthetic exports.
- Support only historical manifests that embed a per-file integrity inventory (v8, v10, v11 and later). The retired v7 export and its companion-inventory mechanism are out of scope and removed.
- Introduce the `bronze-payments` ODPS product and one ODCS contract per output port. Preserve raw business content, identical duplicate deliveries, delayed labels and upstream contract references while declaring normalized schemas and ingestion provenance.
- Treat generator cardinality/distribution/scenario bands as informational dataset diagnostics rather than bronze acceptance gates. Retain integrity, conservation, schema and contract checks, contextual lifecycle/linkage rules and label-availability restrictions. Preserve published source metadata and previous run outcomes.
- Execute bronze contract checks with current GX on Spark, generating suites from ODCS and rejecting incomplete rule coverage. Keep generator publication on the existing source ODCS/Data Contract CLI/DuckDB path; verify equivalent business-rule outcomes across both engines on small fixtures.
- Orchestrate separate ingest and validate stages through Apache Spark Operator `spark.apache.org/v1` SparkApplications created and watched by Argo. Upgrade the application runtime to Spark 4.1.3 and Iceberg 1.12.0 for Scala 2.13, retaining Python 3.12 and verifying GX compatibility. Include namespace/RBAC prerequisites, bounded retries, graceful cancellation, recoverable table commits and snapshot-pinned validation. Add a platform-owned native Kubernetes admission guard in `fraud-data-prod` requiring terminal Workflow status before deletion and rejecting hard termination of active workflows; use stop, completed cleanup, then deletion.
- Use platform-owned OpenMetadata, PostgreSQL and OpenSearch, with measured resource/storage sizing and private authenticated access. Use Argo-owned metadata publication without Kafka or Airflow. Verify existing backing-service compatibility and isolated state before reuse.
- Publish source/bronze product metadata, Iceberg schemas, source-to-job-to-table lineage, ODCS contracts and successful/failed/errored GX results into OpenMetadata. Persist reports before publication and allow metadata-only retries without re-ingestion.
- **BREAKING:** Governance clients, identities, endpoint/token configuration and acceptance use OpenMetadata only, installed fresh. Retain source data, lake snapshots and durable evidence.
- Prove ingestion, failure reporting and safe replay with bounded resources on a small `v11` smoke export, then accept the published `v12` calendar-shifted source (manifest format 3, `task-fragments-v1`, 24,982 files, 123,289,031,513 physical bytes, validation deferred to `dp1-spark-gx`) as the selected DP1 scale input. Record missing live prerequisites honestly. No simulator or Kafka-to-landing implementation, direct Kafka consumption by DP1, general database CDC, DP2/DP3 curation, deduplication, new Iceberg catalog service or production payment-storage deployment is included. Recurring ingestion consumes the completed landing-file interface coordinated with the live-streaming change; this planning extension does not claim that mode is implemented. No existing source-contract or accepted-dataset breaking change is intended.

## Capabilities

### New Capabilities

- `historical-bronze-ingestion`: Content-preserving and recoverable ingestion from historical manifests and completed recurring landing batches into bronze Iceberg snapshots, with durable input progress, persisted-output validation and run acceptance.
- `data-pipeline-orchestration`: Argo-managed finite Spark stages with operator-owned drivers and executors, scoped identities, bounded lifecycle handling and explicit publication/failure stages.
- `data-catalog-governance`: OpenMetadata deployment readiness, product/table metadata, canonical dataset identities, pipeline lineage and reproducible validation evidence.

### Modified Capabilities

- `dataset-contracts`: Add bronze ODPS/ODCS interfaces and distinguish source DuckDB validation from contract-derived Spark/GX validation without a handwritten parallel suite.
- `talos-gitops-deployment`: Add opt-in prod DP1 resources and explicitly separate application-owned pipeline resources from platform-owned Argo/Apache Spark Operator/OpenMetadata services.
- `focused-verification`: Cover contract execution parity, ingestion replay/recovery, workflow failure paths and catalog identity with small fixtures and a separate live v11 smoke run, followed by a v12 source acceptance run through the `dp1-spark-gx` gate.

## Impact

- Proposed implementation locations: `src/data-pipeline/bronze-ingestion/` for locked Spark/validation/metadata jobs and tests; `contracts/data-products/bronze-payments/`; `infra/charts/data-pipelines/`; and dedicated Spark-job/metadata image definitions using the existing reusable image workflow. Extend existing CI entrypoints proportionally, retaining independent Python, Go, Helm and contract checks.
- Coordinated platform work in `talos-proxmox`: retain the completed removal of the unused Kubeflow Spark Operator and install Apache Spark Operator 1.0.0 (Helm chart 1.8.0), scoped to `fraud-data-prod`; deploy OpenMetadata with PostgreSQL/OpenSearch; configure persistence, credentials, private access and resource budgets. This proposal records that work; it does not edit the other repository or deploy anything. Preserve shared operators/services; OpenMetadata starts with new state.
- Reuse RustFS object storage and its Iceberg REST endpoint, subject to authenticated Spark and PyIceberg/OpenMetadata compatibility checks. Source-read, lake-write and metadata identities must remain separate from the generator's source-writer credential.
- Update only affected sections of existing data-products, data-pipeline, deployment and roadmap documentation, plus generated references for the new contracts. `scale-historical-data-generation` is archived: it published `v8` as source only and handed full-data validation and accepted-version evidence to this change. v11 is a small smoke export with no scale claims; v12 acceptance reports its own measured resources and does not claim optimization beyond them.
- The coursework rubric names a different catalog UI for screenshots. OpenMetadata lineage, contract and validation screenshots will be equivalent-tool evidence; instructor acceptance is not established by this plan.
