# Proposal

## Why

The accepted historical v7 dataset can exercise DP1 while payment-gateway work continues, but this repository has no source-to-bronze pipeline, bronze contracts or verified catalog governance. Ingesting that small dataset through the existing RustFS Iceberg catalog gives us a correctness baseline before the separate 100 GB generation and optimization work.

## What Changes

- Add a historical DP1 that reads a completed source manifest, verifies its files and ingests payment events and fraud labels into isolated bronze Iceberg tables. Reuse `s3://synthetic-source/historical/v7/` at `https://minio.home.phuchoang.sbs`; do not regenerate v7 or modify accepted source objects.
- Add an explicitly selected, SHA-256-pinned companion inventory for completed manifests that predate per-file integrity evidence (v7 today, and any later v8/v9 export using the same mechanism). Build it with bounded source reads and store it separately in lake evidence; preserve the original manifest and source contracts. New generator manifests continue using their embedded inventory.
- Introduce the `bronze-payments` ODPS product and one ODCS contract per output port. Preserve raw business content, identical duplicate deliveries, delayed labels and upstream contract references while declaring normalized schemas and ingestion provenance.
- Execute bronze contract checks with current GX on Spark, generating suites from ODCS and rejecting incomplete rule coverage. Keep generator publication on the existing source ODCS/Data Contract CLI/DuckDB path; verify equivalent business-rule outcomes across both engines on small fixtures.
- Orchestrate separate smoke, ingest and validate Spark stages as Argo container steps that run Spark in Kubernetes client mode, with no Spark operator. Include namespace/RBAC prerequisites, bounded retries, cancellation, recoverable table commits and snapshot-pinned validation.
- Replace DataHub entirely with platform-owned OpenMetadata, PostgreSQL and OpenSearch, with measured resource/storage sizing and private authenticated access. Use Argo-owned metadata publication without Kafka or Airflow. Verify existing backing-service compatibility and isolated state before reuse.
- Publish source/bronze product metadata, Iceberg schemas, source-to-job-to-table lineage, ODCS contracts and successful/failed/errored GX results into OpenMetadata. Persist reports before publication and allow metadata-only retries without re-ingestion.
- **BREAKING:** Replace DataHub-specific clients, GX plugins, identities, endpoint/token configuration and governance acceptance with OpenMetadata equivalents; remove the DataHub deployment and only its dedicated dependencies as part of a fresh installation. Retain source data, lake snapshots and durable evidence.
- Prove v7 ingestion, failure reporting and safe replay with bounded resources. Record missing live prerequisites honestly. No streaming generator, DP2/DP3, deduplication, large-scale run, new Iceberg catalog service or production payment-storage integration is included. No existing source-contract or accepted-dataset breaking change is intended.

## Capabilities

### New Capabilities

- `historical-bronze-ingestion`: Manifest-selected, content-preserving and recoverable ingestion into versioned bronze Iceberg snapshots, with persisted-output validation and run acceptance.
- `data-pipeline-orchestration`: Argo-managed finite Spark stages run directly by Argo pods, scoped identities, bounded lifecycle handling and explicit publication/failure stages.
- `data-catalog-governance`: OpenMetadata deployment readiness, product/table metadata, canonical dataset identities, pipeline lineage and reproducible validation evidence.

### Modified Capabilities

- `dataset-contracts`: Add bronze ODPS/ODCS interfaces and distinguish source DuckDB validation from contract-derived Spark/GX validation without a handwritten parallel suite.
- `talos-gitops-deployment`: Add opt-in prod DP1 resources and explicitly separate application-owned pipeline resources from platform-owned Argo/OpenMetadata services.
- `focused-verification`: Cover contract execution parity, ingestion replay/recovery, workflow failure paths and catalog identity with small fixtures and a separate live v7 acceptance run.

## Impact

- Proposed implementation locations: `src/data-pipeline/bronze-ingestion/` for locked Spark/validation/metadata jobs and tests; `contracts/data-products/bronze-payments/`; `infra/charts/data-pipelines/`; and dedicated Spark-job/metadata image definitions using the existing reusable image workflow. Extend existing CI entrypoints proportionally, retaining independent Python, Go, Helm and contract checks.
- Coordinated platform work in `talos-proxmox`: remove the unused Spark Operator; replace the deployed DataHub with OpenMetadata and PostgreSQL/OpenSearch; configure persistence, credentials, private access and resource budgets. This proposal records that work; it does not edit the other repository or deploy anything. Preserve shared operators/services; remove the disposable DataHub instance and start with new OpenMetadata state, without catalog migration.
- Reuse RustFS object storage and its Iceberg REST endpoint, subject to authenticated Spark and PyIceberg/OpenMetadata compatibility checks. Source-read, lake-write and metadata identities must remain separate from the generator's source-writer credential.
- Update only affected sections of existing data-products, data-pipeline, deployment and roadmap documentation, plus generated references for the new contracts. Keep `scale-historical-data-generation` independent; DP1 does not require a 100 GB source or claim large-scale optimization evidence from v7.
- The coursework rubric explicitly names DataHub UI screenshots. OpenMetadata lineage, contract and validation screenshots will be equivalent-tool evidence; instructor acceptance is not established by this plan.
