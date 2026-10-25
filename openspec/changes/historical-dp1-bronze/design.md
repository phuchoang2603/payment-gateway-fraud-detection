# Design

## Context

See [proposal.md](proposal.md) for motivation and scope. Source evidence and platform observations from the initial investigation:

- `historical-payments` describes immutable source files, not bronze. `v10` is a small smoke export (100,000 customers, six days, same generator, schema cutover and manifest format as `v8`); duplicates, delayed labels and payment histories cross date boundaries.
- Source ODCS contracts declare DuckDB SQL, including nested structures, `UNNEST`, list functions and windowed lifecycle checks. Data Contract CLI 1.2.7's GX exporter maps top-level properties and GX-native quality implementations; it does not translate our existing `type: sql` quality blocks. Its output alone is not complete enforcement of the source contracts.
- The initial platform inspection found an unused Spark Operator 2.5.2 in `talos-proxmox/apps/components/kubeflow`, watching only `kubeflow`; DP1 does not use it. Argo Workflows included `fraud-data-prod` in its workflow namespaces. Shared controllers are platform-owned. Live prod inspection on 2026-10-23 confirmed the operator workloads/RBAC/webhooks removed, no Spark custom-resource instances and a Synced/Healthy Kubeflow Application; three unused Spark CRD definitions remain. Live DP1 execution still requires verification.
- RustFS is provided at `https://minio.home.phuchoang.sbs`; `/iceberg` is the expected catalog base. An unauthenticated config request required a signature. Native Spark/PyIceberg compatibility was verified through isolated operator-managed checks; production batches do not repeat disposable-table rollout probes.
- Live prod inspection on 2026-10-24 found OpenMetadata server 2.0.6, CNPG PostgreSQL 16.13 and OpenSearch 2.19.5 Synced/Healthy and Running with Bound PVCs, synced ExternalSecrets and anonymous API rejection (HTTP 401). Authenticated DP1 governance readback and combined capacity with Spark remain unverified.
- CardFraudPlatform demonstrates raw preservation and GX evidence publication, but uses legacy GX 0.18. RecSys demonstrates Parquet-to-Iceberg and metadata publishing, but uses a Hadoop catalog and its current `all_success` publication dependency does not cover validation failures. Reuse these responsibilities, not their incompatible catalog/API/failure assumptions.

## Goals / Non-Goals

**Goals:** Establish an accepted pair of bronze snapshots from the v10 smoke export, with complete contract checks, safe recovery and visible OpenMetadata governance. Keep source validity, persisted-output validity and governance publication distinguishable in run evidence.

After the v10 smoke run, accept the published `v8` source (generated and published by the archived `scale-historical-data-generation`, which deferred all full-data validation to the `dp1-spark-gx` gate) with the same persisted-snapshot and governance gates.

**Non-Goals:** General incremental CDC, live traffic ingestion, dataset generation, silver/gold transformations, automatic maintenance or performance claims from the small v10 run. v8 acceptance reports measured resources only; tuning beyond them is later work. Do not turn OpenMetadata into the data-quality execution engine or expose unvalidated latest table state as the product interface.

## Decisions

### 1. Separate source and bronze product interfaces

Create `contracts/data-products/bronze-payments/{product.odps.yaml,payment-events.odcs.yaml,fraud-labels.odcs.yaml}` during implementation. The product is source-aligned, owned by the fraud data platform, with distinct product/contract IDs and independently versioned output ports. Ports identify `bronze.raw_payment_events` and `bronze.raw_fraud_labels` in the configured REST catalog; upstream source contract IDs/versions are input dependencies, and labels also depend on bronze events.

Keep business field names and semantics. Append reserved ingestion fields `_source_dataset_version`, `_source_manifest_sha256`, `_source_file`, `_ingestion_run_id` and `_ingested_at`. Preserve source recording timestamps. Normalize v1's absent `attempt_started.signals.user_agent_family` to a nullable field while retaining `schema_version`; reject unsupported schemas. Interpret nested required fields conditionally on the parent payload being present. Use UTC microsecond timestamp semantics and lossless compatible signed integer types where Iceberg cannot represent source unsigned types; reject overflow rather than narrowing silently.

An alternative is extending the source contracts with audit fields and Spark rules. That would conflate the generator's published file interface with DP1's storage interface and disturb accepted source identities. New contracts make the actual consumer boundary explicit.

### 2. Manifest-selected, isolated and recoverable writes

Each workflow submission selects a completed export through source-root and dataset-version parameters. Reuse the workflow for subsequent versions; a clock schedule over an unchanged version is a replay, not discovery of new input. Read only files named by a completed manifest; verify declared schema/contract identities, file size, SHA-256 and row counts without loading whole files into driver memory. Do not treat multipart ETags as content hashes. Reject missing, altered or unsupported inputs before writes. A manifest digest identifies immutable source content; a reused dataset version with a different digest fails.

Only manifests with an embedded per-file inventory are supported (v8, v10 and later); the retired v7 export, its separate companion inventory and the explicit URI/SHA-256 pin are removed. Reject manifests without an embedded inventory before writes. Source validation must declare both native ports passed, or explicitly declare `result: skipped`, `gate: dp1-spark-gx` and a nonblank reason without conflicting port declarations. Preserve the declaration in run context and successful/failed validation reports, reject changed gate evidence on replay, and require matching evidence plus passing Spark/GX results for both persisted tables before acceptance. Snapshot and governance requirements remain in force.


Use a dedicated lake bucket/warehouse and namespace, separate from `synthetic-source`, KFP artifacts and production payment storage. Source credentials are read-only; a separate pipeline identity writes only its warehouse/run-evidence scope. Do not reuse the generator's writer credential. Use the REST catalog directly with the required signing/TLS configuration and compatible Iceberg S3 file IO. Do not deploy another catalog or copy a Hadoop catalog configuration.

Partition bronze tables by `_source_dataset_version` initially. For this small full-history batch, do not create tiny per-day output partitions or one Spark job per source day. Preserve every delivered event, including identical duplicates; curation belongs to DP2. Keep labels in a separate table. Reads and cross-table checks always select the intended source version, so separate historical exports cannot accidentally form one history.

Create durable run context before writes, including manifest digest, stable ingestion identity/time, image digest and contract hashes. Serialize writers to the bronze product with an Argo mutex. Each table uses an atomic overwrite restricted to that source-version partition, never blind append or full-table overwrite. Retries reuse the same context and deterministically replace only an incomplete candidate's partition. An already accepted source version reuses its recorded snapshots; a changed contract triggers revalidation, not data mutation.

Record per-table committed snapshot IDs and reconcile an uncertain commit using run identity/snapshot metadata before resuming. Two table commits are not a transaction: a failure after events commits must leave a resumable candidate, not an accepted product. A run record pins the exact pair of snapshots and source digest. Failed candidates remain outside the accepted interface. Do not expire snapshots referenced by run records; retention/maintenance policy must preserve acceptance evidence.

### 3. ODCS remains authoritative; GX executes bronze checks

Keep source ODCS and generator Data Contract CLI/DuckDB validation intact. Scope the existing durable DuckDB requirement explicitly to historical-source publication; introduce a separate bronze execution requirement.

Use the pinned native GX exporter for supported schema constraints and GX-native ODCS quality blocks. For bronze business checks, put Spark SQL implementations in ODCS GX-native rules, with stable semantic rule IDs corresponding to the source rules. GX unexpected-row checks return violating rows, not a scalar `count(*)` row. Do not silently send DuckDB SQL to Spark or infer correctness from export success.

The pipeline builds suites at runtime/build preparation from the versioned contract files. A small coverage preflight ensures every declared quality-rule ID and required schema constraint has an execution path; reject omissions, unsupported expectations or incompatible engine types. Account explicitly for nested/conditional constraints that the native export does not recurse into. Keep physical schema verification generated from the same contract rather than hand-maintaining another schema or suite.

Validate the persisted tables by pinned snapshot IDs, selecting business columns for source-content comparisons so audit values do not make identical deliveries appear conflicting. Register both selected event and label views for cross-table rules. Run the full history and label export, including late availability dates, rather than independently checking each occurrence date. Check row/content conservation separately against the manifest and normalized source.

Before live acceptance, exercise source DuckDB and bronze Spark/GX on shared small fixtures: valid duplicates, conflicting content, missing lifecycle versions, bad amounts, invalid/early labels, unsuccessful-attempt chargebacks and the schema cutover. Physical audit/type differences are intentional; equivalent business rules must agree. Persist rule IDs, coverage, counts, failures/errors, snapshot IDs, source/contract hashes and image/workflow identities before returning a failed status. Bound failure samples; do not collect whole datasets or all invalid rows into the driver.

An alternative is validating bronze with DuckDB alone. That can check a small export, but it would not establish the chosen distributed execution path for later datasets. Two independently handwritten suites are also rejected: engine-specific implementations live in their respective contracts and semantic parity protects their overlap.

### 4. Argo owns workflow retries and Apache Spark Operator owns Spark applications

Add `infra/charts/data-pipelines` with namespace-scoped WorkflowTemplates, configuration, ExternalSecrets and distinct Spark/metadata service accounts in `fraud-data-prod`. The application chart owns this data namespace; it is not a Kubeflow Profile namespace. Preserve current dev and service rollout defaults; DP1 is an explicit prod opt-in after prerequisites pass.

Use Argo resource stages to create and watch one `spark.apache.org/v1` SparkApplication per ingest and validate attempt. The platform owns Apache Spark Operator 1.0.0 and its vendored Helm chart 1.8.0, installed in `spark-operator` and watching only `fraud-data-prod` with namespace-scoped operator Roles/RoleBindings. Do not adopt the application-owned data namespace, driver service account or workload RBAC. The removed Kubeflow operator and its `sparkoperator.k8s.io` CRDs are a separate project and API group.

The application chart owns SparkApplication definitions, driver/executor configuration and scoped workload identities. The Apache operator manages drivers and their Spark-created executors; Argo owns the DAG, writer mutex, bounded stage retries and durable logical run identity. Set operator restart policy to `Never` to prevent multiplied retries. Resource names distinguish attempts; application names/UIDs and workflow/run references identify execution evidence. Determine success and failure from the pinned operator's terminal status, not merely resource creation or driver scheduling.

Target Spark 4.1.3, Iceberg 1.12.0 Spark 4.1/Scala 2.13 runtime and AWS bundle, Python 3.12 and Java 17. Rebuild locked images with matching PySpark/jars and verify GX, ANSI SQL behavior, timestamp/type normalization and native RustFS read/write compatibility through CI and live smoke. Apache operator 1.0.0 supports Spark 4.1 and Kubernetes 1.36; the observed cluster is 1.36.3. Spark 4.2.0 is newer, but no dedicated Iceberg Spark 4.2 runtime artifact was available at selection time. Keep the native RustFS catalog, source contracts and immutable datasets unchanged; Spark Connect is outside this migration.

Workflow stages are manifest/run-context preflight, ingest, validate, publish governance and final acceptance. There is no disposable-table smoke stage or manual rollout-verification flag. Exit handling publishes available failure evidence even when validation fails; absence of a validation report is an execution error, never a pass. Governance tasks run in lightweight metadata pods, not another Spark cluster. Persist reports before external publication. Enable bounded publication retries and a metadata-only replay mode taking an existing run record; final success requires validation and governance publication, while their failure states remain separately recorded.

Graceful cancellation and stage deadlines must delete the active SparkApplication, explicitly delete and wait for labelled driver/executor pods, and complete exit cleanup before writer serialization is released. The platform Argo chart owns a native ValidatingAdmissionPolicy and binding scoped to Workflows in `fraud-data-prod`: deletion requires a terminal Workflow phase (`Succeeded`, `Failed` or `Error`), and setting `spec.shutdown: Terminate` is rejected unless the existing Workflow is Pending or terminal. Operators use `argo stop`, wait for completion and verify cleanup before deletion. Workflows without a recorded phase cannot be deleted or hard-terminated. Other workflow namespaces remain unaffected. This replaces support for direct active Workflow deletion; owner-reference garbage collection does not retain the Argo mutex during background deletion. The guard prevents this cancellation bypass but does not establish safety after failed cleanup; cleanup-error recovery and writer handoff remain rollout gates. Configure bounded resource retention/TTL for diagnostics; TTL alone does not establish cancellation safety. Run reports and diagnostics live in the durable run records, not in resource TTLs. Do not retry a deterministic failed quality check as a way to obtain a pass.

### 5. Platform-owned OpenMetadata

The shared platform repository owns a pinned official OpenMetadata Helm deployment and persistent PostgreSQL/OpenSearch services in a separate namespace. Kafka, Airflow and a separate OpenLineage/Marquez service are not required for this design. Keep Argo as the pipeline scheduler and run metadata ingestion/publication in its existing metadata steps using the supported OpenMetadata SDK/API. Do not enable UI-managed ingestion scheduling or deploy Airflow merely to publish DP1 metadata.

Start with clean OpenMetadata state. Check for an existing OpenMetadata instance before installing one to avoid duplicates. Use the existing CNPG operator for PostgreSQL and a supported OpenSearch version. Reuse backing services only after version, ownership, database/index isolation and credential checks; otherwise provision dedicated instances. Keep authentication enabled, provide a least-privilege pipeline bot identity and private UI/API access through the platform access mechanism. Start with a single-instance course deployment and state its absence of HA.

Budget requests, limits, JVM heap, PVCs, migrations and concurrent Spark headroom before rollout. PostgreSQL/OpenSearch compatibility and reduced component count do not establish measured savings. AWS bursting was declined; measure and fit OpenMetadata plus the bounded Spark driver/executor on the on-prem cluster before acceptance. The previous unschedulable Spark attempt remains failed evidence, not an OpenMetadata capacity result. Publish `https://openmetadata.phuchoang.sbs` through the existing Cloudflare ClusterTunnel. Use a dedicated confidential Dex OAuth2/OIDC client and make the existing Dex identity `xuanphuc.a1gv@gmail.com` an OpenMetadata admin. OpenMetadata handles its own OIDC session; the Kubeflow-specific OAuth2 Proxy remains unchanged.

Pin a compatible Spark/Java/Python/Iceberg/GX/PyIceberg/OpenMetadata server/SDK matrix, including an OpenMetadata release with the required ODCS import and GX integration APIs. Prove the existing Spark custom expectations, GX 1.x reports and ODCS dialect work with those versions. The root Python 3.14 tooling does not dictate job-image Python. Publication uses the OpenMetadata REST API through a stdlib client, adding no catalog SDK to either locked image. Keep Spark-job and smaller metadata images; isolate publication dependencies to the metadata image where practical.

Both images remain self-contained multi-stage builds under the existing service-image CI workflow; no shared Spark base image is introduced. Build the locked environment in a separate stage, omit build tools from final images, prune unused tests/caches and symlink duplicate Spark jars. Download version-pinned Iceberg/S3 jars directly in the Dockerfile with checksum verification and bundle them so production stages do not resolve Maven packages at runtime. CI owns image builds/publication and deployment selects published digests. Test-time Spark uses standard Maven resolution without custom jar preparation tooling. Helm verification uses generic chart checks and chart-owned values fixtures.

Use OpenMetadata endpoint/token variables, ExternalSecret mappings, permissions and health checks with a single publisher path. Keep secret values out of Git, manifests and reports. Pipeline deletion must not remove OpenMetadata or its backing stores. Authenticated writes/readback, indexing readiness and restart persistence are acceptance gates.

### 6. One OpenMetadata identity across schemas, contracts, lineage and GX results

Register the existing RustFS-backed Iceberg tables under one stable OpenMetadata service/database/schema/table identity. Prefer the supported Iceberg connector and verify REST-catalog signing separately from S3 warehouse access. If the selected connector cannot express the existing RustFS configuration, use the existing authenticated PyIceberg schema discovery with supported OpenMetadata entity APIs; this is metadata registration, not a new Iceberg catalog. Verify this boundary before application rollout, without changing lake storage or duplicating assets under Spark execution names. Reuse the discovered entity IDs and fully qualified names across every publisher and acceptance readback.

Represent source ports as logical versioned assets linked to manifest URLs/hashes, not one entity per Parquet file. Project ODPS product membership, ownership and upstream references into OpenMetadata data products and assets. Project versioned ODCS contracts into their matching assets using the native OpenMetadata contract API, preserving the complete original YAML, IDs, versions, hashes and artifact references. OpenMetadata 2.0.6 rejects the source `local` environment and structured bronze custom properties through its ODCS YAML importer; native contract schemas and quality-rule metadata avoid changing the Git contracts or immutable source manifests. Serialize structured GX implementations as JSON strings for its native quality-rule field. Git remains authoritative; catalog edits do not replace contract files. Do not populate native contract quality-test references or create automatic tests; publish only the GX-derived test cases from persisted evidence. Verify publication/readback with the actual contracts rather than assuming all custom quality implementations translate automatically.

Register Argo pipeline/tasks and publish actual input/output relationships, execution outcomes, workflow/run links and known direct field mappings through the supported SDK/API. Include source-port to ingest-job to bronze-table lineage and the bronze-event dependency used for label validation. Keep manifest digest, exact snapshot IDs, contract hashes and workflow/run identity in durable evidence and linked metadata. Do not assume Airflow instrumentation discovers Argo executions.

Persisted GX reports are projected onto one `GreatExpectations` test definition and per-rule test cases, with results addressed by table, rule and the report timestamp so metadata-only replay verifies identical existing evidence rather than creating duplicates or overwriting outcomes. The OpenMetadata GX action is not used because it requires a live GX checkpoint and would start Spark during recovery. A failed rule is `Failed`; raised exceptions and stage errors are `Aborted`, never `Success`. Durable reports remain the exact provenance record, and result text cites the run, manifest, contract and snapshot.

Acceptance requires OpenMetadata readback of product membership, schemas, imported contracts, lineage, execution outcomes and validation results under the same canonical identities. Bounded indexing retries preserve the original data outcome; metadata-only recovery must not duplicate assets/results or ingest data again. A successful HTTP submission or healthy server is insufficient. Verify one real table, ODCS import, GX result and lineage path end to end before replacing the remaining publisher paths.

### 7. Bound the small run and leave scale optimization measurable

Use one bounded full-history v10 smoke batch. Start with one driver and one small executor, fixed resources, a small shuffle partition count, adaptive query execution and distributed reads/writes. Proposed starting requests are 1 CPU/2 GiB Spark memory per driver/executor plus explicit container overhead; validate scheduling and actual peaks before pinning accepted values. Use configurable Iceberg target file size around 128 MiB as a starting point, not a promise of full-sized files for small tables. Avoid per-day jobs, unnecessary repartitioning by high-cardinality IDs and unconditional compaction.

Retain the existing duplicates, skew, high cardinality and physical schema change. Introduce corrupt cases only in isolated failure fixtures, never mutate accepted source exports. Record read/write bytes, output sizes/counts, duration, shuffle/spill, skew and peak memory. The pilot may expose configuration issues; it cannot prove 100 GB throughput. Later scale work must measure and tune partitioning, shuffle counts, broadcast behavior, storage requests and file compaction against the larger source.

### 8. v8 is the first scale acceptance and the dp1-spark-gx gate owner

`v8` is a completed task-fragment export: its manifest lists exactly 24,982 raw files with per-file checksum, bytes, rows and schema version, 123,286,939,620 physical and 243,759,611,982 uncompressed Parquet bytes, and original fragments retained. Rows are unordered, may repeat, and one file may span several dates; Bronze partitions derive from timestamp columns. The producer skipped native checks, so Spark/GX results on both persisted tables plus governance are the only quality acceptance, including the cardinality, distribution and scenario bands the generator no longer measures. Size Spark from the pilot and the uncompressed bytes, record actual peaks, and report failure honestly if the on-prem executors cannot complete it. v8 acceptance runs only after the v10 smoke run.

### 9. v7 and the companion inventory are removed up front

No companion-inventory code, configuration or evidence is built. The v7 source export is deleted and DP1 supports only embedded-inventory manifests, so there is no compatibility path to retire after acceptance.

## Risks / Trade-offs

- RustFS REST authentication or commit behavior differs between clients -> retain diagnostics and report execution failure rather than adding an unplanned catalog.
- Source and bronze SQL dialects diverge -> stable semantic rule mapping, explicit coverage and shared valid/invalid fixture parity; keep accepted source contracts untouched.
- Partial table commits or metadata outages -> snapshot-pinned durable run state, version-scoped replacement, serialized writers, no acceptance until all required stages complete, metadata-only retries.
- OpenMetadata still has always-on server/search/database costs -> measure combined on-prem capacity and document availability limits; do not promise savings from component counts alone.
- ODCS import and GX custom-result support vary by OpenMetadata release -> pin the server/SDK pair and prove actual contracts and success/failure/error publication before application rollout.
- The coursework rubric names a different catalog UI for screenshots -> capture equivalent OpenMetadata lineage, contracts and validation evidence and document the tool substitution; instructor acceptance remains unverified.
- Latest bronze table state can contain a failed candidate -> consumers use accepted snapshot-pair records; do not publish a candidate as ready, and preserve the previous accepted record.
- Search indexing is eventually consistent -> bounded readback retries; never treat immediate search absence or a successful HTTP emit as full catalog acceptance.

## Fresh Platform Installation

1. Start OpenMetadata with empty state; no metadata migration, reconstruction or backup/cutover workflow is required. Keep shared operators/services and all source, lake and DP1 evidence state.
2. Deploy the pinned OpenMetadata chart with dedicated PostgreSQL/OpenSearch, ExternalSecrets and measured on-prem resource budgets. Use the existing Cloudflare ClusterTunnel for `openmetadata.phuchoang.sbs` and a dedicated confidential Dex OAuth2/OIDC client with `/callback`; configure the existing Dex user as admin. Keep Kubeflow's OAuth2 Proxy unchanged because OpenMetadata supports native OIDC.
3. Verify fresh bootstrap, authenticated UI/API behavior, Dex login/admin authorization, TunnelBinding readiness and persistent platform state. This application request covers platform installation only; DP1 publisher/configuration/image changes and live ingestion remain pending tasks.
4. In a later application phase, switch publishers, workload credentials and acceptance readback together using CI images, then verify catalog and Spark acceptance.
5. Rollback stops the new platform/application resources while preserving source/lake/evidence data; it does not create a parallel publisher.

## Open Questions

- Exact chart/image versions and accepted resource values are determined by the bounded compatibility/capacity gates above and recorded before deployment; live acceptance still requires verification of the deployed revisions, resources and endpoints.
- The final internal OpenMetadata hostname, warehouse bucket name and ExternalSecret remote-key names follow available platform allocations. These do not change the ownership or acceptance interfaces.

## References

- Local source interfaces: `contracts/data-products/historical-payments/`, `docs/data-ml/data-products.md` and `src/data-pipeline/simulator/notebooks/historical-sanity.ipynb`.
- Platform configuration: `talos-proxmox/apps/components/ (Kubeflow, Argo Workflows, CNPG and catalog services)`; reference job implementations in `card-fraud-platform/pipelines/spark` and `RecSys-MLops/apps/data-platform/src`.
- [Data Contract CLI GX export](https://docs.datacontract.com/exports/great-expectations), [Apache Spark Operator 1.0.0](https://github.com/apache/spark-kubernetes-operator/tree/1.0.0), [Spark 4.1 migration](https://spark.apache.org/docs/4.1.3/sql-migration-guide.html).
- [OpenMetadata deployment](https://docs.open-metadata.org/v2.0.x/deployment/kubernetes/on-prem), [ODCS import/export](https://docs.open-metadata.org/v2.0.x/api-reference/data-contracts/odcs), [GX integration](https://docs.open-metadata.org/v2.0.x/connectors/ingestion/great-expectations), [Helm charts](https://github.com/open-metadata/openmetadata-helm-charts).
