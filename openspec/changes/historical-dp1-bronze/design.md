# Design

## Context

See [proposal.md](proposal.md) for motivation and scope. Current evidence:

- `historical-payments` describes immutable source files, not bronze. Accepted v7 has 2,215,309 exported event rows, 2,171,314 unique events and 442,817 labels. Events span 90 occurrence dates; delayed labels and payment histories cross date boundaries. The event export is about 237 MiB, suitable for correctness acceptance.
- Source ODCS contracts declare DuckDB SQL, including nested structures, `UNNEST`, list functions and windowed lifecycle checks. Data Contract CLI 1.2.7's GX exporter maps top-level properties and GX-native quality implementations; it does not translate our existing `type: sql` quality blocks. Its output alone is not complete enforcement of the source contracts.
- `talos-proxmox/apps/components/kubeflow` configures an unused Spark Operator 2.5.2 watching only `kubeflow`; DP1 does not use it. Argo Workflows already includes `fraud-data-prod` in its workflow namespaces. Operators are platform-owned. Live readiness is unverified because no Kubernetes context is selected.
- RustFS is provided at `https://minio.home.phuchoang.sbs`; `/iceberg` is the expected catalog base. An unauthenticated config request required a signature. Spark/PyIceberg write compatibility is proven by the workflow's own preflight smoke stage; catalog availability alone does not prove it.
- No DataHub deployment was found in the inspected platform manifests. Existing CNPG and Strimzi operators are not proof that a usable database or Kafka cluster exists.
- CardFraudPlatform demonstrates raw preservation and GX evidence publication, but uses legacy GX 0.18. RecSys demonstrates Parquet-to-Iceberg and metadata publishing, but uses a Hadoop catalog and its current `all_success` publication dependency does not cover validation failures. Reuse these responsibilities, not their incompatible catalog/API/failure assumptions.

## Goals / Non-Goals

**Goals:** Establish an accepted pair of bronze snapshots from v7, with complete contract checks, safe recovery and visible DataHub governance. Keep source validity, persisted-output validity and governance publication distinguishable in run evidence.

**Non-Goals:** General incremental CDC, live traffic ingestion, dataset generation, 100 GB acceptance, silver/gold transformations, automatic maintenance or performance claims from the small run. Do not turn DataHub into the data-quality execution engine or expose unvalidated latest table state as the product interface.

## Decisions

### 1. Separate source and bronze product interfaces

Create `contracts/data-products/bronze-payments/{product.odps.yaml,payment-events.odcs.yaml,fraud-labels.odcs.yaml}` during implementation. The product is source-aligned, owned by the fraud data platform, with distinct product/contract IDs and independently versioned output ports. Ports identify `bronze.raw_payment_events` and `bronze.raw_fraud_labels` in the configured REST catalog; upstream source contract IDs/versions are input dependencies, and labels also depend on bronze events.

Keep business field names and semantics. Append reserved ingestion fields `_source_dataset_version`, `_source_manifest_sha256`, `_source_file`, `_ingestion_run_id` and `_ingested_at`. Preserve source recording timestamps. Normalize v1's absent `attempt_started.signals.user_agent_family` to a nullable field while retaining `schema_version`; reject unsupported schemas. Interpret nested required fields conditionally on the parent payload being present. Use UTC microsecond timestamp semantics and lossless compatible signed integer types where Iceberg cannot represent source unsigned types; reject overflow rather than narrowing silently.

An alternative is extending the source contracts with audit fields and Spark rules. That would conflate the generator's published file interface with DP1's storage interface and disturb accepted source identities. New contracts make the actual consumer boundary explicit.

### 2. Manifest-selected, isolated and recoverable writes

Each workflow submission selects a completed export through source-root and dataset-version parameters, with optional pinned companion inventory parameters. Reuse the workflow for subsequent versions; a clock schedule over an unchanged version is a replay, not discovery of new input. Read only files named by a completed manifest; verify declared schema/contract identities, file size, SHA-256 and row counts without loading whole files into driver memory. Do not treat multipart ETags as content hashes. Reject missing, altered or unsupported inputs before writes. A manifest digest identifies immutable source content; a reused dataset version with a different digest fails.

v7 predates the embedded per-file inventory. For this accepted version, build a separate companion at `s3://fraud-lakehouse-prod/dp1-evidence/source-inventories/v7.json` using streamed SHA-256 reads and Parquet footer row counts. Reconcile every file and partition against the original manifest's row, file and byte totals, supported schemas, product/contract identities and passed source-validation declarations; reject missing, extra, duplicate or escaping paths. Use the generator's canonical `inventory` entries with `key`, `table`, `partition`, `bytes`, `rows` and `sha256`. Bind the companion format version, source root/version, original manifest SHA-256 and source product/contract identities and hashes. Create it only if absent in the lake evidence scope; never update source data, its original manifest or source contracts. This evidence records current source integrity and does not retroactively prove checksums at original publication. The command and selection path are version-generic: a later v8/v9 export without embedded inventory uses its own explicitly configured companion URI and pin, while the version/manifest binding prevents cross-version reuse.

Require both an explicit companion URI and its SHA-256 pin in configuration/preflight when the original manifest lacks inventory; never discover or create it implicitly during ingestion. Verify those bytes and all source/manifest bindings before selecting files, and repeat normal file-integrity checks before bronze writes. Persist the companion URI/digest alongside the original manifest digest in run evidence so replay cannot silently change its selected inventory. Reject absent, stale, tampered or mismatched evidence. Embedded inventories in newer completed manifests do not require a companion. Source validation must declare both native ports passed, or explicitly declare `result: skipped`, `gate: dp1-spark-gx` and a nonblank reason without conflicting port declarations. Only the embedded-inventory path permits this deferred gate; legacy companion selection still requires passed native declarations. Preserve the deferred declaration in run context and successful/failed validation reports, reject changed gate evidence on replay, and require matching evidence plus passing Spark/GX results for both persisted tables before acceptance. Snapshot and governance requirements remain in force.

Use a dedicated lake bucket/warehouse and namespace, separate from `synthetic-source`, KFP artifacts and production payment storage. Source credentials are read-only; a separate pipeline identity writes only its warehouse/run-evidence scope. Do not reuse the generator's writer credential. Use the REST catalog directly with the required signing/TLS configuration and compatible Iceberg S3 file IO. Do not deploy another catalog or copy a Hadoop catalog configuration.

Partition bronze tables by `_source_dataset_version` initially. For this small full-history batch, do not create tiny per-day output partitions or one Spark job per source day. Preserve every delivered event, including identical duplicates; curation belongs to DP2. Keep labels in a separate table. Reads and cross-table checks always select the intended source version, so separate historical exports cannot accidentally form one history.

Create durable run context before writes, including manifest digest, stable ingestion identity/time, image digest and contract hashes. Serialize writers to the bronze product with an Argo mutex. Each table uses an atomic overwrite restricted to that source-version partition, never blind append or full-table overwrite. Retries reuse the same context and deterministically replace only an incomplete candidate's partition. An already accepted source version reuses its recorded snapshots; a changed contract triggers revalidation, not data mutation.

Record per-table committed snapshot IDs and reconcile an uncertain commit using run identity/snapshot metadata before resuming. Two table commits are not a transaction: a failure after events commits must leave a resumable candidate, not an accepted product. A run record pins the exact pair of snapshots and source digest. Failed candidates remain outside the accepted interface. Do not expire snapshots referenced by run records; retention/maintenance policy must preserve acceptance evidence.

### 3. ODCS remains authoritative; GX executes bronze checks

Keep source ODCS and generator Data Contract CLI/DuckDB validation intact. Scope the existing durable DuckDB requirement explicitly to historical-source publication; introduce a separate bronze execution requirement.

Use the pinned native GX exporter for supported schema constraints and GX-native ODCS quality blocks. For bronze business checks, put Spark SQL implementations in ODCS GX-native rules, with stable semantic rule IDs corresponding to the source rules. GX unexpected-row checks return violating rows, not a scalar `count(*)` row. Do not silently send DuckDB SQL to Spark or infer correctness from export success.

The pipeline builds suites at runtime/build preparation from the versioned contract files. A small coverage preflight ensures every declared quality-rule ID and required schema constraint has an execution path; reject omissions, unsupported expectations or incompatible engine types. Account explicitly for nested/conditional constraints that the native export does not recurse into. Keep physical schema verification generated from the same contract rather than hand-maintaining another schema or suite.

Validate the persisted tables by pinned snapshot IDs, selecting business columns for source-content comparisons so audit values do not make identical deliveries appear conflicting. Register both selected event and label views for cross-table rules. Run the full v7 history and label export, including late availability dates, rather than independently checking each occurrence date. Check row/content conservation separately against the manifest and normalized source.

Before live acceptance, exercise source DuckDB and bronze Spark/GX on shared small fixtures: valid duplicates, conflicting content, missing lifecycle versions, bad amounts, invalid/early labels, unsuccessful-attempt chargebacks and the schema cutover. Physical audit/type differences are intentional; equivalent business rules must agree. Persist rule IDs, coverage, counts, failures/errors, snapshot IDs, source/contract hashes and image/workflow identities before returning a failed status. Bound failure samples; do not collect whole datasets or all invalid rows into the driver.

An alternative is validating bronze with DuckDB alone. That can check v7, but it would not establish the chosen distributed execution path for later datasets. Two independently handwritten suites are also rejected: engine-specific implementations live in their respective contracts and semantic parity protects their overlap.

### 4. Argo owns workflow retries and Spark runs directly in Argo pods

Add `infra/charts/data-pipelines` with namespace-scoped WorkflowTemplates, configuration, ExternalSecrets and distinct Spark/metadata service accounts in `fraud-data-prod`. The application chart owns this data namespace; it is not a Kubeflow Profile namespace. Preserve current dev and service rollout defaults; DP1 is an explicit prod opt-in after prerequisites pass.

Use one Argo container template per Spark stage that runs `spark-submit` semantics through PySpark in Kubernetes client mode: the Argo pod is the driver, executors are created from a ConfigMap pod template with the driver pod as owner, and the step's exit code is the result. Success, failure and deadlines are Argo's; there is no operator, SparkApplication state or status polling. The Spark service account gets only pod, service, ConfigMap and PVC verbs in `fraud-data-prod`. Resource names distinguish attempts; the logical data run identity stays stable.

Workflow stages are preflight, smoke, ingest, validate, publish governance and final acceptance. The smoke stage is a Spark step that creates a uniquely named table in the lake namespace, replaces one partition, reads the earlier snapshot, confirms PyIceberg sees the same snapshot and drops the table; its result lives in workflow status and logs only. Exit handling publishes available failure evidence even when validation fails; absence of a validation report is an execution error, never a pass. Governance tasks run in lightweight metadata pods, not another Spark cluster. Persist reports before external publication. Enable bounded publication retries and a metadata-only replay mode taking an existing run record; final success requires validation and governance publication, while their failure states remain separately recorded.

Stopping or deleting a stage pod removes its executors through owner references, so cancellation and deadlines need no reaper. Run reports and diagnostics live in the durable run records, not in resource TTLs. Do not retry a deterministic failed quality check as a way to obtain a pass.

### 5. Include platform DataHub deployment and verify its dependencies

The shared platform repository owns a pinned official DataHub Helm deployment, GMS/frontend, setup/migration jobs and dependencies in a separate namespace. First check for a live compatible instance and reuse it when verified. Otherwise deploy dedicated persistent DataHub state: PostgreSQL via the existing CNPG operator, Kafka via Strimzi, and a dedicated supported search backend. The selected chart/server versions must support those combinations; never treat an operator as an existing database/broker. Start with a small single-instance course environment and state the absence of HA. Use an authenticated GMS endpoint and private frontend access through the existing platform access/identity mechanism.

Explicitly budget requests, limits, JVM heap, PVCs, dependency startup/migrations and free cluster headroom before enabling the deployment. DataHub services and backing stores remain live while Spark jobs run, so include their combined peak footprint. Pin a compatible supported Spark/Java/Python/Iceberg/GX/GX-plugin/PyIceberg/DataHub-client matrix; the root Python 3.14 tooling does not dictate the job-image Python version. Keep a Spark-job image and a smaller metadata image with their own locked compatible dependencies.

Reuse actual compatible shared backing services only with isolated database/topic/index ownership and credentials; otherwise use the dedicated services above. Do not repurpose ClickStack storage. Deliver credentials through External Secrets/Doppler without Git values or credential-bearing reports. Pipeline deletion must not remove DataHub or its backing stores. DataHub indexing readiness, authenticated writes/readback and restart persistence are acceptance gates, not merely healthy Helm releases.

### 6. One catalog identity across ingestion, lineage and assertions

Use DataHub's native Iceberg source, which delegates REST catalog access to PyIceberg. Verify RustFS catalog signing separately from S3 warehouse access, including path-style/TLS requirements. Capture canonical dataset URNs from the native ingestion result, then reuse those exact identities in the GX plugin and lineage publisher. Do not create a second Spark-named dataset for the same Iceberg table.

Represent source ports as logical versioned S3 datasets linked to manifest URLs/hashes, not one DataHub entity per file. Project ODPS ownership, port membership and upstream references into DataHub products; attach ODCS IDs, versions and artifact links to the corresponding assets. DataHub's internal contract model is not assumed to be an automatic ODCS importer or a complete replacement for the files.

Emit an Argo DataFlow with ingest/validate DataJobs, actual inputs/outputs, execution outcome and workflow/run links using the supported SDK. Do not assume the reference repos' Airflow plugin discovers Argo jobs. Publish dataset lineage from each source port to its corresponding bronze output and the event dependency used to validate labels. Include source digest and pinned snapshots in run metadata; record direct field lineage where the ingestion mapping is known.

Prefer the current `datahub_gx_plugin.action_v1.DataHubValidationAction` for GX 1.x assertions and results, with explicit Iceberg dataset identity. Retain serializable GX reports so a metadata-only worker can reconstruct publication through the same supported action, or narrowly use the assertion SDK for a proven plugin gap. Assert readback against the expected table URNs and stable contract-rule identities. Publishing failure must not trigger source ingestion again or erase the original validation outcome. Verify both success and deliberate failure visibility before accepting governance.

### 7. Bound the small run and leave scale optimization measurable

Use one bounded full-history v7 batch. Start with one driver and one small executor, fixed resources, a small shuffle partition count, adaptive query execution and distributed reads/writes. Proposed starting requests are 1 CPU/2 GiB Spark memory per driver/executor plus explicit container overhead; validate scheduling and actual peaks before pinning accepted values. Use configurable Iceberg target file size around 128 MiB as a starting point, not a promise of full-sized files for small tables. Avoid per-day jobs, unnecessary repartitioning by high-cardinality IDs and unconditional compaction.

Retain the existing duplicates, skew, high cardinality and physical schema change. Introduce corrupt cases only in isolated failure fixtures, never mutate v7. Record read/write bytes, output sizes/counts, duration, shuffle/spill, skew and peak memory. The pilot may expose configuration issues; it cannot prove 100 GB throughput. Later scale work must measure and tune partitioning, shuffle counts, broadcast behavior, storage requests and file compaction against the larger source.

### 8. The v7 companion inventory is removed after acceptance

The companion inventory exists only to unblock v7. Once a later export with an embedded inventory is accepted end to end, remove the companion path, its configuration and the lake inventory object. Accepted v7 run records and snapshots are retained.

## Risks / Trade-offs

- RustFS REST authentication or commit behavior differs between clients -> the preflight smoke stage must pass before any ingest write; report an unresolved compatibility failure rather than adding an unplanned catalog.
- Source and bronze SQL dialects diverge -> stable semantic rule mapping, explicit coverage and shared valid/invalid fixture parity; keep accepted source contracts untouched.
- Partial table commits or metadata outages -> snapshot-pinned durable run state, version-scoped replacement, serialized writers, no acceptance until all required stages complete, metadata-only retries.
- DataHub introduces more always-on cost than v7 processing -> provision dependencies explicitly, measure combined capacity and document the course deployment's availability limitations.
- Latest bronze table state can contain a failed candidate -> consumers use accepted snapshot-pair records; do not publish a candidate as ready, and preserve the previous accepted record.
- Search indexing is eventually consistent -> bounded readback retries; never treat immediate search absence or a successful HTTP emit as full catalog acceptance.

## Migration Plan

1. Prove the dependency/client matrix locally; RustFS operations are proven in the cluster by the preflight smoke stage. Preserve accepted source and existing warehouse data.
2. Land the new product/contracts, jobs, behavioral checks, images and opt-in application chart. Keep existing source validation commands and generator outputs unchanged.
3. Land coordinated platform Spark namespace and DataHub changes. Verify persistence, authenticated access, backing-service readiness and the joint resource budget.
4. Enable prod DP1, ingest/validate v7 and publish/read back governance. Run replay, interrupted-commit, quality-failure and metadata-outage acceptance; retain evidence in the existing data-platform documentation/roadmap.
5. Once an embedded-inventory export is accepted, remove the v7 companion path (tasks 8.1-8.2).
6. Rollback stops new workflows and removes only application-owned running resources. Keep immutable source, Iceberg snapshots, acceptance records, reports and platform DataHub state. Repoint consumers to the previous accepted snapshot pair; never use destructive table or bucket deletion as rollback.

## Open Questions

- Exact chart/image versions and accepted resource values are determined by the bounded compatibility/capacity gates above and recorded before deployment; live cluster state is currently unverified.
- The final internal DataHub hostname, warehouse bucket name and ExternalSecret remote-key names follow available platform allocations. These do not change the ownership or acceptance interfaces.

## References

- Local source interfaces: `contracts/data-products/historical-payments/`, `docs/data-ml/data-products.md` and `src/data-pipeline/simulator/notebooks/historical-sanity.ipynb`.
- Platform configuration: `talos-proxmox/apps/components/{kubeflow,argo-workflows,cnpg,strimzi}`; reference job implementations in `card-fraud-platform/pipelines/spark` and `RecSys-MLops/apps/data-platform/src`.
- [Data Contract CLI GX export](https://docs.datacontract.com/exports/great-expectations), [Spark on Kubernetes client mode](https://spark.apache.org/docs/3.5.8/running-on-kubernetes.html#client-mode).
- [DataHub Iceberg connector](https://docs.datahub.com/docs/generated/ingestion/sources/iceberg), [GX integration](https://docs.datahub.com/docs/metadata-ingestion/integration_docs/great-expectations), [DataHub Helm](https://github.com/acryldata/datahub-helm).
