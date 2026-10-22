# Tasks

## 1. Compatibility and storage prerequisites

- [x] 1.1 Create the locked `src/data-platform` runtime/tooling setup and pin a supported Spark/Java/Python/Iceberg/GX/GX-plugin/PyIceberg/DataHub-client matrix; verify local Spark startup, native GX export/import and plugin imports with the selected versions, independently of root Python 3.14.
- [ ] 1.2 Add a bounded RustFS compatibility probe for authenticated REST namespace/table creation, write, snapshot-pinned read and version-filtered overwrite in an isolated disposable test scope; verify equivalent PyIceberg metadata access and record catalog-signing versus S3/TLS settings without secret values.
- [x] 1.3 Allocate a separate lake/evidence scope and source-read, lake-write and metadata identities through the existing external-secret mechanism; verify source reads succeed, pipeline source writes are denied and warehouse access is confined to its intended scope.
- [x] 1.4 Pin the official DataHub chart/server and compatible PostgreSQL/Kafka/search dependency versions in the platform implementation, checking for an existing live deployment first; deliver a rendered dependency/capacity inventory covering PVCs, JVM/container memory and concurrent Spark headroom, with unresolved live checks clearly identified.

## 2. Bronze interfaces and contract execution

- [x] 2.1 Add the `bronze-payments` ODPS descriptor and both ODCS output contracts with distinct identities, upstream source references, provenance, normalized physical types, conditional nested requirements and label restrictions; verify native descriptor/contract lint and regenerate only their required schema references.
- [x] 2.2 Encode bronze business checks as GX-native ODCS rules using Spark SQL and stable source-rule semantic mappings; verify valid duplicates, conflicting content, lifecycle gaps, bad amounts and invalid/early/unsuccessful-attempt labels with small shared fixtures and the existing source DuckDB rules.
- [x] 2.3 Derive GX suites and physical-schema checks from the contracts, adding explicit coverage for nested/conditional requirements; verify an intentionally unsupported or omitted rule fails preparation and that scalar-count versus unexpected-row semantics cannot yield a false pass/failure.
- [x] 2.4 Add snapshot-selected Spark/GX validation with both event and label views, business-only duplicate comparisons and separate source-to-output conservation checks; verify cross-date histories, v1/v2 normalization and preserved duplicates on small local Iceberg fixtures.
- [x] 2.5 Persist bounded validation reports with rule coverage/results, source/contract/image identities, snapshot IDs and execution errors before returning failed status; verify failure samples stay bounded and a crashed/missing report cannot be interpreted as valid data.
- [x] 2.6 Update affected sections of the existing data-products guide to describe the new ports and source-versus-bronze execution commands; verify documented commands against fixtures and `bash scripts/ci-checks.sh contracts`, keeping accepted source contracts unchanged.

## 3. Manifest ingestion and recovery

- [x] 3.1 Implement manifest-selected distributed file verification and source preparation without whole-file driver buffering; verify missing manifests, hash/size/count mismatches and unsupported identities/schemas fail before a table write using small fixtures.
- [x] 3.2 Implement separate event/label Iceberg writes with source-version partition isolation, lossless normalization and stable provenance; verify exact delivered row/content conservation, rejected overflow and no changes to unrelated version partitions.
- [x] 3.3 Persist logical run context before commits and record/reconcile per-table snapshots; inject a stop after a commit but before progress recording and verify recovery neither appends duplicates nor advertises a partial pair as accepted.
- [x] 3.4 Implement accepted-version replay and contract-only revalidation using recorded snapshots; verify repeat v7-shaped input performs no data rewrite, a changed manifest under the same version fails and changed contracts revalidate without re-ingestion.
- [x] 3.5 Implement durable candidate/validated/governance/accepted run states and accepted snapshot-pair records; verify failed candidates do not replace previous acceptance and update the existing data-pipeline guide with replay, pinned consumption and snapshot-retention behavior.
- [x] 3.6 Add the approved version-generic companion inventory command, explicit URI/SHA-256 selection and preflight configuration, and durable run evidence; verify bounded streamed hashes/footer counts, source immutability, create-only lake evidence, original manifest/product/contract bindings, exact file/partition/row/file-byte reconciliation, rejection of missing/stale/tampered/extra/duplicate/escaping evidence and unchanged embedded-inventory behavior. Generate and verify the real v7 companion without claiming live bronze acceptance.

## 4. Shared platform deployment

- [ ] 4.1 In the coordinated platform change, add `fraud-data-prod` to Spark Operator watched namespaces while retaining existing namespace/controller ownership; verify Helm rendering and live reconciliation of a bounded SparkApplication in that namespace.
- [ ] 4.2 Deploy or verify the platform-owned DataHub chart and persistent PostgreSQL, Kafka and supported search dependencies using the inventory from 1.4; verify setup/migrations, authenticated GMS access, private frontend access and restart persistence, and document the course deployment's availability limits in the platform's existing guide.
- [ ] 4.3 Configure DataHub external credentials, workload connectivity and least-privilege publication/readback access; verify no secrets appear in rendered manifests/logs and a data-platform identity can publish/read back only through the configured authenticated endpoint.
- [ ] 4.4 Measure dependency readiness and combined DataHub/Spark scheduling, memory and storage footprint against cluster headroom; record accepted requests/limits/PVC budgets and block rollout if the required workloads cannot coexist.

## 5. Argo execution and application rollout

- [x] 5.1 Add the opt-in `data-pipelines` chart, application-owned data namespace, configuration, ExternalSecrets and scoped workflow/Spark/metadata RBAC; verify existing defaults remain unchanged, enabled prod renders correctly and neither mode installs shared controllers or DataHub services.
- [ ] 5.2 Add shared Argo SparkApplication resource templates with completion/failure conditions, `Never` restart policy, bounded retries/deadlines and serialized writers; verify rendered resources against the installed API and exercise submission-failure, stalled-status and stable-run retry behavior.
- [ ] 5.3 Wire preflight, ingest, validate, governance and acceptance stages with success/failure publication paths; verify deterministic quality failure stays failed, missing reports are execution errors and governance-only replay launches no Spark ingest/validation applications.
- [ ] 5.4 Implement active-application cleanup on cancellation/deadline and diagnostic/report retention independent of resource TTLs; verify a cancelled workflow stops its Spark pods, releases serialization and retains durable run evidence.
- [ ] 5.5 Add dedicated Spark-job and metadata image definitions plus explicit jobs using the existing reusable image workflow; extend existing Python/Helm CI entrypoints for the new project/charts and required Java setup, verifying locked fixture checks and PR image builds without live credentials or publication.
- [x] 5.6 Update only affected deployment/data-pipeline guide sections with prerequisites, enablement, commands and rollback; remove superseded native-submit/operator-candidate wording in this area and verify the documented rendered configuration does not alter service/dev rollout defaults.

## 6. Catalog metadata, lineage and assertions

- [ ] 6.1 Configure native DataHub Iceberg REST ingestion and canonical dataset identity discovery; verify schema readback from RustFS-backed tables and ensure platform-instance/environment naming is shared with all subsequent publishers.
- [ ] 6.2 Publish ODPS-derived source/bronze products, port assets, owners/classifications and ODCS identity/artifact references; verify membership and upstream references by readback without creating one source entity per Parquet file.
- [ ] 6.3 Publish Argo DataFlow/DataJobs, execution outcomes and source-to-bronze lineage including label-validation event dependencies and known direct field mappings; verify graph readback against the actual workflow and pinned source/snapshot references without relying on an Airflow plugin.
- [ ] 6.4 Integrate the GX 1.x DataHub action with explicit canonical Iceberg identities and stable contract-rule/run mapping; verify success, failed-rule and execution-error evidence attaches to existing table assets, using the supported assertion SDK only for a demonstrated plugin limitation.
- [x] 6.5 Implement publication from persisted reports with bounded retries and catalog readback; simulate DataHub unavailability and verify metadata-only recovery preserves outcomes/identities without duplicate table assets or re-ingestion, and document this command in the existing data-pipeline guide.

## 7. Live v7 acceptance

- [ ] 7.1 Enable the prod DP1 application after prerequisites pass and execute complete v7 ingestion/validation with explicit bounded resources; verify 2,215,309 event rows and 442,817 label rows against the accepted manifest, exact normalized content conservation, preserved duplicates and recorded snapshot pairs.
- [ ] 7.2 Replay accepted v7 and exercise interruption after one table commit using an isolated candidate; verify no extra delivered rows, safe recovery, source immutability and preservation of the previous accepted product.
- [ ] 7.3 Execute an isolated invalid candidate and a metadata-outage/recovery run; verify failed assertion visibility, original data outcomes, metadata-only retry and readback of products, schemas, job/table lineage and results under canonical table identities.
- [ ] 7.4 Record revision/image/contract/source/snapshot identities and measured duration, IO/file layout, memory and available shuffle/spill evidence in affected existing pipeline/roadmap sections; verify the final acceptance record distinguishes v7 correctness/governance proof from the separate 100 GB scale work and lists any remaining unverified prerequisites.

## Verification and outstanding prerequisites (2026-10-22)

The final generator-backed ingestion/runtime check passed all 5 tests in 661.21 seconds; the focused manifest/governance/run/lifecycle check passed all 17 tests. Final Ruff, formatting, treefmt and diff checks passed. OpenSpec strict validation passed.

Completed checkboxes represent verified local behavior or the parent’s explicit provisioning evidence, not live DP1 acceptance. Contract lint/generated references, source DuckDB versus bronze Spark SQL parity, nested/conditional schema coverage, generator-backed manifest integrity, local Iceberg recovery/replay/conservation, metadata-outage replay, Helm renders and installed-API server dry runs were exercised. The runtime and metadata images were built locally; final CI builds/publication and immutable deployment digests remain pending.

The canonical generator `inventory`/`key` manifest was verified against the immutable `historical/runtime-process-check-2800fb7138e74bd699970de8f4ccfdc8` export: 31 objects, 3,957 event rows and 882 labels, with every listed hash/size/row count verified. Its manifest SHA256 is `6b2312130f0d6c80e235ca80e187561af203257b983bed49c03fd94f38fb6cde`. Accepted v7 remains unchanged. The user explicitly approved a separate companion inventory for its legacy manifest; the create-only companion at `s3://fraud-lakehouse-prod/dp1-evidence/source-inventories/v7.json` (SHA256 `408e44f10b20eaab9d875578d11fc5daa686b13e465fb04ae068a9d0d777ebf3`, original manifest `020523c95eb960748cfc7953b740c204418479213c7e87f7a7e02d9a7c0a5dd7`) lists 202 Parquet files, 271,619,442 bytes, 2,215,309 event rows and 442,817 label rows; recreation was rejected with HTTP 412 and source object metadata was unchanged. This records current source integrity only; v7 bronze acceptance remains unverified.

Task 1.2 remains incomplete: the bounded live Spark probe returned `Py4JJavaError`; its failed report is retained at `s3://fraud-lakehouse-prod/dp1-evidence/probes/dp1_probe_bacb6ca21284.json`, with its unique test table retained. The live probe created its table and initial append snapshot `9130457916581444078`; PyIceberg subsequently loaded that table metadata. The probe did not complete overwrite/pinned-read verification. Read-only PyIceberg namespace/table listing and the parent's REST create/load/drop and scoped IAM tests passed, but they do not establish complete Spark interoperability. Namespace `bronze` is preprovisioned; runtime checks existence before requesting creation, which is intentionally ungranted. Source S3A disables root bucket probing to respect prefix-scoped listing.

Platform implementation is committed separately in `talos-proxmox` at `88749337eda154bcf5d4c4158567f47feda7a716` on `t3/historical-dp1-bronze`; shared Spark ownership is retained and DataHub remains disabled. Tasks 4.*, 5.2–5.5, 6.1–6.4 and 7.* retain their required live/CI checks. Deployment requires integration and reconciliation of that platform change, measured DataHub/Spark/storage headroom and persistence, DataHub external authentication secrets including `DP1_DATAHUB_TOKEN`, final image digests, successful Spark/Argo probes, authenticated metadata graph/assertion readback and approved v7 integrity evidence. RustFS console-reported free capacity is not retention or reservation proof.


## Deferred source handoff review fixes (2026-10-22)

- [x] Permit the explicit `dp1-spark-gx` skipped-source declaration only with an embedded inventory and a nonblank reason; reject unknown gates and conflicting native-port declarations while retaining the legacy passed-source companion path.
- [x] Persist the declaration in run and validation evidence, reject gate changes on replay and require matching evidence plus passing results from both persisted tables before acceptance; retain the snapshot and governance requirements.
- [x] Verify failed Spark/GX checks and execution-error reports preserve the declaration and do not create acceptance evidence; reconcile the affected pipeline guidance and specs.

In the locked Python 3.12 devenv, all 12 focused manifest/run tests passed in 34.45 seconds. The selected Spark/Iceberg recovery/replay and failed-validation tests plus inventory/governance suites passed all 10 tests in 541.93 seconds. Ruff, formatting and strict OpenSpec validation passed. A clean reinstall of altered shared-cache package files restored the existing lockfile environment; no dependency pin changes were required. These are local checks, not live v7/v8 or cluster/governance acceptance.
