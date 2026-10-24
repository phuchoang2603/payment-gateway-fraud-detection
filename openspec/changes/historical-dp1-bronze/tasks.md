# Tasks

## OpenMetadata revision (2026-10-24)

The approved target replaces DataHub entirely with OpenMetadata, PostgreSQL and OpenSearch, with Argo-owned publication and no Kafka/Airflow dependency. DataHub-related completion and verification below describe the prior implementation, not OpenMetadata readiness. Affected completed tasks are reopened; unrelated ingestion, contracts, source integrity and Spark Operator removal remain verified. Start OpenMetadata with empty state, without DataHub metadata migration/reconstruction; preserve source/lake/evidence data and shared services. Only the platform tasks are authorized for implementation in this phase.

## 1. Compatibility and storage prerequisites

- [ ] 1.1 Pin a compatible Spark/Java/Python/Iceberg/GX/PyIceberg/OpenMetadata server/SDK matrix in the locked runtime; verify Spark startup, contract compilation, ODCS import and existing custom GX success/failure/error publication with one table/lineage example before application rollout. Remove DataHub clients and the GX plugin without weakening current validation coverage.
- [ ] 1.2 Verify the implemented preflight smoke stage in the cluster: create, partition overwrite, snapshot-pinned read, PyIceberg check and table drop in a uniquely named lake table; record catalog-signing versus S3/TLS settings without secret values and persist no evidence beyond workflow status/logs. The standalone probe CLI/module has been removed; live compatibility remains unverified.
- [ ] 1.3 Replace DataHub publication credentials with a least-privilege OpenMetadata identity through the existing external-secret mechanism; reverify source-read/lake-write separation, denied source writes, scoped warehouse access and authenticated metadata publication/readback without exposed secret values.
- [ ] 1.4 Pin supported OpenMetadata chart/server/SDK and PostgreSQL/OpenSearch versions in the platform plan/implementation; verify compatibility, state isolation and rendered requests/limits/PVC inventory against on-prem concurrent Spark headroom. Do not enable Kafka or Airflow for governance.

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
- [ ] 3.5 Update durable governance/accepted run states to require OpenMetadata readback while preserving candidate/validated states and accepted snapshot-pair records; reuse acceptance-boundary checks to verify incomplete contracts/lineage/results cannot replace previous acceptance, and update only affected pipeline guidance.
- [x] 3.6 Add the approved version-generic companion inventory command, explicit URI/SHA-256 selection and preflight configuration, and durable run evidence; verify bounded streamed hashes/footer counts, source immutability, create-only lake evidence, original manifest/product/contract bindings, exact file/partition/row/file-byte reconciliation, rejection of missing/stale/tampered/extra/duplicate/escaping evidence and unchanged embedded-inventory behavior. Generate and verify the real v7 companion without claiming live bronze acceptance.

## 4. Shared platform deployment

- [x] 4.1 Remove the unused Spark Operator controller and its scoped roles; verify live reconciliation leaves Kubeflow Synced/Healthy with no operator workloads, service accounts, roles, bindings or webhooks and no SparkApplication/ScheduledSparkApplication/SparkConnect instances. Three unused Spark Operator CRD definitions remain installed; DP1 uses none of them.
- [x] 4.2 Deploy platform-owned OpenMetadata with isolated persistent PostgreSQL/OpenSearch state, authenticated private UI/API access and no Airflow/Kafka dependency; verify setup/migrations, readiness, authenticated access and metadata persistence across restarts. Existing DataHub readiness is historical evidence only.
- [ ] 4.3 Replace DataHub endpoints, ExternalSecret mappings and access configuration with OpenMetadata equivalents; verify DP1 publication/readback using its scoped bot identity and absence of secret values in rendered manifests and logs.
- [ ] 4.4 Measure combined OpenMetadata/PostgreSQL/OpenSearch and bounded Spark driver/executor scheduling, memory and storage footprint on the on-prem cluster; record accepted requests/limits/PVC budgets and block rollout if they cannot coexist. AWS bursting was declined; previous DataHub measurements and the unschedulable smoke are not OpenMetadata capacity acceptance.

- [x] 4.5 Configure the existing Cloudflare ClusterTunnel binding at openmetadata.phuchoang.sbs and a dedicated Dex OAuth2/OIDC client; grant xuanphuc.a1gv@gmail.com OpenMetadata admin permissions. Verify public HTTPS, callback/login, authenticated admin identity, anonymous API rejection and unchanged Kubeflow/Grafana auth; use native OIDC without adding another OAuth2 Proxy.
- [x] 4.6 Remove all platform DataHub instances, GitOps/chart configuration, TunnelBindings, Dex clients, dedicated credentials and dedicated PostgreSQL/Kafka/OpenSearch resources; verify no active DataHub workloads remain. Start OpenMetadata fresh without migrating metadata, preserving shared operators/services and source/lake/DP1 evidence state.

## 5. Argo execution and application rollout

- [ ] 5.1 Update the opt-in data-pipelines chart configuration, ExternalSecrets and preflight for OpenMetadata; verify prod renders, missing-prerequisite failure and unchanged service/dev defaults through generic Helm checks. The application chart must not install shared catalog/controller services.
- [ ] 5.2 Verify the implemented Argo Spark container template against the installed API and exercise stage failure, deadline and stable-run retry behavior in the cluster; confirm client mode, executor pod templates, owner-referenced executors, bounded retries/deadlines and serialized writers.
- [ ] 5.3 Exercise preflight, smoke, ingest, validate, OpenMetadata publication and acceptance success/failure paths in the cluster; verify deterministic quality failure stays failed, missing reports are execution errors and metadata-only replay starts no Spark stages. Verify per-run source/version/inventory selection remains unchanged.
- [ ] 5.4 Verify cancellation and deadlines remove the stage pod and its executors, release serialization and leave durable run evidence intact.
- [ ] 5.5 Remove DataHub dependencies from the locked Spark/metadata images and add compatible OpenMetadata publication dependencies where needed; verify CI image builds and focused runtime imports/contract checks. Preserve self-contained multi-stage builds, checksum-verified baked jars, cache/test pruning, CI publication, generic chart checks and existing automatic Python project discovery without a shared Spark base.
- [ ] 5.6 Update only affected deployment/data-pipeline/roadmap guidance and secret/endpoint references for the complete OpenMetadata switch; verify rendered examples and record the explicit coursework DataHub UI wording as an equivalent-tool evidence difference without claiming instructor approval.

## 6. Catalog metadata, lineage and assertions

- [ ] 6.1 Register RustFS-backed Iceberg schemas with one canonical OpenMetadata service/database/schema/table identity; verify authenticated REST-catalog/S3 discovery and schema readback. Use the supported Iceberg connector where compatible, or existing PyIceberg discovery with supported entity APIs if its REST configuration is insufficient, without a second catalog or Spark-named duplicates.
- [ ] 6.2 Publish ODPS-derived source/bronze data products, membership, owners/classifications and import Git-authoritative ODCS contracts with original identities, versions, hashes and artifact references; verify readback without per-file assets or automatic duplicate quality-test creation.
- [ ] 6.3 Publish Argo pipeline/tasks, actual execution outcomes and source-to-bronze job/table lineage, label-validation event dependencies and direct field mappings through OpenMetadata SDK/API; verify graph/run readback against workflow, source and pinned snapshot references without Airflow instrumentation.
- [ ] 6.4 Integrate the supported OpenMetadata GX action with explicit suite/table mapping and stable semantic rule/run identities; verify persisted custom Spark GX success, failed-rule and execution-error results attach to existing tables. Use supported test-definition/test-case/result APIs only for demonstrated action limitations; refuse incomplete governance rather than dropping evidence.
- [ ] 6.5 Replace DataHub publication/recovery and acceptance readback with OpenMetadata publication from persisted reports, bounded retries and idempotent result identities; reuse outage/replay checks to verify no Spark rerun, duplicate assets/results or changed data outcomes, and document metadata-only recovery in the existing guide.

## 7. Live v7 acceptance

- [ ] 7.1 Enable the prod DP1 application after prerequisites pass and execute complete v7 ingestion/validation with explicit bounded resources; verify 2,215,309 event rows and 442,817 label rows against the accepted manifest, exact normalized content conservation, preserved duplicates and recorded snapshot pairs.
- [ ] 7.2 Replay accepted v7 and exercise interruption after one table commit using an isolated candidate; verify no extra delivered rows, safe recovery, source immutability and preservation of the previous accepted product.
- [ ] 7.3 Execute an isolated invalid candidate and an OpenMetadata outage/recovery run; verify failed-rule and execution-error visibility, preserved data outcomes, metadata-only retry and readback of products, schemas, imported contracts, job/table lineage and results under canonical identities.
- [ ] 7.4 Record revision/image/contract/source/snapshot identities and measured duration, IO/file layout, memory and available shuffle/spill evidence in affected existing pipeline/roadmap sections; verify the final acceptance record distinguishes v7 correctness/governance proof from the separate 100 GB scale work and lists any remaining unverified prerequisites.

## 8. Remove temporary scaffolding

- [ ] 8.1 After a v8 or later export that carries an embedded inventory is ingested, validated, governed and accepted, remove the v7 companion path: `source-inventory` stage and companion creation in `data_platform/inventory.py`, `--inventory-*` options, companion selection in manifest/run records, the chart's `SOURCE_INVENTORY_URI`/`SOURCE_INVENTORY_SHA256` values and validation, their tests, documentation and spec scenarios, and the lake `source-inventories/v7.json` object. Keep accepted v7 run records and snapshots.
- [ ] 8.2 Run the repository CI entrypoints and strict OpenSpec validation, and confirm no companion or v7-specific configuration remains outside accepted run records.

## Current verification and outstanding prerequisites

PR #84 at `3c22e17` passes all applicable CI checks, including Python, contracts, Helm and both data-platform image builds; the publication/pinning job is skipped for the PR. Local verification passed all 34 bronze tests before the build simplification, followed by focused standard-Maven Iceberg write/read and network-disabled Spark-image checks covering imports, contract compilation, Parquet, GX, bundled jars, Iceberg write/read and Python workers. Ruff, formatting, workflow lint and strict OpenSpec validation passed. The simulator resource-metrics snapshot fix and its regression test are in parent PR #83 (`aa3540d`); #84 has no simulator diff.

The fresh OpenMetadata platform is deployed; DP1 integration and acceptance remain unverified. Reopened tasks cover its compatibility matrix, credentials, workflow/image configuration, publication/recovery and final acceptance gate; task 4.5 covers Tunnel/Dex/admin access and task 4.6 covers DataHub retirement without metadata migration. Existing source/bronze checks remain useful, but prior DataHub local/CI/live evidence does not verify the new integration. Next prove one table/ODCS/GX/lineage example and combined on-prem capacity before Spark smoke and live v7 acceptance. Temporary companion removal remains deferred until its existing acceptance prerequisite is met.

## Historical DataHub platform inspection (2026-10-23 UTC)

Read-only inspection used `admin@prod-talos`. Argo CD `datahub` and `kubeflow` are Synced/Healthy at platform revision `980b411bb474931c9ab8a4ec87a80a88149b4d18` on `main`, with successful sync operations. DataHub GMS/frontend `v1.7.0.1`, CNPG PostgreSQL, Strimzi Kafka `4.2.1` and OpenSearch `2.19.5` are Ready. Both system-update jobs completed. PostgreSQL/Kafka/search PVCs are Bound at 10/10/20 GiB. All four DataHub ExternalSecrets and both DP1 ExternalSecrets report SecretSynced/Ready; GMS has `METADATA_SERVICE_AUTH_ENABLED=true`. Both DP1 WorkflowTemplates exist in `fraud-data-prod`. GMS `/health` returns HTTP 200 through a temporary Kubernetes API port-forward, and an unauthenticated entity read returns HTTP 401. Direct access to its LAN LoadBalancer address timed out from this host; the port-forward was closed after inspection.

No Spark Operator controller, scoped/cluster RBAC, service account or webhook remains, and no SparkApplication, ScheduledSparkApplication or SparkConnect instances exist. Their three CRD definitions remain installed. Only application-owned `dp1-spark` RBAC/service-account resources remain for operatorless Spark execution. Kubeflow's Application is Healthy.

DataHub's five running pods currently use about 4.3 GiB combined memory; node memory utilization is 60%, 68% and 74%. These point-in-time readings do not prove scheduling headroom or peak capacity with Spark. Bound PVCs and restarted healthy pods do not by themselves prove retained governance metadata. No authenticated DP1 publication/readback, live smoke, cancellation, ingestion or acceptance was performed during this inspection. These observations describe the retiring DataHub deployment and inform resource planning; they do not verify the OpenMetadata replacement.

## Historical execution attempt with DataHub (2026-10-24 UTC)

The current chart passed installed-API server dry run and was applied only to `fraud-data-prod`, using the CI-published Spark image `sha256:6224ce577b8fa94d3edd4a8c929d2c180ee61552be577921d90865b84bd41c9f` and metadata image `sha256:cf0ec87db44db2729e990a2e2f32ca98ce7dfb1c2c0312617fb8a8a3bbc832ec` from run `40056568770` (PR head `3c22e17`, merge revision `13b642b77ff378d1220fae8132b42556321d58ec`). No local images were built or published for this attempt.

The metadata readiness pod successfully authenticated to GMS `/health` and `/config` (HTTP 200) using the DP1 token, and PyIceberg listed namespace `bronze` with no tables. The v7 source manifest exists, but its previously recorded companion object returned 404. The bounded, create-only `source-inventory` command regenerated the companion successfully in about two minutes. Both the original manifest digest `020523c95eb960748cfc7953b740c204418479213c7e87f7a7e02d9a7c0a5dd7` and companion SHA-256 `408e44f10b20eaab9d875578d11fc5daa686b13e465fb04ae068a9d0d777ebf3` match the previous evidence exactly: 202 files, 271,619,442 bytes, 2,215,309 event rows and 442,817 labels. Source objects remain unchanged; task 3.6 is reverified.

Smoke workflow `dp1-smoke-2pcnz` could not schedule its 500m CPU/2560Mi driver: `0/3 nodes are available: 1 Insufficient memory, 2 Insufficient cpu`. Approximate remaining container-request capacity was 100m CPU/8.1 GiB on server1, 2470m/1.52 GiB on server2 and 60m/6.7 GiB on server3. No existing node fits the driver, and acceptance also requires an executor. The workflow was terminated, its pending pod removed and its writer lock released. No Spark executor, smoke table or bronze write was created; this does not prove active-executor cancellation.

The existing `aws-burst` NodePool is Ready with no NodeClaims, but rejects these pods without its stateless-workload toleration. The user declined AWS bursting. Task 4.4 blocks ingestion until the on-prem deployment supports OpenMetadata plus the bounded driver/executor together. This terminated workflow remains a failed scheduling attempt, not proof of Spark lifecycle behavior.

## Historical verification and outstanding prerequisites (2026-10-22)

The final generator-backed ingestion/runtime check passed all 5 tests in 661.21 seconds; the focused manifest/governance/run/lifecycle check passed all 17 tests. Final Ruff, formatting, treefmt and diff checks passed. OpenSpec strict validation passed.

Completed checkboxes represent verified local behavior or the parent’s explicit provisioning evidence, not live DP1 acceptance. Contract lint/generated references, source DuckDB versus bronze Spark SQL parity, nested/conditional schema coverage, generator-backed manifest integrity, local Iceberg recovery/replay/conservation, metadata-outage replay, Helm renders and installed-API server dry runs were exercised. Service-image CI builds and publishes the runtime and metadata images; live deployment and acceptance remain pending.

The canonical generator `inventory`/`key` manifest was verified against the immutable `historical/runtime-process-check-2800fb7138e74bd699970de8f4ccfdc8` export: 31 objects, 3,957 event rows and 882 labels, with every listed hash/size/row count verified. Its manifest SHA256 is `6b2312130f0d6c80e235ca80e187561af203257b983bed49c03fd94f38fb6cde`. Accepted v7 remains unchanged. The user explicitly approved a separate companion inventory for its legacy manifest; the create-only companion at `s3://fraud-lakehouse-prod/dp1-evidence/source-inventories/v7.json` (SHA256 `408e44f10b20eaab9d875578d11fc5daa686b13e465fb04ae068a9d0d777ebf3`, original manifest `020523c95eb960748cfc7953b740c204418479213c7e87f7a7e02d9a7c0a5dd7`) lists 202 Parquet files, 271,619,442 bytes, 2,215,309 event rows and 442,817 label rows; recreation was rejected with HTTP 412 and source object metadata was unchanged. This records current source integrity only; v7 bronze acceptance remains unverified.

An earlier local Spark probe was inconclusive and has been replaced by the workflow smoke stage. Task 1.2 remains open until that stage proves live compatibility. Namespace `bronze` is preprovisioned; runtime checks existence before requesting creation, which is intentionally ungranted. Source S3A disables root bucket probing to respect prefix-scoped listing.

At this earlier verification point, platform implementation was committed separately in `talos-proxmox` at `88749337eda154bcf5d4c4158567f47feda7a716` on `t3/historical-dp1-bronze`; the then-current platform change retained shared Spark ownership and left DataHub disabled. The later follow-up below records DataHub running; those historical checks did not establish live DP1 acceptance. That revision required DataHub configuration including `DP1_DATAHUB_TOKEN`; the OpenMetadata migration replaces it and re-verifies credentials, headroom, persistence, final image digests, smoke and governance readback. RustFS console-reported free capacity is not retention or reservation proof.


## Deferred source handoff review fixes (2026-10-22)

- [x] Permit the explicit `dp1-spark-gx` skipped-source declaration only with an embedded inventory and a nonblank reason; reject unknown gates and conflicting native-port declarations while retaining the legacy passed-source companion path.
- [x] Persist the declaration in run and validation evidence, reject gate changes on replay and require matching evidence plus passing results from both persisted tables before acceptance; retain the snapshot and governance requirements.
- [x] Verify failed Spark/GX checks and execution-error reports preserve the declaration and do not create acceptance evidence; reconcile the affected pipeline guidance and specs.

In the locked Python 3.12 devenv, all 12 focused manifest/run tests passed in 34.45 seconds. The selected Spark/Iceberg recovery/replay and failed-validation tests plus inventory/governance suites passed all 10 tests in 541.93 seconds. Ruff, formatting and strict OpenSpec validation passed. A clean reinstall of altered shared-cache package files restored the existing lockfile environment; no dependency pin changes were required. These are local checks, not live v7/v8 or cluster/governance acceptance.

## Historical DataHub follow-up verification (2026-10-22)

At that revision, PR #84 passed all nine checks, including 35 DP1 tests and both image builds. The recorded platform follow-up found DataHub GMS/frontend and its persistent dependencies running with synced external secrets. Authenticated governance and restart persistence remain unverified.

The follow-up removes duplicate tests and reuses one source preparation across SQL parity cases, reducing the suite from 38 to 35 cases and its shared fixture from 20 to 8 customers. The fixture still spans both source schemas and produces 21 labels for the 20-row failure-sample bound. Full integration verification runs in CI. A local REST/Spark probe exceeded its 240-second limit without proving interoperability; no v7 ingestion or acceptance is claimed.


## Fresh OpenMetadata platform verification (2026-10-24 UTC)

Platform-only deployment is in `talos-proxmox` PR #65 at `722b8fc`, including the latest platform `main`. Argo's platform root currently follows `t3/openmetadata-platform`; return it to `main` after merging. The DataHub Application, namespace, workloads, dedicated PVCs, TunnelBinding, Dex client and seven platform `DATAHUB_*` credentials were removed. No metadata was migrated; shared operators/services and source/lake/DP1 evidence were retained. Application DataHub publishers and configuration are outside this platform-only deployment and remain pending replacement.

OpenMetadata chart/server 2.0.6, CNPG PostgreSQL 16.13 and OpenSearch chart 2.39.0/image 2.19.5 initialized successfully without Airflow or Kafka. Four ExternalSecrets are Ready; dedicated PostgreSQL/OpenSearch PVCs are Bound at 10/20 GiB. Native OpenMetadata/Dex confidential authorization-code login, callback and session refresh succeeded with PKCE; `xuanphuc.a1gv@gmail.com` read back as `isAdmin: true`. The Cloudflare TunnelBinding is configured, public HTTPS returned 200 and anonymous metadata reads returned 401. Existing Grafana/Kubeflow login routes still reach Dex. The local LAN resolver still negatively caches the new hostname; HTTPS checks used public Cloudflare resolution with the original hostname and TLS verification.

Configured steady requests total 600m CPU and 4.5 GiB memory, with limits of 5 CPU and 7 GiB. A post-bootstrap reading measured approximately 2.5 GiB combined memory; this is not peak or concurrent Spark capacity acceptance. Platform CI, installed-API dry run, Helm lint, formatting and strict OpenSpec validation passed. DP1 SDK/publication, bot permissions, workflow/image changes, combined Spark capacity and live v7 acceptance remain open.

After sequential PostgreSQL, OpenSearch and OpenMetadata server restarts, native Dex login/admin readback and anonymous API rejection passed again. The admin user ID, Certification classification ID and indexed admin search result matched the pre-restart values. Argo platform/auth/OpenMetadata/Kubeflow were Synced/Healthy after the rollout. Tasks 4.2, 4.5 and 4.6 are complete; this does not complete application integration or live DP1 acceptance.
