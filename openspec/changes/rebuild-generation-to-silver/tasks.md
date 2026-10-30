## 1. Shared entity world (part of the generator PR)

- [x] 1.1 Implement `simulator/world.py` with the interface in design.md decision 1 (`WorldConfig`, `World.build`, `digest`, `start_date`, `day_purchases`, `purchase` and `card_test` keyed by day, customer and purchase number, `SCENARIOS`/`CATEGORIES`/`CONDITIONS`, unchanged `entity_id`), built from configuration and seed only. The live simulator now uses this interface in this PR, so the live change rebases onto it. Verify with the identity-vector test, moved to `world.py`, passing in CI.
- [x] 1.2 Done in PR #100: `simulator/live/` imports `world.py`, `HistoricalModel` builds the world from the manifest and checks its digest, and the old modules are deleted. Verify in CI that `test_live.py` passes against `world.py`. Tick after CI passes.

## 2. Contracts reset to v1

- [x] 2.1 Replace `contracts/data-products/historical-payments` with v1 ODPS/ODCS for the v18 layout: schema versions 2/3 declared by `sinceSchemaVersion: 3` on the added fields, `schema_version` as the only version column, manifest as completion marker, no feature-definition reference, no `versions/` or physical-schema files. Verify with the CI contract lint and the regenerated references.
- [x] 2.2 Replace `contracts/data-products/bronze-payments` with v1 contracts for the shared Bronze tables (historical and Kafka provenance, partitioning) and Bronze raw-schema SQL rules. Verify with the CI contract lint.
- [x] 2.3 Add `contracts/data-products/silver-payments` (product plus `payment-events`, `attempts`, `labels` contracts with Silver SQL rules) and delete `contracts/quality/` and `contracts/landing/`. Verify with the CI contract lint and `rg` finding no references to the deleted paths.

## 3. Historical generator rewrite

- [x] 3.1 Rewrite the configuration model with dates, counts, skew, the hot merchant share, `duplicate_rate` as the republished-export rate, ordered `schema_changes` (dates and version 3 only), label delays, foreign-IP probability, optional `minimum_source_bytes` and output root. Run settings (mode, shard, workers, contract directory) come from the environment, so the YAML holds no chunk, worker or row-group settings. Replace `configs/historical-smoke.yaml` with `historical-dev.yaml` (v18: 2026-07-31 to 2026-10-28) and `historical-full.yaml` (v19: 2025-10-29 to 2026-10-28, 3M customers, 100 GB minimum). Reject invalid schedules before writing. Verify with an invalid-schedule behaviour test in CI.
- [x] 3.2 Rewrite generation as per-day units on `world.py` keyed by day, customer and purchase number: lifecycle histories, scenarios, truth and chargeback labels, duplicates, the gateway fraud policy with both reason codes, and schema versions projected from the contract. Verify in CI that records are equal across split and worker count, that interruption does not change them, and that the schema boundary and duplicates hold.
- [x] 3.3 Implement storage:
  - write `config.json` first, recording the generator digest, and refuse a changed configuration or digest;
  - write atomic parts under `date=`, one per schema version for events (multipart upload for large parts), and a per-day `_day.json` marker once all of the day's parts finish;
  - skip complete days, regenerate incomplete ones, and submit all pending parts of a shard together;
  - write `manifest.json` last and only if absent: logical configuration, generator digest, contract identities, totals and the file list, refusing below `minimum_source_bytes`;
  - work the same for local and S3 roots.

  Verify that CI tests for rerun, incomplete-day regeneration, changed-config and changed-contract refusal, published-version refusal and the undersized-source refusal pass.
- [x] 3.4 Delete the replaced modules (`profiles`, `behavior`, `identity`, `calibration` if unused, `fragment_index`, `publication`, `resources`, `run_state`, `statistics`, `object_store` as superseded), `notebooks/historical-sanity.ipynb` and their tests, after 1.2. Keep `simulator/landing/` until 7.2 removes its deployment, adapting only its schema import if needed. Verify that `rg` finds no imports of deleted modules and that ruff passes locally.
  - PR #100 note: deleted the replaced generator modules and their tests, `simulator/landing/`, the sanity notebook, the `notebook` dependency group, and `config.py`, `profiles.py`, `behavior.py`, `identity.py` and `calibration.py` after `live/` moved to `world.py`. `arrow.py` stays for the generator's Arrow schemas. Tick after CI passes.

- [x] 3.5 Add the `generate-historical` WorkflowTemplate to `infra/charts/data-pipelines` as a DAG: `generator.shards` shard pods in parallel, then one publish pod, both built from one named container template in `_helpers.tpl`. Settings come from the environment (`GENERATOR_MODE`, `SHARD_INDEX`/`SHARD_COUNT`, `GENERATOR_WORKERS` from the CPU request). RBAC is a ServiceAccount with a Role for `workflowtaskresults` `create, patch` and its RoleBinding, plus the `gateway-storage` ExternalSecret. Enable the chart in prod through `infra/argocd/app-of-apps/values-prod.yaml` with only this workflow. Verify with CI helm lint and template showing the shard and publish steps and the prod shard count. The generator-only values switch is removed because nothing else renders.
- [x] 3.6 Land groups 1, 2.1 and 3 as the generator PR to `main` (with user confirmation), leaving old DP1 code, the archiver and the Bronze contracts for the pipeline PR, except for upstream contract references that CI lint requires. Verify that all CI checks pass on the PR and on `main`, and that the dev and prod releases record the new simulator image.
- [x] 3.7 Submit `generate-historical` in dev to publish v18. Measure rows per second and peak memory per worker, and set the prod resources and deadline from them. Project v19's bytes before submitting prod: divide v18's manifest `physical_bytes` by its purchase count (the `payment_created` rows across its parts) for bytes per purchase, then multiply by v19's expected purchases (customers × `purchases_per_customer_day` × 365). If the projection is under about 110 GB, raise the v19 customer count in `historical-full.yaml` before submitting prod. Verify that `historical/v18/manifest.json` exists in `platform-dev` with both schema versions and the expected duplicate rate. Result: v18 published in 11m50s with 823,963 events, 161,007 labels and 112.4 MB; 1.996% duplicates; 56.3% top-merchant share; about 115 purchases/s per worker and 721 bytes per purchase, projecting v19 at about 137 GB. Peak memory per worker was not captured.
- [x] 3.8 With user confirmation, submit `generate-historical` in prod for v19, and continue groups 4–9 on dev while it runs. Verify that `historical/v19/manifest.json` exists in the prod bucket with at least 100,000,000,000 physical bytes, or report the observed failure and the completed day count. Result: v19 published in 49 minutes on 12 spot shards (ten finished in about 25 minutes; one started late at the pool's CPU limit). 983,727,083 events and 192,204,996 labels in 2,924 parts over 365 days, 133.5 GB, no empty parts; sampled parts show about 2.0% republished duplicates and a 56% top-merchant share.

## 4. Batch project and Bronze

- [x] 4.1 Create `src/data-pipeline/batch` (one pyproject and lockfile, Spark session, catalog and storage settings from the environment, a payments descriptor set via the `codegen:proto` `descriptor` option) and delete `src/data-pipeline/bronze-ingestion`. Verify that `uv lock` and ruff pass locally and the CI Python job imports the package.
  - PR #100 note: reduced `src/data-pipeline/bronze-ingestion` to `data_platform/integrations.py` and `catalog.py` for metadata integrations, and removed its tests, config and Spark image. The `batch` project replaces it in the pipeline change.
- [x] 4.2 Implement Bronze historical mode: list the dataset root once and compare its parquet objects with the manifest (missing, extra or size-mismatched files fail before any read); read with `mergeSchema`; cast to the Bronze schema, failing the run on any lossy cast; overwrite this version's rows with `writeTo(...).overwrite(_dataset_version = v)` so a rerun is idempotent; compare rows written per source file with the manifest and name the file on mismatch; write the run report. Verify with CI tests for each file-inventory failure, the per-file row mismatch, idempotent rerun and preserved duplicates.
- [x] 4.3 Implement Bronze Kafka mode: each partition resumes at the next offset after the highest `_kafka_offset` the table already holds for the topic, or at the configured starting offsets for partitions without rows; no snapshot property records progress; read with `failOnDataLoss`; decode with `from_protobuf` and reject records that fail decoding or a required-field check into `bronze.rejected_records`, committed before the accepted rows; a value that decodes but fails a cast fails the run; empty and failed runs commit nothing; the run report records offsets per partition and the rejected count. Verify with CI tests on a Kafka test container for offset resume and empty runs, rejected reasons, a compaction between runs that replays no offset, and decoding parity with historical Parquet (same Bronze schema and values).

## 5. Silver

- [ ] 5.1 Implement event deduplication and conflict handling over pinned Bronze snapshots: identical copies collapse; a conflicting payment is written in full to `silver.rejected_events` and excluded from the accepted tables; the run report counts conflicts and rejected rows before the write. Verify that CI tests for identical collapse, conflict quarantine (the accepted tables exclude the payment and its rejected rows are complete) and the zero-historical-rejections check pass.
- [ ] 5.2 Implement `silver.attempts` (top-level `merchant_id`, `customer_id`, `payment_method_fingerprint` and `amount_minor` from AttemptStarted; start, client `attempted_at`, decision, outcome, payment status, lifecycle validity). Verify that CI tests for retried, in-flight and lifecycle-invalid attempts pass.
- [x] 5.3 Implement `silver.labels` with observation identity, `linkage` classes and availability against attempt start and success. Verify that CI tests cover each linked, pending and violation case, including the two linkage defect cases an earlier attempt got wrong.
- [x] 5.4 Write all three tables as one full replacement each, recording Bronze snapshot IDs, and complete the run report. Verify that a CI rerun-equality test passes.

## 6. Checks and governance

- [x] 6.1 Derive native OpenMetadata test definitions from Bronze and Silver ODCS rules, failing preparation on any uncovered rule. Verify with a CI preparation test over the real contracts.
- [x] 6.2 Implement the checks stage (native discovery for the run's tables, then the test suite, success only if all expected tests pass) and the publish stage (products, contracts, Kafka/historical → Bronze → Silver lineage, pipeline execution status on success and failure). Remove the old readback, accept and replay code. Verify with focused CI tests of result evaluation and lineage construction.

## 7. Workflows, images and CI

- [x] 7.1 Rewrite `infra/charts/data-pipelines`:
  - `dp1-bronze` and `dp2-silver` workflows (Spark and checks, with the exit-handler publish), each with one retry level and a mutex, and SparkApplications created by `resource` steps with owner references;
  - an on-demand benchmark template;
  - a suspended Kafka-mode CronWorkflow;
  - v18 defaults;
  - a Kafka read identity.

  Remove the Spark utility templates and the old graph. Verify with CI helm lint and template for all values files.
- [x] 7.2 Delete `simulator/landing/` and its tests, and remove the archiver workload, values and permissions from `infra/charts/live-traffic` and its consumer group configuration. Verify that `helm template` renders no archiver, and coordinate the diff with the live change.
  - PR #100 note: removed the archiver workload, values, live-source gate for it, its network policy and the `payments-parquet-landing-v17` group from `infra/charts/live-traffic`, and deleted `contracts/landing/`. The live simulator's Deployment and config are unchanged.
- [ ] 7.3 Update `infra/docker/data-platform-{spark,metadata}.Dockerfile` (pinned checksums for the Kafka and protobuf Spark connectors and their transitive jars, batch package, contracts) and `infra/docker/live-simulator.Dockerfile` (no landing). Make the `.github/service-images.json` paths cover every copied directory, and update `.github/workflows/python-ci.yml` paths. Verify that CI image builds succeed and the release plan rebuilds the images on a contracts-only change.

- [x] 7.4 Add the `maintain-lakehouse` WorkflowTemplate, on demand, and a suspended daily CronWorkflow to `infra/charts/data-pipelines`. One Spark stage runs, for each Bronze and Silver table: `rewrite_data_files` to the 256 MB target (`binpack`, or `sort` by `zorder(customer_id,payment_method_fingerprint)` for `silver.attempts`), `rewrite_manifests`, and `expire_snapshots` (`older_than` seven days, `retain_last` 10). The report gives files and bytes before and after from each table's `files` metadata table, the planned files and planning time for one customer's 30-day window on `silver.attempts` before and after the Z-order rewrite, and the planned files for a one-day filter versus no filter on partitioned Bronze. Verify with CI helm lint and template, the compaction test in 4.3, and the planning test that a one-day filter plans fewer files.

## 8. Benchmark

- [ ] 8.1 Implement the benchmark job over one pinned Silver snapshot: a skewed merchant join as a sort-merge join with three variants (AQE skew join off, AQE skew join on, salting into 16 buckets), each run once and timed through the phase helper. Each variant's output is reduced to its row count and an order-independent checksum and compared with the baseline. The report gives each variant's duration, Spark job, stage and task IDs, and the hot-key share with its denominator. Verify with CI tests that every variant returns the baseline output, that an unequal row set and a same-count changed value each fail the guard, that the salted join matches the baseline for the hot merchant, and that the hot-key share uses all attempts as its denominator. Also run the `dedup` family over the pinned Bronze events: a full-width group-by and a narrow detection variant that joins at the strategy Silver chooses, each run once and timed, reporting wall time and spill per variant, with the same row count and checksum guard. Verify with CI tests that both variants return the same output and that the narrow dedup equals the full-width pass with a shuffled and with a broadcast join.

## 10. ClickHouse instance

- [x] 10.1 Add `infra/charts/clickhouse` (ClickHouseInstallation, `clickhouse` Service, one ExternalSecret per user, PostSync setup Job) and its app-of-apps entry for dev and prod. Verify with CI helm lint and template for every values file.
- [x] 10.2 Move the `openmetadata` user out of `lakehouse`; its setup creates only the `lakehouse` DataLakeCatalog database as admin, after the instance accepts connections. Verify with CI helm template.
- [ ] 10.3 Confirm the chart defines no operator management user and no NetworkPolicy that could block the operator in namespace `operators`. Verify by grep and CI helm template.
- [ ] 10.4 Cluster, blocked until the dev rebuild and readiness: the instance is ready, the three users exist, `langfuse` exists, the `lakehouse` database is created, and `openmetadata` reads `lakehouse.*` with its profile applied. Run the spike items in design.md section 13, including the exact OpenMetadata FQN and the masked credentials in `SHOW CREATE DATABASE` and `system.query_log`.

## 9. Docs, specs and final checks

- [x] 9.1 Add the generator documentation (now `docs/data/offline.md`: configuration, schema changes, layout, rerun) and remove landing and old DP1 instructions. Bronze and Silver results go in `docs/data/offline.md`, `storage.md` and `orchestration-governance.md`. Verify that links resolve and generated references match.
- [ ] 9.2 At archive time, update the Purpose of `historical-bronze-ingestion` and `live-traffic-simulation` to drop landing batches. Verify that `openspec validate --strict` passes.
- [x] 9.3 Run the local non-test checks (ruff, treefmt, helm lint, `openspec validate rebuild-generation-to-silver --strict`, `git diff --check`), push, and inspect every CI check. Verify that all required checks pass at the pushed head, reporting any pending or failed check honestly.

## 11. Lakehouse catalog

- [ ] 11.1 Merge `lakehouse-integration` and the Polaris component into `infra/charts/lakehouse`: vendored Polaris 1.8.0 with `fullnameOverride: iceberg-catalog`, the CNPG store, the `iceberg-database`, `iceberg-bootstrap`, `iceberg-signing`, `iceberg-storage` and `lakehouse-admin` ExternalSecrets, the network policies, a warehouse registration PostSync job and a ClickHouse `lakehouse` DataLakeCatalog PostSync job. One app-of-apps entry in `data`, sync wave 1, enabled in dev and prod. Verify with CI helm lint and template for every values file and the app-of-apps render for both environments.
- [ ] 11.2 Push only after the platform's Polaris is pruned from dev. Verify at the pushed head that `deploy/dev` creates `lakehouse` and `clickhouse`, and that the registration and database jobs succeed after retries, and that `iceberg-catalog.data.svc.cluster.local:8181` answers.

## 12. Evidence and archive

- [ ] 12.1 Write Spark event logs for every application to `runs/payment-gateway/spark-events/` in dev and prod (`spark.eventLog.*` in the Spark application helper, `sparkEventLogDir` in values). Verify with `helm template` for both environments and by reading the event log directory after a dev run.
- [ ] 12.2 Capture the evidence screenshots and save them under `docs/data/images/`:
  - Spark UI (`docs/data/offline.md`): DP1 Bronze historical and DP2 Silver jobs with jobs, stages and durations; the skew benchmark's baseline stage task-duration distribution showing the straggler task, the AQE variant's SQL plan with the skew-split join, and the salted variant's stage; the dedup benchmark's shuffle read, shuffle write and spill for `full_width_group` and `narrow_detect`; the Bronze read plan or the v2/v3 null-count query result for schema evolution.
  - Argo Workflows UI (`docs/data/orchestration-governance.md`): successful runs of `dp1-bronze` (Spark, then checks, with the publish exit handler), `dp2-silver`, `maintain-lakehouse` and `dp2-benchmark`; the `dp1-bronze-kafka` CronWorkflow and one of its runs.
  - OpenMetadata UI (`docs/data/orchestration-governance.md`): the Bronze and Silver tables in the ClickHouse service; the lineage graph from the historical source and the Kafka topics through Bronze to Silver, with the Argo pipelines on the edges; the pipeline entities with run status; the data-quality results per table.
  - Storage (`docs/data/storage.md`): the `maintain-lakehouse` report's before and after files and bytes, with the Z-order and partition probes.
- [ ] 12.3 Write the recorded prod v19 results into `docs/data/offline.md` (run IDs, row counts, duplicates collapsed, conflicts, rejected rows, label linkage, task counts and stage times, check outcomes) and link the screenshots from the pages named in 12.2. Replace the pending `Dev runs` text where it no longer applies.
- [ ] 12.4 After the prod v19 evidence run is recorded: tick 12.2 and 12.3, sync the delta specs to `openspec/specs/`, and run `openspec archive rebuild-generation-to-silver --yes` in a docs-only follow-up commit on main.
- [ ] 12.5 Open #102 squash-merge only after review, with this change still open. Tasks 9.2 and 12.2 to 12.4 stay open until the archive step.

## 13. Catalog registration

- [ ] 13.1 Fold `metadata-integrations` into `data-pipelines`: connector configuration and ExternalSecrets, a PostSync register job, and an on-demand discovery template. Delete the chart, its Application and its Kafka client. Verify that `deploy/dev` prunes the old Application and that the register job completes.
- [ ] 13.2 Rerun the checks on dev after the fold, and confirm that the ClickHouse tables, native test results and lineage still appear in OpenMetadata.
- [ ] 13.3 Blocked: import ODCS contracts with `PUT /v1/dataContracts/odcs/yaml` once OpenMetadata accepts the ODCS v3 files as written. Blocked on OpenMetadata 2.0.6; see design section 16 and the open questions.
- [ ] 13.4 Blocked: publish data products once OpenMetadata supports ODPS natively, or with a minimal product create.
- [ ] 13.5 At archive, move the requirements of `openspec/specs/metadata-integrations` into `data-pipeline-orchestration` and remove that capability.
