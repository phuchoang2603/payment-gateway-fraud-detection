## 1. Shared entity world (part of the generator PR)

- [x] 1.1 Implement `simulator/world.py` with the interface in design.md decision 1 (`WorldConfig`, `World.build`, `digest`, `start_date`, `day_purchases`, `purchase` and `card_test` keyed by day, customer and purchase number, `SCENARIOS`/`CATEGORIES`/`CONDITIONS`, unchanged `entity_id`), built from configuration and seed only. The live simulator now uses this interface in this PR, so the live change rebases onto it. Verify with the identity-vector test, moved to `world.py`, passing in CI.
- [x] 1.2 Done in PR #100: `simulator/live/` imports `world.py`, `HistoricalModel` builds the world from the manifest and checks its digest, and the old modules are deleted. Verify in CI that `test_live.py` passes against `world.py`. Tick after CI passes.

## 2. Contracts reset to v1

- [x] 2.1 Replace `contracts/data-products/historical-payments` with v1 ODPS/ODCS for the v18 layout: schema versions 2/3 declared by `sinceSchemaVersion: 3` on the added fields, `schema_version` as the only version column, manifest as completion marker, no feature-definition reference, no `versions/` or physical-schema files. Verify with the CI contract lint and the regenerated references.
- [ ] 2.2 Replace `contracts/data-products/bronze-payments` with v1 contracts for the shared Bronze tables (historical and Kafka provenance, partitioning) and Bronze raw-schema SQL rules. Verify with the CI contract lint.
- [ ] 2.3 Add `contracts/data-products/silver-payments` (product plus `payment-events`, `attempts`, `labels` contracts with Silver SQL rules) and delete `contracts/quality/` and `contracts/landing/`. Verify with the CI contract lint and `rg` finding no references to the deleted paths.

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

- [ ] 4.1 Create `src/data-pipeline/batch` (one pyproject and lockfile, Spark session, catalog and storage settings from the environment, a payments descriptor set via the `codegen:proto` `descriptor` option) and delete `src/data-pipeline/bronze-ingestion`. Verify that `uv lock` and ruff pass locally and the CI Python job imports the package.
  - PR #100 note: reduced `src/data-pipeline/bronze-ingestion` to `data_platform/integrations.py` and `catalog.py` for metadata integrations, and removed its tests, config and Spark image. The `batch` project replaces it in the pipeline change.
- [ ] 4.2 Implement Bronze historical mode: manifest verification, `mergeSchema` read, lossless cast to the Bronze schema, overwrite by `_dataset_version`, manifest row-total check, run report. Verify that CI tests for altered-file failure, idempotent rerun and preserved duplicates pass.
- [ ] 4.3 Implement Bronze Kafka mode: each partition's start is the next offset after the highest `_kafka_offset` the table already holds for the topic, or the configured starting offsets for partitions without rows; no snapshot property records progress; `from_protobuf` decoding with the same cast; append of the good rows; quarantine of records that fail decoding to `bronze.rejected_records`, committed before the accepted rows; no commit on empty or failed runs; `failOnDataLoss`; the run report records start and end offsets per partition. Verify Spark 4.1 `from_protobuf` and Kafka options against the official docs; check offset continuity, empty runs and quarantined records with the opt-in broker test (`KAFKA_TEST_BROKERS`), a CI test that a compaction-style rewrite between two runs replays no offset, and decoding parity with historical Parquet in CI. The rejected count appears in the run report and an OpenMetadata check.

## 5. Silver

- [ ] 5.1 Implement event deduplication and conflict handling over pinned Bronze snapshots: identical copies collapse; a conflicting payment is written in full to `silver.rejected_events` and excluded from the accepted tables; the run report counts conflicts and rejected rows before the write. Verify that CI tests for identical collapse, conflict quarantine (the accepted tables exclude the payment and its rejected rows are complete) and the zero-historical-rejections check pass.
- [ ] 5.2 Implement `silver.attempts` (top-level `merchant_id`, `customer_id`, `payment_method_fingerprint` and `amount_minor` from AttemptStarted; start, client `attempted_at`, decision, outcome, payment status, lifecycle validity). Verify that CI tests for retried, in-flight and lifecycle-invalid attempts pass.
- [ ] 5.3 Implement `silver.labels` with observation identity, `linkage` classes and availability against attempt start and success. Verify that CI tests cover each linked, pending and violation case, including the two linkage defect cases an earlier attempt got wrong.
- [ ] 5.4 Write all three tables as one full replacement each, recording Bronze snapshot IDs, and complete the run report. Verify that a CI rerun-equality test passes.

## 6. Checks and governance

- [ ] 6.1 Derive native OpenMetadata test definitions from Bronze and Silver ODCS rules, failing preparation on any uncovered rule. Verify with a CI preparation test over the real contracts.
- [ ] 6.2 Implement the checks stage (native discovery for the run's tables, then the test suite, success only if all expected tests pass) and the publish stage (products, contracts, Kafka/historical → Bronze → Silver lineage, pipeline execution status on success and failure). Remove the old readback, accept and replay code. Verify with focused CI tests of result evaluation and lineage construction.

## 7. Workflows, images and CI

- [ ] 7.1 Rewrite `infra/charts/data-pipelines`:
  - `dp1-bronze` and `dp2-silver` three-stage workflows, each with one retry level and a mutex, and SparkApplications created by `resource` steps with owner references;
  - on-demand diagnostics, benchmark and profiling templates;
  - a suspended Kafka-mode CronWorkflow;
  - v18 defaults;
  - a Kafka read identity.

  Remove the Spark utility templates and the old graph. Verify with CI helm lint and template for all values files.
- [x] 7.2 Delete `simulator/landing/` and its tests, and remove the archiver workload, values and permissions from `infra/charts/live-traffic` and its consumer group configuration. Verify that `helm template` renders no archiver, and coordinate the diff with the live change.
  - PR #100 note: removed the archiver workload, values, live-source gate for it, its network policy and the `payments-parquet-landing-v17` group from `infra/charts/live-traffic`, and deleted `contracts/landing/`. The live simulator's Deployment and config are unchanged.
- [ ] 7.3 Update `infra/docker/data-platform-{spark,metadata}.Dockerfile` (pinned checksums for the Kafka and protobuf Spark connectors and their transitive jars, batch package, contracts) and `infra/docker/live-simulator.Dockerfile` (no landing). Make the `.github/service-images.json` paths cover every copied directory, and update `.github/workflows/python-ci.yml` paths. Verify that CI image builds succeed and the release plan rebuilds the images on a contracts-only change.

- [ ] 7.4 Add the `maintain-lakehouse` WorkflowTemplate, on demand, and a suspended daily CronWorkflow to `infra/charts/data-pipelines`: one Spark stage that runs a bin-pack `rewrite_data_files` to the target size, `rewrite_manifests` and `expire_snapshots` (`older_than` seven days, `retain_last` 10) on each Bronze and Silver table, reporting files and bytes before and after from each table's `files` metadata table. Verify with CI helm lint and template, and the compaction test in 4.3.

## 8. Diagnostics and benchmark

- [ ] 8.1 Implement the diagnostics job (per-version row and null counts for fields declared since version 3, top merchant, top-10 merchant and top category shares, near-duplicate rate, cardinality labelled exact or estimate, each with its denominator) writing a JSON report. Verify with a CI test on fixture Bronze data.
- [ ] 8.2 Implement the benchmark job over one pinned Silver snapshot: the skewed merchant aggregation and the high-cardinality distinct-count aggregation, each as baseline and optimized variants with one warm-up, three timed runs, medians and two-way `exceptAll` equality. Verify with a CI test showing an unequal variant fails for each aggregation.
- [ ] 8.3 Implement the `storage-layout` job (code only): compaction of a Bronze copy written as many small appends, then bin-packed; a one-day query on an unpartitioned copy versus partitioned Bronze; and one customer's 30-day attempts on the default layout versus `zorder(customer_id,payment_method_fingerprint)`. Each case reports files and bytes planned, one warm-up, three timed runs, medians and a two-way `exceptAll` equality assertion. Verify with a CI test showing an unequal result fails, and a CI test showing a one-day partition filter plans fewer files than an unpartitioned copy.

## 9. Docs, specs and final checks

- [x] 9.1 Add the generator documentation (now `docs/data/offline.md`: configuration, schema changes, layout, rerun, diagnostics) and remove landing and old DP1 instructions. Bronze and Silver results go in `docs/data/offline.md`, `storage.md` and `orchestration-governance.md`. Verify that links resolve and generated references match.
- [ ] 9.2 At archive time, update the Purpose of `historical-bronze-ingestion` and `live-traffic-simulation` to drop landing batches. Verify that `openspec validate --strict` passes.
- [ ] 9.3 Run the local non-test checks (ruff, treefmt, helm lint, `openspec validate rebuild-generation-to-silver --strict`, `git diff --check`), push, and inspect every CI check. Verify that all required checks pass at the pushed head, reporting any pending or failed check honestly.
