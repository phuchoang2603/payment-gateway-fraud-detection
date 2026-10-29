## 1. Shared entity world (part of the generator PR)

- [ ] 1.1 Implement `simulator/world.py` with the interface in design.md decision 1 (`WorldConfig`, `World.build`, `digest`, `start_date`, `day_purchases`, `purchase` and `card_test` keyed by day, customer and purchase number, `SCENARIOS`/`CATEGORIES`/`CONDITIONS`, unchanged `entity_id`), built from configuration and seed only. The live simulator now uses this interface in this PR, so the live change rebases onto it. Verify with the identity-vector test, moved to `world.py`, passing in CI.
- [ ] 1.2 Done in PR #100: `simulator/live/` imports `world.py`, `HistoricalModel` builds the world from the manifest and checks its digest, and the old modules are deleted. Verify in CI that `test_live.py` passes against `world.py`. Tick after CI passes.

## 2. Contracts reset to v1

- [ ] 2.1 Replace `contracts/data-products/historical-payments` with v1 ODPS/ODCS for the v18 layout: physical schema versions 1/2 declared by `sinceSchemaVersion` on the added fields, envelope `schema_version` 3, `physical_schema_version` column, manifest as completion marker, no feature-definition reference, no `versions/` or physical-schema files. Verify with the CI contract lint and the regenerated references.
- [ ] 2.2 Replace `contracts/data-products/bronze-payments` with v1 contracts for the shared Bronze tables (historical and Kafka provenance, partitioning) and Bronze raw-schema SQL rules. Verify with the CI contract lint.
- [ ] 2.3 Add `contracts/data-products/silver-payments` (product plus `payment-events`, `attempts`, `labels` contracts with Silver SQL rules) and delete `contracts/quality/` and `contracts/landing/`. Verify with the CI contract lint and `rg` finding no references to the deleted paths.

## 3. Historical generator rewrite

- [ ] 3.1 Rewrite the configuration model with dates, counts, skew, `duplicate_rate`, ordered `schema_changes` (dates and versions 1/2 only), label delays, foreign-IP probability, optional `minimum_source_bytes` and output root. Run settings (mode, shard, workers, contract directory) come from the environment, so the YAML holds no chunk, worker or row-group settings. Replace `configs/historical-smoke.yaml` with `historical-dev.yaml` (v18) and `historical-full.yaml` (v19: 365 days, about 3M customers, 100 GB minimum). Reject invalid schedules before writing. Verify with an invalid-schedule behaviour test in CI.
- [ ] 3.2 Rewrite generation as per-day units on `world.py` keyed by day, customer and purchase number: lifecycle histories, scenarios, truth and chargeback labels, duplicates, the gateway fraud policy with both reason codes, and physical versions projected from the contract. Verify in CI that records are equal across split and worker count, that interruption does not change them, and that the schema boundary and duplicates hold.
- [ ] 3.3 Implement storage:
  - write `config.json` first, recording the generator digest, and refuse a changed configuration or digest;
  - write atomic parts under `date=`, one per physical version for events (multipart upload for large parts), and a per-day `_day.json` marker once all of the day's parts finish;
  - skip complete days, regenerate incomplete ones, and submit all pending parts of a shard together;
  - write `manifest.json` last and only if absent: logical configuration, generator digest, contract identities, totals and the file list, refusing below `minimum_source_bytes`;
  - work the same for local and S3 roots.

  Verify that CI tests for rerun, incomplete-day regeneration, changed-config and changed-contract refusal, published-version refusal and the undersized-source refusal pass.
- [ ] 3.4 Delete the replaced modules (`profiles`, `behavior`, `identity`, `calibration` if unused, `fragment_index`, `publication`, `resources`, `run_state`, `statistics`, `object_store` as superseded), `notebooks/historical-sanity.ipynb` and their tests, after 1.2. Keep `simulator/landing/` until 7.2 removes its deployment, adapting only its schema import if needed. Verify that `rg` finds no imports of deleted modules and that ruff passes locally.
  - PR #100 note: deleted the replaced generator modules and their tests, `simulator/landing/`, the sanity notebook, the `notebook` dependency group, and `config.py`, `profiles.py`, `behavior.py`, `identity.py` and `calibration.py` after `live/` moved to `world.py`. `arrow.py` stays for the generator's Arrow schemas. Tick after CI passes.

- [ ] 3.5 Add the `generate-historical` WorkflowTemplate to `infra/charts/data-pipelines` as a DAG: `generator.shards` shard pods in parallel, then one publish pod, both built from one named container template in `_helpers.tpl`. Settings come from the environment (`GENERATOR_MODE`, `SHARD_INDEX`/`SHARD_COUNT`, `GENERATOR_WORKERS` from the CPU request). RBAC is a ServiceAccount with a Role for `workflowtaskresults` `create, patch` and its RoleBinding, plus the `gateway-storage` ExternalSecret. Enable the chart in prod through `infra/argocd/app-of-apps/values-prod.yaml` with only this workflow. Verify with CI helm lint and template showing the shard and publish steps and the prod shard count. The generator-only values switch is removed because nothing else renders.
- [ ] 3.6 Land groups 1, 2.1 and 3 as the generator PR to `main` (with user confirmation), leaving old DP1 code, the archiver and the Bronze contracts for the pipeline PR, except for upstream contract references that CI lint requires. Verify that all CI checks pass on the PR and on `main`, and that the dev and prod releases record the new simulator image.
- [ ] 3.7 Submit `generate-historical` in dev to publish v18. Measure rows per second and peak memory per worker, and set the prod resources and deadline from them. Verify that `historical/v18/manifest.json` exists in `platform-dev` with both schema versions and the expected duplicate rate.
- [ ] 3.8 With user confirmation, submit `generate-historical` in prod for v19, and continue groups 4–9 on dev while it runs. Verify that `historical/v19/manifest.json` exists in the prod bucket with at least 100,000,000,000 physical bytes, or report the observed failure and the completed day count.

## 4. Batch project and Bronze

- [ ] 4.1 Create `src/data-pipeline/batch` (one pyproject and lockfile, Spark session, catalog and storage settings from the environment, a payments descriptor set via the `codegen:proto` `descriptor` option) and delete `src/data-pipeline/bronze-ingestion`. Verify that `uv lock` and ruff pass locally and the CI Python job imports the package.
  - PR #100 note: reduced `src/data-pipeline/bronze-ingestion` to `data_platform/integrations.py` and `catalog.py` for metadata integrations, and removed its tests, config and Spark image. The `batch` project replaces it in the pipeline change.
- [ ] 4.2 Implement Bronze historical mode: manifest verification, `mergeSchema` read, lossless cast to the Bronze schema, overwrite by `_dataset_version`, manifest row-total check, run report. Verify that CI tests for altered-file failure, idempotent rerun and preserved duplicates pass.
- [ ] 4.3 Implement Bronze Kafka mode: start offsets from the latest snapshot property or configured starting offsets, `from_protobuf` decoding with the same cast, append with the `kafka.offsets` snapshot property, no commit on empty or failed runs, `failOnDataLoss`. Verify Spark 4.1 `from_protobuf` and Kafka options against the official docs; check offset continuity and empty runs with the opt-in broker test, and decoding parity with historical Parquet in CI.

## 5. Silver

- [ ] 5.1 Implement event deduplication and conflict detection over pinned Bronze snapshots, writing the run report before failing. Verify that CI tests for identical collapse and conflict failure leaving Silver unchanged pass.
- [ ] 5.2 Implement `silver.attempts` (start, client `attempted_at`, decision, outcome, payment status, lifecycle validity). Verify that CI tests for retried, in-flight and lifecycle-invalid attempts pass.
- [ ] 5.3 Implement `silver.labels` with observation identity, `linkage` classes and availability against attempt start and success. Verify that CI tests cover each linked, pending and violation case, including both PR #97 defect scenarios.
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
- [ ] 7.2 Delete `simulator/landing/` and its tests, and remove the archiver workload, values and permissions from `infra/charts/live-traffic` and its consumer group configuration. Verify that `helm template` renders no archiver, and coordinate the diff with the live change.
  - PR #100 note: removed the archiver workload, values, live-source gate for it, its network policy and the `payments-parquet-landing-v17` group from `infra/charts/live-traffic`, and deleted `contracts/landing/`. The live simulator's Deployment and config are unchanged.
- [ ] 7.3 Update `infra/docker/data-platform-{spark,metadata}.Dockerfile` (pinned checksums for the Kafka and protobuf Spark connectors and their transitive jars, batch package, contracts) and `infra/docker/live-simulator.Dockerfile` (no landing). Make the `.github/service-images.json` paths cover every copied directory, and update `.github/workflows/python-ci.yml` paths. Verify that CI image builds succeed and the release plan rebuilds the images on a contracts-only change.

## 8. Diagnostics and benchmark

- [ ] 8.1 Implement the diagnostics job (version split and v4 nulls, skew, cardinality labelled exact or estimate, duplicate rate with denominator) writing a JSON report. Verify with a CI test on fixture Bronze data.
- [ ] 8.2 Implement the benchmark job (pinned Silver snapshot, baseline vs. AQE skew join plus salting, one warm-up and three timed runs each, medians, two-way `exceptAll` equality). Verify with a CI test showing an unequal variant fails.

## 9. Docs, specs and final checks

- [ ] 9.1 Add `docs/data-ml/data-generator.md` (configuration, schema changes, layout, rerun, diagnostics) and minimally update `data-pipeline.md` and `data-products.md`, removing landing and old DP1 instructions. Verify that links resolve and generated references match.
- [ ] 9.2 At archive time, update the Purpose of `historical-bronze-ingestion` and `live-traffic-simulation` to drop landing batches. Verify that `openspec validate --strict` passes.
- [ ] 9.3 Run the local non-test checks (ruff, treefmt, helm lint, `openspec validate rebuild-generation-to-silver --strict`, `git diff --check`), push, and inspect every CI check. Verify that all required checks pass at the pushed head, reporting any pending or failed check honestly.
