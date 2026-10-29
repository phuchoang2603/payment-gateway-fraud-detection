## Context

See proposal.md (Why) for motivation. On `main` (`3a73e2e`) the path is:

- **Generator:** `src/data-pipeline/simulator/simulator/` (about 2,600 lines, schema 3 only, task-fragment layout v17).
- **Landing archiver:** `simulator/landing/` (Kafka → decoded Parquet).
- **Bronze:** `src/data-pipeline/bronze-ingestion/` (Bronze only, about 2,400 lines).
- **Orchestration:** a six-stage Argo graph with recurring landing selection and full catalog readback.

There is no Silver on `main`. PR #97 attempted one and is being replaced, not merged.

Constraints:

- **Platform:** Spark 4.1.3 on the Apache Spark Operator; Iceberg 1.12 through the Polaris REST catalog; StarRocks reads Iceberg for SQL checks; OpenMetadata is the catalog; Argo Workflows runs batch stages.
- **Kafka topics:** `payments.events.v1` and `fraud.labels.v1` carry raw protobuf bytes (Debezium outbox with `ByteArrayConverter`, no registry framing) with 7-day retention.
- **Repository rules:** tests and image builds run only in CI, and there is no legacy compatibility within the changed area.
- **Parallel live work:** the live simulator change runs at the same time and owns `simulator/live/`.

## Goals / Non-Goals

**Goals:**
- One implementation per stage, each small enough to read in one sitting.
- Historical and live data meet in the same Bronze tables, and Silver treats them identically.
- Every rule that can fail is either a Spark job failure before writing (conflicting duplicates, unreadable input) or a contract-derived SQL check. Nothing is checked twice.

**Non-Goals:**
- Incremental Silver, accepted-snapshot pointers, recurring modes, metadata-only replay, writer takeover or resumable partial files.
- Running Bronze or Silver at full scale, evidence capture, gold/feature marts, drift detection.
- A raw S3 archive of Kafka that outlives topic retention. Iceberg Bronze is the archive.

## Decisions

### 1. Entity world as one shared module, landed first

`simulator/world.py` exposes the minimal surface both generators need, agreed with the live change:

- **`WorldConfig`:** seed, integration id, synthetic key, start date, entity counts, behaviour, scenarios, label delays and chargeback reporting probability.
- **`World.build(config)`,** plus `world.digest` (config, seed and calibration identity) and `world.start_date`.
- **`world.purchase(rng, day, ordinal)`** accepts any non-negative day, including days past the historical range. It returns one purchase:
  - customer and merchant external IDs and account creation times;
  - item category, condition, quantity and unit price;
  - device, browser, IP network, IP country and shipping address;
  - payment-method index and fingerprint;
  - scenario, processor outcome, fraud and friendly-fraud flags, and label delay.
- **Constants:** `SCENARIOS`, `CATEGORIES`, `CONDITIONS`, and `entity_id`, kept byte-compatible with today's `identity.py`.

Entity pools, Zipf weights, familiar-merchant lists and compromise windows stay internal. Compromise windows are per customer and seeded from the world seed.

It lands on `main` as a working module before the rest of the rewrite. The live change switches `live/` to it, and this change then deletes `profiles.py`, `behavior.py`, `identity.py`, `calibration.py` and the other generator modules. The manifest records the full generator configuration, so the live simulator rebuilds the same world from configuration instead of a manifest-specific profile format.

*Alternatives:*
- Keep the old modules until the live change is done. Rejected: it keeps two implementations alive.
- Have this change edit `live/` directly. Rejected: it collides with the parallel branch.

### 2. Generation unit is one purchase-start day

- **Determinism.** Each day's events and labels come from RNG streams keyed by `(seed, stage, day, ordinal)`. A day can be generated without earlier days, and worker count cannot change records.
- **Output.** A day is split into fixed customer chunks (`chunk_customers` in the config), each written as one part per table: `payment_events/date=D/part-NNNNN.parquet` and `fraud_labels/date=D/part-NNNNN.parquet`, with row groups appended as purchases are generated. Part names are deterministic, so regenerating a day overwrites the same keys. The partition date is the purchase-start day, not the row timestamp. Lifecycle tails and delayed labels stay in their purchase day's files and carry their own timestamps.
- **Bounded resources.** A worker holds one chunk in memory and one local temporary part at a time, uploads it, and deletes it. Memory and scratch are bounded by chunk size and worker count, not by day or dataset size. This is what makes the full-scale run possible without the old scratch budgeting.
- **Atomic files.** A part is written to a local temporary file and then uploaded with a single PUT (multipart for large parts), or moved into place locally.
- **Start.** `config.json` (the logical configuration and its hash) is written first. A rerun with a different hash refuses to run.
- **Day completion.** After all of a day's parts are uploaded, a `date=D/_day.json` marker records each part's rows, bytes and SHA-256. A rerun skips days with a marker and regenerates days without one. Publication builds the manifest from the markers, without rereading data files.
- **Publication.** Once every day has a marker, it writes `manifest.json` last with an if-absent condition, listing each part with rows, bytes and SHA-256, plus total physical bytes. If the config declares `minimum_source_bytes` and the total is lower, publication refuses with both numbers. A version that already has a manifest is never modified.

This replaces task receipts, `_run/` state, scratch budgeting, writer takeover and cleanup. Day markers stay as part of the published dataset; they are small and document how each day was written.

*Alternative:* keep the old customer-block tasks with receipts and resume adoption. Rejected: day markers give the same restart granularity with one rule, and chunked parts bound resources without a separate budget model.

### 3. Schema change as data, not code paths

- **Configuration.** `schema_changes: [{effective_at, version}]` declares the change, starting at version 3. An event's physical version is chosen by its `occurred_at`.
- **Shape.** Version 3 omits `assessment.assessment_id` and `assessment.evidence_sha256`; version 4 is the full current protobuf shape.
- **Files.** A day spanning the change writes one file containing both shapes: the omitted fields are null and `physical_schema_version` tells them apart.
- **Contract.** The historical contract declares both fields as optional, conditioned on `physical_schema_version >= 4`.
- **Unknown versions.** A version other than 3 or 4 is rejected before writing.

*Alternative:* write separate files per version for a `mergeSchema` demonstration. Rejected for the source: Bronze still reads with `mergeSchema`, but a version column is the explicit contract. The benchmark job can demonstrate merge behaviour separately if it is needed later.

### 4. Bronze tables are shared by both sources

`bronze.payment_events` and `bronze.fraud_labels` hold business columns typed as the payments protobuf, plus provenance:

| Column | Meaning |
|---|---|
| `_source` | `historical` or `kafka` |
| `_dataset_version`, `_source_file` | historical files |
| `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp` | Kafka records |
| `_ingested_at`, `_run_id` | every row |

Events are partitioned by `_source` and `days(occurred_at)`; labels by `_source` and `days(label_available_at)`.

**Historical mode.**
1. Read the manifest and verify each listed file's SHA-256 and row count.
2. Read the files with `mergeSchema`.
3. Cast to the Bronze schema; any lossy cast fails.
4. Replace this dataset's rows with `writeTo(t).overwrite(_dataset_version = v)`, so a rerun is idempotent.
5. Check that written row counts per table equal the manifest totals.

**Kafka mode.**
- **Start offsets.** Each table's start offsets come from the latest snapshot property `kafka.offsets`. The first run uses configured starting offsets.
- **Reading.** Spark reads `format("kafka")` from the start offsets to `latest`, with `failOnDataLoss=true`, so expired offsets fail the run instead of being skipped.
- **Decoding.** `from_protobuf` decodes records using the payments descriptor set, generated by the existing `codegen:proto` `descriptor` option into the batch package.
- **Commit.** The append carries `snapshot-property.kafka.offsets`, so rows and progress commit atomically per table. A failed run commits nothing.
- **Empty runs.** A run with no new records commits nothing.

*Alternatives:*
- Kafka Connect S3 sink (rejected: raw protobuf bytes have no registry schema, so it cannot write typed files without a framing change across Flink, Feast and Fraud).
- Keep the Python archiver (rejected: a second ingestion system and landing format).
- Separate Bronze tables per source (rejected: Silver would union them anyway).

### 5. Silver is a pure function of pinned Bronze snapshots

The job reads both Bronze tables at their current snapshot IDs, which it records, and writes three tables. Each table is fully replaced with `overwrite(true)`, and the snapshot carries the Bronze snapshot IDs as properties. Rerunning on the same Bronze snapshots produces equal content.

**`silver.payment_events`**
- A business hash covers all non-provenance columns. `physical_schema_version` counts as provenance.
- Rows are grouped by `event_id`. More than one distinct hash is a conflict: the job writes the run report and then fails before writing any table.
- Otherwise one row is kept per event, the earliest `_ingested_at`.

**`silver.attempts`** has one row per `(integration_id, payment_id, attempt_id)` with:
- `started_at` (AttemptStarted `occurred_at`) and client `attempted_at`;
- the risk decision (outcome, reason codes, policy and model versions, score, `assessment_id`);
- `outcome`: `succeeded`, `declined`, `unknown` or `pending`, plus `outcome_at`;
- the payment-level `payment_status`: `open`, `succeeded` or `expired`.

Lifecycle violations get `lifecycle_valid = false` and are counted by an SQL check. These are more than one start or terminal event, events out of `aggregate_version` order, or a terminal event before the start.

**`silver.labels`** has one row per distinct observation. Labels have no ID, so identity is a hash of every business field; identical copies collapse. Each label gets a `linkage` value:

| `linkage` | Condition |
|---|---|
| `linked` | The attempt exists. For a chargeback, the attempt also succeeded. |
| `pending` | The attempt or its outcome has not arrived, and the payment is not `succeeded`/`expired`, or is unknown. |
| `violation` | The payment is terminal and the attempt does not exist, or a chargeback references a non-succeeded attempt. Also any label whose `label_available_at` is earlier than the attempt's `started_at`, or, for chargebacks, earlier than `outcome_at`. |

The availability comparison uses the attempt start even while the payment is open. This fixes both PR #97 defects: unlinkable labels on finished payments no longer stay pending forever, and early labels on in-flight attempts are now caught.

**Run report.** It records Bronze snapshot IDs, input rows, distinct events, identical duplicates, conflicts, and label counts per `linkage`. It is written to `runs/payment-gateway/dp2/<run>/report.json`.

*Alternative:* provenance tables per delivery (PR #97). Rejected: Bronze already holds every delivery, so counts are enough for accounting.

### 6. Checks are contract-derived StarRocks SQL run by OpenMetadata

Each ODCS contract declares `quality` rules as scalar violation-count SQL with `mustBe: 0`, which the batch package turns into native OpenMetadata test cases.

- **Bronze rules:** required fields and conditional nested fields, valid enums, and `physical_schema_version` in {3, 4}.
- **Silver rules:**
  - zero `lifecycle_valid = false`;
  - zero `linkage = 'violation'`;
  - unique keys;
  - non-negative amounts;
  - truth labels have a scenario, chargebacks have none;
  - `label_available_at >= occurred_at`.

The checks stage runs native discovery for the run's tables, then the test suite, in one container. It fails unless every expected test for this run is Success.

The Spark jobs own only conservation and conflicts: manifest row equality, offset bookkeeping and duplicate conflicts.

### 7. Two workflows, three stages, one retry level

`dp1-bronze` (parameters: `source=historical|kafka`, `dataset-version`) and `dp2-silver` each run three stages:

1. **spark:** an Argo `resource` step creating a SparkApplication, with `setOwnerReference`, operator restarts disabled, and success/failure conditions.
2. **checks:** the native OpenMetadata container.
3. **publish:** the metadata-image SDK publishes lineage, the pipeline execution and contracts. It runs on success and failure, so failed runs are visible.

Retries:
- Each stage has `retryStrategy` with a limit of 2 for errors only.
- Spark retries are safe because historical overwrite is idempotent and a failed Kafka append commits nothing.
- A synchronization mutex per workflow serializes writers.

A `CronWorkflow` for `dp1-bronze source=kafka`, followed by `dp2-silver`, exists but is disabled in both environments.

Diagnostics and benchmark are separate on-demand `WorkflowTemplate`s with a single Spark stage. Native profiling keeps its existing on-demand template, pointed at the new tables.

*Removed:* select/plan, discover, profile, readback and accept stages; the Spark utility templates (delete pods, cleanup, outcome); nested retries; the publish-only replay entrypoint.

### 8. Contracts reset to v1

- `contracts/data-products/{historical-payments,bronze-payments,silver-payments}/` each contain `product.odps.yaml` plus one `*.odcs.yaml` per table, all at version 1.0.0. There are no `versions/` copies and no `physical-schemas-*.yaml`.
- Silver business rules live in the Silver contracts; `contracts/quality/` is removed.
- Generated references under `docs/data-ml/data-products/` are regenerated by the existing CI check.

### 9. Diagnostics and benchmark are code only

- **Diagnostics** read Bronze and write JSON with:
  - per-version row and null counts for v4 fields;
  - merchant and category top-share and Gini skew;
  - `approx_count_distinct` cardinality, with exact counts where cheap, labelled as such;
  - the identical-duplicate rate with its denominator.
- **Benchmark** pins one Silver snapshot and runs the same skewed aggregation (attempts joined with per-merchant aggregates, grouped by merchant and day) two ways:
  - **Baseline:** AQE skew handling off, no salting.
  - **Optimized:** AQE skew join on, plus salting of the hot merchant keys.

  It runs one warm-up, then three timed runs each, and reports medians. It asserts equal output both ways with `exceptAll`.

Neither job runs in the pipeline workflows.

### 10. Images and CI

- **Spark image:** adds `spark-sql-kafka-0-10_2.13:4.1.3` with its transitive `kafka-clients` and `commons-pool2`, plus `spark-protobuf_2.13:4.1.3`, each pinned by checksum. The image copies `contracts/data-products`.
- **Metadata image:** copies the same contracts.
- **Rebuild paths:** `.github/service-images.json` lists every copied directory as an image rebuild path.
- **CI:** the Python CI job points at `src/data-pipeline/batch` and the rewritten simulator; there are no new jobs.

### 11. Generation runs as an on-demand cluster workflow

A `generate-historical` Argo `WorkflowTemplate` in the data-pipelines chart runs the generator from the simulator image as one pod. `SIMULATOR_CONFIG` selects the environment's config, which is baked into the image:

| Environment | Config | Output |
|---|---|---|
| dev | `historical-dev.yaml`: v18, 90 days, 10k customers | `s3://platform-dev/datasets/payment-gateway/historical/v18` |
| prod | `historical-full.yaml`: v19, 365 days from 2025-01-01, 3M customers, schema change mid-year, `minimum_source_bytes: 100000000000` | `s3://platform-prod-142737101504-us-east-1/datasets/payment-gateway/historical/v19` |

- **Credentials:** the existing gateway object identity, scoped to `datasets/payment-gateway/`.
- **Resources:** explicit CPU, memory and ephemeral-storage requests sized from the chunk size and worker count, plus `activeDeadlineSeconds`.
- **Retries:** `retryStrategy` on errors. A retried pod skips completed days by their markers, so a node replacement only costs the day in progress.
- **Prod:** the data-pipelines Argo CD application is enabled with a values switch that renders only the generator template; Bronze, Silver, schedules and their credentials stay off.

*Alternatives:*
- A Helm-managed `Job`: rejected, because a Job runs at sync time, is immutable, and needs a rename for every run.
- Workstation CLI: rejected by the user; a multi-hour run should not depend on a laptop.

The local CLI remains for tests and development only.

## Risks / Trade-offs

- [Bronze not run within Kafka retention (7 days)] → `failOnDataLoss` fails the run loudly. Enabling the schedule must come with a cadence or retention that covers it. Raising retention is a Kafka chart change owned by the live change.
- [`from_protobuf` types differ from the historical Parquet types (timestamps, enums, `uint64`)] → One explicit cast to the Bronze contract schema serves both sources. A CI test decodes protobuf fixtures and generated Parquet into the same Bronze schema, and any mismatch fails. Exact Spark 4.1 `from_protobuf` options are verified against the Spark docs during implementation.
- [The full-scale run takes hours and its throughput is unmeasured for the new code] → The dev v18 run measures rows per second per worker first, and the prod resources and deadline are set from that. A pod lost to node replacement resumes from day markers. The run is reported only as a published manifest with its byte total; no coursework evidence is captured.
- [Prod compute and storage cost] → One bounded pod on prod nodes, with no Spark or OpenMetadata workloads enabled in prod. About 100+ GB is stored in the prod bucket. The template is on demand and does nothing until submitted.
- [Full Silver recompute grows with history] → Acceptable at development scale. Incremental Silver is a later change once volume justifies it.
- [Historical overwrite-by-filter rewrites data files] → Partitioning by `_source` keeps rewrites within historical partitions.
- [Spec deltas overlap the live change] → This change modifies only these requirements of shared specs:
  - `live-traffic-simulation`: "One shared current flow", plus four removed landing requirements;
  - `talos-gitops-deployment`: "Shared topics and Python Parquet landing" (removed; replaced by the added "Shared payment topics"), "Runtime credentials and image delivery" (replaced by "Simulator credentials and image delivery"), "Environment data-pipeline deployment", "Recurring DP1 schedule ownership", "Environment-configurable Python entrypoints";
  - `focused-verification`: "Cross-language simulation contracts" (modified), "Generator behavior tests" and "Data-pipeline behavioral verification" (replaced under new names), plus nine removed landing, scale and acceptance requirements.

  The archiver sentence in "Single simulator workload" is left to the live change, which already modifies that requirement for delivery faults.

  The live change is told to avoid them; whichever change archives second rebases its delta.
- [Generator and Silver correctness depend on CI only] → Behaviour tests cover:
  - per-day determinism and refused config changes;
  - the schema change boundary and duplicates;
  - Bronze idempotent historical rerun, plus Kafka offsets and empty runs (against a Kafka test container, opt-in like the existing broker tests);
  - Silver conflicts, linkage cases and availability.

## Migration Plan

Prod releases come only from `main`, and trusted PRs release to dev. The work therefore lands in two PRs:

1. **Generator PR → `main`:**
   - `simulator/world.py`, the rewritten generator and the reset `historical-payments` v1 contracts. Old modules that `live/` still imports stay until the live change switches to `world.py`;
   - the `generate-historical` template;
   - the dev and full-scale configs;
   - the prod generator-only enablement.

   After its releases:
   - submit `generate-historical` in dev (v18) to measure throughput;
   - set the prod resources and deadline from that measurement, then submit it in prod (v19).

   The old DP1 on `main` cannot read v18. That is accepted, because it is replaced by PR 2 and is not scheduled.
2. **Pipeline PR (this branch, rebased):** Bronze, Silver, contracts, workflows, images and cleanup. Its trusted-PR releases exercise Bronze and Silver on dev against v18 while the prod generation runs.

Steps 3 and 4 run when the whole pipeline is ready, not as tasks of this change.

3. **Dev reset:**
   - drop the old `bronze` Iceberg tables and DP1 run state;
   - remove obsolete DP1 OpenMetadata assets;
   - remove the archiver deployment and its consumer group.

   Leave v8–v17 source objects and Kafka topics untouched.
4. Run `dp1-bronze` historical on v18, then `dp2-silver`. Running against live Kafka waits for the live change. Prod Bronze and Silver stay disabled.

**Rollback:** revert the branch. Old source datasets are untouched, but old Bronze state is not restored; it was already superseded.

## Open Questions

- Exact transitive jar versions for the Kafka connector under Spark 4.1.3, resolved when the Dockerfile is written.
- Whether the benchmark should also show a `mergeSchema` on/off read over v3/v4 files. It can be added to the benchmark job later without spec changes.
