## Context

See proposal.md (Why) for motivation. On `main` (`3a73e2e`) the path is:

- **Generator:** `src/data-pipeline/simulator/simulator/` (about 2,600 lines, schema 3 only, task-fragment layout v17).
- **Landing archiver:** `simulator/landing/` (Kafka → decoded Parquet).
- **Bronze:** `src/data-pipeline/bronze-ingestion/` (Bronze only, about 2,400 lines).
- **Orchestration:** a six-stage Argo graph with recurring landing selection and full catalog readback.

There is no Silver on `main`. An earlier Silver attempt was not merged.

Constraints:

- **Platform:** Spark 4.1.3 on the Apache Spark Operator; Iceberg 1.12 through the Polaris REST catalog; ClickHouse reads Iceberg for SQL checks through a DataLakeCatalog database; OpenMetadata is the catalog; Argo Workflows runs batch stages.
- **Kafka topics:** `payments.events.v1` and `fraud.labels.v1` carry raw protobuf bytes (Debezium outbox with `ByteArrayConverter`, no registry framing) with 7-day retention.
- **Repository rules:** tests and image builds run only in CI, and there is no legacy compatibility within the changed area.
- **Parallel live work:** the live simulator change runs at the same time and owns `simulator/live/`.

## Goals / Non-Goals

**Goals:**
- One implementation per stage, each small enough to read in one sitting.
- Historical and live data meet in the same Bronze tables, and Silver treats them identically.
- Every rule that can fail is either a Spark job failure before writing (unreadable historical input), a row routed to a rejected-records table (undecodable Kafka records, conflicting Silver payments), or a contract-derived SQL check. Nothing is checked twice.

**Non-Goals:**
- Incremental Silver, accepted-snapshot pointers, recurring modes, metadata-only replay, writer takeover or resumable partial files.
- Running Bronze or Silver at full scale, evidence capture, gold/feature marts, drift detection.
- A raw S3 archive of Kafka that outlives topic retention. Iceberg Bronze is the archive.

## Decisions

### 1. Entity world as one shared module, landed in this PR

`simulator/world.py` is the only domain module. It holds the configuration models, calibration loading, identity functions and constants, and the entity world that both generators use.

- **`WorldConfig`:** seed, integration id, synthetic key, start date, entity counts, behaviour (including the foreign-IP probability), scenarios and label delays.
- **`World.build(config)`,** plus `world.digest` (configuration, calibration bytes and tzdb version) and `world.start_date`.
- **`world.day_purchases(day, customers)`** returns the `(customer, number)` purchases of a customer range. Counts come from one per-day draw over all customers, so the same purchases come out however the range is split. **`world.purchase(rng, day, customer)`** and **`world.card_test(rng, day, burst)`** build one purchase each, with streams from `purchase_rng(day, customer, number)` and `burst_rng(day, burst, number)`.
- **Constants and helpers:** `SCENARIOS`, `CATEGORIES`, `CONDITIONS`, `entity_id` and `fingerprint` (byte-compatible with the identity vectors), `StrictModel` and `stage_rng`.

Entity pools, Zipf weights, familiar-merchant lists and compromise windows stay internal and are seeded from the world seed. Customer activity and region are per-customer traits fixed by the seed.

This PR also switches `live/` to `world.py`. `HistoricalModel` builds a `World` from the manifest's recorded world configuration and rejects a manifest whose `world_digest` differs. `profiles.py`, `behavior.py`, `identity.py`, `calibration.py` and `config.py` are deleted in the same change. The live simulator's behaviour is otherwise unchanged.

*Alternative:* keep the old modules until the live change is done. Rejected: it keeps two implementations alive.

### 2. Generation unit is one purchase-start day, split into parts

- **Keys.** Each purchase is keyed by its purchase-start day, its customer and that customer's purchase number that day. Card-testing bursts are keyed by (day, burst, number). A purchase's random stream and identity depend only on its key, never on how the day is split or how many workers run.
- **Counts.** A day's purchase counts come from one Poisson draw over all customers, scaled by each customer's activity and the region's local-hour, weekday and seasonal intensity. The draw is computed once per process and is independent of the split.
- **Parts.** Internal constants size a part: customers per part (`PART_CUSTOMERS`, 750,000, so the full dataset has about four parts per day and table, on the order of 70 MB each as an estimate from v8's size) and rows per Parquet row group. A part holds one table's rows for a customer range. Names are `payment_events/date=D/part-NNNNN.v{2|3}.parquet` (events are split by `schema_version`, see decision 3) and `fraud_labels/date=D/part-NNNNN.parquet`. Part names are deterministic, so regenerating a day overwrites the same keys. The partition date is the purchase-start day, not the row timestamp.
- **Scheduling.** `produce` submits every pending (day, part) pair of a shard to the pool at once. Each day's `_day.json` marker is written as soon as all of that day's parts have finished, with no per-day barrier.
- **Bounded resources.** A worker holds one part's rows per writer and one local temporary file at a time. It uploads the file and deletes it. Memory and scratch are bounded by part size and worker count, not by day or dataset size.
- **Atomic files.** A part is written to a local temporary file and uploaded with a single PUT (multipart for large parts), or moved into place locally.
- **Start.** `config.json` (the logical configuration, the generator digest and their hash) is written first. A rerun with a different hash refuses to run.
- **Day completion.** A `date=D/_day.json` marker records each part's rows, bytes and SHA-256 once all of the day's parts are uploaded. A rerun skips days with a marker and regenerates days without one.
- **Publication.** Once every day has a marker, `publish` writes `manifest.json` last with an if-absent condition. It lists the logical configuration, generator digest, contract identities, row and byte totals and the file list. If the configuration declares `minimum_source_bytes` and the total is lower, publication refuses with both numbers. A version that already has a manifest is never modified.

*Alternative:* keep the old customer-block tasks with receipts and resume adoption. Rejected: day markers give the same restart granularity with one rule, and parts bound resources without a separate budget model.

### 3. Schema versions declared by the contract

- **Single version column.** `schema_version` (field 7 of `PaymentEvent`) is the only version of a payment event. There is no separate physical-version column.
- **Declaration.** The historical `payment-events` ODCS contract is the only declaration of which fields exist in which version. Each field added after version 2 carries a property-level `customProperties` entry `sinceSchemaVersion: 3`. Today those are `assessment.assessment_id`, `assessment.evidence_sha256` and `signals.user_agent_family`.
- **Versions.** The historical source starts at `schema_version` 2. The configuration's `schema_changes: [{effective_at, version}]` declares a change to `version: 3`, dated per dataset: dev on 2026-09-15, full on 2026-04-30. An event's version is chosen by its `occurred_at`. Version 3 is the gateway's current shape, so Kafka rows need no special handling.
- **Meaning of "version 2".** Version 2 is defined by the v18 historical contract, not by the gateway's older wire format. A later gateway change (for example to 4) uses the same mechanism: a new contract field marked `sinceSchemaVersion: 4`, and a configuration change entry to version 4. The config accepts only versions the contract declares, and produce refuses others before writing.
- **Projection.** The generator builds each event in the full shape and projects it onto the fields available at its `schema_version`, using the paths the contract declares. Field names are not coded.
- **Files.** Events of each version go to separate part files named `.v2.parquet` or `.v3.parquet`. Their Parquet schemas really differ: v2 parts physically lack the declared columns. A day that spans the change has parts of both versions.
- **Contract rules.** `schema_version` must be in (2, 3). The declared fields are present exactly when `schema_version >= 3`.

*Alternative:* one file per table holding both shapes, with the omitted fields null. Rejected: the Parquet schema would not show which version lacks the fields, so Bronze would have to infer the layout from the rows.

### Handoff: long-window features (plan only)

This belongs to the later feature change and the live simulator thread, not to this PR.

- Long windows (7-day and 30-day) are computed by a daily Spark batch from Silver and materialized into Feast. Flink keeps only the short windows (5 minutes, 1 hour and 24 hours).
- There is no HybridSource and no Flink state bootstrap. Short windows warm up within 24 hours of live traffic.
- Offline training features come from the same Spark job, so online and offline long windows match by construction.
- The history's recent end (decision 11 dates) gives the batch full 30-day lookbacks when live traffic starts.

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
1. Read the manifest and list the dataset root once, metadata only: its parquet objects must equal the listed files, with each size equal to the listed `bytes`.
2. Read the files once with `mergeSchema`.
3. Cast to the Bronze schema; any lossy cast fails.
4. Replace this dataset's rows with `writeTo(t).overwrite(_dataset_version = v)`, so a rerun is idempotent.
5. Group the written rows by source file and compare each count with the manifest `rows`; a mismatch names the file. Bronze does not recompute the generator's SHA-256 values.

**Kafka mode.**
- **Start offsets.** Each partition's next offset is `max(_kafka_offset) + 1` over the rows the table already holds for the topic: `_source = 'kafka'` rows of `bronze.payment_events` or `bronze.fraud_labels`, and every row of `bronze.rejected_records`. Partitions with no rows start at the configured starting offsets (`KAFKA_STARTING_OFFSETS`, `earliest` or `latest`). No table property records progress, so compaction and snapshot expiry cannot move it.
- **Reading.** Spark reads `format("kafka")` from the start offsets to `latest`, with `failOnDataLoss=true`, so expired offsets fail the run instead of being skipped.
- **Decoding.** `from_protobuf` decodes records using the payments descriptor set, generated by the existing `codegen:proto` `descriptor` option into the batch package.
- **Commit.** Progress is the rows, so each table's good rows and their offsets commit together. Records that fail decoding are written to `bronze.rejected_records` in their own commit, which runs first: the rejected table never lags the accepted one, so the read window starts from accepted progress alone and covers only unprocessed records. A failed run leaves a retry to re-read from the last committed rows. Partial failure between the two commits cannot lose quarantined records, because the rejected table is written first and holds its own rows.
- **Empty runs.** A run with no new records commits nothing.
- **Rejected records.** `bronze.rejected_records` holds the raw key and value bytes, topic, partition, offset, Kafka timestamp, error, `_run_id` and `_ingested_at` of each record that fails protobuf decoding or a required-field check taken from the Bronze contract (an empty string counts as missing). A value that decodes but cannot be cast to the Bronze schema fails the run. Historical mode has no such table: any bad file fails the run, because published sources are immutable. The run report and an OpenMetadata check show the rejected count.

*Alternatives:*
- Kafka Connect S3 sink (rejected: raw protobuf bytes have no registry schema, so it cannot write typed files without a framing change across Flink, Feast and Fraud).
- Keep the Python archiver (rejected: a second ingestion system and landing format).
- Separate Bronze tables per source (rejected: Silver would union them anyway).

### 5. Silver is a pure function of pinned Bronze snapshots

The job reads both Bronze tables at their current snapshot IDs, which it records, and writes four tables. Only the duplicated event IDs, the collapsed duplicates and the conflict frames (conflicting events, their payments, the reasons and their events) are persisted. Accepted events are written first, and attempts, payment status and label linkage are then derived from that written snapshot, read back at its ID, so no copy of the full deduplicated events is cached. Dedup is narrow: a pass over `event_id` alone finds the IDs with more than one copy and persists them with an estimated size (rows times key length plus 64 bytes). Under `SILVER_DEDUP_BROADCAST_BYTES` (default 256 MB) they join back to Bronze as a broadcast semi or anti join, otherwise as a shuffled join. Unique events pass through the anti join unchanged, and only the duplicated rows are grouped in full. The run report records the strategy, the duplicated-ID count and the estimate. Each table is fully replaced with `overwrite(true)`, and the snapshot carries the Bronze snapshot IDs as properties. Rerunning on the same Bronze snapshots produces equal content.

**`silver.payment_events`**
- A business hash covers all columns except provenance and `recorded_at`, and includes `schema_version`. Copies whose hashes agree collapse to the one with the earliest `recorded_at`; republished exports are duplicates, not conflicts.
- Rows are grouped by `event_id`. More than one distinct hash is a conflict: every row of that event's payment is written to `silver.rejected_events` (below) and the payment is excluded from the accepted tables. The run succeeds.
- Otherwise one row is kept per event, the earliest `_ingested_at`.

**`silver.attempts`** has one row per `(integration_id, payment_id, attempt_id)` with:
- top-level `merchant_id`, `customer_id`, `payment_method_fingerprint` and `amount_minor`, taken from the AttemptStarted context and signals, because feature, benchmark and storage queries filter on them;
- `started_at` (AttemptStarted `occurred_at`) and client `attempted_at`;
- the risk decision (outcome, reason codes, policy and model versions, score, `assessment_id`);
- `outcome`: `succeeded`, `declined`, `unknown` or `pending`, plus `outcome_at`;
- the payment-level `payment_status`: `open`, `succeeded` or `expired`.

Lifecycle violations get `lifecycle_valid = false` and are counted by an SQL check. These are more than one start or terminal event, events out of `aggregate_version` order, or a terminal event before the start.

**`silver.labels`** has one row per distinct observation. Labels have no ID, so identity is a hash of every business field; identical copies collapse. Each label gets a `linkage` value:

| `linkage` | Condition |
|---|---|
| `linked` | The attempt exists. For a chargeback, the attempt also succeeded. |
| `pending` | A live (Kafka) label whose payment has not arrived, or whose attempt or outcome has not arrived while the payment is open. |
| `orphan` | A historical label whose payment has no events at all. The historical source is complete, so this fails the Silver checks. |
| `violation` | A historical label whose attempt is missing while its payment exists; the payment is terminal and the attempt does not exist; a chargeback references a non-succeeded attempt. Also any label whose `label_available_at` is earlier than the attempt's `started_at`, or, for chargebacks, earlier than `outcome_at`. |

The availability comparison uses the attempt start even while the payment is open. It covers two linkage cases: unlinkable labels on finished payments no longer stay pending forever, and early labels on in-flight attempts are now caught.

**`silver.rejected_events`** holds every row of a payment whose events conflict. Columns: `rejected_table` (`payment_events`, `attempts` or `labels`), `reason` (the conflicting `event_id`, or the availability or linkage rule), `source` (`historical` or `kafka`, from Bronze), `payment_id` and the row content. Rejection is complete per payment: no row of a conflicting payment enters `silver.payment_events`, `silver.attempts` or `silver.labels`. A Silver check requires zero rejected rows with source `historical`, because published sources are immutable.

**Run report.** It records Bronze snapshot IDs, input rows, distinct events, identical duplicates, conflicts, rejected rows by table and source, label counts per `linkage`, the Silver snapshot IDs with their added-record counts, and the tasks, stages and wall time of each phase. It is written to `runs/payment-gateway/dp2/<run>/report.json`.

*Alternative:* provenance tables per delivery. Rejected: Bronze already holds every delivery, so counts are enough for accounting.

### 6. Checks are contract-derived ClickHouse SQL run by OpenMetadata

Bronze and Silver ODCS contracts declare `quality` rules as scalar violation-count SQL with `mustBe: 0`, which the batch package turns into native OpenMetadata test cases. The historical source contracts declare no executable rules: their semantics live in field descriptions, and the generator tests check them.

- **Bronze rules:** required fields and conditional nested fields, valid enums, and `schema_version` in {2, 3}.
- **Silver rules:**
  - zero `lifecycle_valid = false`;
  - zero `linkage = 'violation'`;
  - unique keys;
  - non-negative amounts;
  - truth labels have a scenario, chargebacks have none;
  - `label_available_at >= occurred_at`.

The checks stage runs native discovery for the run's tables, then the test suite, in one container. It fails unless every expected test for this run is Success.

The Spark jobs own only conservation and quarantine accounting: manifest row equality, offset bookkeeping, and the rejected-record counts that the run report and checks read.

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

The benchmark is an on-demand `WorkflowTemplate` with a single Spark stage.

*Removed:* select/plan, discover, profile, readback and accept stages; the Spark utility templates (delete pods, cleanup, outcome); nested retries; the publish-only replay entrypoint.

### 8. Contracts reset to v1

- `contracts/data-products/{historical-payments,bronze-payments,silver-payments}/` each contain `product.odps.yaml` plus one `*.odcs.yaml` per table, all at version 1.0.0. There are no `versions/` copies and no `physical-schemas-*.yaml`.
- Silver business rules live in the Silver contracts; `contracts/quality/` is removed.
- Generated references under `docs/data/products/` are regenerated by the existing CI check.
- The Bronze product adds a `rejected-records` port for the quarantined Kafka records, and the Silver product adds a `rejected-events` port. Each port's contract declares its rule: the Silver contract requires zero rejected rows with source `historical`.

### 9. The benchmark is code only

- **Benchmark** pins one Silver snapshot and joins `silver.attempts` with per-merchant totals as a sort-merge join (broadcast disabled), aggregated by merchant and day. The hot merchant takes about half of purchases, and the report records its share with its denominator. Three variants, each run once and timed:
  - `baseline_no_skew_join`: AQE on, skew join off;
  - `aqe_skew_join`: AQE skew join on, with `forceOptimizeSkewedJoin` so the split applies even where Spark would decline the extra shuffle. The run fails unless the final adaptive plan contains a skew-split sort-merge join or a skewed shuffle read;
  - `salted`: the hot merchant's attempts spread over 16 buckets by `pmod(hash(attempt_id), 16)`, with the totals replicated to every bucket, AQE skew join off.
- **Equality guard:** each variant's output is reduced to its row count and an order-independent checksum (the sum over output rows of `xxhash64` of all columns, held as a decimal). A variant that differs from the baseline on either fails the run.
- **Warm-up:** one untimed count of `silver.attempts` runs before the timed variants, so the first variant does not pay JVM and S3 warm-up.
- **Report:** per variant, the duration of the checked job, the output row count and checksum, the Spark job, stage and task IDs for the Spark UI, and the hot-key share with its denominator.

Neither job runs in the pipeline workflows.

### 10. Images and CI

- **Spark image:** adds `spark-sql-kafka-0-10_2.13:4.1.3` with its transitive `kafka-clients` and `commons-pool2`, plus `spark-protobuf_2.13:4.1.3`, each pinned by checksum. The image copies `contracts/data-products`.
- **Metadata image:** copies the same contracts.
- **Rebuild paths:** `.github/service-images.json` lists every copied directory as an image rebuild path.
- **CI:** the Python CI job points at `src/data-pipeline/batch` and the rewritten simulator; there are no new jobs.

### 11. Generation runs as an on-demand cluster workflow

A `generate-historical` Argo `WorkflowTemplate` in the data-pipelines chart runs the simulator image as a DAG: `generator.shards` shard pods in parallel, then one publish pod. The configuration, shard count and CPU come from the chart. Each environment's `SIMULATOR_CONFIG` file is baked into the image:

| Environment | Config | Output |
|---|---|---|
| dev | `historical-dev.yaml`: v18, 90 days from 2026-07-31, 10k customers, one shard | `s3://platform-dev/datasets/payment-gateway/historical/v18` |
| prod | `historical-full.yaml`: v19, 365 days from 2025-10-29, 3M customers, change to version 3 on 2026-04-30, `minimum_source_bytes: 100000000000`, four shards | `s3://platform-prod-142737101504-us-east-1/datasets/payment-gateway/historical/v19` |

- **Settings:** the YAML holds only the logical dataset. Run settings come from the environment: `GENERATOR_MODE` (`generate`, `shard` or `publish`), `SHARD_INDEX` and `SHARD_COUNT` for shard steps, `GENERATOR_WORKERS` (set from the CPU request), `GENERATOR_CONTRACTS` (the contract directory, baked into the image) and `SIMULATOR_CONFIG`.
- **Shards:** shard `i` of `n` generates the days whose index modulo `n` is `i`. A publish step refuses until every day has a marker. Prod uses twelve shards, the `aws-cpu-burst` pool's node limit, with one shard per spot xlarge node.
- **Digest:** the generator digest is a SHA-256 over `world.py`, `lifecycle.py`, `historical.py`, `store.py`, the calibration file and the historical contracts. The configuration hash includes it, and `config.json` and the manifest record it. The image carries the same files, so the digest is reproducible between CI and the cluster.
- **Credentials:** the existing gateway object identity, held in the `gateway-storage` secret from the `datasets/payment-gateway/` scope.
- **Resources:** CPU, memory and ephemeral-storage requests sized to fit one spot xlarge node, plus `activeDeadlineSeconds`. Ephemeral storage stays modest because each worker's scratch is one part.
- **Retries:** `retryStrategy` on errors. A retried pod skips completed days by their markers, so a node replacement only costs the day in progress.
- **RBAC:** a `historical-generator` ServiceAccount, a Role with `workflowtaskresults` `create, patch` and its RoleBinding. The platform repository has no executor ServiceAccount bound in `data`, so this one is created here. The `gateway-storage` ExternalSecret supplies the S3 credentials.
- **Shared spec:** the common container spec (image, env, credentials, resources, node selector, retry and deadline) is one named template in `_helpers.tpl`, used by the shard and publish steps.
- **Prod:** the data-pipelines Argo CD application is enabled and renders only this workflow; Bronze, Silver, schedules and their credentials stay off.

*Alternatives:*
- A Helm-managed `Job`: rejected, because a Job runs at sync time, is immutable, and needs a rename for every run.
- Workstation CLI: rejected by the user; a multi-hour run should not depend on a laptop.

The local CLI remains for tests and development only.

### 12. Lakehouse storage layout and maintenance

- **Partitioning.** Bronze payment events are partitioned by `_source` and `days(occurred_at)`, labels by `_source` and `days(label_available_at)`. Silver payment events, attempts and labels are partitioned by `days(occurred_at)`, `days(started_at)` and `days(label_available_at)`. Rejected tables are unpartitioned or partitioned by ingestion day.
- **Writes.** Every Bronze and Silver table is created with `write.distribution-mode = hash` and `write.target-file-size-bytes = 268435456`, so each partition writes a few large files instead of one per task.
- **Order.** Silver overwrites every table on each run, so a Z-order lasts only until the next Silver run. Maintenance runs after Silver: daily at 03:43 UTC for historical data, and Kafka mode runs Silver every six hours. The evidence run keeps the order Silver, then `maintain-lakehouse`, then the benchmark.
- **Maintenance.** `maintain-lakehouse` runs one Spark stage over each Bronze and Silver table: `rewrite_data_files` with `binpack` to the target size (for `silver.attempts`, `strategy => 'sort'` with `zorder(customer_id,payment_method_fingerprint)`), `rewrite_manifests`, then `expire_snapshots` with `older_than` seven days and `retain_last` 10. The last keeps the snapshots that Silver's pinned reads need. The stage reports files and bytes from each table's `files` metadata table before and after. Compaction does not change row content or Kafka progress (decision 4).
- **Probes.** `maintain-lakehouse` also records, for one customer's 30-day window on `silver.attempts` (the customer with the most attempts), the files, bytes and planning time that Iceberg's planner selects before and after the Z-order rewrite. For partitioned Bronze it records the planned files for a one-day filter and for no filter. These are plan-only measurements read from the Iceberg scan's file tasks; no copies are made.
- *Alternative:* sort or Z-order every write. Rejected: write-time sorting adds a full shuffle to each Silver run. Maintenance applies the Z-order to `silver.attempts` once per day instead, because each Silver run overwrites every file anyway.

### 13. ClickHouse instance and users belong to this change

- **Ownership.** `infra/charts/clickhouse` (own app-of-apps entry, namespace `data`, enabled in dev and prod) defines the `ClickHouseInstallation`, the `clickhouse` Service, one ExternalSecret per user and a PostSync setup Job. The platform installs only the Altinity clickhouse-operator 0.27.4, which watches `data`.
- **Contract for Langfuse.** Service `clickhouse.data.svc.cluster.local` (HTTP 8123, native 9000), image `clickhouse/clickhouse-server:26.10.2.103`, one shard and one replica, and `allow_database_iceberg = 1` in the default profile.
- **Users.** `admin` (`CLICKHOUSE_ADMIN_PASSWORD`, access management, used by the setup jobs), `langfuse` (`CLICKHOUSE_LANGFUSE_PASSWORD`, `ALL` on `langfuse.*`, `SELECT` on `system.*`) and `openmetadata` (`CLICKHOUSE_OPENMETADATA_PASSWORD`, profile `openmetadata` with `readonly = 2` and `allow_database_iceberg = 1`, `SELECT` on `lakehouse.*`, `system.tables` and `system.columns`). The operator's management user is added by the Altinity normalizer with its own profile and is not defined here.
- **Passwords.** Each comes from an ExternalSecret on ClusterSecretStore `doppler`. Password references use `valueFrom.secretKeyRef`, the 0.27.4 form; the removed `k8s_secret_` prefix is not used.
- **Lakehouse split.** `lakehouse` now creates only the `lakehouse` DataLakeCatalog database, as admin, after the instance accepts connections. It authenticates the Polaris client through `catalog_credential` with `auth_scope` and `oauth_server_uri`, and recreates the database only when its rendered definition changes; the definition digest is stored in the database comment. A recreate drops the database object while it is rebuilt, so checks that run in that window fail. Iceberg tables are not touched.
- **Spike.** Before the chart is accepted, confirm on the 26.10 server: the exact OpenMetadata FQN and schema listing for the `clickhouse` connector; `catalog_credential` and the OAuth settings; that `COMMENT` on `CREATE DATABASE` round-trips through `system.databases`; that `DROP DATABASE` on a DataLakeCatalog leaves the Polaris tables in place; that `SHOW CREATE DATABASE lakehouse` as `openmetadata` and `system.query_log` show the client secret masked; and that the connector lists `lakehouse` with only `system.tables` and `system.columns` granted, adding the minimal grant to the instance if not.
- **Sizing.** Dev keeps the draft size, 2 CPU and 8 GiB requests with a 12 GiB limit, against the dev Proxmox workers, about 20 vCPU and 64 GiB across three VMs. Prod requests 500m CPU and 3 GiB with a 4 GiB memory limit and no CPU limit, sized beside the other databases on two On-Demand `m6a.xlarge` workers (4 vCPU and 16 GiB each). The platform docs do not record rendered prod requests, so the fit is unconfirmed until rollout.

### 14. The Polaris catalog belongs to this repository

The `lakehouse` application deploys the Polaris REST catalog that Spark, ClickHouse and the warehouse registration call, with its CNPG store, signing keys, object-storage secret and network policy. Its ExternalSecrets are in wave -2, the CNPG cluster in wave -1, and Polaris in wave 0 of that application. The `lakehouse` application is wave 1 and data-pipelines is wave 2. These waves order when Applications are created, not when they become healthy: this Argo CD has no Application health customization, so a later wave does not wait for an earlier application's health. Ordering therefore rests on retries. The warehouse registration and ClickHouse database jobs are PostSync hooks that retry until Polaris and ClickHouse answer, and both are idempotent. ClickHouse stays a separate application, and nothing in `lakehouse` depends on a later wave being healthy. The Service `iceberg-catalog.data.svc.cluster.local:8181`, realm `PLATFORM` and client `platform` do not change. Polaris chart 1.8.0 is vendored under `infra/charts/lakehouse/charts` because CI lints without fetching dependencies. That chart exposes liveness and readiness values but no startup probe, so liveness waits 60 seconds before its first check and tolerates six failures. Whether `CREATE DATABASE ... ENGINE = DataLakeCatalog` contacts the catalog at creation is a spike item; the job retries until ClickHouse answers and is idempotent either way.

### 15. Evidence is recorded from the prod v19 run

Spark applications write rolling event logs to `runs/payment-gateway/spark-events/` in each environment's bucket, using the S3A credentials the jobs already have. The Spark UI for each job is viewed after the run through a throwaway History Server pod, documented in `docs/data/offline.md`. The evidence set covers the Spark UI for DP1, DP2 and the benchmark, the Argo Workflows graphs and the Kafka CronWorkflow run, the OpenMetadata tables, lineage and test results, and the maintenance before-and-after report. Images go under `docs/data/images/`. The evidence is recorded after the prod v19 run, and the change is archived after that record.

### 16. Catalog registration belongs to data-pipelines

The `data-pipelines` application owns the OpenMetadata connector configuration, its ExternalSecrets and the register job. A PostSync job creates the services on each deploy. Every checks run ingests the ClickHouse tables before its native tests, so no scheduled ClickHouse discovery remains. Kafka topics are ingested by the on-demand `metadata-discovery` WorkflowTemplate. The `metadata-integrations` application is removed.

## Risks / Trade-offs

- [Bronze not run within Kafka retention (7 days)] → `failOnDataLoss` fails the run loudly. Enabling the schedule must come with a cadence or retention that covers it. Raising retention is a Kafka chart change owned by the live change.
- [`from_protobuf` types differ from the historical Parquet types (timestamps, enums, `uint64`)] → One explicit cast to the Bronze contract schema serves both sources. A CI test decodes protobuf fixtures and generated Parquet into the same Bronze schema, and any mismatch fails. Exact Spark 4.1 `from_protobuf` options are verified against the Spark docs during implementation.
- [The full-scale run takes hours and its throughput is unmeasured for the new code] → The dev v18 run measures rows per second per worker first, and the prod resources, shard count and deadline are set from that. The `aws-cpu-burst` pool caps prod at four xlarge nodes, so four shards is the ceiling until the pool limit changes. A pod lost to node replacement resumes from day markers. The run is reported only as a published manifest with its byte total; no coursework evidence is captured.
- [Prod compute and storage cost] → Four shard pods and one publish pod on spot prod nodes, with no Spark or OpenMetadata workloads enabled in prod. About 100+ GB is stored in the prod bucket. The template is on demand and does nothing until submitted.
- [Full Silver recompute grows with history] → Acceptable at development scale. Incremental Silver is a later change once volume justifies it.
- [Historical overwrite-by-filter rewrites data files] → Partitioning by `_source` keeps rewrites within historical partitions.
- [Spec deltas overlap the live change] → This change modifies only these requirements of shared specs:
  - `live-traffic-simulation`: "One shared current flow", plus four removed landing requirements;
  - `talos-gitops-deployment`: "Shared topics and Python Parquet landing" (removed; covered by the added "Shared payment topics"), "Runtime credentials and image delivery" (covered by "Simulator credentials and image delivery"), "Environment data-pipeline deployment", "Recurring DP1 schedule ownership", "Environment-configurable Python entrypoints";
  - `focused-verification`: "Cross-language simulation contracts" (modified), "Generator behavior tests" and "Data-pipeline behavioral verification" (replaced under new names), plus nine removed landing, scale and acceptance requirements.

  The archiver sentence in "Single simulator workload" is left to the live change, which already modifies that requirement for delivery faults.

  The live change is told to avoid them; whichever change archives second rebases its delta.
- [A standalone `dp2-silver` run can consume a Bronze snapshot that failed its checks] → Accepted for this change. Next step: Bronze checks set an Iceberg `validated` tag on the checked snapshots and Silver reads that tag, as one `versionAsOf` pin, with a failure when the tag is missing. The tag holds its snapshot's files, so `expire_snapshots` cannot remove them until the tag moves.
- [Generator and Silver correctness depend on CI only] → Behaviour tests cover:
  - per-day determinism and refused config changes;
  - the schema change boundary and duplicates;
  - Bronze idempotent historical rerun, Kafka offsets and empty runs (against a Kafka test container in CI), and decoding parity between Kafka and historical Parquet;
  - Silver conflict quarantine, linkage cases and availability; Kafka records that fail decoding are quarantined, not dropped.

## Migration Plan

Prod releases come only from `main`, and trusted PRs release to dev. The work therefore lands in two PRs:

1. **Generator PR → `main`:**
   - `simulator/world.py`, the rewritten generator and the reset `historical-payments` v1 contracts. This PR switches `live/` to `world.py` and deletes the old modules;
   - the `generate-historical` template;
   - the dev and full-scale configs;
   - the prod enablement of the generator workflow.

   After its releases:
   - submit `generate-historical` in dev (v18) to measure throughput;
   - set the prod resources and deadline from that measurement, then submit it in prod (v19).

   The DP1 on `main` cannot read v18. That is accepted: the pipeline PR supersedes it, and it is not scheduled.
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

- Contracts and data products are not in OpenMetadata. OpenMetadata 2.0.6 rejects the ODCS v3 files as written, found with `POST /v1/dataContracts/odcs/validate/yaml`:
  - schema-level `customProperties` as a list, which the importer reads as an object (400);
  - `servers.environment` as a string in the historical contracts, which the importer reads as an object (400);
  - a server-side null-pointer error (`String.hashCode`) on the bronze and silver contracts that stayed after removing quality rules, servers and top-level fields.
  Our files are valid ODCS v3. Rewriting them to the importer's dialect would break the `contracts` lint and the generator digest recorded in the v18 and v19 manifests. Git stays the authority. Retry `PUT /v1/dataContracts/odcs/yaml` on a newer OpenMetadata, and publish data products once ODPS is supported natively.
- Incremental Silver is not planned. The full recompute stays because cross-source dedup, conflict rejection and label linkage can change any payment's result, so a partial run cannot be exact. Revisit only if the v19 Silver run time is a problem.
- Exact transitive jar versions for the Kafka connector under Spark 4.1.3, resolved when the Dockerfile is written.
- Whether the benchmark should also show a `mergeSchema` on/off read over schema versions 2/3 files. It can be added to the benchmark job later without spec changes.
