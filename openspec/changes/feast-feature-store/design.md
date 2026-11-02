## Context

See proposal.md for the motivation. The current state that shapes the design:

- **DP3 on `main`** (the archived `payment-data-pipeline` change: one day-range `dp-pipeline` with per-layer publishing). Spark computes the daily features. The tables are Iceberg tables in the Polaris REST catalog `lake`, namespace `gold`, partitioned by `days(event_timestamp)`:
  - **`gold.feat_customer_daily`** is keyed by (`integration_id`, `customer_id`), and **`gold.feat_payment_method_daily`** by (`integration_id`, `payment_method_fingerprint`). Their columns follow `contracts/features/fraud-history-batch-v1.yaml`:
    - `event_timestamp` is a UTC-midnight anchor `s`, with windows `[s − w, s)` for `w` of 7 and 30 days.
    - Rows are written only at change points, so the latest row at or before `t` is the value at `t`. An entity's last row is all zeros.
    - Coverage nulls and an absent row both mean unknown.
  - **`gold.feat_attempt_rt`** (Spark's per-attempt short windows) was removed from DP3's code at the user's request (`371d2ca`, 2026-10-31): Flink alone owns the short windows (decision 8). Feast and training never read the retired table.
  - **`gold.label_attempt`** is keyed by (`integration_id`, `payment_id`, `attempt_id`, `label_source`). It also holds `customer_id`, `payment_method_fingerprint`, `label`, `label_available_at` and `occurred_at`. `event_timestamp` is the attempt's `started_at`, so every point-in-time join uses the scoring time. The two label sources are separate rows, and a missing chargeback is not a negative.
  - **`gold.dim_customer`:** SCD2 over the familiar-device set.
  - **`created`** on the daily tables is the commit time of the `silver.attempts` snapshot the writing run read. It is at or after `event_timestamp`, and it is only a tie-breaker.
- **DP3 runs:**
  - **One pipeline.** The WorkflowTemplate `dp-pipeline` runs Silver, Gold and the marts for one `start`/`end` range of UTC days, with write-audit-publish per layer: Silver, its checks and its publish; then Gold and the marts from published Silver, their checks, `publish-gold`, `publish-marts` and the profile. Its `pipeline` template holds the mutex `dp-writer` wherever it is referenced, and so does nightly `maintain-lakehouse` (03:43 UTC).
  - **Idempotent ranges.** Each run overwrites only its days' rows, so a rerun changes nothing.
  - **Daily.** The suspended `dp-daily` CronWorkflow (04:17 UTC) loads Kafka into Bronze, then runs the pipeline over yesterday plus `pipeline.lookbackDays` (2) days before it, to catch late outcomes. Rows at older anchors can therefore change or disappear between runs.
  - **Backfill.** `dp-backfill` runs the pipeline month by month, in order. The prod v19 backfill covers 2025-10-29 to 2026-10-28.
  - **Commit properties.** Every gold commit records the Silver snapshot IDs, `dp3.run_id`, `dp3.range`, `dp3.logic_version` (3) and `dp3.history_start`. `main` only moves when `publish-tables` fast-forwards it after the checks.
  - **Placement.** Prod Spark drivers and workflow pods run on the on-demand `cpu` nodes, and executors on the `spot` pool. The on-demand pool has little free CPU.
  - Gold snapshots expire after 7 days, and the last 10 are always kept (`maintain-lakehouse`).
- **Streaming:**
  - Flink writes `fraud-snapshot-v2` updates exactly-once to `fraud.feature-updates.v2.<epoch>`, carrying integration and entity IDs, `revision`, `anchor`, `online_write_at`, coverage and values.
  - `online_write_at` increases strictly per entity. Flink emits on every expiry, so the latest snapshot at or before `t` is exact at `t`, apart from Flink's processing delay.
  - Logical-clock mode (`CLOCK_MODE=logical`, `CLOCK_TOPIC`) replays recorded events deterministically. The job refuses it unless the input topic is isolated, the epoch is not a live epoch and a clock topic is set.
  - It reads raw `payments.v1` protobuf envelopes and decodes them with `wire.py`. Bronze `payment_events` holds the same events as decoded columns.
  - Flink runs today only in dev, with epoch `live-v18`. Prod disables live traffic, Flink, Feast and Fraud retrieval by spec (`talos-gitops-deployment`).
- **Today's Feast code:** a v1 writer and server with a synthetic `snapshot_key`, a `/tmp` registry and `ttl=0`. It is disabled everywhere. Redis (`payment-gateway-feast-redis`, password key `GATEWAY_FEAST_REDIS_PASSWORD`, 512 MB) is defined in `data-pipelines`.
- **Go Fraud** makes two v1 reads keyed by `snapshot_key`. It is disabled and shadow-only. Its evidence is a `features.v1` message inside `fraud.v1.DecideResponse`; Payments reads only the assessment ID and the evidence digest.
- **Feast 0.67.0,** checked against the published package source (Context7's quota was exhausted):
  - `SparkSource(table=…)` and `SparkSource(query=…)` read through the active Spark catalogs. `IcebergSource` loads a whole table into the driver, so it is not used.
  - Point-in-time SQL joins on `event_timestamp <= entity_timestamp`, within the TTL. It breaks ties with `MAX(created)`, and filters on `created` only with `filter_by_created_timestamp`.
  - A `PushSource` takes a `batch_source` for offline retrieval, and `push(to=ONLINE)` writes only the online store.
  - `materialize` records intervals in the persistent registry. The default local engine writes to Redis from the same pod.
  - By default, Redis skips writes that are not newer than the stored event timestamp. `skip_dedup` turns that off, and `key_ttl_seconds` expires entity hashes after their last write.
  - Python online reads never apply the TTL.
- **The generator** has volume seasonality (month multipliers of 1.35 for November and December, 0.8–0.85 for January and February, plus weekday and hour patterns) but constant fraud scenario rates. Training uses simulation truth over 2026-07-29 through 2026-10-28, without chargeback maturity gaps.

## Goals / Non-Goals

**Goals:**

- Use the platform's existing Argo orchestration, Iceberg on Polaris, and Flink v2 journal.
- **One engine per feature.** Spark owns the daily windows, for training and serving. Flink owns the short windows, for history (a replay of v19) and live, so no feature has two implementations that could drift apart.
- **One feature service, `payment_risk_v1`,** for training and serving.
- **Idempotent, range-addressable runs.** Rerunning the stream history or materialization over the same range leaves the same rows and values, with no duplicates. History takes start/end days, and materialization takes an optional since day with its end always now.
- **Online values that follow DP3's reruns,** including the lookback days that rewrite older anchors.
- **Running in prod,** where DP3 and its evidence run.

**Non-Goals:**

- **Training itself:** the notebook, Kubeflow pipeline and model belong to the training change, which calls `get_historical_features(payment_risk_v1)`.
- **A batch implementation of the short windows.** No Spark job computes `fraud-snapshot-v2`.
- **Seeding the live job's state from history.** Live Flink starts empty, and its windows report unknown coverage until they are covered.
- **Benchmarks** beyond the rubric, including Arrow performance comparisons.
- **`gold.dim_customer` as a view.** Its `valid_from_ts` equals the scored attempt's own start, so a `<= t` lookup would leak it.
- **Feast lineage in OpenMetadata.** Feast has no connector.

## Decisions

### 1. One Feast project, two runtimes, a persistent registry

`src/data-pipeline/feast` (`fraud_feast`) is the Feast repository:

| Module | What it runs | Runs as | Image |
|---|---|---|---|
| `repo.py` | Entities, sources, views and the service | Imported by the others | — |
| `apply.py` | `feast apply` and the stream tables | Argo CD Sync hook, before the writer and server roll out | `data-platform-spark` with the Feast virtualenv |
| `materialize.py` | Decision 6 | Argo container step, local Spark | `data-platform-spark` with the Feast virtualenv |
| `verify_online.py` | The online read-back check | Argo container step | `feast` |
| `stream_writer.py` | Live online and offline writes, `--history` offline-only | One Deployment and the history WorkflowTemplate | `feast` |
| server | `feast serve` on port 6566 | Deployment | `feast` |

- **History seed.** `fraud_streaming.history` (decision 8) lives with the Flink job, because it encodes events with the job's own `wire.py` descriptors. It runs on the streaming image.
- **Packaging.** Python 3.12. The base dependencies are `feast[redis]==0.67.0`, `pyiceberg` and `confluent-kafka`. A `spark` extra (`feast[spark]`) goes only into the Spark image and the training image, so the `feast` image has no JVM. `fraud_feast` is an installable package, and `spark.master` comes from the environment: `local[*]` by default for materialization and training retrieval (decision 7).
- **Training image.** The training change owns its image and `fraud_ml`, installing `fraud_feast[spark]` for local Spark retrieval. `fraud_feast/spark_jars.json` holds the shared Spark 4.1.3 and Iceberg 1.12.0 JAR pins and checksums; both image builds use `python -m fraud_feast.spark_jars <spark-jars-directory>`. `SPARK_MASTER=local[*]` selects local retrieval. When unset, Feast uses `local[*]`. Materialization uses `/opt/data-pipeline/feast/.venv/bin/python` in a single Argo container with a large spill `emptyDir`. No Feast SparkApplications are created.
- **Registry.** `registry_type: file` at `s3://<bucket>/runs/payment-gateway/feast/registry.pb`, inside the prefixes the `OBJECT_GATEWAY_*` keys already write,, with `cache_ttl_seconds: 60`. It uses the existing `OBJECT_GATEWAY_*` keys, plus `FEAST_S3_ENDPOINT_URL` in dev.
- **`feature_store.yaml`**, rendered from environment variables:
  - `project: payment_fraud`, `provider: local`, `entity_key_serialization_version: 3`;
  - `online_store`: Redis with `key_ttl_seconds: 2764800` (32 days). Hashes not rewritten for longer than the longest TTL drop out, which bounds prod memory.
  - `offline_store: {type: spark, spark_conf: …}`, with the `lake` REST catalog on Polaris (as in `batch.spark`), UTC, and `spark.sql.execution.arrow.pyspark.enabled: "true"`;
  - no `batch_engine` override: the default local engine writes Redis from the same pod;
  - `materialization: {pull_latest_features: true}`.
  - `materialize.py` sets `online_store.skip_dedup = true` in its own process only (decision 6). The server and feature writer keep the default, which never lets an older stream snapshot replace a newer one.

### 2. Natural-key entities and four views

```python
integration    = Entity(name="integration",    join_keys=["integration_id"])
customer       = Entity(name="customer",       join_keys=["customer_id"])
payment_method = Entity(name="payment_method", join_keys=["payment_method_fingerprint"])
```

Feast 0.67 allows one join key per entity, so every view lists `integration` plus its own entity.

| View | Source | Entities | Fields |
|---|---|---|---|
| `customer_daily` | `SparkSource(table="lake.gold.feat_customer_daily", timestamp_field="event_timestamp", created_timestamp_column="created")` | integration, customer | every `fraud-history-batch-v1` customer feature, for 7 and 30 days: `attempt_count`, `amount_sum_minor`, `decided_count`, `declined_count`, `risk_declined_count`, `decline_rate`, `distinct_merchant_count`, `distinct_device_count`, `distinct_payment_method_count` |
| `payment_method_daily` | the same, over `lake.gold.feat_payment_method_daily` | integration, payment_method | the shared seven measures and `distinct_customer_count`, for 7 and 30 days |
| `customer_stream` | `PushSource("customer_stream_push", batch_source=SparkSource(query="SELECT … FROM lake.gold.feat_stream_customer WHERE epoch IN (<configured epochs>)", timestamp_field="event_timestamp", created_timestamp_column="created"))` | integration, customer | `customer_snapshot_attempt_count_5m`, `_1h`, `_24h`, `customer_snapshot_distinct_devices_24h` |
| `payment_method_stream` | the same pattern, over `lake.gold.feat_stream_payment_method` | integration, payment_method | `method_snapshot_attempt_count_5m`, `_1h`, `_24h` |

- **Stream tables.** `gold.feat_stream_customer` and `gold.feat_stream_payment_method` hold every Flink snapshot: the natural keys, `event_timestamp` (the snapshot's `online_write_at + 1µs`), `created`, the values, `epoch`, `revision`, `_kafka_partition` and `_kafka_offset`. They are partitioned by `day(event_timestamp)`, created by `apply.py` with PyIceberg, written only by feature writer, and covered by `maintain-lakehouse`.
- **Epochs.** History rows carry `history-v19`, and live rows `live-v19`. The view query selects the configured epochs (`features.epochs`), so an old epoch drops out by configuration, not by deleting rows. History ends before live starts (decision 8), so the epochs never overlap in time.
- **Exact as-of values.** Flink emits a snapshot on every change, including expiries, so the latest snapshot at or before any `t` is exact at `t`, offline and online alike.
- **Schemas.** The schemas are explicit and match the definition files and the ODCS contracts, so `apply` infers nothing. A CI test compares `repo.py`'s fields with each definition's feature list.
- **Tags.** Every view is online and tagged with its definition ID: `fraud-history-batch-v1` for the daily views, `fraud-snapshot-v2` for the stream views.

### Request features

`request_context = RequestSource(...)` carries `scored_at`, `amount_minor`, `customer_account_created_at`, `merchant_account_created_at`, nullable `shipping_country` and `ip_country`, `item_count` and `has_device`. Timestamps use Feast UnixTimestamp, counts use Int64, countries String and device presence Bool.

`request_features` is a pure Python `on_demand_feature_view(mode="python", sources=[request_context])`. It returns amount_minor, customer_account_age_days, merchant_account_age_days, ip_shipping_country_mismatch, item_count and has_device. Ages are elapsed whole days at scored_at, or null when the account creation timestamp is absent. Country mismatch is null when either country is null. Item count is the number of checkout line items, and has_device is fingerprint presence. The slim Feast image needs no Spark to transform requests. Feast applies the same implementation through transform_arrow offline and transform_dict online. Feast 0.67.0 has a Python-mode online naming bug: full-name filtering drops unprefixed transformed outputs. The server installs a focused transform_dict shim that also exposes each declared output under its projected full name before Feast filters the response. It preserves short names and leaves transform_arrow unchanged; no feature calculations are duplicated. The HTTP parity test exercises this serving entrypoint. A CI test compares historical retrieval with the HTTP feature server for identical inputs, including nullable IP.

### 3. TTLs

| View | TTL | Reason |
|---|---|---|
| daily views | 31 days 12 hours | **Longest window:** a non-zero 30-day state always has a change-point row within 30 days plus one anchor day. **Delay:** 12 hours covers `dp-daily` (04:17 UTC, Bronze then the pipeline) and the materialization at 06:47 UTC, with margin for a slow run. **Expiry:** only an all-zero dormant row expires. |
| stream views | 24 hours 5 minutes | **Longest window:** 24 hours. Flink emits on each expiry, so a non-zero state always has a snapshot within 24 hours. **Delay:** 5 minutes bounds the delay from the journal to Redis. The history replay emits the same expiry snapshots, so the rule holds offline too. |

- **Rule.** Training and serving apply the same rule: a value older than `t − ttl` is unknown.
  - **Offline:** Feast applies it.
  - **Online:** Feast's online read ignores the TTL, so Go Fraud applies it, using the scoring time.
- **Where the values live:** in `repo.py`, and in the Fraud chart (`FRAUD_FEATURE_TTL_DAILY`, `FRAUD_FEATURE_TTL_STREAM`).
- **Consequences:**
  - An entity dormant for longer than window plus TTL becomes unknown, not zero, in both paths.
  - After an epoch change, old-epoch stream snapshots expire within 24 hours.

### 4. One feature service

```python
payment_risk_v1 = FeatureService(name="payment_risk_v1",
                                features=[customer_daily, payment_method_daily,
                                          customer_stream, payment_method_stream, request_features])
```

- **Shared by both paths.** Training requests it, and so does Go Fraud, so model inputs and served features cannot drift.
- **Why the stream views need live Flink.** Training on the short windows only makes sense if scoring sees them fresh. Without live Flink they would be stale or null at scoring time, so Flink `live-v19` and the feature writer ship with the stream views.
- **Versioning.** A model with other inputs gets `payment_risk_v2`.

### 5. One feature writer

`fraud_feast.stream_writer` runs as one `feature-writer` Deployment with consumer group `feature-writer`, read-committed isolation and manual commits.

1. **Decode and map.** Validate the v2 identity against the SHA-256 read from the contract definition file at import. One shared mapper sets natural keys, `event_timestamp = online_write_at + 1µs`, Kafka timestamp as `created`, values, epoch, revision and Kafka coordinates. Invalid records stop consumption without committing.
2. **Push online.** Each batch calls `store.push("<view>_push", df, to=PushMode.ONLINE)` before any offline commit. Every stream view also includes the always-set snapshot `epoch`, excluded from the model service, so public push writes null coverage over any previous epoch’s counts. Redis ignores repeated or older timestamps.
3. **Append offline.** Buffer rows until `offline_interval` (default 300 seconds) or `offline_rows` (default 100,000), then append once per Iceberg table with exclusive Kafka offsets in the table property `kafka.offsets.<epoch>` set in the same transaction; graceful shutdown flushes the buffer. Table properties survive snapshot expiry, so stream tables remain in nightly maintenance. Live and history writes share one refresh-and-retry helper: on `CommitFailedException`, refresh the table and retry the short transaction up to four attempts with exponential backoff. Live writes recompute the batch against the refreshed offsets on every attempt. PyIceberg's public append API is used because Feast's SparkSource query/table write path does not implement offline push.
4. **Restart.** Resume from the minimum of the two tables' Iceberg offsets, filtering out each table's already committed rows. A crash between online push and offline commit only re-pushes those rows. Iceberg offset properties are the only recovery source; the writer never commits Kafka consumer-group offsets. No repository dashboard or alert uses its group lag.
5. **Modes.** Live writes online and offline. `--history --history-start <day> --history-end <day>` deletes only the unit’s epoch and time range in one short commit per table, then buffers through the same StreamWriter as live mode (100,000 rows or 300 seconds), committing each flushed buffer through IcebergWriter and the same retry helper; the final flush writes the remainder. No transaction stays open across batches, so live writes and nightly compaction can proceed. It reads temporary topics from offset zero, skips lead-in and out-of-range rows, and never stores or resumes history offsets. Failed units retry from zero through Argo. A running or failed unit is partially visible; training must wait for the entire replay to pass its gates. Redis stream recovery uses `fraud_feast.materialize --stream-recovery` over the last STREAM_TTL from the stream batch sources.

CI covers push-before-commit, timed and row-count flushing, partial-table commit recovery, older and repeated Redis writes, new-epoch null coverage replacement and fail-before-commit on malformed journal records. History retries failed units by deleting their partial rows and replaying from offset zero. CI covers real commit conflicts, concurrent live commits between history batches and an interrupted unit rerun without duplicates.

### 6. Materialization: `feast-materialize`

Each run calls `store.materialize(start, now, feature_views=[view])` for each daily view with `pull_latest_features` and `skip_dedup`. The default start is the view's registry last end minus (`pipeline.lookbackDays` + 1) days, covering DP3's lookback rewrites. The first run starts at `now − ttl`. Stream views are never materialized offline.

An optional `since` day overrides the start after a backfill; the end is always now. An earlier end could overwrite newer online values with older rows. Rerunning against the same Gold data writes the same latest values. There is no separate state file, snapshot traversal, entity refresh set, deletion tombstone or full-refresh fallback.

**Known gap.** When DP3 deletes all in-window rows of an entity, its previous online value remains until its TTL. Materialization cannot discover a deleted entity from the current rows.

**Workflow.** Under `dp-writer`, one Argo container on `data-platform-spark` runs local Spark (`local[*]`) with the Feast virtualenv, the default local materialization engine and a large spill `emptyDir`. A second container runs `verify_online`, sampling up to 200 newest keys per daily view and checking their timestamps and values through explicit daily feature references. It fails on missing or differing values. No refreshed-entity sample remains. The exit handler publishes status, windows, optional since and sample counts. `feast-materialize-daily` stays suspended, scheduled after `dp-daily` at 06:47 UTC. After `dp-backfill`, submit with `since=<backfill start>`.

### 7. Training retrieval and Arrow

- **Entity rows.** `lake.gold.label_attempt` rows with `label_source = LABEL_SOURCE_SIMULATION_TRUTH` and `label_available_at <= cutoff`, one per attempt, within the cohort window, as a Spark DataFrame carrying both entities' keys. `fraud_feast.training.entity_rows(spark, start, end, cutoff)` joins these labels to lifecycle-valid `silver.attempts` and the request fields of their `attempt_started` in `silver.payment_events`. Their `label` is the training label (user decision 2026-11-01: a learning project, so simulation truth stands in for real labels; it also labels declined attempts, and the metrics are better than a real deployment could reach). The entity `event_timestamp` is the attempt's `started_at`, the scoring time, exactly as in serving.
- **Runtime.** Training retrieval runs in one Kubeflow Pipelines step: Feast's Spark offline store in local mode (`spark.master=local[*]`) on the training image, which installs `fraud_feast[spark]` with the Spark 4.1.3 and Iceberg 1.12 jars pinned in `data-platform-spark`. The node needs local disk for Spark's spill. The step writes the dataset to a versioned location, which the training change owns.
- **Call.** `store.get_historical_features(entity_df=entity_df, features=store.get_feature_service("payment_risk_v1"), full_feature_names=True).to_arrow()`. Columns are named `<view>__<feature>`; full names prevent collisions between the daily views' seven shared feature names.
- **No `created` filter.** `filter_by_created_timestamp` stays off. `created` is a Silver commit time or a Kafka record time, at or after `event_timestamp` and usually later than the scored time `t`, so the filter would drop valid rows. `created` only breaks ties.
- **Training prerequisite.** Training reads only through `get_historical_features(payment_risk_v1)` after the Flink history exists. There is no interim Spark dry run. The training change owns single-GPU training through Kubeflow.
- **History.** The daily history is DP3's `dp-backfill` over v19 (2025-10-29 to 2026-10-28). The first short-window replay for training uses `start=2026-07-29`, `end=2026-10-29` (exclusive), the last three months of v19, with the workflow adding its own one-day lead-in (decision 8). A full-year replay is not a training prerequisite; a longer replay uses different window parameters.
- **Splits.** Ordered by time, never random. Simulation-truth labels are complete at `started_at`, so no maturity gaps or immature tail are needed. The training change owns the windows.
- **Arrow** is on in the offline store's Spark configuration (decision 1). No performance comparison is planned.
- **Cohort window.** The heavy step is the point-in-time join of the stream snapshots (several per attempt) and the daily tables with the labels, so training can pick a cohort window.
- **Test.** One CI test on a local warehouse applies the repository and retrieves `payment_risk_v1`. It covers:
  - the daily row on the scored day;
  - a row whose `created` is later than `t`;
  - TTL expiry to null;
  - a stream snapshot written at exactly `started_at` that counts the scored attempt, invisible at `started_at` because of the `+ 1µs` mapping;
  - stream values read from both a history and a live epoch, and a deselected epoch ignored;
  - a label row with a null payment method, whose method values are null.

### 8. Stream history: a Kappa replay through the live Flink job

**Why one engine.** A second, batch implementation of the short windows would need replay and consistency checks to prove it agrees with Flink. That is the classic Lambda drift risk (Jay Kreps, "Questioning the Lambda Architecture"). Replaying history through the same job gives training the same short-window logic used in serving. The cost is replay time, which runs in month-sized, restartable ranges.

**Seed runtime.** The seed module lives in the streaming project with a `history` dependency group and one lockfile. Flink's Beam dependency requires PyArrow <17 while PyIceberg requires >=17, so the image installs the conflicting `flink` and `history` groups into separate Python 3.11 virtualenvs from that lockfile. DuckDB remains for the out-of-core, deterministically ordered Bronze sort.

**`stream-history`** is an on-demand WorkflowTemplate with `start` and `end` days, defaulting to `[2026-07-29, 2026-10-29)`. It processes one month at a time, in order, under mutex `stream-history`, and stops at the first failed month. For each month:

1. **Topics.** Recreate the epoch's topics: `payments.events.history-v19`, `fraud.clock.history-v19`, `fraud.feature-updates.v2.history-v19` and `fraud.feature-rejects.v1.history-v19`. Cleanup removes them after each unit.
2. **Seed.** `fraud_streaming.history` reads the month from Bronze `payment_events`, plus one day of lead-in, in source order. It encodes each event as the `payments.v1` envelope with the descriptors Flink decodes (`wire.py`), and writes the input and clock topics.
   - Seeding finishes before Flink starts; Kafka retains the whole unit until cleanup, without a size or age retention cap.
   - A CI round-trip test proves that a Bronze row, encoded, decodes to the same event in `wire.decode`. The seed uses the streaming project's history dependency group.
3. **Replay ordering (approved 2026-10-31).** History input and clock topics each have one partition. Each clock control carries a sequence and exclusive input-offset barrier. The source buffers both streams in checkpointed state, releases input only through the next barrier, then advances the clock. An initial offset-zero barrier fixes the bootstrap instant. Missing, regressing or oversized barriers fail the replay. This extends the existing Flink logical-clock protocol because Kafka orders records only within a topic partition; without the offset barrier, a clock can overtake input and change lateness and expiry results.

   **Replay.** Each unit runs topics → seed → Flink FINISHED → writer → cleanup. In logical-clock mode only, the Kafka SQL source uses `scan.bounded.mode=latest-offset` with earliest startup offsets. The mutex-serialized `fraud-streaming-<history epoch>` FlinkDeployment uses the live job’s image in logical-clock mode with epoch `history-v19`, from empty state. Flink’s shared Role permits get only on the named live and history Deployments. One day of lead-in fills every 24-hour window and the dedup horizon, so months are independent. History sets `execution.shutdown-on-application-finish=false` so REST counters remain available until cleanup. Verified against Flink 2.2.1 and Kafka connector 5.0.0-2.2: the [final checkpoint before task exit](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/checkpointing/#waiting-for-the-final-checkpoint-before-task-exit) completes the two-phase sink commit before FINISHED; no checkpoint-count heuristic is needed.
4. **Offline write.** The history writer deletes the unit’s history-epoch rows in one short commit per table, then buffers through StreamWriter with the live row/time bounds and commits each flushed buffer through IcebergWriter, including the final remainder. Deletes and appends share the live writer’s refresh, bounded retry and backoff helper; history writes no offsets or properties. It skips lead-in rows (`event_timestamp < start`) and rows at or after the exclusive end. After Flink reaches FINISHED, reads start at zero and stop at read-committed partition EOF, including the final committed sink transaction. There is no seed polling, checkpoint heuristic or high-watermark end calculation. The month is visible partially while it runs or after failure. Argo retries from the initial range delete, rebuilding one copy of its rows. Nothing trains during a replay; training starts only after all units pass the gate.
5. **Checks.** Before deleting the unit's range, the writer checks the decode-reject count is zero, the seed completed, and the bounded Flink job reached FINISHED, which requires consuming both seeded topics to their end. Duplicates and too-late rejects are reported, not gated. Snapshots carry no attempt IDs, so an attempts-in-snapshots gate is impossible. Counts, seeded offsets and job state go to the run report and pipeline status.
6. **Cleanup.** An exit handler deletes the FlinkDeployment and the epoch's topics, on success and failure.

**History and live ranges.** History covers the v19 dataset range. Live `live-v19` starts afterwards, from the live simulator's own events, so the epochs never overlap. Live Flink starts empty, and its windows report unknown coverage for their first 24 hours. Training uses only history.

**Throughput.** Full v19 is about 984M events, so the training range is about 250M. One logical-clock job is single-partition by design, so a replay scales only by running units side by side: units of `historyUnitDays` (4 in prod), each with its own run name `history-v19-<YYYYMMDD>`, topics, deployment, transactional IDs and consumer group, up to `historyParallelism` at once (24 in prod, two per 4-vCPU spot node) on the spot pool. Each unit's one-day lead-in makes units independent. Inside a unit, the seed writes one bootstrap and one final clock control, so the barrier releases inputs immediately instead of after a control per Arrow batch; the producer batches with `linger.ms=50`, 1 MiB batches and zstd; the TaskManager requests 1.4 CPUs and 5 GiB, keeps keyed state in RocksDB with incremental checkpoints, and a finished seed is reused instead of repeated; history commits retry up to eight times with jitter because concurrent units commit to the same tables. Units can be rerun individually.

**Skew.** History rows carry logical time, with no processing delay, while live serving reads snapshots after Flink's processing delay. The page states this, and Go Fraud's evidence records the served event timestamps for sampling.

**Kept checks:** Flink's existing conformance test on `fraud-snapshot-v2.examples.json`, which gains attempts just inside and just past the 24-hour lateness horizon; the seed round trip; and the per-month gate. No cross-engine parity check is needed.

### 9. Go Fraud feature client

The synchronous submission guard is removed, including registration, thresholds, evidence and its dedicated Redis instance. The 5-minute Flink stream window covers bursts at the cost of seconds-level detection; feature retrieval remains shadow-only. Payments' own Redis and Feast's online Redis remain separate and unchanged. Assessment evidence reserves fields 11 and 12 after removing the submission observations.


The single online request sends the eight request-context fields in the same `entities` map as the natural keys. Go computes no model request features; rules retain their own logic. Evidence includes request_features and nullable booleans. Feast's ODFV response timestamps are epoch timestamps, so Go records request_features as available at scored_at. Stored views retain the existing TTL, future, missing and failure statuses.


- **Request.** One `POST /get-online-features` with `{"feature_service": "payment_risk_v1", "entities": {"integration_id": [i], "customer_id": [c], "payment_method_fingerprint": [m], "scored_at": [t], "amount_minor": [amount], "customer_account_created_at": [customer_created], "merchant_account_created_at": [merchant_created], "shipping_country": [shipping], "ip_country": [ip_or_null], "item_count": [count], "has_device": [present]}, "full_feature_names": true}`.
- **Per-view statuses:**
  - **missing:** all null at the Unix epoch;
  - **expired:** scoring time − event timestamp > TTL;
  - **future:** event timestamp > scoring time, which excludes a stream snapshot that already counts the scored attempt;
  - **timeout** and **error** (including a malformed response);
  - **available:** otherwise.
- **Scoring clock.** The application captures its server UTC clock once before feature retrieval; it sends this as `scored_at`, evaluates TTL and future timestamps against it, and persists it as `EvaluatedAt`. Client `signals.attempted_at` does not control feature freshness. Training uses the server `started_at`.
- **Configuration.** `FRAUD_FEATURE_SERVICE`, `FRAUD_FEATURE_TTL_DAILY` and `FRAUD_FEATURE_TTL_STREAM` replace the epoch, definition-hash, anchor-age and progress-age settings. The policy stays `rules-v1`, shadow-only, and the evidence schema version becomes 2.
- **Evidence contract.** `contracts/fraud/v1/evidence.proto` defines a fraud-owned `AssessmentEvidence`: the feature service, a `FeatureViewEvidence` per view, with no synchronous submission-counter messages or fields.
  - `DecideResponse` reserves field 3 and uses field 4.
  - Payments needs only regenerated bindings.
  - A v1-schema assessment replayed within the minutes-long retry horizon after the deploy fails the replay check; the deploy happens while live traffic is off.

### 10. Environments: dev and prod

The published v19 manifest was read from its recorded S3 URI on 2026-10-31; its SHA-256 is `78ee54fe2a607ae0c128bc6765d89238f28d00270fca5ad48a8fed44fd0f8608` (722,999 bytes, last modified 2026-10-29T09:13:16Z).

| | dev | prod |
|---|---|---|
| Flink epoch | `live-v18` | `live-v19` (new) |
| Live simulator | v18 manifest | v19 manifest, 1 payment every 30 s, bounded evidence runs (7 days by default) |
| Redis | existing, 512 MB | about 6 GiB `maxmemory` on `cloud-block`: about 3M customer and 3M payment-method hashes for the daily views, plus the live stream keys |
| Flink, Redis, feature writer, server and Feast workflows (`features.enabled`) | on | off until platform provisioning |

The prod simulator uses a Job with no retry, so completing the seven-day duration does not trigger a Deployment restart and begin another evidence run.

Prod feature infrastructure stays disabled under the single `data-pipelines.features.enabled` flag until platform provisioning. Its eventual enablement modifies the `talos-gitops-deployment` environment requirement, which no unarchived change touches. Dev stays the PR smoke environment, and the evidence comes from prod, as for DP3.

### 11. Governance (roadmap step 8)

Kafka access follows the existing unauthenticated-listener and network-policy pattern, as the user confirmed on 2026-10-31. The feature-writer Deployment and `data-pipelines` are admitted from `data`. Authenticated per-client topic ACLs are optional future work and are not a prerequisite for this change.

- **Flink connector.** `catalog.connectors.flink` (`kind: pipeline`, `type: Flink`, JobManager REST on port 8081) runs in `metadata-discovery`. The `fraud-streaming` network policy admits the metadata job's pods.
- **Fallback.** If the connector does not resolve the PyFlink Kafka lineage, `openmetadata.lineage` publishes `payments.events.v1` → `fraud-streaming` → the journal and reject topics.
- **Kafka connector.** Its topic filter adds the journal and reject topic patterns.
- **New pipelines:**
  - `feature-writer`: the journal topic → the pipeline → `gold.feat_stream_*`; Redis is not catalogued.

  The continuous writer is published by `metadata-discovery`, and history runs add their range to the run status.
  - `feast-materialize`: `gold.feat_*_daily` → the pipeline, with status, materialization windows, optional since and sample count, and no output edge.
  - `stream-history`: Bronze `payment_events` → `gold.feat_stream_*`, with each month's range, epoch, counts and gate outcome.

### 12. Cleanup

- **Python:** the `fraud_feast` v1 modules and tests, `fraud_feast/contracts/features/v1`, and `features/reference.py` with its v1-only tests.
- **Contracts:** `contracts/features/v1/`, `fraud-snapshot-v1.*`.
- **Go:** the `features/v1` bindings and the v1 evidence mapping.
- **Build:** the v1 paths in `schema-register.Dockerfile` and `service-images.json`.
- **Chart values:** `feast.writerGroup`, `definitionHash`.
- **Docs:** the stale text on the feature-store page.

### Platform requests

After merge, the user/platform owner can delete the Doppler key `GATEWAY_AUTHORIZATION_REDIS_PASSWORD`; no deployment consumes it.

No new Doppler keys. Each request was checked against `talos-proxmox` and the live prod cluster on 2026-11-01; none needs a platform change:

1. **Prod Redis capacity:** about 6 GiB `maxmemory` (a request near 7 GiB) and a 10 Gi gp3 volume in `data`. The `aws-cpu-burst` pool allows 48 vCPU / 192 GiB, and gp3 allows volume expansion.
2. **Flink in prod:** `flink-kubernetes-operator` 1.16.1 runs in `operators` and watches `data`. Checkpoints go under `runs/payment-gateway/flink`, which the gateway keys already write.
3. **Network:** `data` has no default-deny policy, so pods there reach Redis on 6379.
4. **Flink connector:** the OpenMetadata ingestion image is this repository's (`openmetadata/ingestion:2.0.6`); the SDK fallback covers lineage if the connector is missing.
5. **Bucket access:** the registry lives under `runs/payment-gateway/feast/`, which the foundation-owned prod policy and the dev MinIO policy already grant, so no `feast/` prefix is needed.
6. **Flink history runs:** the operator reconciles any FlinkDeployment in `data`; `aws-cpu-burst` has room for the history TaskManagers.
7. **Training retrieval access:** `iceberg-catalog-private` already admits `ml` on 8181. Object credentials for `ml` belong to the training change.
8. **Kafka disk:** prod Kafka is one broker with a 5Gi gp3 volume; this repository's `kafka` chart raises it to 256Gi (64Gi filled about 9 hours into the replay) for the seeded input and journals of all history units, and the history topics use replication factor 1 to match the single broker.

## Risks / Trade-offs

- **[Training/serving skew of the DP3 delay]** → Training sees the daily anchor at midnight, while serving gets it after `dp-daily` and materialization (about 07:00 UTC). The definition's `availability` block records this, and the TTL's 12 hours covers it.
- **[The v19 replay takes days]** → restartable units (4 days in prod) run up to 24 at a time, and the first training replay is limited to `[2026-07-29, 2026-10-29)`.
- **[The seed encodes events differently from production]** → A CI round trip through `wire.decode`, and the per-month gate on decode rejects and the bounded job reaching FINISHED.
- **[Kafka disk during the replay]** → Kafka must hold the entire seeded month and its output until unit cleanup. The history topics use zstd compression; prod Kafka storage is 256Gi for the 23 four-day units (about 37 GB of input); the journals are the larger part and filled the first 64Gi volume.
- **[Training/serving skew of Flink's processing delay]** → History has none; serving misses attempts Flink has not processed yet. The page states it, and the evidence samples it.
- **[Flink's first 24 hours have unknown coverage]** → Serving reports those values as unknown, never zero. Training history uses the replay's one-day lead-in.
- **[DP3 rewrites older anchors]** → Overlap covers daily lookback; backfills require an explicit since day.
- **[DP3 deletes all rows of an entity]** → Its old online value remains until TTL.

- **[Prod Redis memory]** → Only keys within the TTL are written, and `key_ttl_seconds` drops stale hashes. The first prod materialization records key counts and memory.
- **[TTL expiry turns dormant zeros into unknowns]** → Accepted, and applied identically offline and online.
- **[Stream Redis loss]** → Materialize the last STREAM_TTL from the stream batch sources with `--stream-recovery`.
- **[Feature server latency inside Go's 100 ms budget]** → One call. A shadow timeout never changes the decision.
- **[The Flink connector cannot see PyFlink Kafka sources]** → The SDK fallback.

## Rollout Plan

1. **Start gate.** The implementation branch is rebased onto `origin/main` at `371d2ca`, which removes DP3's Spark short windows. The user authorized implementation and a draft PR. Validate with `--strict`.
2. **Implement, validate, then push the draft PR.** Dev can be recreated; no resource adoption, old-data migration or legacy compatibility is required. The dev deploy is the v18 smoke run:
   - apply, then feature writer on `live-v18`;
   - a `dp-backfill`, then `feast-materialize`;
   - a `dp-pipeline` run without a range (yesterday plus the lookback) and `feast-materialize` again, refreshing the lookback days;
   - `feast-materialize` with an explicit since day, run twice;
   - stream Redis recovery through materialization, run twice;
   - `stream-history` over one v18 month, then rerun over one of its days, with equal row counts;
   - a Fraud read.
3. **After merge and platform provisioning, enable `data-pipelines.features.enabled` in prod:**
   - the apply Job runs;
   - after the v19 `dp-backfill`, run `feast-materialize` with `since=<backfill start>`;
   - run `stream-history` with `start=2026-07-29`, `end=2026-10-29`, month by month; the served model's training waits for this range;
   - start Flink `live-v19` and feature writer;
   - run the simulator at 1 payment every 30 s for the evidence window, with `dp-daily` and `feast-materialize-daily` unsuspended, and sample Fraud evidence;
   - run a sample `get_historical_features(payment_risk_v1)` over a v19 cohort.

   Record the Argo graphs, the Spark UI, row and sample counts, the history replay's rate, counts and gates, Redis keys and memory, lineage, the Flink connector outcome, and the online check.
4. **Evidence in the docs** (task 8.3): figures, screenshots and mermaid, one section per rubric row, with no text or JSON evidence files and no CI screenshots.

**Rollback:** revert the PR. Then delete the registry object, the Feast Redis keys, `gold.feat_stream_*`, and any leftover `history-v19` topics. Prod returns to Flink, Feast and retrieval disabled.
