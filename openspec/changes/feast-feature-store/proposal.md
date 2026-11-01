## Why

DP3 (`dp3-features-and-marts`, rebuilt as one day-range `dp-pipeline` in #109 and #110) produces the daily `gold.feat_` tables and `gold.label_attempt` in Iceberg. Flink produces `fraud-snapshot-v2` short-window snapshots on the feature journal topic. Nothing connects either of them to a feature store:

- the only Feast code is an online-only writer for the retired `fraud-snapshot-v1`, with a synthetic epoch-scoped key and a throwaway `/tmp` registry;
- nothing materializes DP3 features into Redis;
- the journal reaches neither store;
- there is no short-window history to train on;
- Go Fraud's feature client still reads the v1 views and is disabled.

The final-coursework feature-store rows ask for:

- an incremental materialization pipeline;
- jobs that push streaming features into the offline and online stores;
- a TTL for each feature table, with its reason;
- an offline store that a training notebook reads through Feast, joined with the label table.

This change assigns one engine to each feature definition so training and serving share its meaning. Spark owns the daily windows. Flink owns the short windows, both live and for history, which comes from a range-addressable Kappa replay through the same job. No feature has two implementations that could drift apart. The first training replay uses `start=2026-07-29`, `end=2026-10-29` (exclusive), the last three months of v19, and runs in prod, where DP3 runs. Simulation-truth training labels require no chargeback maturity gaps.

## What Changes

- **Feast repository, rebuilt (BREAKING for the Feast writer, server and Go client).**
  - **Entities:** one project with three natural-key entities, `integration`, `customer` and `payment_method`.
  - **Daily views:** `customer_daily` and `payment_method_daily`, as `SparkSource`s over `lake.gold.feat_customer_daily` and `lake.gold.feat_payment_method_daily` on the Polaris catalog.
  - **Stream views:** `customer_stream` and `payment_method_stream`, as `PushSource`s whose offline source is `gold.feat_stream_customer` and `gold.feat_stream_payment_method`, filtered to the configured epochs.
  - **TTLs** follow the longest window plus delay: 31 days 12 hours for the daily views, 24 hours 5 minutes for the stream views. The documentation records the reasons.
  - **Registry:** persistent, in object storage, written by a deploy-time `feast apply`.
- **One feature service, `payment_risk_v1`.** It covers the four stored views and the pure Python request_features on-demand view. Training requests it through `get_historical_features`, with `gold.label_attempt`'s simulation-truth rows as the entity dataframe at plain `started_at`, and so does Go Fraud. Stream rows become visible one microsecond after they are written, so a snapshot that counts the scored attempt is never returned. Training retrieval runs Feast's Spark offline store in local mode, in one Kubeflow Pipelines step.
- **Stream writers.** One `stream_writer` module runs as two Deployments; each commits its Kafka offsets only after its write:
  - **Online writer** (`--store online`) pushes journal snapshots to Redis through `FeatureStore.push`;
  - **Offline writer** (`--store offline`) appends them to the stream tables with PyIceberg, with the Kafka offsets in the Iceberg commit.

  Only the online writer replays an explicit offset or time range. The offline writer has one exactly-once append path.
- **Stream history by Kappa replay.** An on-demand `stream-history` workflow, run month by month:
  1. it seeds isolated topics from Bronze;
  2. it replays them through the live Flink job in logical-clock mode under epoch `history-v19`;
  3. the workflow deletes the month's history rows, then the offline writer appends through its normal path.

  It takes explicit start and end days, and rerunning a range replaces the same rows.
- **Materialization.** A `feast-materialize` Argo workflow runs on its own daily schedule after `dp-daily`, and on demand after a `dp-backfill`:
  1. it runs local Spark materialization with lookback plus one day of overlap;
  2. a verify_online stage validates a sample of newest keys.

  An optional since day starts a backfill reload, always ending now. Deleted entities retain old online values until TTL.
- **Idempotent runs.** History deletes and rebuilds its month; online replay keeps newest snapshots; materialization writes latest rows through now.
- **Arrow.** Arrow transfer is enabled wherever data crosses into Python: the Spark offline store's Arrow setting, `to_arrow()` retrieval, and offline writer's Arrow appends.
- **Go Fraud (BREAKING for the evidence contract).**
  - It makes one `get-online-features` call for `payment_risk_v1`, with natural keys and raw request fields, transformed by Feast.
  - It applies the same TTL rule as training, at the scoring time.
  - It stays shadow-only, and rules remain authoritative.
  - Its evidence moves from `features.v1` into a fraud-owned `fraud.v1` message, with the old field number reserved.
- **Prod enablement.** Prod enables the following, with dev staying the PR smoke environment:
  - Flink (new epoch `live-v19`);
  - the live simulator on the v19 manifest at 1 payment every 30 seconds, in bounded evidence runs;
  - Redis sized for v19 (about 6 GiB);
  - the stream writers, the feature server, `feast-materialize` and `stream-history`;
  - Go Fraud retrieval in shadow mode.
- **Governance (roadmap step 8).**
  - **Flink:** OpenMetadata's native Flink connector catalogues the streaming job, with SDK lineage as the fallback for the PyFlink Kafka edges.
  - **New jobs:** the stream writers, the stream history and materialization publish lineage and status.
- **Removed:**
  - the synchronous submission guard, its dedicated Redis deployment and credentials, counter-based observations and evidence fields;
  - `fraud-snapshot-v1` and the `features.v1` protobuf package, with its Python and Go bindings;
  - the v1 reference calculation and the v1 Feast writer;
  - the synthetic `snapshot_key`;
  - the build and image paths that copy them.

  The DP3 thread separately removes `gold.feat_attempt_rt` and its Spark short-window path.

## Capabilities

### New Capabilities

- `feature-store`: covers the Feast project and its registry; the natural-key entities; the daily and stream views with their TTLs; the single feature service; point-in-time offline retrieval with Arrow; the stream writers and their replay; the stream history replay; materialization with overlap and an online check; and online serving.

### Removed Capabilities

- Synchronous submission registration: removes the authorization guard capability (see the REMOVED delta); Flink's 5-minute stream window covers bursts with seconds-level delay.

### Modified Capabilities

- `streaming-fraud-features`: removes the journal-only output requirement, because the stream writers now consume the journal.
- `payment-contracts`: removes synchronous submission observations from the risk dependency contract.
- `fraud-decision`: removes synchronous submission observations; historical-feature retrieval becomes enabled shadow-only through `payment_risk_v1`, with per-view statuses and the shared TTL rule.
- `fraud-feature-contracts`: adds serving parity, and requires short-window history to come from the streaming job itself. It is ADDED only while DP3 is unarchived; retiring v1 waits for DP3's archive.
- `data-pipeline-orchestration`: adds the `feast-materialize`, `feature-writer-replay` and `stream-history` workflows (ADDED only).
- `data-catalog-governance`: adds the streaming-job catalogue and lineage, plus the stream-writer, stream-history and materialization lineage and status (ADDED only).
- `talos-gitops-deployment`: prod enables Flink, the live simulator, the feature store and Fraud retrieval in shadow mode.

## Impact

- **Code:**
  - `src/data-pipeline/feast`: rewritten on Python 3.12 with `feast[redis]==0.67.0`, `pyiceberg` and `confluent-kafka`, plus a `spark` extra. It holds the repository, apply, stream writers, materialize, online verification and server.
  - `src/data-pipeline/streaming`: the history seed, which encodes Bronze events into the replay topics.
  - `src/data-pipeline/features`: the v1 reference is removed.
  - `src/data-pipeline/openmetadata`: the Flink connector, lineage and pipelines.
  - `src/payment-gateway/fraud`: the Feast adapter, evidence mapping and configuration.
- **Contracts:**
  - `contracts/fraud/v1` gains fraud-owned evidence messages;
  - `contracts/features/v1` and `fraud-snapshot-v1.*` are removed;
  - `fraud-snapshot-v2.examples.json` gains lateness-boundary cases.
- **Infra:**
  - `infra/charts/fraud-features`: the stream writers, the server, the PostSync apply, Redis sizing and prod enablement, Flink `live-v19`;
  - `infra/charts/data-pipelines`: the `feast-materialize`, `feature-writer-replay` and `stream-history` templates, the materialization CronWorkflow, RBAC for the history FlinkDeployment, and the Flink connector;
  - `infra/charts/kafka`: ACLs for the history topics;
  - `infra/charts/live-traffic`: prod values;
  - `infra/charts/payment-gateway`: Fraud feature configuration and removal of the dedicated submission Redis;
  - the `data-platform-spark` image gains a Feast virtualenv.
- **Platform requests:** no new Doppler keys. The requests are:
  - prod Redis capacity;
  - the Flink operator and checkpoint path in prod;
  - `data` network paths;
  - the Flink connector in OpenMetadata ingestion;
  - bucket write access for `feast/`;
  - spot capacity for the multi-day history FlinkDeployment.

  See design, Platform requests.
- **Sequencing:** implementation on `feat/feast-feature-store`, rebased onto `origin/main` at `371d2ca`, with the user's go-ahead. Training reads only through Feast after the stream history for `[2026-07-29, 2026-10-29)` exists; there is no interim Spark dry run.
