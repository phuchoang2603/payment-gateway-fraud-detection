## 0. Start gate

- [x] 0.1 Rebase `plan/feast-store` onto `main` at `bb8dc67`, with DP3 run by `dp-pipeline`, `dp-daily` and `dp-backfill` under the `dp-writer` mutex. Update the artifacts for one engine per feature (user decision 2026-10-31):
  - Spark DP3 owns the daily windows; take the rerun ranges from `dp3.range`, use the `dp-writer` mutex, schedule materialization after `dp-daily`, and set the daily TTL delay to 12 hours;
  - Flink owns the short windows: live through push jobs A and B, and history through the `stream-history` Kappa replay into `gold.feat_stream_*` (decision 8);
  - the training entity timestamp is `started_at − 1µs`.

  Verify with `openspec validate feast-feature-store --strict`.
- [ ] 0.2 Confirm with the DP3 thread that `gold.feat_attempt_rt` and its Spark short-window path are removed or scheduled for removal. Wait for the user's go-ahead. If `dp3-features-and-marts` is archived by then, add REMOVED or MODIFIED `fraud-feature-contracts` requirements that retire `fraud-snapshot-v1`; otherwise record that they stay pending. Verify with `openspec validate --strict --all`.
- [ ] 0.3 Send the design's platform requests through the user: prod Redis capacity, the Flink operator and checkpoint path in prod, on-demand capacity for the materialize driver, `data` network paths, the Flink connector in ingestion, `feast/` bucket write, and spot capacity for the multi-day history FlinkDeployment in prod. Make no cluster changes. Verify that each answer is recorded in design.md.

## 1. Contracts and v1 cleanup

- [ ] 1.1 Add `contracts/fraud/v1/evidence.proto` (decision 9). In `fraud.proto`, reserve `DecideResponse` field 3 and use field 4. Regenerate the Go bindings. Verify with `buf lint`, generation, and `go build ./...` for fraud and payments.
- [ ] 1.2 Delete:
  - `contracts/features/v1/`, `fraud-snapshot-v1.*`, and the Go and Python v1 bindings;
  - `src/data-pipeline/features/reference.py` and its v1-only tests;
  - the v1 paths in `schema-register.Dockerfile` and `service-images.json`.

  Verify that `git grep -e features/v1 -e fraud-snapshot-v1 -e featuresv1` matches only archived changes, and that the v2 conformance tests still run in CI.
- [ ] 1.3 Add v2 conformance cases to `fraud-snapshot-v2.examples.json`: attempts just inside and just past the 24-hour lateness horizon. The definition hash covers only `fraud-snapshot-v2.yaml`'s bytes, so the identity and the live epoch stay valid; confirm this before editing. Verify that the Flink conformance test passes the new cases in CI.

## 2. Feast project

- [ ] 2.1 Rework `src/data-pipeline/feast`:
  - Python 3.12, `feast[redis]==0.67.0`, `pyiceberg`, `confluent-kafka`, and a `spark` extra;
  - `feature_store.yaml` per decision 1: the Polaris catalog and Arrow in `spark_conf`, `spark.engine`, `pull_latest_features`, Redis `key_ttl_seconds`, and the configured stream epochs;
  - delete the v1 modules and tests.

  Verify that `uv lock`, `ruff` and `ruff format --check` pass locally.
- [ ] 2.2 Add `fraud_feast/repo.py` (decisions 2–4: entities, the four views with explicit schemas, the stream views' epoch-filtered queries, TTLs and tags, and `fraud_model_v1`) and `fraud_feast/apply.py` (creates the two stream tables, then applies). Verify in CI with:
  - a test that `repo.py`'s fields equal each definition file's feature list per entity;
  - one local-warehouse test that applies the repository and runs `get_historical_features(fraud_model_v1).to_arrow()` with `label_attempt` rows. It covers the daily row on day D, a row whose `created` is later than `t`, TTL expiry to null, a snapshot at exactly `started_at` excluded by the `− 1µs` timestamp, values read across a history and a live epoch with an unconfigured epoch ignored, and null method values for a null payment method.
- [ ] 2.3 Add `fraud_feast/push_job.py` (decision 5): `--to online|offline`, the continuous modes, the `--from`/`--until` replay, and B's history mode (per-day overwrite of the history epoch, lead-in dropped). Replace `tests/test_writer.py` with `test_push_job.py` on the existing Redis container and a local warehouse, covering:
  - an older revision not replacing a newer online value;
  - offline resume from the commit's offsets after a crash before the Kafka commit;
  - an offline replay run twice over a committed range, leaving one row per record and the watermark unchanged;
  - a replay past the watermark failing without writing;
  - a history-mode day written twice, with equal rows and no lead-in rows;
  - an invalid record stopping the job without a commit.

  Verify in CI.
- [ ] 2.4 Add `fraud_feast/materialize.py` (decision 6: snapshot pinning, `materialize_incremental`, rerun ranges from `dp3.range`, both-snapshot entity sets with null rows for removed entities, the `skip_dedup` override, the explicit range, the full-refresh fallback and the state file) and `fraud_feast/check_online.py`. Verify in CI with local-warehouse and Redis tests:
  - a lookback day rewritten by a second gold commit reaches Redis;
  - a deleted entity reads back as unknown;
  - a range over old anchors leaves newer online rows, the registry interval and the state unchanged;
  - a rerun writes the same values;
  - a missing state triggers the full refresh;
  - a missing online entity fails the check and leaves the state unchanged.

- [ ] 2.5 Add `fraud_streaming/history.py` (decision 8): reads a Bronze range plus one day of lead-in in source order, encodes each event with the `wire.py` descriptors, writes the history input and clock topics, pauses while the replay's lag exceeds a bound, and reports the duplicate and too-late counts the gate compares. Verify in CI with a round-trip test (Bronze row → encoded bytes → `wire.decode`) and a small logical-clock replay whose snapshots match `features/reference_v2.py`.

## 3. Images and charts

- [ ] 3.1 `data-platform-spark.Dockerfile`: add the Feast virtualenv with the `spark` extra, and add `src/data-pipeline/feast` to its rebuild paths. `feast.Dockerfile`: Python 3.12 slim with the push job, check and server entrypoints. Verify that the CI image builds pass.
- [ ] 3.2 `infra/charts/fraud-features`:
  - the `feast-push-online` and `feast-push-offline` Deployments, and the feature server;
  - the PostSync apply Job;
  - the object-storage and Iceberg ExternalSecrets;
  - Redis client labels and sizing values;
  - the JobManager REST policy for metadata pods;
  - remove the v1 writer, `feast.writerGroup` and `definitionHash`;
  - prod values: `feast.enabled`, `streaming.enabled`, the epoch and topic `live-v19`, Redis about 6 GiB on gp3.

  Verify with CI helm lint and template for every values file.
- [ ] 3.3 `infra/charts/live-traffic`: enable the simulator in prod on the v19 manifest and digest, at 1 payment every 30 seconds. Verify with CI helm template.
- [ ] 3.4 `infra/charts/data-pipelines`:
  - `feast-materialize` (SparkApplication, then check, then publish; optional `start` and `end`; mutex `dp-writer`; the shared Spark helper and placement) and the suspended `feast-materialize-daily` CronWorkflow at 06:47 UTC;
  - on-demand `feast-push-replay` (`to`, `from`, `until`; a mutex per store; publish);
  - on-demand `stream-history` (`start`, `end`; mutex `stream-history`): per month, recreate the epoch topics, seed, a logical-clock FlinkDeployment, push job B in history mode, the gate, and an exit handler that deletes the deployment and topics and publishes; RBAC for FlinkDeployments in `data`; TaskManagers on the spot pool;
  - the Redis-password ExternalSecret;
  - `featureStore.enabled` in both environments.

  Verify with CI helm lint and template.
- [ ] 3.5 `infra/charts/payment-gateway`: Fraud `FRAUD_FEAST_URL`, `FRAUD_FEATURE_SERVICE=fraud_model_v1` and the two TTLs, in both environments. Remove the old feature variables. Verify with CI helm template.
- [ ] 3.6 `infra/charts/kafka`: ACLs that let `data-pipelines` create, write and delete the `history-*` input, clock, journal and reject topics, and that let `fraud-streaming` read the history input topics and `feast-writer` read the journals. Verify with CI helm template.

## 4. Go Fraud

- [ ] 4.1 Rewrite `internal/adapters/feast/client.go` (decision 9). Update the application observation types, `cmd/fraud/main.go` and `transport/grpc/evidence.go` to evidence schema version 2. Rework `client_test.go` to cover each status boundary once (TTL edge, future, missing at the Unix epoch, malformed), removing the v1 cases. Verify locally with `golangci-lint`, and run the tests in CI.

## 5. Governance

- [ ] 5.1 `catalog.connectors`: add the `flink` pipeline connector, and the journal and reject patterns to the Kafka topic filter. Verify with CI helm template and the integrations test.
- [ ] 5.2 `openmetadata.lineage`:
  - add the `feast-push-online`, `feast-push-offline`, `feast-materialize` and `stream-history` pipelines and their edges, plus the streaming SDK fallback edges;
  - record the materialization window or range and refresh count, each replay's range, and each history unit's range, counts and gate.

  Extend the existing lineage and publish tests in CI.

## 6. Docs

- [ ] 6.1 Update the existing pages only:
  - `docs/ml/feature-store.md`: the flow diagram (one engine per feature), entities, views, the TTL table and reasons, `fraud_model_v1` and the `− 1µs` training timestamp, the parity exceptions, push jobs A and B and their replay, the Kappa stream history and why it replaced a batch implementation, materialization with rerun ranges and explicit ranges, and Arrow;
  - `docs/data/online.md`: the push jobs and prod `live-v19`, replacing the "disabled until" sentences;
  - `docs/data/orchestration-governance.md`: `feast-materialize`, `feast-push-replay`, `stream-history`, their range parameters, and the Flink connector;
  - `docs/data/offline.md`: the stream tables and their epochs;
  - `docs/overview/roadmap.md`: steps 2–8.

  Verify that the links resolve.

## 7. Local checks and handoff

- [ ] 7.1 Run local non-test checks only: `ruff` and `ruff format --check`, `golangci-lint`, treefmt, helm lint and template, `buf lint`, `openspec validate feast-feature-store --strict`, and `git diff --check`. Commit locally.
- [ ] 7.2 Push and open the PR only with the user's agreement, since the trusted push publishes `deploy/dev`. Report every CI check honestly.

## 8. Runs and evidence (cluster)

- [ ] 8.1 Dev smoke, after the PR deploys:
  - confirm the apply Job and push jobs A and B on `live-v18`;
  - run a `dp-backfill`, then `feast-materialize`;
  - run `dp-pipeline` without a range and `feast-materialize` again;
  - run `feast-materialize` twice over an explicit range;
  - run a push replay twice;
  - run `stream-history` over one v18 month, then again over one of its days, with equal rows and nothing left behind;
  - do a Fraud read.

  Record the outcomes, the refresh counts and the history replay's rate and counts.
- [ ] 8.2 Prod, after merge:
  - confirm the apply Job;
  - after the v19 `dp-backfill`, run `feast-materialize` (a full refresh);
  - run `stream-history` over v19 month by month, setting parallelism from the first month's measured rate; the served model's training waits for it;
  - start Flink `live-v19` and push jobs A and B;
  - unsuspend `dp-daily` and `feast-materialize-daily`, run the simulator for the evidence window (7 days by default), and sample Fraud evidence;
  - run a sample `get_historical_features(fraud_model_v1)` over a v19 cohort;
  - hand the training thread the history epoch, its range and the Gold snapshots, so it trains the served `fraud_model_v1` through `get_historical_features`.

  Record the Argo graphs, the Spark UI, row and refresh counts, the history replay's rate, counts and gates, Redis keys and memory, the online check, lineage and the Flink connector outcome. Results that miss expectations are recorded as observed.

- [ ] 8.3 Write the prod evidence into the existing docs: figures in the text, PNG screenshots under `docs/ml/images/` and `docs/data/images/`, and mermaid diagrams. No `.txt` or `.json` evidence files, no GitHub Actions screenshots (the Argo runs show CI/CD), and no re-recording of evidence this change did not touch. Remove superseded images. Each rubric row gets its evidence and a link to its code:
  - **Materialize pipeline:**
    - the Argo graph of a successful `feast-materialize` run showing the stage order;
    - `feast-materialize-daily` after `dp-daily`;
    - the online check's figures (sampled, missing, stale, refreshed);
    - a Spark UI screenshot of the materialize job.
  - **Stream push jobs, OFFLINE and ONLINE:**
    - both Deployments running in Argo CD;
    - Redis key counts per view and a sample `get-online-features` response for one entity;
    - `gold.feat_stream_*` row counts per epoch, and push job B's latest `kafka.offsets` commit.
  - **Stream history:**
    - the Argo graph of `stream-history` across months;
    - the Flink UI of one replay month (job graph, throughput, checkpoints);
    - per month: the measured events/s, rows written and the gate counts.
  - **Live Flink `live-v19`:** the job graph and checkpoint history in the Flink UI, and the Flink pipeline in OpenMetadata from the connector.
  - **TTL:** the TTL table with its reasons, and one expired-view case from Fraud evidence.
  - **Lineage (Data Governance):** OpenMetadata screenshots of:
    - the journal → `feast-push-offline` → `gold.feat_stream_*`;
    - Bronze `payment_events` → `stream-history` → `gold.feat_stream_*`;
    - `gold.feat_*_daily` → `feast-materialize`;
    - the streaming job from `payments.events.v1`.
  - **Offline retrieval:** the time and row counts of `get_historical_features(fraud_model_v1)` over a v19 cohort, with Arrow on.
  - **CI/CD for the materialize pipeline and the stream jobs:** the Argo CD sync and the Argo Workflows runs that deployed and ran them.
  - **Flow:** the mermaid diagram on `docs/ml/feature-store.md`, matching decision 8 and the deployed state.

  Verify that every image link resolves, and that each rubric row above points to its section.

## 9. Archive

- [ ] 9.1 After the evidence is recorded, archive the change. Verify with `openspec validate --strict --all`.
