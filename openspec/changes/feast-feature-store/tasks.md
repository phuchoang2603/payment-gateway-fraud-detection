## 0. Start gate

- [x] 0.1 Rebase `plan/feast-store` onto `main` at `bb8dc67`, with DP3 run by `dp-pipeline`, `dp-daily` and `dp-backfill` under the `dp-writer` mutex. Update the artifacts for one engine per feature (user decision 2026-10-31):
  - Spark DP3 owns the daily windows; materialize with lookback plus one day of overlap, use the `dp-writer` mutex, schedule materialization after `dp-daily`, and set the daily TTL delay to 12 hours;
  - Flink owns the short windows: live through online and offline writers, and history through the `stream-history` Kappa replay into `gold.feat_stream_*` (decision 8);
  - the training entity timestamp is plain `started_at`; stream rows map `event_timestamp = online_write_at + 1µs` in both writers; simulation-truth labels; training retrieval runs Feast's Spark offline store in local mode in one KFP step.

  Verify with `openspec validate feast-feature-store --strict`.
- [x] 0.2 Rebase `feat/feast-feature-store` onto `origin/main` at `371d2ca`. Confirm that DP3 no longer computes `gold.feat_attempt_rt` or Spark short windows. The user authorized code implementation and a draft PR on 2026-10-31. `dp3-features-and-marts` remains unarchived, so its v1 retirement requirements stay pending. Validate this change with `openspec validate feast-feature-store --strict`.
- [ ] 0.3 Send the design's platform requests through the user: prod Redis capacity, the Flink operator and checkpoint path in prod, `data` network paths, the Flink connector in ingestion, `feast/` bucket write, and spot capacity for the multi-day history FlinkDeployment in prod. Make no cluster changes. Verify that each answer is recorded in design.md.

## 1. Contracts and v1 cleanup

- [x] 1.1 Add `contracts/fraud/v1/evidence.proto` (decision 9). In `fraud.proto`, reserve `DecideResponse` field 3 and use field 4. Regenerate the Go bindings. Verify with `buf lint`, generation, and `go build ./...` for fraud and payments. Local generation, both builds and fraud-scoped Buf lint pass; repository-wide Buf lint reports existing RPC naming violations in Payments, Processor and Vault.
- [x] 1.2 Delete:
  - `contracts/features/v1/`, `fraud-snapshot-v1.*`, and the Go and Python v1 bindings;
  - `src/data-pipeline/features/reference.py` and its v1-only tests;
  - the v1 paths in `schema-register.Dockerfile` and `service-images.json`.

  Verify that no executable v1 code remains. The remaining text references are planning cleanup lists and the main spec's conditional retirement clause, retained until archival. The v2 conformance tests remain in CI.
- [x] 1.3 Add v2 conformance cases to `fraud-snapshot-v2.examples.json`: attempts just inside and just past the 24-hour lateness horizon. The definition hash covers only `fraud-snapshot-v2.yaml`'s bytes, so the identity and the live epoch stay valid; confirm this before editing. Verify that the Flink conformance test passes the new cases in CI.

## 2. Feast project

- [x] 2.1 Rework `src/data-pipeline/feast`:
  - `fraud_feast` installable with the `spark` extra, and `spark.master` taken from the environment (default: `local[*]` for materialization and training retrieval).
  - shared JAR pins and checksums in `fraud_feast/spark_jars.json`, with `python -m fraud_feast.spark_jars` usable by both image builds; CI covers download integrity and matching Spark versions.
  - Python 3.12, `feast[redis]==0.67.0`, `pyiceberg`, `confluent-kafka`, and a `spark` extra;
  - `feature_store.yaml` per decision 1: the Polaris catalog and Arrow in `spark_conf`, the default local materialization engine, `pull_latest_features`, Redis `key_ttl_seconds`, and the configured stream epochs;
  - delete the v1 modules and tests.

  Verify that `uv lock`, `ruff` and `ruff format --check` pass locally.
- [x] 2.2 Add `fraud_feast/repo.py` (decisions 2–4: entities, the four stored views and pure Python request_features with explicit schemas, the stream views' epoch-filtered queries, TTLs and tags, and `payment_risk_v1`) and `fraud_feast/apply.py` (creates the two stream tables, then applies). Verify in CI with:
  - a test that `repo.py`'s fields equal each definition file's feature list per entity;
  - one local-warehouse test that applies the repository and runs `get_historical_features(payment_risk_v1).to_arrow()` with `full_feature_names=True` and `label_attempt` rows. It covers the daily row on day D, a row whose `created` is later than `t`, TTL expiry to null, a snapshot written at exactly `started_at` invisible at `started_at` while a daily row anchored at that instant is returned, values read across a history and a live epoch with an unconfigured epoch ignored, and null method values for a null payment method.
- [x] 2.3 Add `fraud_feast/stream_writer.py` (decision 5): `--store online|offline`, the continuous modes, online-only `--from`/`--until` replay, and the offline writer's history mode (delete month before normal append, lead-in dropped). Replace `tests/test_writer.py` with `test_stream_writer.py` on the existing Redis container and a local warehouse, covering:
  - Both modes map `event_timestamp = online_write_at + 1µs` in one shared row mapper, with a unit test of the mapping.
  - an older revision not replacing a newer online value;
  - offline resume from the commit's offsets after a crash before the Kafka commit;
  - history month preparation repeated with normal appends, preserving other months and live epochs;
  - an invalid record stopping the job without a commit.

  Verify in CI.
- [x] 2.4 Replace materialization with overlap (decision 6): per-view registry end minus lookback plus one day, first-run TTL, optional since day, end always now, skip_dedup and default local engine. Remove snapshot pinning, state, refresh sets, tombstones and fallback. verify_online samples newest keys only. CI covers rewritten equal anchors, the documented deleted-entity TTL gap, explicit since through now, repeated writes and missing online samples.

- [x] 2.5 Add `fraud_streaming/history.py` (decision 8): reads a Bronze range plus one day of lead-in in source order, encodes each event with the `wire.py` descriptors, writes the history input and clock topics, pauses while the replay's lag exceeds a bound, and reports duplicate and too-late counts without gating on them; history gates on zero decode rejects and consumed seeded input offsets. Verify in CI with a round-trip test (Bronze row → encoded bytes → `wire.decode`) and a small logical-clock replay whose snapshots match `features/reference_v2.py`.

- [x] 2.6 Add request_context and request_features to payment_risk_v1; one pure Python implementation for historical Arrow and online dict transforms. Add training.entity_rows for valid Silver attempts and available simulation-truth labels joined to request context. CI covers truth false without chargebacks, unavailable labels, invalid lifecycle, cohort bounds, nullable request fields, and historical/server parity. The server adds projected output aliases around transform_dict to fix Feast 0.67.0's full-name filtering bug, which the first CI parity run exposed.

## 3. Images and charts

- [x] 3.1 `data-platform-spark.Dockerfile`: add the Feast virtualenv with the `spark` extra, and add `src/data-pipeline/feast` to its rebuild paths. `feast.Dockerfile`: Python 3.12 slim with the stream writer, verification and server entrypoints. Verify that the CI image builds pass.
- [x] 3.2 `infra/charts/fraud-features`:
  - the `feature-writer-online` and `feature-writer-offline` Deployments, and the feature server;
  - the PostSync apply Job;
  - the object-storage and Iceberg ExternalSecrets;
  - Redis client labels and sizing values;
  - the JobManager REST policy for metadata pods;
  - remove the v1 writer, `feast.writerGroup` and `definitionHash`;
  - prod values: `feast.enabled`, `streaming.enabled`, the epoch and topic `live-v19`, Redis about 6 GiB on gp3.

  Verify with CI helm lint and template for every values file.
- [x] 3.3 `infra/charts/live-traffic`: enable the simulator in prod on the v19 manifest and digest, at 1 payment every 30 seconds. Verify with CI helm template. The manifest was read from S3 with user authorization; its digest is pinned and the seven-day prod run is enabled.
- [x] 3.4 `infra/charts/data-pipelines`:
  - `feast-materialize` (local Spark Argo container with Feast virtualenv and large spill emptyDir, then verify and publish; optional `since`; end always now; mutex `dp-writer`) and the suspended `feast-materialize-daily` CronWorkflow at 06:47 UTC;
  - on-demand `feature-writer-replay` (online writer only, `from`, `until`; online mutex; publish);
  - on-demand `stream-history` (`start`, `end`; mutex `stream-history`): per month, delete history rows, recreate the epoch topics, seed, a logical-clock FlinkDeployment, offline writer in history mode, the gate, and an exit handler that deletes the deployment and topics and publishes; RBAC for FlinkDeployments in `data`; TaskManagers on the spot pool;
  - the Redis-password ExternalSecret;
  - `featureStore.enabled` in both environments.

  Verify with CI helm lint and template.
- [x] 3.5 `infra/charts/payment-gateway`: Fraud `FRAUD_FEAST_URL`, `FRAUD_FEATURE_SERVICE=payment_risk_v1` and the two TTLs, in both environments. Remove the old feature variables. Verify with CI helm template.
- [x] 3.6 `infra/charts/kafka`: follow the existing network access pattern for `data-pipelines`, `fraud-streaming` and the two Feast push Deployments, including prod `live-v19` journal and reject topics. Verify with CI helm template. Current main has unauthenticated Kafka listeners and no authorizer. On 2026-10-31 the user chose the existing access pattern; authenticated topic ACLs are optional future work, not a prerequisite (decision 11).

## 4. Go Fraud

- [x] 4.1 Rewrite `internal/adapters/feast/client.go` (decision 9). Update the application observation types, `cmd/fraud/main.go` and `transport/grpc/evidence.go` to evidence schema version 2. Rework `client_test.go` to cover each status boundary once (TTL edge, future, missing at the Unix epoch, malformed), removing the v1 cases. Send raw request fields with natural keys, include request_features and nullable booleans in evidence, and preserve rule logic and stored-view statuses. Verify locally with `golangci-lint`, and run the tests in CI.

## 5. Governance

- [x] 5.1 `catalog.connectors`: add the `flink` pipeline connector, and the journal and reject patterns to the Kafka topic filter. Verify with CI helm template and the integrations test.
- [x] 5.2 `openmetadata.lineage`:
  - add the `feature-writer-online`, `feature-writer-offline`, `feast-materialize` and `stream-history` pipelines and their edges, plus the streaming SDK fallback edges;
  - record materialization windows, since and sample counts, each replay's range, and each history unit's range, counts and gate.

  Extend the existing lineage and publish tests in CI.

- [x] 5.3 Use payment_risk_v1, stream_writer --store, feature-writer-online/offline (Deployments, consumer groups and catalog pipelines), feature-writer-replay and verify_online with the verify workflow stage. Remove external project references from active artifacts; keep archived changes and openspec/config.yaml unchanged. Update existing CI coverage to use the names without aliases.

## 6. Docs

- [ ] 6.1 Update the existing pages only:
  - `docs/ml/feature-store.md`: the flow diagram (one engine per feature), entities, views, the TTL table and reasons, `payment_risk_v1`, the `+ 1µs` visibility of stream rows, the parity exceptions, online and offline writers and their replay, the Kappa stream history and why it replaced a batch implementation, materialization with overlap and since, and Arrow;
  - `docs/data/online.md`: the stream writers and prod `live-v19`, replacing the "disabled until" sentences;
  - `docs/data/orchestration-governance.md`: `feast-materialize`, `feature-writer-replay`, `stream-history`, their range parameters, and the Flink connector;
  - `docs/data/offline.md`: the stream tables and their epochs;
  - `docs/overview/roadmap.md`: steps 2–8.

  Verify that the links resolve.

Implementation checkboxes record code and CI coverage written; test outcomes are reported by the draft PR. No local test suites were run.

## 7. Local checks and handoff

- [x] 7.1 Re-run local non-test checks after the naming update: `ruff` and `ruff format --check`, `golangci-lint`, treefmt, helm lint and template, `buf lint`, `openspec validate feast-feature-store --strict`, and `git diff --check`. Ruff, formatting/treefmt, Fraud golangci-lint, all-chart Helm lint/template, fraud-scoped Buf lint, protobuf generation, dependency locks, strict OpenSpec validation (all 25 items), contract lint/generated references and git diff checks pass. The active-artifact reference scan is clean. No local tests. Buf is not in devenv, so lint used nixpkgs#buf. Earlier repository-wide Buf lint reported pre-existing Payments, Processor and Vault RPC naming/reuse violations; those were not changed.
- [x] 7.2 Push `feat/feast-feature-store` with an explicit same-name refspec and open a draft PR against `main`, as authorized by the user. Keep it draft: marking a trusted PR ready publishes `deploy/dev`. Draft PR: https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/111. CI results are reported by the PR; no local test suites were run. Report every CI check honestly.

## 8. Runs and evidence (cluster)

- [ ] 8.1 Dev smoke, after the PR deploys:
  - confirm the apply Job and online and offline writers on `live-v18`;
  - run a `dp-backfill`, then `feast-materialize`;
  - run `dp-pipeline` without a range and `feast-materialize` again;
  - run `feast-materialize` twice with an explicit since day;
  - run a push replay twice;
  - run `stream-history` over one v18 month, then again over one of its days, with equal rows and nothing left behind;
  - do a Fraud read.

  Record the outcomes, the sample counts and the history replay's rate and counts.
- [ ] 8.2 Prod, after merge:
  - confirm the apply Job;
  - after the v19 `dp-backfill`, run `feast-materialize` with `since=<backfill start>`;
  - run `stream-history` with `start=2026-07-29`, `end=2026-10-29` (exclusive), month by month, setting parallelism from the first month's measured rate; the workflow adds its own one-day lead-in and the served model's training waits for this range;
  - start Flink `live-v19` and online and offline writers;
  - unsuspend `dp-daily` and `feast-materialize-daily`, run the simulator for the evidence window (7 days by default), and sample Fraud evidence;
  - run a sample `get_historical_features(payment_risk_v1)` over a v19 cohort;
  - hand the training thread the history epoch, its range and the Gold snapshots, so it trains the served `payment_risk_v1` through `get_historical_features`.

  Record the Argo graphs, the Spark UI, row and sample counts, the history replay's rate, counts and gates, Redis keys and memory, the online check, lineage and the Flink connector outcome. Results that miss expectations are recorded as observed.

- [ ] 8.3 Write the prod evidence into the existing docs: figures in the text, PNG screenshots under `docs/ml/images/` and `docs/data/images/`, and mermaid diagrams. No `.txt` or `.json` evidence files, no GitHub Actions screenshots (the Argo runs show CI/CD), and no re-recording of evidence this change did not touch. Remove superseded images. Each rubric row gets its evidence and a link to its code:
  - **Materialize pipeline:**
    - the Argo graph of a successful `feast-materialize` run showing the stage order;
    - `feast-materialize-daily` after `dp-daily`;
    - the online check's figures (sampled and checked);
    - a Spark UI screenshot of the materialize job.
  - **Stream writers, OFFLINE and ONLINE:**
    - both Deployments running in Argo CD;
    - Redis key counts per view and a sample `get-online-features` response for one entity;
    - `gold.feat_stream_*` row counts per epoch, and offline writer's latest `kafka.offsets` commit.
  - **Stream history:**
    - the Argo graph of `stream-history` across months;
    - the Flink UI of one replay month (job graph, throughput, checkpoints);
    - per month: the measured events/s, rows written and the gate counts.
  - **Live Flink `live-v19`:** the job graph and checkpoint history in the Flink UI, and the Flink pipeline in OpenMetadata from the connector.
  - **TTL:** the TTL table with its reasons, and one expired-view case from Fraud evidence.
  - **Lineage (Data Governance):** OpenMetadata screenshots of:
    - the journal → `feature-writer-offline` → `gold.feat_stream_*`;
    - Bronze `payment_events` → `stream-history` → `gold.feat_stream_*`;
    - `gold.feat_*_daily` → `feast-materialize`;
    - the streaming job from `payments.events.v1`.
  - **Offline retrieval:** the time and row counts of `get_historical_features(payment_risk_v1)` over a v19 cohort, with Arrow on.
  - **CI/CD for the materialize pipeline and the stream jobs:** the Argo CD sync and the Argo Workflows runs that deployed and ran them.
  - **Flow:** the mermaid diagram on `docs/ml/feature-store.md`, matching decision 8 and the deployed state.

  Verify that every image link resolves, and that each rubric row above points to its section.

## 9. Archive

- [ ] 9.1 After the evidence is recorded, archive the change. Verify with `openspec validate --strict --all`.
