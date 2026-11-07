## 0. Start gate

- [x] 0.1 Rebase `plan/feast-store` onto `main` at `bb8dc67`, with DP3 run by `dp-pipeline`, `dp-daily` and `dp-backfill` under the `dp-writer` mutex. Update the artifacts for one engine per feature (user decision 2026-10-31):
  - Spark DP3 owns the daily windows; materialize with lookback plus one day of overlap, use the `dp-writer` mutex, schedule materialization after `dp-daily`, and set the daily TTL delay to 12 hours;
  - Flink owns the short windows: live through the feature writer, and history through the `stream-history` Kappa replay into `gold.feat_stream_*` (decision 8);
  - the training entity timestamp is plain `started_at`; stream rows map `event_timestamp = online_write_at + 1µs` in the writer; simulation-truth labels; training retrieval runs Feast's Spark offline store in local mode in one KFP step.

  Verify with `openspec validate feast-feature-store --strict`.
- [x] 0.2 Rebase `feat/feast-feature-store` onto `origin/main` at `371d2ca`. Confirm that DP3 no longer computes `gold.feat_attempt_rt` or Spark short windows. The user authorized code implementation and a draft PR on 2026-10-31. DP3 now lives in the archived `payment-data-pipeline` change, whose specs are the main specs. Validate this change with `openspec validate feast-feature-store --strict`.
- [x] 0.3 Send the design's platform requests through the user: prod Redis capacity, the Flink operator and checkpoint path in prod, `data` network paths, the Flink connector in ingestion, `feast/` bucket write, spot capacity for the multi-day history FlinkDeployment in prod, and prod Kafka storage sized for one seeded v19 month plus its journal. Make no cluster changes. Verify that each answer is recorded in design.md.

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
- [x] 2.3 Add `fraud_feast/stream_writer.py` (decision 5): live online and buffered offline writes, the feature writer's history mode (short range delete and buffered commits, no history offsets, lead-in dropped). Replace `tests/test_writer.py` with `test_stream_writer.py` on the existing Redis container and a local warehouse, covering:
  - All modes map `event_timestamp = online_write_at + 1µs` in one shared row mapper, with a unit test of the mapping.
  - an older revision not replacing a newer online value;
  - offline resume from the table property's offsets after a crash before the Kafka commit;
  - history month replacement after partial failure repeated with normal appends, preserving other months and live epochs;
  - an invalid record stopping the job without a commit.

  Verify in CI.
- [x] 2.4 Replace materialization with overlap (decision 6): per-view registry end minus lookback plus one day, first-run TTL, optional since day, end always now, skip_dedup and default local engine. Remove snapshot pinning, state, refresh sets, tombstones and fallback. verify_online samples newest keys only. CI covers rewritten equal anchors, the documented deleted-entity TTL gap, explicit since through now, repeated writes and missing online samples.

- [x] 2.5 Add `fraud_streaming/history.py` (decision 8): reads a Bronze range plus one day of lead-in in source order, encodes each event with the `wire.py` descriptors, writes the history input and clock topics, reports Flink's duplicate and too-late counts without gating on them; history gates on zero decode rejects, a completed seed and a FINISHED bounded job. Verify in CI with a round-trip test (Bronze row → encoded bytes → `wire.decode`) and a small logical-clock replay whose snapshots match `features/reference_v2.py`.

- [x] 2.6 Add request_context and request_features to payment_risk_v1; one pure Python implementation for historical Arrow and online dict transforms. Add training.entity_rows for valid Silver attempts and available simulation-truth labels joined to request context. CI covers truth false without chargebacks, unavailable labels, invalid lifecycle, cohort bounds, nullable request fields, and historical/server parity. The server adds projected output aliases around transform_dict to fix Feast 0.67.0's full-name filtering bug, which the first CI parity run exposed.

## 3. Images and charts

- [x] 3.1 `data-platform-spark.Dockerfile`: add the Feast virtualenv with the `spark` extra, and add `src/data-pipeline/feast` to its rebuild paths. `feast.Dockerfile`: Python 3.12 slim with the stream writer, verification and server entrypoints. Verify that the CI image builds pass.
- [x] 3.2 `infra/charts/data-pipelines`:
  - the `feature-writer` Deployment, and the feature server;
  - the Sync-hook apply Job, ahead of the writer and server;
  - the object-storage and Iceberg ExternalSecrets;
  - Redis client labels and sizing values;
  - the JobManager REST policy for metadata pods;
  - remove the v1 writer, `feast.writerGroup` and `definitionHash`;
  - prod values: `features.enabled: false`, epoch `live-v19`, Redis capacity configured for about 6 GiB on `cloud-block` when enabled.

  Verify with CI helm lint and template for every values file.
- [x] 3.3 `infra/charts/live-traffic`: enable the simulator in prod on the v19 manifest and digest, at 1 payment every 30 seconds. Verify with CI helm template. The manifest was read from S3 with user authorization; its digest is pinned and the seven-day prod run is enabled.
- [x] 3.4 `infra/charts/data-pipelines`:
  - `feast-materialize` (local Spark Argo container with Feast virtualenv and large spill emptyDir, then verify and publish; optional `since`; end always now; mutex `dp-writer`) and the suspended `feast-materialize-daily` CronWorkflow at 06:47 UTC;
  - on-demand `stream-history` (`start`, `end`; mutex `stream-history`): per month, recreate the epoch topics, seed, a logical-clock FlinkDeployment, feature writer in history mode with short retried commits, and an exit handler that deletes the deployment and topics and publishes; RBAC for FlinkDeployments in `data`; TaskManagers on the spot pool;
  - the Redis-password ExternalSecret;
  - `features.enabled`: on in dev, off in prod.

  Verify with CI helm lint and template.
- [x] 3.5 `infra/charts/payment-gateway`: Fraud `FRAUD_FEAST_URL`, `FRAUD_FEATURE_SERVICE=payment_risk_v1` and the two TTLs, in both environments. Remove the old feature variables. Verify with CI helm template.
- [x] 3.6 `infra/charts/kafka`: follow the existing network access pattern for `data-pipelines`, `fraud-streaming` and the feature-writer Deployment, including prod `live-v19` journal and reject topics. Verify with CI helm template. Current main has unauthenticated Kafka listeners and no authorizer. On 2026-10-31 the user chose the existing access pattern; authenticated topic ACLs are optional future work, not a prerequisite (decision 11).

## 4. Go Fraud

- [x] 4.1 Rewrite `internal/adapters/feast/client.go` (decision 9). Update the application observation types, `cmd/fraud/main.go` and `transport/grpc/evidence.go` to evidence schema version 2. Rework `client_test.go` to cover each status boundary once (TTL edge, future, missing at the Unix epoch, malformed), removing the v1 cases. Send raw request fields with natural keys, include request_features and nullable booleans in evidence, and preserve rule logic and stored-view statuses. Verify locally with `golangci-lint`, and run the tests in CI.

## 5. Governance

- [x] 5.1 `catalog.connectors`: add the `flink` pipeline connector, and the journal and reject patterns to the Kafka topic filter. Verify with CI helm template and the integrations test.
- [x] 5.2 `openmetadata.lineage`:
  - add the `feature-writer`, `feast-materialize` and `stream-history` pipelines and their edges, plus the streaming SDK fallback edges;
  - record materialization windows, since and sample counts, each history unit's range, counts and gate.

  Extend the existing lineage and publish tests in CI.

- [x] 5.3 Use payment_risk_v1, stream_writer, feature-writer (Deployment, consumer group and catalog pipeline), verify_online with the verify workflow stage. Remove external project references from active artifacts; keep archived changes and openspec/config.yaml unchanged. Update existing CI coverage to use the names without aliases.

## 6. Docs

- [x] 6.1 Update the existing pages only:
  - `docs/ml/feature-store.md`: the flow diagram (one engine per feature), entities, views, the TTL table and reasons, `payment_risk_v1`, the `+ 1µs` visibility of stream rows, the parity exceptions, the feature writer and stream recovery, the Kappa stream history and why it replaced a batch implementation, materialization with overlap and since, and Arrow;
  - `docs/data/online.md`: the stream writer and prod `live-v19`, replacing the "disabled until" sentences;
  - `docs/data/orchestration-governance.md`: `feast-materialize`, `stream-history`, their range parameters, and the Flink connector;
  - `docs/data/offline.md`: the stream tables and their epochs;
  - `docs/ml/feature-store.md#build-order` (moved from the roadmap in the new layout) and the `Home.md` roadmap row: steps 2–8.

  Verify that the links resolve.

Implementation checkboxes record code and CI coverage written; test outcomes are reported by the draft PR. No local test suites were run.

## 7. Local checks and handoff

- [x] 7.1 Re-run local non-test checks after the naming update: `ruff` and `ruff format --check`, `golangci-lint`, treefmt, helm lint and template, `buf lint`, `openspec validate feast-feature-store --strict`, and `git diff --check`. Ruff, formatting/treefmt, Fraud golangci-lint, all-chart Helm lint/template, fraud-scoped Buf lint, protobuf generation, dependency locks, strict OpenSpec validation (all 25 items), contract lint/generated references and git diff checks pass. The active-artifact reference scan is clean. No local tests. Buf is not in devenv, so lint used nixpkgs#buf. Earlier repository-wide Buf lint reported pre-existing Payments, Processor and Vault RPC naming/reuse violations; those were not changed.
- [x] 7.2 Push `feat/feast-feature-store` with an explicit same-name refspec and open a draft PR against `main`, as authorized by the user. Keep it draft: marking a trusted PR ready publishes `deploy/dev`. Draft PR: https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/111. CI results are reported by the PR; no local test suites were run. Report every CI check honestly.

## 8. Runs and evidence (cluster)

- [x] 8.1 Prod, after merge: after the v19 `dp-backfill`, run `feast-materialize`; start Flink `live-v19`, the feature writer and the simulator for the evidence window; read back a sample with `get_online_features`.

  **Observed 2026-11-04:**
  - The v19 `dp-backfill` for 2025-10-29 to 2026-10-29 succeeded; October ran as two halves, `oct-a` and `oct-b`, after the whole month lost executors.
  - `feast-materialize` ran with `since` unset, so each daily view loaded its TTL window, 2026-10-03 to 2026-11-04: 24 m 54 s, an online check of 400 sampled rows that passed, and 5,697,609 Redis keys at 2.72 GB. A rerun took 5 m 07 s. `since=2025-10-29` was stopped after 57 minutes still on its first view; serving ignores rows older than the TTL, so the default window is the one to use.
  - Flink `live-v19` and the feature writer run. The prod simulator (04d0219, one payment per second, b3f8728) admitted 7,556 payments in 2 h 6 m, until the autoscaler's node scale-down replaced the single `edge` pod and ended the Job at 06:40:44.
  - A first prod `stream-history` over 2026-07-29 to 2026-10-29 failed on the 400 GB `SSD_TOTAL_GB` quota (fixed by `features.scratchClass: cloud-standard`, 566949b) and on `Response checksums mismatch` reading Bronze from GCS (fixed by a35961e). The full replay then completed as `stream-history-jvm-v19-8trbp`: 2026-11-04 to 2026-11-07 (about 2 d 8 h), Succeeded, all 23 units FINISHED and published. `gold.feat_stream_customer` held 236,441,166 rows and `gold.feat_stream_payment_method` 236,453,680 under the `live-v19` epoch.

- [x] 8.2 Write the rubric evidence into the existing docs: figures in the text, PNG screenshots under `docs/ml/images/`, and mermaid diagrams. No `.txt` or `.json` evidence files, no GitHub Actions screenshots (the Argo runs show CI/CD), and no re-recording of evidence this change did not touch. Each rubric row gets its evidence and a link to its code:
  - **Materialize pipeline:** the Argo graph of a successful `feast-materialize` run showing the stage order, and the online check's figures.
  - **Stream writer, OFFLINE and ONLINE:** both Deployments running in Argo CD, `gold.feat_stream_*` row counts per epoch and the persisted `kafka.offsets.<epoch>` table property.
  - **Live Flink `live-v19`:** the job graph and checkpoint history in the Flink UI.
  - **TTL:** the TTL table with its reasons.
  - **CI/CD for the stream jobs:** the Argo CD sync.
  - **Flow:** the mermaid diagram on `docs/ml/feature-store.md`.

  Recorded 2026-11-04 in [Online and offline paths](../../../docs/ml/feature-store.md#online-and-offline-paths), with the Spark UI of a materialize run and the lineage of the journal and the daily table as extras. The completed prod `stream-history` replay is recorded in [Stream history replay](../../../docs/ml/feature-store.md#stream-history-replay): the Argo summary, the 23/23 unit and publish counts, the gold row counts and the `writers` semaphore note.

  Dropped because the rubric does not ask for them: the dev smoke runs; what depends on the replay (`get_historical_features` over a v19 cohort, the training handoff); enabling `dp-daily` and `feast-materialize-daily`; Redis key counts per view; the expired-view Fraud case; lineage from Bronze through `stream-history` and from `payments.events.v1`; and the Flink pipeline from the connector.

## 9. Archive

- [x] 9.1 After the evidence is recorded, archive the change. Verify with `openspec validate --strict --all`.

## 10. Remove synchronous submission registration

- [x] 10.1 Remove the guard adapter, application wiring, guard-specific tests, evidence messages and fields; reserve removed protobuf field numbers and regenerate bindings. Keep durable assessment ownership and shadow Feast retrieval.
- [x] 10.2 Remove the dedicated submission Redis, credentials and guard-only network configuration in both environments. Preserve Payments and Feast Redis. Record the Doppler cleanup after merge.
- [x] 10.3 Add removal deltas for every submission-registration requirement and modify Fraud and Payments dependency requirements. Synchronize the affected main requirements so active specifications describe the removed behavior consistently.
- [x] 10.4 Run the requested non-test checks, push with the explicit same-name refspec, keep the PR draft and report CI. Fraud and Payments build and pass golangci-lint; all-chart/all-values Helm lint/template, formatting, strict OpenSpec validation and the active-reference scan pass. Fraud-scoped Buf lint passes. Repository-wide Buf lint fails on existing Payments, Processor and Vault RPC naming/reuse violations. No local tests or cluster actions. CI results are reported on the PR.

## 11. Review fixes and simplification

- [x] 11.1 Consolidate live consumption into one feature-writer process, Deployment, consumer group and catalog pipeline. Push online before buffered Iceberg commits; resume from Iceberg offsets; retain offline-only --history and stream recovery materialization. Cover buffering, flush boundaries and partial commits in CI.
- [x] 11.2 Use server scoring time for Feast requests, TTL/future checks and EvaluatedAt; cover a store-and-forward attempt. Make country mismatch null when either input country is null and extend historical/server parity coverage.
- [x] 11.3 Use public store.push only, include the always-set epoch in stream views outside the model service, select explicit stream-source columns and read definition hashes from the contract file at import.
- [x] 11.4 Remove the separate seed project and lockfile, consolidate dependency groups in streaming, remove duplicate seed auditing, and share bounded batch consumption with history. Retain DuckDB for the out-of-core sort and offset barriers for cross-topic ordering. Sweep dropped options and update affected artifacts.
- [x] 11.5 Run non-test checks, push the explicit branch refspec, retain the draft and report CI. Ruff, formatting/treefmt, Fraud build/golangci-lint, all-chart/all-values Helm lint/template, contract lint/generated references, streaming lock consistency, strict OpenSpec validation (24 items) and diff checks pass. CI runs the tests and image builds; no local tests or cluster actions.

## 12. Review fixes

- [x] 12.1 Merge feature infrastructure into data-pipelines under features.enabled (dev on, prod off), deduplicate secrets/helpers, restore unchanged values comments. Dev can be recreated; no legacy migration or resource-adoption work.
- [x] 12.2 Replace history using a short delete per table and per-batch commits; remove prepare and history watermarks/resume, add Argo retry from the range delete and drain bounded reads to committed partition EOF. A month is partially visible; training waits for the completed replay and its gates.
- [x] 12.3 Persist live offsets as transactional table properties, retry commit conflicts against refreshed offsets, retain nightly snapshot maintenance and cover expiry recovery.
- [x] 12.4 Push null coverage with an always-set epoch excluded from model inputs, null-guard account ages, restore scoped Flink Deployment access and move imports.
- [x] 12.5 Run affected unit tests, lint, dev/prod Helm rendering and strict OpenSpec validation; push the explicit branch refspec, keep the draft and report CI. All 17 affected tests, Ruff, formatting, dev/prod Helm lint/template and strict OpenSpec validation (24 items) pass. Context7 is unavailable; APIs verified against installed Feast 0.67.0, PyIceberg 0.10.0 and confluent-kafka 2.12.0 source. No cluster actions.

## 13. Concurrent history writes

- [x] 13.1 Share one refresh/retry/backoff helper between live and history writes. Delete a history unit’s epoch/range in a short commit per table and append each batch in its own commit, without history offsets/properties or a final commit.
- [x] 13.2 Cover concurrent live writes between batches, actual delete/append commit conflicts and rerunning a partially failed unit without duplicates. Document partial visibility, the counts gate and no training during replay.
- [x] 13.3 Run affected tests, lint and strict OpenSpec validation; push the explicit branch refspec, keep the draft and report CI. All 14 affected writer/history tests, Ruff, formatting and strict OpenSpec validation (24 items) pass; CI status is reported with the pushed head.

## 14. Shared history buffering

- [x] 14.1 Delete HistoryWriter and route every mode through StreamWriter, with IcebergWriter as the only offline sink. History uses the live row/time buffer bounds and final flush, an initial retried range delete, and plain appends without offset state; Kafka consumer-group offsets are never committed.
- [x] 14.2 Cover history buffer commits and final remainder, retaining concurrent-live-write and failed-unit-rerun coverage. Run affected tests, lint and validation; push the explicit branch refspec and retain the draft. All 16 affected tests, Ruff, formatting and strict OpenSpec validation (24 items) pass; CI is reported with the pushed head.

## 15. Sequential history and two writer modes

- [x] 15.1 Seed each unit before bounded logical Flink, await FINISHED with final checkpoint completion, retain REST until cleanup, and read history from zero to EOF. Remove polling, checkpoint heuristic, seed lag throttle and topic retention caps.
- [x] 15.2 Remove online range replay and its workflow, simplify decode, and use only Iceberg offsets for live recovery without Kafka commits. Add stream Redis recovery through materialization over STREAM_TTL.
- [x] 15.3 Run affected tests, lint, dev/prod Helm templates and strict validation; push the explicit branch refspec and retain the draft. All 26 affected tests pass (19 Feast under UTC, 4 streaming and 3 governance), as do Ruff, formatting, dev/prod Helm lint/template and strict OpenSpec validation (24 items). Context7 is unavailable; bounded-source and application-finish options were checked against the published Kafka connector 5.0.0 and Flink 2.2.1 sources; Flink documents final checkpoint completion before task exit. No repository dashboard or alert reads feature-writer group lag. CI is reported with the pushed head.
