## Context

See proposal.md for the motivation. What shapes the design:

- **Feast owns the data path** (`feast-feature-store`, PR #111; plan approved by the user on 2026-11-01).
  - **`payment_risk_v1`** covers the daily views (`gold.feat_*_daily`, Spark DP3), the stream views (`gold.feat_stream_*`, Flink, replayed by `stream-history`), and the on-demand view `request_features` (python mode).
  - **`fraud_feast.training.entity_rows(spark, start, end, cutoff)`** returns simulation-truth `label_attempt` rows with `label_available_at <= cutoff`, joined to each attempt's request fields from `silver.payment_events` (`attempt_started`), at `event_timestamp = started_at`.
  - **Visibility rule:** stream snapshots carry `event_timestamp = write time + 1µs`, so `<= started_at` never sees the attempt's own snapshot. Serving and training both use plain `started_at`.
  - **Offline store:** Spark, over the Polaris `lake` catalog. `spark.master` is overridable; training uses `local[*]`, and so does Feast's materialization (no SparkApplications anywhere).
  - **Install:** `fraud_feast` installs with a `spark` extra.
  - **Training replay:** `[2026-07-29, 2026-10-29)`, the last three v19 months, a flat season.
- **Labels.** Simulation truth is treated as real for this learning project, and is available at `started_at`. So there are no maturity gaps and no immature tail.
- **Platform (talos-proxmox, through `8321dec`).**
  - The Model Registry 0.3.17 is at `http://model-registry-service.ml.svc.cluster.local:8080`, and has native version state (LIVE or ARCHIVED) but no aliases.
  - KServe 0.21 in `Standard` (raw Deployment) mode runs a chart-owned `ServingRuntime` container, here Triton; the registry stores any artifact URI.
  - KFP (single-user) and Katib 0.19 (`Push`, `restricted`) are validated.
  - Runtime `xgboost-gpu-distributed`: DRA claim, non-root, image, command and args overridable. On prod it selects the platform's `gpu` node pool (at most two nodes), which has no cloud GPU capacity yet. The stock XGBoost image's NCCL is too old; a single GPU needs no NCCL.
  - Notebooks 1.11 cannot take DRA claims.
  - The Dashboard lists `ml` through a platform-owned `Profile/ml`.
  - The GPUs are shared with model-server benchmarks.
- **Capacity:** the prod `spot` pool has up to 12 e2-standard-4 nodes. The boot disk leaves about 24 Gi for pod scratch space.
- **Course rubric (MLE):** notebook (offline store plus labels, split, train, evaluate, save); KFP with at least load, split, train and evaluate; model and incremental data versions; KServe; five key classes; property-based tests.

## Goals / Non-Goals

**Goals:**

- One plain path: Feast, then a training set, then XGBoost on one GPU, then the registry, then KServe. The training code holds no feature, label or entity logic.
- Every model and deployment comes from a Kubeflow pipeline run on the platform's components.
- Code written now against the Feast interface, and run as soon as the replay exists.

**Non-Goals:**

- Feature, label or request-field logic in training code; any source other than Feast; writes to Gold.
- Distributed or CPU training.
- GitHub-driven promotion, shadow (non-enforcing) scoring, a second telemetry stack, and regenerating historical data with drift.

## Decisions

### 1. Windows

| Window | Range |
|---|---|
| train | 2026-07-29..2026-09-30 (64 days) |
| validation | 2026-10-01..10-14 (14 days) |
| test | 2026-10-15..10-28 (14 days) |

The cutoff is 2026-10-29. The windows are pipeline parameters. They are consecutive and time-ordered, and `WindowSplitter` rejects overlap or misordering.

### 2. Dataset step: Feast in local Spark, parallel chunks

`risk_model.dataset` builds the training set in three KFP steps on the `ml` image, each with a local `SparkSession` (`local[*]`) that uses the Feast repository's Spark settings, 16 shuffle partitions and a 24 Gi `emptyDir` for spill. They run on spot r-family nodes.

1. **`plan_examples`.** Checks that the stream history covers the window and records the current snapshot IDs of the tables Feast reads (the fence): `label_attempt`, `silver.attempts`, `silver.payment_events`, and the daily and stream tables. It creates an empty Iceberg staging table `lake.ml.payment_risk_v1_stage`, partitioned by day, and returns the window as 16-day chunks.
2. **`stage_examples`, one per chunk, four at a time (`dsl.ParallelFor`).** For its chunk it takes `rows = fraud_feast.training.entity_rows(spark, start, end, cutoff)`, adds `bucket = pmod(xxhash64(key), 1000)`, and keeps every positive and the negatives with `bucket < 100`. Then, one day at a time, `store.get_historical_features(rows, features=store.get_feature_service("payment_risk_v1"), full_feature_names=True).to_arrow()`. Feast's own `staging_location` (a local directory) makes `to_arrow()` read Spark's Parquet output instead of going through pandas, and applies the on-demand views. The step deletes its chunk's rows from the staging table and appends the new ones, so a retry replaces the chunk.
3. **`assemble_examples`.** Fails if a fenced table moved since the plan. Otherwise one `MERGE` of the staging table into `lake.ml.payment_risk_v1_examples`: Iceberg v2, merge-on-read, partitioned by day. It matches on attempt key plus `row_hash`, with `INSERT` and `NOT MATCHED BY SOURCE → DELETE`, so each version stores only the changes. Snapshot properties record the fenced IDs, the window, the cutoff, the sample rate and the feature list. It drops the staging table and returns the new snapshot ID.

A given `training_set_snapshot` makes `plan_examples` return no chunks and `assemble_examples` return the snapshot.

The fenced snapshot IDs are written to the KFP run (as run metadata and `Metrics` tags) and onto the Model Registry version. This is the data-versioning record: the daily, stream and label tables, plus the feature service. There's no MLflow on the platform.

This is the only Spark in the training code.

### 3. `risk_model`: five classes, plain functions

| Class | Responsibility |
|---|---|
| `DatasetReader` | Reads a training-set snapshot (PyIceberg → Arrow → pandas) by split, with weights; validation checks |
| `WindowSplitter` | Explicit consecutive windows; row checks |
| `RiskTrainer` | LR baseline; XGBoost on `device=cuda`, deterministic order, early stopping on validation |
| `GateEvaluator` | Thresholds on validation: decline where at most 0.1% of legitimate payments are flagged (one wrongly declined customer in a thousand), step-up at 1% (extra friction that is tolerable); weighted test metrics; the gate |
| `RegistryClient` | The platform registry client: register a version with metadata; find `production`; mark `production` (custom property) and archive the previous version (native ARCHIVED state) |

The other modules are `dataset.py`, `steps.py` (one function per pipeline step), `worker.py` (the TrainJob entrypoint), `kube.py` (the cluster calls, including the deploy check's predict request) and `cluster.py` (the names they share). The notebook calls `steps`.

### 4. Pipeline `payment-risk-training`

KFP v2 with `kfp-kubernetes`, on the `ml` image. It is compiled into `infra/charts/ml-training/files/` and deployed by Argo CD as a `Pipeline`/`PipelineVersion`. Runs use the ServiceAccount `fraud-training` with caching off. CPU steps run on spot.

| Step | Does |
|---|---|
| `plan_windows` | Decision 7a: rolling windows, and a skip when there is no new data. Every step below runs under `dsl.If(run)` |
| `plan_examples`, `stage_examples` (parallel chunks), `assemble_examples` | Decision 2, which returns the snapshot ID. A given `training_set_snapshot` skips it |
| `check_examples` | Spec checks; the report goes to the run directory; fails the run on failure |
| `split_windows` | Window checks; counts and fraud rate per split |
| `tune_params` (`dsl.If(tune)`) | A Katib Experiment: `tpe`, 8 trials, 2 in parallel, each trial a single-GPU Job with the DRA claim template, `Push` metrics |
| `fit_on_gpu` | A `TrainJob` on `xgboost-gpu-distributed` with `numNodes: 1`, image and command overridden by `python -m risk_model.worker`; uploads `model.json` and its feature list, then exits 0 |
| `gate` | Baseline, production version and new model on test; the gate (beats the baseline; at least +0.005 PR-AUC over production without a worse recall at 1% FPR. The margin sits above the spread between retrains on the same data, so a new version replaces production only on a real gain); KFP `Metrics` and the PR curve |
| `record_version` (`dsl.If(passed)`) | Writes the version next to the model file and registers a registry version with the spec's metadata, including the fenced snapshot IDs and the feature service. The artifact URI is the model directory in object storage |
| `roll_out` (`dsl.If(passed)`) | Patches the InferenceService to `model-registry://payment-risk/<version>`. KServe resolves the registry version, downloads the model and rolls the Deployment out to it. Then sends one request with an empty instance and fails the step unless the answer is a score from the new version. Then marks `production` and archives the previous version |

- **`payment-risk-rollout`** (`version`) reruns `roll_out` for an earlier passing version, for rollback.
- **Run directory:** run files go to `s3://<bucket>/runs/payment-gateway/ml/<kfp-run-id>/`.
- **GPU runs are serialized** with the model-server benchmarks.

### 5. The `ml` image and the GPU worker

- **Image.** A CUDA 12 runtime base with:
  - a Java runtime;
  - Spark 4.1.3 with the Iceberg 1.12 and AWS jars, pinned as in `data-platform-spark`;
  - `fraud_feast[spark]` from `src/data-pipeline/feast`;
  - pinned `xgboost`, `pyiceberg`, `scikit-learn`, `kfp`, and `model-registry`;
  - JupyterLab.

  It runs non-root, and is never the stock Trainer image.
- **Worker.** It reads the train and validation splits, sorts them by key, trains with `device=cuda` and `tree_method=hist`, early-stops on validation `aucpr`, uploads `model.json` with its feature list, and returns. There is no collective, so no NCCL is involved.
- **Run record:** the GPU model and the CUDA and XGBoost versions.

### 6. Serving

- **Triton on the GPU.** A chart-owned `ServingRuntime` runs `nvcr.io/nvidia/tritonserver` (pinned by digest) with the FIL backend on `KIND_GPU`. The InferenceService names that runtime and a `model-registry://payment-risk/<version>` storage URI; KServe's storage initializer resolves the registry version and downloads the model directory into `/mnt/models`, where `record_version` has written `payment-risk/config.pbtxt` and `payment-risk/<N>/xgboost.json`. The runtime, the fit and trial TrainJobs and the gate and experiment jobs all reference one `ResourceClaim`, because a TrainJob cannot patch `resourceClaims`; the chart therefore owns a `TrainingRuntime` that holds the claim.
- **Named features.** The booster is trained on unnamed columns and stores the ordered feature list as the `feature_order` attribute; `config.pbtxt` repeats it as a parameter with the registry version. Go reads the order from the model's config, lays the row out in that order, sends it as binary FP32 with NaN for anything missing, and reads the fraud class from the two-class answer. The response's `model_version` is the number of the version directory, so a score from another version than the cached order is discarded.
- **InferenceService `payment-risk`:** `Standard` mode (a Deployment and Service, no Knative), `minReplicas: 1`, on the GPU node, cluster-internal, run as `fraud-training`, whose storage secret carries the S3 settings KServe needs; created and patched by `roll_out`, not by Argo CD. Callers reach it at the InferenceService's in-cluster Service address.
- **Not model-inside-image.** The course rubric names that pattern. A model image needs a registry write credential in the cluster (or a platform-run registry) and OCI support in KServe, so this change serves the registered model from object storage and records the gap in the docs. The artifact stays immutable in practice: the pipeline writes each run's directory once, and the bucket is versioned.

### 6a. Derived request features

These are added to Feast's `request_features` on-demand view (python mode) in `fraud_feast`. The Feast change implements them, and they are part of `payment_risk_v1`. Each is computed only from values the view already receives, so no data contract changes:

| Candidate | Computed from | Null when |
|---|---|---|
| amount relative to the 30-day average | the request amount ÷ (`amount_sum_minor_30d` ÷ `attempt_count_30d`), per customer and per payment method | no 30-day history, or the 30-day count is 0 |
| hour of day of the scoring time | the request's scoring time (`started_at` offline, the scoring time online), UTC | never |
| days since the latest daily row | the scoring time minus the daily row's `event_timestamp` | no daily row within the TTL |

- **Condition on the last candidate:** Feast passes on-demand views feature values, not their row timestamps. It is kept only if PR #111 can expose that timestamp without a contract change; otherwise it is dropped.
- **Choosing the list.** The notebook runs an ablation on validation (each candidate on and off, PR-AUC). Candidates with no gain are removed from the view before the first `payment-risk-training` run, because changing the inputs after a version is registered would need `payment_risk_v2`.
- **Serving parity.** Go Fraud's online request to Feast carries the same request fields, including the scoring time, so the view computes identical values online.

### 7. Notebook

A stopped `Notebook` `fraud-explore` (the `ml` image, 20 Gi, CPU), listed in the Dashboard under `ml`. It runs `steps`:

1. build or load a training set;
2. split;
3. submit the same single-GPU TrainJob and wait;
4. evaluate on validation;
5. save `model.json` to its PVC.

Notebooks can't take GPU claims, which is why training goes through the TrainJob.

The notebook also compares five XGBoost models on the GPU, from the depth-1 additive baseline to the pipeline's settings, on validation at the gate's two operating thresholds, evaluates on test, and shows calibration and feature importance. Its windows come from `risk_model.windows.rolling`, the rule the scheduled run uses.

### 7a. Rolling retraining

- **`plan_windows`** is the pipeline's first step. From the run's schedule time (its creation time for a manual run), a lag and the newest day with rows in the stream tables for the configured epochs, it takes the latest day at or before `schedule date - lag` where every day of the three windows has rows, and lays out contiguous test, validation and train windows ending there (`risk_model.windows.rolling`, `latest_covered`). Candidate days come from the two stream tables' day partitions, and each day of a candidate span is confirmed with a filtered one-row scan, so a gap between the replay and live data is never spanned. There is no Spark. With no covered span the run stops.
- **Skip guard.** The rest of the pipeline runs under `dsl.If(run == "true")`. `run` is false when the cutoff is not later than the `cutoff` property of the highest registered version, unless `force` is set. A gate that fails retrains on the next run over the same data, since only passing models are registered.
- **Explicit windows.** A run that passes all four of `train`, `validation`, `test` and `cutoff` uses them as given.
- **No recurring run.** Retraining is started by the drift check (section 10) or by hand, so the recurring run, its `ensure_schedule` step and its PostSync hook Job are removed. Window sizes are chart values the check passes as run parameters: 56, 14 and 14 days on prod, 36, 9 and 9 on dev. Prod stays off until a prod run has been done.

### 8. Chart `ml-training`

- **Resources:**
  - ServiceAccount `fraud-training`, with a Role in `ml` (TrainJobs, Katib Experiments and Trials, InferenceServices, `workflowtaskresults`, Pods and logs read);
  - ExternalSecrets `ml-training-storage` (`OBJECT_GATEWAY_*`) and `ml-training-iceberg` (`ICEBERG_CLIENT_SECRET`);
  - the Katib trial template;
  - the two `Pipeline`/`PipelineVersion`s;
  - the stopped Notebook.
- **App:** in app-of-apps for dev and prod, with an environment value that picks the compiled pipelines (`files/dev`, `files/prod`). Dev runs on the local GPU and smaller Spark sizes. It creates no Namespace or Profile.
- **Images:** `ml` (rebuilt when `src/ml` or `src/data-pipeline/feast` change) in `service-images.json`; Triton comes from NGC, pinned by digest.

### 9. Tests in CI, no local runs

- a `dataset` test with Feast's test repository on a local warehouse: sampling stability, the fence failure, and an identical rerun writing an empty version;
- a `WindowSplitter` window test;
- a `hypothesis` property: the same rows in any order train the same model (CPU in CI);
- a gate test;
- Triton packaging and client: the config is GPU-only and carries the feature order and version, the repository layout is `payment-risk/<N>/xgboost.json`, NaN survives the binary framing, and the probe rejects another version or an impossible probability. The Go adapter is tested against a fake v2 server for every failure and for a version change between calls.

These are added to the existing Python test job.

### 10. Drift detection and retraining trigger

Rubric rows: configurable generated drift; a real-time drift API deployed with autoscaling; a periodic distribution comparison without immediate labels, dashboard metrics, and retraining triggered through the Kubeflow API.

- **What is compared.** The model's own inputs at scoring time, which is what drift would hurt. A version's *reference profile* is computed in `record_version` from its weighted training split: up to 10 weighted quantile edges per feature over the non-missing values, plus the weighted missing share (bin `m`). It is stored as the registry property `reference` and as a Triton `config.pbtxt` parameter next to `feature_order`, so it is versioned with the model and the Go client reads both from `GET /v2/models/payment-risk/config`.
- **Current distribution.** After each model call, Go Fraud increments `fraud.model.feature_bin{feature, bin, model_version}` once per feature (a missing or unavailable value goes to bin `m`). That is at most 50 features by 11 bins, about 550 series per version, through the existing OTLP export into VictoriaMetrics. No second telemetry stack, no Spark and no Feast read at check time, and no labels.
- **Statistic.** PSI per feature between the reference proportions and the proportions from `increase(fraud.model.feature_bin[window])`, with a small additive smoothing so an empty bin does not produce infinity, and the shift in missing share on its own. The summary is the number of features with PSI at or above 0.2, the maximum, and the PSI weighted by the registry's feature gain.
- **Drift service `payment-risk-drift`** (FastAPI Deployment in `ml`, read-only and stateless): `GET /v1/drift?window=24h` returns per-feature PSI, the sample count and a state (`insufficient`, `ok`, `drifting`); `GET /v1/reference` returns the serving version's profile; `/healthz`, `/readyz` and `/metrics` (`ml_feature_drift_psi{feature,model_version}`, `ml_drift_state`, `ml_drift_triggers`). Helm rolling update with probes, a KEDA `ScaledObject` on its own request rate (1 to 2 replicas). The Grafana ML dashboard gains the drift panels from these gauges.
- **Policy** (chart values, small samples allowed on dev): at least 2,000 scored requests in the window (200 on dev); drifting when at least 3 features reach PSI 0.2, or any of the 5 highest-gain features reaches 0.25, in both the 6 h and the 24 h windows (so one noisy hour never triggers); warning from 0.1 only on the dashboard.
- **Trigger.** CronJob `payment-risk-drift-check` (hourly, `concurrencyPolicy: Forbid`, the only writer) reads the drift API and, when the policy says drifting, creates one `payment-risk-training` run through the KFP REST API, named `drift-<version>-<UTC hour>` with the drift summary in its description. It does nothing when: a `drift-*` run is pending or running (deduplication); the last `drift-*` run started less than 24 h ago or the serving version was registered less than 2 days ago (cooldown); 3 `drift-*` runs started in the last 7 days (bound). Its state is the KFP run list and the registry, so there is no new database and a restarted job decides the same way.
- **Gates unchanged.** The triggered run is the normal pipeline: `plan_windows` still stops when its cutoff is not newer, and the gate still needs the baseline and the production margin. A run that fails the gate registers nothing, the drift stays visible, and the cooldown and the weekly bound limit the repeats. A registered version records `trigger` (`schedule` or `drift`) and the drift summary that caused it.
- **Drift is the only automatic trigger.** The recurring run is removed (section 7a). One clause of the same check, the age ceiling, starts a run when the serving version was registered more than 14 days ago, so a broken metrics path or a model that never meets traffic cannot freeze the model; it uses the same dedupe, cooldown, bound and gate. The first model is still a manual run. Prod stays off until a prod run has been done.
- **Generated drift.** The live simulator's config gains `drift`, a list of steps `{after_seconds, behavior, scenarios}` that override named values such as `foreign_ip_probability`, `familiar_context_probability` and scenario rates or amount multipliers from that offset on. The overrides apply when a purchase is drawn, so the manifest's world digest is unchanged, the stream stays deterministic for a seed, and unknown keys are rejected. The dev values carry one example step. The historical generator is not regenerated (133 GB); it already has `schema_changes`, and a `drift` list with `effective_at` dates can follow.
- **Alternatives rejected.**
  - *Offline Feast windows for the current side:* reuses the dataset step, but needs the 8 GB local-Spark staging pod for every check and a label join in `entity_rows`, which the dev workers could not place.
  - *Comparing raw gold tables:* cheap, but daily and stream snapshots are not the values the model scored.
  - *A drift library or a monitoring product:* an equivalent PSI is 30 lines of numpy and the bins are already in the model's config.

### 11. Model A/B experiment

Rubric rows: two inference versions with an A/B traffic split delivered through CI/CD, and per-version monitoring. The Edge canaries (Argo Rollouts on checkout releases) stay separate, and this reuses their pieces, not their Rollout.

- **What was tried (2026-11-03, dev).** KServe `canaryTrafficPercent` is Knative-only, and Knative Serving (1.23.0 and 1.24.0) rejects the DRA `resourceClaims` the Triton runtime needs. A KServe `InferenceGraph` Splitter runs once a `MutatingAdmissionPolicy` adds the router's seccomp profile, but it discards response headers, so binary tensor answers fail, and it adds 4.7 ms at the median. An Argo Rollouts `Rollout` cannot shift traffic between KServe-owned Deployments, because it must own the pods (a `workloadRef` Rollout fights KServe and the KEDA scaler). What works: a Gateway API `HTTPRoute` on an Istio ambient waypoint, with binary requests and responses intact (80/20 weights gave 309 to 91 of 400 requests; +2.4 ms median, +9.8 ms p99). The user chose this with Argo Rollouts' analysis for gating (2026-11-03).
- **Two services, one route.** The champion is the existing `payment-risk` InferenceService. A challenger is a second one, `payment-risk-challenger`, on the same `ServingRuntime`, the shared `ml-gpu` claim and its own KEDA scaler, created by `roll_out` with `role=challenger` and removed on promotion or abort. The chart owns an Istio waypoint `Gateway` in `ml` and an `HTTPRoute` whose parent is the champion's Service (labelled `istio.io/use-waypoint`) and whose two backends are the two predictor Services, weighted 100 and 0 when no experiment runs. The fraud service keeps calling the champion's Service; it does not know about arms. The waypoint pod needs a platform `MutatingAdmissionPolicy` for its seccomp profile under the `restricted` namespace (requested from the platform thread; on in dev, off in prod until the user flips it).
- **Weights belong to the pipeline.** The chart renders the route with weights 100/0, and Argo CD ignores the weights (`ignoreDifferences` on the backend weights, as Edge does for its controller-owned weights). The `payment-risk-promote` pipeline patches them in steps through the Kubernetes API, never above the cap (20 percent until promotion), so the split is delivered by CI/CD and changed by reviewed pipeline runs, not by hand.
- **Gating with Argo Rollouts analysis.** An `AnalysisTemplate` `payment-risk-challenger` (Prometheus provider against the shared VictoriaMetrics, like Edge's `edge-checkout`) checks, per step and over a window with a minimum sample: the challenger's Triton failure rate, its mean queue plus compute time per request, and the difference between its enforced-decline share and the champion's (at most 2 percentage points; a ratio of two shares near 0.3% is noise at a cohort of a few hundred requests). The pipeline creates a standalone `AnalysisRun` from it for each step and waits for the result; a failed run sets the weights back to 0, deletes the challenger service, and fails the pipeline, so the cohort falls back to the champion. The pipeline's service account gets a Role for `analysisruns`.
- **Assignment is per request.** The waypoint picks the arm at random with the weights; a customer can see both, which is acceptable for attempt-level randomization, and an assessment is made once per attempt (a replay returns the recorded result). The user accepted this when choosing the route.
- **The Go client learns each version.** The answer's `model_version` identifies the arm. The client keeps a layout per version (feature order and decline threshold from that version's Triton config) instead of one, fetches the config of an unseen version (retrying a few times, since the route may answer from the other arm) and discards a score until it has it, and scores each answer under its own version's threshold. The threshold is a registry property of the version, packaged into `config.pbtxt` as `decline_threshold` by `record_version`, so the chart's single `FRAUD_MODEL_THRESHOLD` becomes the fallback for a version whose config lacks one. Evidence already records the version and threshold. `fraud.model.calls` gains `model_version` and `declined` attributes so the analysis can compare decline rates per version.
- **Safe enforcement.** Only a version that passed its gate can be deployed as challenger (the existing `require_passing`), the cap is checked by the pipeline, and a challenger that cannot answer leaves the rules decision standing (the existing failure path), so it can never approve what the rules decline.
- **Comparison without immediate labels.** Per version, from the shared stack: Triton's request, queue and compute metrics per service (the scrape selects both services, labelled by InferenceService), the fraud counters by `model_version`, and, from the ClickHouse mart, decline rate, step-up rate and score distribution by `model_version` (no labels needed). The arms' score distributions compare with the same PSI. Labels join later through the same mart (`truth_is_fraud`, `chargeback_available_at`) and through the offline gate, which already compared the challenger with the champion on the training run's test window when the challenger was registered.
- **Registry and CI/CD.** Stages are `production` (champion), `challenger` and `archived`. A training run started with `experiment` deploys its version as challenger instead of replacing the champion. `payment-risk-promote` shifts the weights stepwise with an analysis at each step, then promotes (updates the champion service to the challenger's version, archives the old one, sets the weights to 0, deletes the challenger) or aborts. `payment-risk-rollout` still returns to any earlier version.
- **Dashboard.** `ml-serving` gains per-version panels and the route's weights; `fraud-outcomes` (ClickHouse) gains a by-version breakdown.
- **Not chosen.** Shadow scoring both arms on every request (extra latency inside the 150 ms budget, removed earlier); a stable per-customer cohort (the route is per request; a header-matched route could add it later); a Go-side split (needs no platform change but the user preferred the platform route).

### 12. Decisions

Decided by the user on 2026-11-03: drift signal from Go-side bin counters; a new small FastAPI image for the drift service; enforced A/B with no shadow scoring; live simulator overlay only for generated drift; dev proof approved (tasks 7.9 and 7.10, dev only); retraining by drift only, with no recurring run (the age ceiling above is part of that check). A KServe `InferenceGraph` split was suggested and tried (section 11).

Later on 2026-11-03 the user asked for Knative's `canaryTrafficPercent` instead. The platform thread checked and found it cannot work: Knative Serving 1.23.0 and the newest 1.24.0 reject `resourceClaims` and `resources.claims` as unknown fields, so the Triton runtime's shared DRA claim cannot run on it (the platform thread read upstream `knative/serving` issue #15332 as still open, which I have not checked myself). The user kept KEDA and Standard mode, and asked to try Argo Rollouts.

A Rollout cannot do it directly: it must own the pods it shifts traffic to, and KServe owns the predictor Deployments (a `workloadRef` Rollout would fight KServe and the KEDA scaler). What does work is the mechanism Rollouts' Gateway API plugin uses for Edge. On dev (2026-11-03) a Gateway API `HTTPRoute` with weights 80/20 on the `payment-risk-predictor` Service, attached through an Istio ambient waypoint in `ml` (label `istio.io/use-waypoint` on the Service), split 400 binary requests 309 to 91 between two KServe services, with binary tensor responses intact and `model_version` identifying the arm. The extra hop cost 2.4 ms at the median and 9.8 ms at p99 (12.1 and 25.9 ms direct, 14.5 and 35.7 ms through the waypoint). The split is random per request. The waypoint pod is rejected by the `restricted` Pod Security for the same missing `seccompProfile` as the InferenceGraph router, so it needs the same kind of platform `MutatingAdmissionPolicy` (the spike used a temporary one, since removed). Argo Rollouts' `AnalysisTemplate` and a standalone `AnalysisRun` could gate each weight step on the per-version metrics, as Edge does; that was not tried.

So two ways to split are on the table and the choice is open (section 12): the waypoint route, whose weights are a GitOps value and whose gating can reuse the Rollouts analysis engine but which assigns per request and needs the platform policy, or the Go-side split, which assigns per customer and needs nothing from the platform.

Decided on 2026-11-03, after the spikes: keep KEDA and Standard mode; split with a waypoint `HTTPRoute` gated by Argo Rollouts analysis; the threshold follows the served version (from its config), and an abort sets the weights to 0 and deletes the challenger.

## Implementation notes

API checks against released sources (task 0.4). Each deviation from the decisions above is stated with its reason.

- **Dependency split.** `kfp` needs `protobuf<7` and `fraud_feast` pins 7.36.1, so the pipelines are a separate uv project, `src/ml/pipelines`. Every step is a container component running `python -m risk_model.steps <step>` in the `ml` image, which holds no `kfp`. Only the metrics step is a lightweight component, on a stock Python image.
  - The `fit_on_gpu` TrainJob and the Katib trial are built from one raw `TrainJob` template (`kube.train_job_spec`), not the Kubeflow SDK. The SDK's `CustomTrainerContainer.env` takes literal values only, and the Trainer webhook rejects secret-backed environment set through a runtime patch, so the credentials go in `spec.trainer.env` as `secretKeyRef`s and the runtime patch only sets the service account (its `manager` must be domain-prefixed). The pods read their own credentials, so `fraud-training` needs no Secret read and no `ClusterTrainingRuntime` read.
  - The InferenceService and the Katib Experiment are raw objects, because the SDK has no KServe client, and its optimizer client takes a function trainer only and cannot set a service account or secret-backed env on the trial.
- **XGBoost 2.1.4, not the newest.** Training (CUDA 12) writes the JSON that Triton's FIL backend loads. A dev check of 215 real rows against CPU XGBoost on the same file agreed within 6e-8, which is the guard against the two reading the format differently; 3.4 wheels are built for CUDA 13.
  - The model is trained on unnamed columns. The ordered feature list is stored as the `feature_order` attribute, on the registry version and in the Triton config, and the Go adapter does the ordering.
  - The booster is cut at the best iteration, so the served score equals the evaluated one.
- **Feast 0.67 Spark offline store.** `to_spark_df()` does not apply on-demand views; only `to_arrow()` and `to_df()` do, and by default `to_arrow()` collects through pandas. With `staging_location` set to a local directory, `to_arrow()` writes Spark's result as Parquet and reads it back with `pyarrow`, then applies the views, with no pandas. Each day of the sampled set is small enough for one Arrow table. `pandas` stays below 3 because Feast requires it.
- **Iceberg.** Snapshot properties come from session configuration `spark.sql.iceberg.snapshot-property.<key>` (Iceberg 1.12 `SparkWriteConf.extraSnapshotMetadata`), because a SQL `MERGE` takes no write options. `WHEN NOT MATCHED BY SOURCE` is covered by Iceberg's own merge tests. The table is format v2, because PyIceberg 0.10 reads position deletes but not deletion vectors.
- **Model Registry client 0.3.18** (server 0.3.17). Custom properties are scalar only, so lists and maps are stored as JSON strings. `production` is a property; replacement sets the native `ARCHIVED` state, and a rollback sets `LIVE` again.
- **KFP Kubernetes-native.** A `PipelineVersion` carries `pipelineName`, `pipelineSpec` and `platformSpec`, so the compiler's two documents are split in the chart. The version name carries a hash of the rendered content, so each changed definition or image tag is a new version. The compiled files hold an image placeholder that the chart replaces.
- **Katib 0.19.** The `tune_params` step creates the Experiment with a `TrainJob` trial on the same runtime, so the GPU claim and placement come from the runtime. Metrics use the default StdOut collector: the worker prints `aucpr=<value>`. The collector sidecar copies the trial container's restricted security context only when the controller sets `injectSecurityContext`, which talos-proxmox now enables. Tuning is off when `tune_trials` is 0. The Push collector would avoid the sidecar but needs the Katib SDK, which cannot share an environment with `fraud_feast`.
- **KServe 0.21.** Triton runs as a chart-owned `ServingRuntime` container with the v2 protocol, not the stock `kserve-tritonserver`, whose image is Triton 23.05 and predates Blackwell GPUs. The host for the smoke request comes from the predictor URL in the InferenceService status. The stock `kserve-xgbserver` is not used: it is a CPU server.
- **Tuning data.** Trials read the pinned training-set snapshot read-only, on a 5% negative sample (`tune_rate`), so parallel trials stay cheap and the final fit uses the full stored sample.
- **Image.** The Spark runtime comes from the `pyspark` wheel with the pinned jars from `spark_jars`, on a CUDA 12.6 runtime base, not from the Apache Spark image.
- **Derived request features (task 1.1b).** The Feast change now provides `customer_amount_to_30d_avg`, `payment_method_amount_to_30d_avg` and `scored_hour_utc` in `request_features`, and offline retrieval with `store(redis=False)`. The days-since-latest-daily-row candidate stays dropped, because Feast passes an on-demand view feature values, not their row timestamps.

## Risks / Trade-offs

- **[The Feast change or the replay slips]** → The training code is written and tested against Feast's test repository now. Runs wait.
- **[Simulation-truth labels flatter the metrics]** → Documented: simulation truth also labels declined attempts, and a real deployment would see delayed, partial chargebacks.
- **[Local Spark on one spot node is slow or runs out of disk]** → Negative sampling happens before the join. The spill volume is 24 Gi. The step retries on spot interruption. The run records its wall time.
- **[A heavy `ml` image (CUDA plus Java plus Spark)]** → One image keeps one dependency set; its size is recorded. Splitting it is a later option.
- **[Rubric gaps]** → Single GPU instead of distributed, and a registry URI instead of model-inside-image, are the user's choices, stated in the docs.
- **[GPU capacity: On-Demand cost, sharing with benchmarks]** → Bounded runs, serialized with the benchmarks.
- **[Drift triggers on noise or loops]** → Two windows, a minimum sample, a cooldown, a weekly bound and a pending-run check; the gate still decides whether anything registers.
- **[Bin counters add series and hot-path work]** → About 550 series per version and 50 counter increments per request, measured before enabling in prod; the profile is bounded to 11 bins.
- **[A challenger changes real decisions]** → Gate-passed versions only, a 20% cap, per-arm thresholds under the same missing-feature policy, rules fallback, and an abort that deletes the challenger.
- **[Two predictors share one GPU]** → The claim is already shared; memory is small for FIL, and the KEDA maximum caps replicas.
- **[KFP master is experimental]** → Thin steps over `risk_model`, and the notebook can run them too.

## Migration Plan

1. Implement and run local non-test checks, then commit locally. Push and open the PR only with the user's agreement.
2. After the Feast change deploys, `stream-history` covers `[2026-07-29, 2026-10-29)`, and the user approves:
   1. sync `ml-training`;
   2. the notebook explores;
   3. `payment-risk-training` runs with `tune=true`;
   4. a direct JSON call, including null features;
   5. a `payment-risk-rollout` rollback;
   6. a second run that shows an incremental training-set version.
3. Record the evidence (figures, screenshots, mermaid) in `docs/ml/{training,pipeline,drift,serving}.md`.

**Rollback:** `payment-risk-rollout` the previous version; disable `ml-training`; revert the PR; drop `ml.payment_risk_v1_examples`.
