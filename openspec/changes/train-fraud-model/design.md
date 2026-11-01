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
  - KServe 0.21 Serverless runs custom predictor containers; the registry stores any artifact URI.
  - KFP (single-user) and Katib 0.19 (`Push`, `restricted`) are validated.
  - Runtime `xgboost-gpu-distributed`: DRA claim, non-root, image, command and args overridable. The pool is at most 2 × g6.xlarge (L4), currently On-Demand. The stock XGBoost image's NCCL is too old; a single GPU needs no NCCL.
  - Notebooks 1.11 cannot take DRA claims.
  - The Dashboard lists `ml` through a platform-owned `Profile/ml`.
  - The GPUs are shared with model-server benchmarks.
- **Capacity:** spot `aws-cpu-burst` has large and xlarge nodes up to 12, including r-family (32 GiB). The root volume leaves about 24 Gi for pod scratch space.
- **Course rubric (MLE):** notebook (offline store plus labels, split, train, evaluate, save); KFP with at least load, split, train and evaluate; model and incremental data versions; KServe; five key classes; property-based tests.

## Goals / Non-Goals

**Goals:**

- One plain path: Feast, then a training set, then XGBoost on one GPU, then the registry, then KServe. The training code holds no feature, label or entity logic.
- Every model and deployment comes from a Kubeflow pipeline run on the platform's components.
- Code written now against the Feast interface, and run as soon as the replay exists.

**Non-Goals:**

- Feature, label or request-field logic in training code; any source other than Feast; writes to Gold.
- Distributed or CPU training.
- Go Fraud calling the model, drift and retraining (roadmap), A/B splits, and GitHub-driven promotion.

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

The other modules are `dataset.py`, `steps.py` (one function per pipeline step), `worker.py` (the TrainJob entrypoint) and `serve_client.py` (a small JSON client used by the deploy check). The notebook calls `steps`.

### 4. Pipeline `payment-risk-training`

KFP v2 with `kfp-kubernetes`, on the `ml` image. It is compiled into `infra/charts/ml-training/files/` and deployed by Argo CD as a `Pipeline`/`PipelineVersion`. Runs use the ServiceAccount `fraud-training` with caching off. CPU steps run on spot.

| Step | Does |
|---|---|
| `plan_examples`, `stage_examples` (parallel chunks), `assemble_examples` | Decision 2, which returns the snapshot ID. A given `training_set_snapshot` skips it |
| `check_examples` | Spec checks; the report goes to the run directory; fails the run on failure |
| `split_windows` | Window checks; counts and fraud rate per split |
| `tune_params` (`dsl.If(tune)`) | A Katib Experiment: `tpe`, 8 trials, 2 in parallel, each trial a single-GPU Job with the DRA claim template, `Push` metrics |
| `fit_on_gpu` | A `TrainJob` on `xgboost-gpu-distributed` with `numNodes: 1`, image and command overridden by `python -m risk_model.worker`; uploads `model.json` and its feature list, then exits 0 |
| `gate` | Baseline, production version and new model on test; the gate (beats the baseline; at least +0.005 PR-AUC over production without a worse recall at 1% FPR. The margin sits above the spread between retrains on the same data, so a new version replaces production only on a real gain); KFP `Metrics` and the PR curve |
| `record_version` (`dsl.If(passed)`) | Writes the version next to the model file and registers a registry version with the spec's metadata, including the fenced snapshot IDs and the feature service. The artifact URI is the model directory in object storage |
| `roll_out` (`dsl.If(passed)`) | Patches the InferenceService to `model-registry://payment-risk/<version>`. KServe resolves the registry version, downloads the model and rolls out a new revision. Then sends one request with an empty instance and fails the step unless the answer is a score from the new version. Then marks `production` and archives the previous version |

- **`payment-risk-rollout`** (`version`) reruns `roll_out` for an earlier passing version, for rollback.
- **Run directory:** run files go to `s3://<bucket>/runs/payment-gateway/ml/<kfp-run-id>/`.
- **GPU runs are serialized** with the model-server benchmarks.

### 5. The `ml` image and the GPU worker

- **Image.** A CUDA 12 runtime base with:
  - a Java runtime;
  - Spark 4.1.3 with the Iceberg 1.12 and AWS jars, pinned as in `data-platform-spark`;
  - `fraud_feast[spark]` from `src/data-pipeline/feast`;
  - pinned `xgboost`, `pyiceberg`, `scikit-learn`, `kfp`, `model-registry` and the Kubeflow SDK;
  - JupyterLab.

  It runs non-root, and is never the stock Trainer image.
- **Worker.** It reads the train and validation splits, sorts them by key, trains with `device=cuda` and `tree_method=hist`, early-stops on validation `aucpr`, uploads `model.json` with its feature list, and returns. There is no collective, so no NCCL is involved.
- **Run record:** the GPU model and the CUDA and XGBoost versions.

### 6. Serving

- **Custom predictor.** `src/ml/predictor` is a `kserve.Model` over `xgboost-cpu`, built by CI as the `predictor` image and referenced by a chart-owned `ServingRuntime`. The InferenceService names that runtime and a `model-registry://payment-risk/<version>` storage URI; KServe's storage initializer resolves the registry version and downloads the model directory into `/mnt/models`.
- **Named features.** The booster is trained on unnamed columns and stores the ordered feature list as the `feature_order` attribute. The predictor takes `{"instances": [{feature: value}]}` (KServe v1 JSON), orders the values by that list, and treats a missing or null feature as NaN, which XGBoost treats as missing. It returns `{"predictions": [...], "model_version": "v7"}`; the version comes from `version.txt`, written next to the model by `record_version`. Callers never depend on column order.
- **InferenceService `payment-risk`:** Serverless, `minScale: 1`, spot, cluster-local, run as `fraud-training`, whose storage secret carries the S3 settings KServe needs; created and patched by `roll_out`, not by Argo CD. Callers reach it through `knative-local-gateway` with the predictor host as `Host`.
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

A stopped `Notebook` `fraud-explore` (the `ml` image, 20 Gi gp3, CPU), listed in the Dashboard under `ml`. It runs `steps`:

1. build or load a training set;
2. split;
3. submit the same single-GPU TrainJob and wait;
4. evaluate on validation;
5. save `.joblib` and `model.json` to its PVC.

Notebooks can't take GPU claims, which is why training goes through the TrainJob.

### 8. Chart `ml-training` (prod only)

- **Resources:**
  - ServiceAccount `fraud-training`, with a Role in `ml` (TrainJobs, Katib Experiments and Trials, InferenceServices, `workflowtaskresults`, Pods and logs read);
  - ExternalSecrets `ml-training-storage` (`OBJECT_GATEWAY_*`) and `ml-training-iceberg` (`ICEBERG_CLIENT_SECRET`);
  - the Katib trial template;
  - the two `Pipeline`/`PipelineVersion`s;
  - the stopped Notebook.
- **App:** prod only, in app-of-apps. It creates no Namespace or Profile.
- **Images:** `ml` (rebuilt when `src/ml` or `src/data-pipeline/feast` change) and `predictor` (rebuilt when `src/ml/predictor` changes), in `service-images.json`.

### 9. Tests in CI, no local runs

- a `dataset` test with Feast's test repository on a local warehouse: sampling stability, the fence failure, and an identical rerun writing an empty version;
- a `WindowSplitter` window test;
- a `hypothesis` property: the same rows in any order train the same model (CPU in CI);
- a gate test;
- predictor properties (`hypothesis`): scores are identical across repeated calls and across key order, a null, a missing and an unknown feature behave alike, and the response names the model version.

These are added to the existing Python test job.

## Implementation notes

API checks against released sources (task 0.4). Each deviation from the decisions above is stated with its reason.

- **Dependency split.** `kfp` needs `protobuf<7` and `fraud_feast` pins 7.36.1, so the pipelines are a separate uv project, `src/ml/pipelines`. Every step is a container component running `python -m risk_model.steps <step>` in the `ml` image, which holds no `kfp`. Only the metrics step is a lightweight component, on a stock Python image.
  - The `fit_on_gpu` TrainJob is created with the Kubeflow SDK (`kubeflow` 0.5.0, `TrainerClient.train` with `CustomTrainerContainer` and the `kubeflow.trainer.options` overrides). The SDK reads `ClusterTrainingRuntime`s, so talos-proxmox grants `ml/fraud-training` a read-only ClusterRole; a namespaced Role cannot.
  - The InferenceService and the Katib Experiment are raw objects, because the SDK has no KServe client, and its optimizer client takes a function trainer only and cannot set a service account or secret-backed env on the trial.
- **XGBoost 2.1.4, not the newest.** Training (CUDA 12) and the predictor (`xgboost-cpu`) use the same version, so a model file always loads in the version that wrote it; 3.4 wheels are built for CUDA 13.
  - The model is trained on unnamed columns. The ordered feature list is stored as the `feature_order` attribute and on the registry version, and the predictor does the ordering.
  - The booster is cut at the best iteration, so the served score equals the evaluated one.
- **Feast 0.67 Spark offline store.** `to_spark_df()` does not apply on-demand views; only `to_arrow()` and `to_df()` do, and by default `to_arrow()` collects through pandas. With `staging_location` set to a local directory, `to_arrow()` writes Spark's result as Parquet and reads it back with `pyarrow`, then applies the views, with no pandas. Each day of the sampled set is small enough for one Arrow table. `pandas` stays below 3 because Feast requires it.
- **Iceberg.** Snapshot properties come from session configuration `spark.sql.iceberg.snapshot-property.<key>` (Iceberg 1.12 `SparkWriteConf.extraSnapshotMetadata`), because a SQL `MERGE` takes no write options. `WHEN NOT MATCHED BY SOURCE` is covered by Iceberg's own merge tests. The table is format v2, because PyIceberg 0.10 reads position deletes but not deletion vectors.
- **Model Registry client 0.3.18** (server 0.3.17). Custom properties are scalar only, so lists and maps are stored as JSON strings. `production` is a property; replacement sets the native `ARCHIVED` state, and a rollback sets `LIVE` again.
- **KFP Kubernetes-native.** A `PipelineVersion` carries `pipelineName`, `pipelineSpec` and `platformSpec`, so the compiler's two documents are split in the chart. The version name carries a hash of the rendered content, so each changed definition or image tag is a new version. The compiled files hold an image placeholder that the chart replaces.
- **Katib 0.19.** The `tune_params` step creates the Experiment with a `TrainJob` trial on the same runtime, so the GPU claim and placement come from the runtime. Metrics use the default StdOut collector: the worker prints `aucpr=<value>`. The collector sidecar copies the trial container's restricted security context only when the controller sets `injectSecurityContext`, which talos-proxmox now enables. Tuning is off when `tune_trials` is 0. The Push collector would avoid the sidecar but needs the Katib SDK, which cannot share an environment with `fraud_feast`.
- **KServe 0.21.** The predictor is a custom `ServingRuntime` container with the v1 JSON protocol. The host for the smoke request comes from the predictor URL in the InferenceService status. The stock `kserve-xgbserver` is not used: it loads `xgboost~=2.1.1`, builds an unnamed matrix, and has no way to take named features.
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
3. Record the evidence (figures, screenshots, mermaid) in `docs/ml/training.md` and `docs/ml/serving.md`.

**Rollback:** `payment-risk-rollout` the previous version; disable `ml-training`; revert the PR; drop `ml.payment_risk_v1_examples`.
