## Why

Nothing trains, versions or serves the fraud model yet. The Feast change (`feast-feature-store`, draft PR #111) provides everything the model reads, through the `payment_risk_v1` feature service:

- Spark's daily windows from DP3;
- Flink's short windows, whose history is a Kappa replay through the live Flink job (`stream-history`);
- request features from an on-demand feature view;
- the training entity rows, from `fraud_feast.training.entity_rows`.

This change trains `payment_risk_v1` on that service, through the platform's Kubeflow components, with as little training-side code as possible. The code is written now against the Feast change's interface, and runs once that change is deployed and `stream-history` has replayed `[2026-07-29, 2026-10-29)`.

## What Changes

- **Training data only through Feast.**
  - The dataset step calls `fraud_feast.training.entity_rows(spark, start, end, cutoff)`, then `get_historical_features(payment_risk_v1, full_feature_names=True)`, with Feast's Spark offline store running in local mode inside one KFP step.
  - Labels are simulation truth, treated as real for this learning project.
  - Each run writes one incremental version of `lake.ml.payment_risk_v1_examples`, recording the source snapshot IDs, the window and the cutoff.
  - Nothing writes to the tables it reads.
  - **Data versioning:** every training run records, on its KFP run and its Model Registry version, the Iceberg snapshot IDs of `gold.feat_customer_daily`, `gold.feat_payment_method_daily`, `gold.feat_stream_*` and `gold.label_attempt`, plus the feature-service name. A model therefore traces to the exact rows it trained on. There is no MLflow on the platform, so the KFP run and the registry version are the run record.
- **Derived request features** in Feast's `request_features` view, under `payment_risk_v1`, computed only from existing view values and request fields, so no data contract changes:
  - amount relative to the entity's 30-day average (null without history);
  - hour of day of the scoring time;
  - days since the entity's latest daily row, if Feast can expose that row's time to the view.

  The final list is chosen with the model on validation. The Feast change implements them and keeps Go Fraud's online request in sync.
- **Time-ordered splits, no gaps:** train 2026-07-29..09-30, validation 10-01..10-14, test 10-15..10-28.
- **`risk_model`**, a small training library (new `src/ml`) with the five key classes. The Kubeflow Notebook and every pipeline step call it.
- **Kubeflow pipeline `payment-risk-training`, the model's CI/CD:**
  1. build the training set;
  2. validate;
  3. split;
  4. Katib tune (optional);
  5. train XGBoost on **one GPU** (Kubeflow Trainer);
  6. evaluate and gate;
  7. register in the platform's Model Registry;
  8. deploy to KServe.

  A `payment-risk-rollout` pipeline redeploys an earlier version, for rollback.
- **Serving:** Triton with the FIL backend on the GPU, referenced by a chart-owned `ServingRuntime`, serves the registered version from `model-registry://payment-risk/<version>`. Callers send one binary FP32 row in the packaged feature order, and NaN marks a missing feature. All scoring, including the gate's and the notebook's, runs on the GPU.
- **Workspace wiring (BREAKING for the stale `kubeflow-workspace` spec).** The app deploys only consumer resources into the platform `ml` namespace: the ServiceAccount and Role, ExternalSecrets, the stopped notebook, and the pipeline definitions. The platform owns the registry, KServe, Katib, Trainer, the GPU runtime and capacity, and the `ml` Profile.
- **Rolling windows, no recurring schedule:** each `payment-risk-training` run derives its windows from its start time, and a run whose cutoff is not newer than the newest registered model stops before building a training set. Runs are started by the drift check (drift, or a model older than an age ceiling) or by hand; there is no Kubeflow recurring run.
- **Go Fraud scores with the model:** a v2 adapter calls the registered model on Triton after the Feast read. The score can decline an attempt only under an explicit missing-feature policy, and is always recorded in the assessment evidence.
- **Drift and drift-triggered retraining.** Go Fraud counts each scored feature into the bins of the served version's reference profile, and a small drift service compares the current window with that reference (PSI per feature, with a minimum sample count) without any labels. An hourly job turns a persistent breach, or a model older than the age ceiling, into one bounded, deduplicated `payment-risk-training` run through the Kubeflow API, so every existing evaluation and promotion gate still applies. It is the only automatic trigger. The live simulator gains a configurable drift schedule to produce the drift.
- **Model A/B experiment.** A registered version that passed its gate can serve as a challenger in its own InferenceService next to the champion. A Gateway API route on an Istio waypoint splits the champion's Service by weights that only the `payment-risk-promote` pipeline changes (capped at 20 percent), each step is gated by an Argo Rollouts analysis of per-version metrics in the shared VictoriaMetrics, and the pipeline promotes or removes the challenger. Go Fraud scores each answer under the version that produced it. This is separate from the Edge checkout canaries.
- **Not in this change:**
  - distributed training (the user chose a single GPU);
  - shadow (non-enforcing) scoring, which an earlier decision removed;
  - a second metrics or monitoring stack, KServe's Knative canary and InferenceGraph (both tried and dropped), and any new course-named product;
  - regenerating the historical dataset with drift.

## Capabilities

### New Capabilities

- `fraud-training-dataset`: the training set built through `payment_risk_v1`, covering its entity rows, sampling, recorded provenance, incremental versions and validation.
- `fraud-model-training`: the notebook, the Kubeflow pipeline, time-ordered splits, single-GPU training, Katib, the evaluation gate and Model Registry versions, the per-version reference profile, and drift measurement with its retraining trigger.
- `fraud-model-serving`: the KServe InferenceService contract, deployment and rollback by pipeline, autoscaling and metrics, and the challenger version with per-version monitoring.

### Modified Capabilities

- `kubeflow-workspace`: drops the app-owned Profile and registry. The app's resources live in the platform `ml` namespace, and the platform owns the Kubeflow services.
- `fraud-decision`: model scoring in the assessment, the missing-feature policy, and the evidence it records, plus experiment arm assignment and the feature-bin counters.
- The live simulator's configurable drift schedule is specified under `fraud-model-training` for now; a delta spec for `historical-data-generation` can be split out if that capability should own it (design, open decision 7).

## Impact

- **Code:** the drift service and the hourly check in `src/ml`, `risk_model.drift`, the arms and bin counters in `src/payment-gateway/fraud`, a `drift` overlay in `src/data-pipeline/simulator`'s live config; new `src/ml`: `risk_model` (including a small JSON client and the rolling-window planner), the pipelines `payment-risk-training` and `payment-risk-rollout`, and the notebook; `src/payment-gateway/fraud` (a Triton adapter and the decision policy).
- **Images:**
  - new `ml`: a CUDA 12 runtime with pinned XGBoost, plus Java, Spark 4.1.3 with the Iceberg 1.12 and AWS jars, and `fraud_feast[spark]` from `src/data-pipeline/feast`;
- **Charts:**
  - new `infra/charts/ml-training` (dev and prod, in `ml`): ServiceAccount and Role, ExternalSecrets, Notebook, `Pipeline`/`PipelineVersion`s, Katib trial template;
  - `infra/argocd/app-of-apps`.
- **Data:** the Polaris namespace `ml` and its table `ml.payment_risk_v1_examples`.
- **Docs:** `docs/ml/training.md`, `docs/ml/serving.md`, the roadmap rows.
- **Dependencies:** the Feast change merged and deployed, and `stream-history` run over `[2026-07-29, 2026-10-29)`.
  - The platform has already provided the registry, KServe, Katib, Trainer, `xgboost-gpu-distributed` and the `ml` Dashboard Profile.
  - Its Spark-operator-watches-`ml` change (`ae12236`) is no longer needed.
- **Sequencing:** planning on `plan/training-on-gold`. Implementation, pushes and every cluster run wait for the user's approval, and GPU runs are serialized with the model-server benchmarks.
