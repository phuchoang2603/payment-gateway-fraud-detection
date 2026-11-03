# Training

**Status: dev pipeline runs done; production runs pending.** The code and the prod chart are under review. Production runs wait for `stream-history` to cover `[2026-07-29, 2026-10-29)`. Code: [`src/ml`](../../src/ml); chart: [`infra/charts/ml-training`](../../infra/charts/ml-training). Dev runs the same chart on the RTX 5060 Ti of `dev-server1`: `files/dev` holds the pipelines compiled without node selectors and with smaller Spark sizes, and the platform's `xgboost-gpu-distributed` runtime selects the local GPU there.

## Training data

- One row is one scored payment attempt. The rows, labels and request fields come from `fraud_feast.training.entity_rows`, and the features from `get_historical_features` over the `payment_risk_v1` feature service ([feature store](feature-store.md)). Training code computes no feature, label or entity.
- The target is simulation truth (`is_fraud`), treated as real for this project and available at the attempt time, so the splits need no maturity gaps. A real deployment would see delayed, partial chargebacks, and its metrics would be lower.
- The dataset step runs Feast in local Spark inside one pipeline pod, in 16-day chunks, four at a time, one day of entity rows per Feast call, merged into the table in one commit. It keeps every positive and the negatives whose stable hash bucket of the attempt key is below 100 of 1000, stores the bucket on the row, and weights negatives by the inverse rate.
- Output is one Iceberg v2 merge-on-read table, `lake.ml.payment_risk_v1_examples`. Each run is one `MERGE` that stores only inserted and deleted rows, so every version is readable by its snapshot ID. The snapshot records the snapshot IDs of `label_attempt`, `silver.attempts`, `silver.payment_events`, the daily tables and the stream tables, the window, the cutoff, the sampling rate, the feature service and its ordered feature list. A build fails if any of those tables moved while it ran, or if the stream history does not cover the window.
- Class imbalance makes accuracy alone unsuitable; report precision, recall and PR-AUC.

## Windows

| Window | Range |
|---|---|
| train | 2026-07-29..2026-09-30 |
| validation | 2026-10-01..2026-10-14 |
| test | 2026-10-15..2026-10-28 |

These are the windows planned for the first prod run. A scheduled run rolls them: contiguous test, validation and train windows of 14, 14 and 56 days ending on the schedule date minus one day, or on the latest earlier day for which every day of the three windows has rows in the stream tables (`risk_model.windows.rolling`, `latest_covered`), so a gap between replayed and live history is never spanned. Passing all four of `train`, `validation`, `test` and `cutoff` overrides the rule. `WindowSplitter` rejects overlapping, gapped or misordered windows, and rows outside every window.

A Kubeflow recurring run, created by a hook Job in the chart after each sync so it follows the deployed pipeline version, starts the pipeline daily with no catch-up. The first step, `plan_windows`, stops the run when its cutoff is not later than the cutoff of the highest registered version, so a day without new replayed history costs one small pod. On dev the schedule is on with 36, 9 and 9 day windows, because 2026-09-02 to 2026-10-27 is replayed; on prod it is off until a prod run has been done.

## Pipeline

`payment-risk-training` is a Kubeflow pipeline deployed from Git as a `Pipeline` and `PipelineVersion`. Every step runs `python -m risk_model.steps <step>` in the `ml` image, so the [notebook](../../src/ml/notebooks/payment_risk_exploration.ipynb) calls the same functions.

| Step | Does |
|---|---|
| `plan_windows` | Rolling windows from the schedule time, and the skip when there is no new data; every step below runs only when it says so |
| `plan_examples`, `stage_examples`, `assemble_examples` | Plan the chunks and fence the source tables, stage each chunk in parallel, then merge into a new training-set version; a given `training_set_snapshot` skips all three |
| `check_examples` | Missing or mistyped columns, duplicate keys, null labels, per-day fraud rate bounds; writes the null share per feature and day |
| `split_windows` | Window checks, counts and sample fraud rate per split |
| `tune_params` | Optional (`tune_trials` above 0). A Katib experiment: `tpe`, 8 trials, 2 in parallel, each a single-GPU TrainJob printing validation PR-AUC on a 5% negative sample, reading the pinned snapshot read-only |
| `fit_on_gpu` | A Trainer `TrainJob` on the chart's `payment-risk-xgboost` runtime with one node, which holds the shared GPU claim. It trains XGBoost on `device=cuda` over rows sorted by key and early-stops on validation `aucpr`, then the depth-1 additive baseline the gate compares against; it never falls back to CPU |
| `gate` | A GPU `TrainJob` scores the new model, the additive baseline and the current production version; thresholds from validation, metrics on test; KFP metrics and a PR-curve artifact. The step only launches the job and reads its report |
| `record_version` | Registers a passing model |
| `roll_out` | Serves the registered version, see [serving](serving.md) |

- **Decision thresholds** come from validation: decline where 0.1% of legitimate payments are flagged, step-up at 1%.
- **Gate:** the model must beat the baseline's test PR-AUC. With a production version it must also gain 0.005 PR-AUC without a lower recall at 1% FPR, a margin above the spread between retrains on the same data.
- **Single GPU.** The trainer uses one L4 and no collective, so NCCL is not involved. Distributed training and a model-inside-image deployment are not used.
- **Registry.** A passing model is a new version of `payment-risk` in the Kubeflow Model Registry. The artifact URI is the model directory; the properties hold hyperparameters, test metrics, thresholds, ordered feature list, feature service, training-set snapshot and the source snapshot IDs, windows, cutoff, GPU and library versions, and the pipeline run. `production` and `archived` are properties; a replaced version is also set to the native `ARCHIVED` state.
- **Run files** are written to `s3://<bucket>/runs/payment-gateway/ml/<run-id>/`.
- All scoring runs on the GPU. Notebooks cannot take GPU claims, so the notebook submits the same kind of TrainJob (`steps.fit_on_gpu`, `steps.gate`, `steps.experiments`) and reads the results; it avoids GPU nodes so it cannot starve them. The baseline is a depth-1 XGBoost model, an additive model stronger than the earlier logistic regression, so the gate rejects more candidates.

## Evidence on dev

Everything below ran on dev (RTX 5060 Ti) against training-set snapshot `7781420152380000469`, which holds the replayed history 2026-09-02 to 2026-10-27 at a 10% negative sample. The Kubeflow screenshots come from `kubeflow-dev.phuchoang.sbs`. Prod runs wait for its stream history ([tasks](../../openspec/changes/train-fraud-model/tasks.md), 7.7).

| Rubric item | Evidence |
|---|---|
| Notebook: offline store and labels, split, train, evaluate, save | [Notebook](#notebook) |
| Kubeflow pipeline with load, split, train and evaluate | [Pipeline runs](#pipeline-runs) |
| Model and incremental data versions | [Versions](#versions) |
| KServe | [Serving](serving.md#model-service) |
| Five key classes, property-based tests | [Code](#code) |

### Notebook

[`payment_risk_exploration.ipynb`](../../src/ml/notebooks/payment_risk_exploration.ipynb) is committed without outputs. It was executed top to bottom with `jupyter nbconvert --execute` in a one-off Job, because the Notebook server avoids GPU nodes while its volume sits on one, and the figures below are that execution's outputs, saved as PNG. Its fits and scores run in GPU TrainJobs (`steps.fit_on_gpu`, `steps.gate`, `steps.experiments`) while Triton holds the same GPU. Environment overrides: `NOTEBOOK_END_DAY`, `NOTEBOOK_TRAIN_DAYS`, `NOTEBOOK_VALIDATION_DAYS`, `NOTEBOOK_TEST_DAYS`, `NOTEBOOK_COMPARE_RATE`, `NOTEBOOK_ABLATION_RATE`, `NOTEBOOK_FOLDS` and `NOTEBOOK_SNAPSHOT`; an unset snapshot builds a new dataset version.

**Split.** Training 2026-09-04..2026-10-09, validation 2026-10-10..2026-10-18, test 2026-10-19..2026-10-27 (36, 9 and 9 days from `windows.rolling`, cutoff 2026-10-28).

| Split | Rows | Positives | Weighted fraud rate |
|---|---:|---:|---:|
| train | 8,863 | 2,676 | 4.1% |
| validation | 1,946 | 389 | 2.4% |
| test | 1,933 | 438 | 2.8% |

![Weighted fraud rate and attempts per day over the replay, and the features with the most nulls](images/notebook-data.png)

Fraud arrives in bursts, with 5 to 159 positives per day. Stream features are null for 51% of legitimate and 9% of fraudulent attempts, so a missing stream snapshot is itself a signal. The features that separate fraud from legitimate attempts most are the customer's 5-minute to 24-hour attempt counts and distinct devices:

![Standardized mean gap between fraud and legitimate attempts, top 12 features](images/notebook-feature-gap.png)

**Which features earn their place.** Each candidate is scored walk-forward over the last 14 days (training on the earlier days, three seeds, 378 fits in one job). The table gives the change in mean PR-AUC against all features when the feature or view is removed, the days (of 14) the set was worse and better, and the worst day:

| Removed | Change in PR-AUC | Days worse / better | Worst day |
|---|---:|---:|---:|
| view `request_features` | -0.2139 | 14 / 0 | -0.6618 |
| view `customer_stream` | -0.0405 | 7 / 7 | -0.2288 |
| `customer_amount_to_30d_avg` | -0.0181 | 10 / 4 | -0.1188 |
| view `payment_method_stream` | -0.0099 | 10 / 4 | -0.0432 |
| `scored_hour_utc` | +0.0046 | 5 / 7 | -0.0053 |
| view `payment_method_daily` | +0.0158 | 5 / 9 | -0.0031 |
| `payment_method_amount_to_30d_avg` | +0.0159 | 5 / 9 | -0.0634 |
| view `customer_daily` | +0.0190 | 6 / 8 | -0.0258 |

Only the first four hurt consistently when removed. The four that help on average win on 7 to 9 of 14 days, which is not a decisive split, so the evidence supports neither dropping nor adding them, and the feature service is unchanged. `request_features__item_count` is 1 on all rows because the simulator builds every order from one purchase.

**Five models, simple to strong.** All train and score on the GPU, from the depth-1 additive baseline the gate uses to the pipeline's own settings. On the test window, with thresholds from validation:

| Model | PR-AUC | Recall at 1% FPR | Recall in top 5% |
|---|---:|---:|---:|
| Pipeline model (depth 6, every stored negative) | 0.9370 | 0.9110 | 0.9498 |
| Boosting, depth 5 | 0.9294 | 0.9110 | 0.9384 |
| Boosting, depth 3 | 0.9231 | 0.8995 | 0.9315 |
| Additive baseline, depth 1 | 0.9156 | 0.8881 | 0.9155 |
| Random forest | 0.9073 | 0.8676 | 0.9224 |

![Test PR-AUC and recall at 1% FPR per model, and the precision-recall curves](images/notebook-models.png)

**Thresholds and test.** The decline threshold is 0.512 (0.1% of legitimate attempts declined on validation) and step-up is 0.078 (1%). Applied unchanged to the test window, the pipeline model declines 379 of 438 weighted frauds and 50 of 14,950 legitimate attempts, and reviewing the top 5% of scores catches 95% of fraud, 19.0 times a random review.

![Validation precision and recall by score with the two thresholds](images/notebook-thresholds.png)

![Test confusion matrix at the decline threshold](images/notebook-confusion.png)

![Top 15 features by gain, and calibration by score decile](images/notebook-importance.png)

The pipeline's fit took 4.0 s and stopped at round 45. The notebook's last cell runs `steps.gate` against production, which by then was this same model, so it reports "did not pass": the gate requires a 0.005 PR-AUC margin over production. Saving writes `model.json` and `baseline.json` to the run directory.

### Pipeline runs

Two `payment-risk-training` runs and one `payment-risk-rollout` run went through Kubeflow Pipelines. The first run searched hyperparameters with Katib, the second reused the snapshot with the default settings.

| Run | Pipeline | Steps | Wall time | Result |
|---|---|---:|---:|---|
| `caef6f06` | training, `tune_trials=4` | 26 of 26 | 19 min 16 s | registered `v2`, rolled out |
| `f3060333` | rollout, `version=v1` | 3 of 3 | 1 min 47 s | Triton served `v1` again |
| `6cb31a0e` | training, `tune_trials=0`, `force=true` | 26 of 26 | 20 min 30 s | registered `v3`, rolled out, now live |

![Run caef6f06: plan_examples, the staging loop, assemble, check, split, tune_params, fit_on_gpu, gate, publish_metrics and the record_version and roll_out branch, all green](images/kfp-training-run.png)

Katib ran four trials, two at a time, in 1 min 52 s. Each trial is a single-GPU TrainJob that prints validation PR-AUC on the 5% negative sample, and `rfvwf8jq` was selected:

![Katib experiment payment-risk-caef6f06: four succeeded trials with their parameters and PR-AUC](images/katib-trials.png)

The fit TrainJob took 24 s with the tuned settings and 44 s with the defaults, the gate TrainJob 21 s and 28 s, all while Triton held the same GPU.

### Versions

| Version | Settings | Test PR-AUC | ROC-AUC | Recall at 1% FPR | Precision at decline | Decline / step-up threshold |
|---|---|---:|---:|---:|---:|---|
| `v2` | tuned | 0.9235 | 0.9836 | 0.8904 | 0.9494 | 0.5607 / 0.1169 |
| `v3` | defaults | 0.9370 | 0.9895 | 0.9110 | 0.8834 | 0.5122 / 0.0783 |

Four trials did not beat the defaults, so `v3` was gated against `v2` and replaced it. `v2` is `ARCHIVED`; the first version was archived too and its model files deleted, because the registry has no delete call. The registry UI hides archived versions.

![Model Registry, payment-risk v3: properties including cutoff, pipeline run, feature service, params, sources and thresholds](images/registry-version.png)

Every version records its data version: the training-set snapshot (`7781420152380000469` for `v2` and `v3`), the snapshot IDs of the daily, stream, label, attempt and payment-event tables, the windows, the cutoff, the feature service and the ordered feature list. The training set is one Iceberg table with one `MERGE` per run, so each version reads back by snapshot ID. The second run reused the snapshot: a run without one waited 16 minutes for the 8 GB staging pod, which the two small dev workers could not place (dev-server3 had about 6.8 GiB of memory requests free), and was terminated. An incremental rebuild is task 7.8.

### Code

The five key classes, each with its own tests in [`src/ml/tests`](../../src/ml/tests):

| Class | Module | Role |
|---|---|---|
| `WindowSplitter` | `split.py` | Holds the train, validation and test windows and selects rows by time; rejects overlap, gaps and rows outside every window |
| `DatasetReader` | `data.py` | Reads one snapshot of the training-set table: sample rate, provenance, validation and weighted numpy `Split`s |
| `RiskTrainer` | `train.py` | GPU XGBoost fit with early stopping, the depth-1 baseline, scoring |
| `GateEvaluator` | `evaluate.py` | Thresholds from validation, test metrics, the pass or fail verdict |
| `RegistryClient` | `registry.py` | Versions, properties, registration, the passed-gate check and promotion in the Model Registry |

Property-based tests (Hypothesis): rolling windows are contiguous, sized and end on the end day, and shift by the same days when the end day moves (`test_windows.py`); the same rows in any order train the same model (`test_train.py`).

## Drift

Planned in the change's design (sections 10 and 12), with no running proof yet. Go Fraud counts each scored feature into the bins of the served version's reference profile, which `record_version` computes from the training split and stores on the registry version and in the served model's config. A read-only drift service compares the last 6 h and 24 h with that profile (PSI per feature, a minimum sample count, no labels) and exports the results for the Grafana ML dashboard. An hourly job starts at most one deduplicated `payment-risk-training` run through the Kubeflow API when both windows breach, within a cooldown and a weekly bound, and the run still passes the normal gate. The recurring run is removed: drift, or a model older than 14 days, is the only automatic trigger. The live simulator gets a configurable drift schedule to produce the drift. Pending verification: every step above, on dev, after the user approves the experiment.
