# Training

**Status: implemented, not yet run.** The code and the prod chart are under review. The first runs wait for the Feast change to be deployed and `stream-history` to cover `[2026-07-29, 2026-10-29)`. Code: [`src/ml`](../../src/ml); chart: [`infra/charts/ml-training`](../../infra/charts/ml-training). Dev runs the same chart on the RTX 5060 Ti of `dev-server1`: `files/dev` holds the pipelines compiled without node selectors and with smaller Spark sizes, and the platform's `xgboost-gpu-distributed` runtime selects the local GPU there.

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

These are the windows of the first run. A scheduled run rolls them: contiguous test, validation and train windows of 14, 14 and 56 days ending on the schedule date minus one day, or on the latest earlier day for which every day of the three windows has rows in the stream tables (`risk_model.windows.rolling`, `latest_covered`), so a gap between replayed and live history is never spanned. Passing all four of `train`, `validation`, `test` and `cutoff` overrides the rule. `WindowSplitter` rejects overlapping, gapped or misordered windows, and rows outside every window.

A Kubeflow recurring run, created by a hook Job in the chart after each sync so it follows the deployed pipeline version, starts the pipeline daily with no catch-up. The first step, `plan_windows`, stops the run when its cutoff is not later than the cutoff of the highest registered version, so a day without new replayed history costs one small pod. On dev the schedule is on with 4, 1 and 1 day windows, because only 2026-10-14 to 2026-10-21 is replayed; on prod it is off until a prod run has been done.

## Pipeline

`payment-risk-training` is a Kubeflow pipeline deployed from Git as a `Pipeline` and `PipelineVersion`. Every step runs `python -m risk_model.steps <step>` in the `ml` image, so the [notebook](../../src/ml/notebooks/payment_risk_exploration.ipynb) calls the same functions.

| Step | Does |
|---|---|
| `plan_windows` | Rolling windows from the schedule time, and the skip when there is no new data; every step below runs only when it says so |
| `plan_examples`, `stage_examples`, `assemble_examples` | Plan the chunks and fence the source tables, stage each chunk in parallel, then merge into a new training-set version; a given `training_set_snapshot` skips all three |
| `check_examples` | Missing or mistyped columns, duplicate keys, null labels, per-day fraud rate bounds; writes the null share per feature and day |
| `split_windows` | Window checks, counts and sample fraud rate per split |
| `tune_params` | Optional (`tune_trials` above 0). A Katib experiment: `tpe`, 8 trials, 2 in parallel, each a single-GPU TrainJob printing validation PR-AUC on a 5% negative sample, reading the pinned snapshot read-only |
| `fit_on_gpu` | A Trainer `TrainJob` on `xgboost-gpu-distributed` with one node. It trains XGBoost on `device=cuda` over rows sorted by key and early-stops on validation `aucpr`; it never falls back to CPU |
| `gate` | Logistic-regression baseline, current production version and new model on test; thresholds from validation; KFP metrics and a PR-curve artifact |
| `record_version` | Registers a passing model |
| `roll_out` | Serves the registered version, see [serving](serving.md) |

- **Decision thresholds** come from validation: decline where 0.1% of legitimate payments are flagged, step-up at 1%.
- **Gate:** the model must beat the baseline's test PR-AUC. With a production version it must also gain 0.005 PR-AUC without a lower recall at 1% FPR, a margin above the spread between retrains on the same data.
- **Single GPU.** The trainer uses one L4 and no collective, so NCCL is not involved. Distributed training and a model-inside-image deployment are not used.
- **Registry.** A passing model is a new version of `payment-risk` in the Kubeflow Model Registry. The artifact URI is the model directory; the properties hold hyperparameters, test metrics, thresholds, ordered feature list, feature service, training-set snapshot and the source snapshot IDs, windows, cutoff, GPU and library versions, and the pipeline run. `production` and `archived` are properties; a replaced version is also set to the native `ARCHIVED` state.
- **Run files** are written to `s3://<bucket>/runs/payment-gateway/ml/<run-id>/`.
- GPU runs are serialized with the model-server benchmarks. Notebooks cannot take GPU claims, so the notebook submits the same TrainJob.

## Derived request features

The [notebook](../../src/ml/notebooks/payment_risk_exploration.ipynb) also compares a linear baseline, a random forest, histogram gradient boosting, CPU XGBoost and the GPU model on validation and test at the gate's two operating thresholds, with calibration and feature importance. It ablates the candidate `request_features` columns on validation PR-AUC before the first registered version, because changing the inputs afterwards needs `payment_risk_v2`.

## Drift

A scheduled job compares current and reference feature distributions without assuming immediate labels, publishes the result to dashboards, and triggers deduplicated, cooldown-controlled retraining. The generator gains a configurable drift schedule for this. Planned.
