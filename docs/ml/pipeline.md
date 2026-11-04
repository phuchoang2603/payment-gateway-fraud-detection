# Pipeline

**Status: dev runs done.** Three Kubeflow pipelines, all compiled from Git by `src/ml/pipelines/compile_pipelines.py` into the chart's `files/{dev,prod}` and deployed as a `Pipeline` and `PipelineVersion`. Dev runs the same chart as prod, compiled without node selectors and with smaller Spark sizes; the platform's `xgboost-gpu-distributed` runtime selects the local GPU there. Screenshots come from `kubeflow-dev.phuchoang.sbs`.

| Pipeline | Does | Started by |
|---|---|---|
| `payment-risk-training` | Builds the training set, fits, gates, registers and rolls out a version | The [drift](drift.md) check, or a manual run |
| `payment-risk-rollout` | Serves any earlier gated version, for rollback | A manual run |
| `payment-risk-promote` | Moves a challenger from 0 to a bounded share of traffic, gated by analysis ([serving](serving.md#ab-experiment)) | A manual run |

## Training pipeline

Every step runs `python -m risk_model.steps <step>` in the `ml` image, so the [notebook](../../src/ml/notebooks/payment_risk_exploration.ipynb) calls the same functions.

| Step | Does |
|---|---|
| `plan_windows` | Rolling windows from the run's start time, and the skip when there is no new data; every step below runs only when it says so |
| `plan_examples`, `stage_examples`, `assemble_examples` | Plan the chunks and fence the source tables, stage each chunk in parallel, then merge into a new training-set version; a given `training_set_snapshot` skips all three |
| `check_examples` | Missing or mistyped columns, duplicate keys, null labels, per-day fraud rate bounds; writes the null share per feature and day |
| `split_windows` | Window checks, counts and sample fraud rate per split |
| `tune_params` | Optional (`tune_trials` above 0). A Katib experiment: `tpe`, 8 trials, 2 in parallel, each a single-GPU TrainJob printing validation PR-AUC on a 5% negative sample, reading the pinned snapshot read-only |
| `fit_on_gpu` | A Trainer `TrainJob` on the chart's `payment-risk-xgboost` runtime with one node, which holds the shared GPU claim. It trains XGBoost on `device=cuda` over rows sorted by key and early-stops on validation `aucpr`, then the depth-1 additive baseline the gate compares against; it never falls back to CPU |
| `gate` | A GPU `TrainJob` scores the new model, the additive baseline and the current production version; thresholds from validation, metrics on test; KFP metrics and a PR-curve artifact. The step only launches the job and reads its report |
| `record_version` | Registers a passing model |
| `roll_out` | Serves the registered version, see [serving](serving.md) |

- **Decision thresholds** come from validation: decline where 0.1% of legitimate payments are flagged, step-up at 1%.
- **Gate:** the model must beat the baseline's test PR-AUC. With a production version it must also gain 0.005 PR-AUC without a lower recall at 1% FPR, a margin above the spread between retrains on the same data. A manual run can lower it with `gate_margin`, which dev uses to register a retrain that only matches production; the drift check never sets it.
- **Single GPU.** The trainer uses one L4 and no collective, so NCCL is not involved. Distributed training and a model-inside-image deployment are not used.
- **Registry.** A passing model is a new version of `payment-risk` in the Kubeflow Model Registry. The artifact URI is the model directory; the properties hold hyperparameters, test metrics, thresholds, ordered feature list, feature service, training-set snapshot and the source snapshot IDs, windows, cutoff, GPU and library versions, and the pipeline run. The stage (`production`, `challenger` or `archived`) is a property; a replaced version is also set to the native `ARCHIVED` state.
- **Run files** are written to `s3://<bucket>/runs/payment-gateway/ml/<run-id>/`.
- All scoring runs on the GPU. Notebooks cannot take GPU claims, so the notebook submits the same kind of TrainJob (`steps.fit_on_gpu`, `steps.gate`, `steps.experiments`) and reads the results; it avoids GPU nodes so it cannot starve them. The baseline is a depth-1 XGBoost model, an additive model stronger than the earlier logistic regression, so the gate rejects more candidates.

No Kubeflow recurring run starts training: the hourly drift check starts a run when the served features drift, or when the serving version was registered more than 14 days ago ([drift](drift.md)). The first step, `plan_windows`, stops the run when its cutoff is not later than the cutoff of the highest registered version, so a check that fires without new replayed history costs one small pod. On dev the windows are 36, 9 and 9 days, because 2026-09-02 to 2026-10-27 is replayed.


## Runs on dev

Two `payment-risk-training` runs and one `payment-risk-rollout` run went through Kubeflow Pipelines for `v2` and `v3`. The runs for `v4`, the drift trigger and the promotion are in the list further down. The first run searched hyperparameters with Katib, the second reused the snapshot with the default settings.

| Run | Pipeline | Steps | Wall time | Result |
|---|---|---:|---:|---|
| `caef6f06` | training, `tune_trials=4` | 26 of 26 | 19 min 16 s | registered `v2`, rolled out |
| `f3060333` | rollout, `version=v1` | 3 of 3 | 1 min 47 s | Triton served `v1` again |
| `6cb31a0e` | training, `tune_trials=0`, `force=true` | 26 of 26 | 20 min 30 s | registered `v3`, rolled out, now live |

![Run caef6f06: plan_examples, the staging loop, assemble, check, split, tune_params, fit_on_gpu, gate, publish_metrics and the record_version and roll_out branch, all green](images/kfp-training-run.png)

Katib ran four trials, two at a time, in 1 min 52 s. Each trial is a single-GPU TrainJob that prints validation PR-AUC on the 5% negative sample, and `rfvwf8jq` was selected:

![Katib experiment payment-risk-caef6f06: four succeeded trials with their parameters and PR-AUC](images/katib-trials.png)

The fit TrainJob took 24 s with the tuned settings and 44 s with the defaults, the gate TrainJob 21 s and 28 s, all while Triton held the same GPU.

The runs that followed, on the same dev cluster, are in the Kubeflow list below (browser time, UTC-5): `dev-challenger-v4-margin` trained and registered `v4` with a lowered `gate_margin`, three `dev-challenger-v4-*` rollouts served it as a challenger, four `dev-promote-v4*` runs moved traffic to it (three aborted by their analysis, the last promoted it, 31 min 53 s), `drift-v4-2026110404` was started by the drift check, and `drift-v4-gate-proof-b` ran the full pipeline with the drift trigger to show the gate rejecting a retrain equal to production (17 min 26 s, nothing registered). The aborts and the promotion are explained on [serving](serving.md#ab-experiment), the drift run on [drift](drift.md#proof-on-dev).

![Kubeflow runs list: the failed and aborted promote runs, the successful dev-promote-v4-d, the challenger rollouts and the drift-triggered run](images/kfp-runs.png)
