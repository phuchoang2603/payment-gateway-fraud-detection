## 0. Start gate

- [x] 0.1 Platform Kubeflow request sent and handed off at talos-proxmox `e01aba9`: Model Registry, KServe from the registry, Katib, Trainer, GPU runtime `xgboost-gpu-distributed`. Recorded in design.md.
- [x] 0.2 Consult the Feast threads and the platform. Recorded in design.md:
  - Feast plan (approved 2026-11-01): request features from the on-demand view `request_features`; `fraud_feast.training.entity_rows`; stream visibility at write time + 1µs, so the entity timestamp is plain `started_at`; local-mode Spark for training and materialization; replay `[2026-07-29, 2026-10-29)`; simulation-truth labels; no gaps.
  - Platform: the Dashboard lists `ml` (`8321dec`); the Spark operator watching `ml` (`ae12236`) is no longer needed.
- [x] 0.3 The user approved this plan. Verify with `openspec validate train-fraud-model --strict`.
- [x] 0.4 Check current APIs with Context7 or released sources, and record any deviation in design.md:
  - the Trainer 2.3 `TrainJob` overrides with `numNodes: 1`;
  - Feast 0.67 Spark offline store in local mode, and the `fraud_feast.training` API as merged in PR #111;
  - the Katib 0.19 `Push` collector with a DRA-claim trial Job;
  - the `model-registry` Python client;
  - KFP Kubernetes-native `Pipeline`/`PipelineVersion`;
  - KServe 0.21 custom predictor containers in `Standard` mode, the in-cluster Service address, and that NaN rows are treated as missing;
  - Iceberg `MERGE ... NOT MATCHED BY SOURCE`;
  - PyIceberg position deletes;
  - a CUDA 12, Java, Spark 4.1.3 and Iceberg 1.12 jars, `fraud_feast[spark]`, XGBoost and PyIceberg set that installs together;
  - whether Feast's on-demand views can read a joined view's row timestamp (needed for the days-since-latest-daily-row candidate), and the request field carrying the scoring time.

## 1. `risk_model`

- [x] 1.1 Create `src/ml`, a uv project on Python 3.12 with pinned dependencies. Write `dataset.py` (design decision 2), `data.py`, `split.py`, `train.py`, `worker.py`, `evaluate.py`, `registry.py` (decision 3), `steps.py`, `kube.py` and `cluster.py` (decision 6). Verify with `uv lock`, `ruff` and `ruff format --check`.
- [x] 1.1a Write the fenced snapshot IDs and the feature service to the KFP run and the registry version. Verify with `ruff`.
- [x] 1.1b Send the derived request-feature candidates (design decision 6a) to the Feast implementation thread through the user, for the `request_features` view and Go Fraud's request. Its answer on the row-timestamp candidate is pending; the candidates and the dropped one are in design.md.
- [x] 1.2 Add the tests of design decision 9, using the Feast change's test repository for `dataset`, and wire `src/ml` into the existing Python test job. Verify in CI.

## 2. Images

- [x] 2.1 Add `infra/docker/ml.Dockerfile` (CUDA 12 runtime, Java, Spark with the Iceberg and AWS jars, `fraud_feast[spark]`, the pinned stack, non-root, JupyterLab, the notebook). Update `service-images.json` (`ml` rebuild paths include `src/data-pipeline/feast`) and the app-of-apps tags. Verify that the CI image builds pass.
- [x] 2.2 Serve with Triton instead of a custom predictor: the chart's `ServingRuntime` runs the pinned Triton image, and `src/ml/predictor`, its Dockerfile and its `service-images.json` entry are removed. Verify in CI.

## 3. Pipelines and notebook

- [x] 3.1 Add `src/ml/pipelines/payment-risk-training.py` and `payment-risk-rollout.py` (design decision 4), plus a `scripts/` compile helper into `infra/charts/ml-training/files/`. Verify that compilation succeeds locally.
- [x] 3.2 Add `src/ml/notebooks/payment_risk_exploration.ipynb` (decision 7), committed without outputs. Verify that it imports only `risk_model` and the standard libraries.

## 4. Chart `ml-training`

- [x] 4.1 Create `infra/charts/ml-training` (design decision 8) and its app-of-apps entry for dev and prod. Verify with `helm lint` and `helm template` for both.
- [x] 4.2 Remove stale text claiming an app-owned Profile or registry (`docs/deployment/infra.md`, `docs/payment-gateway/architecture.md`). Verify with `git grep -n -i 'fraud-ml-prod'`.

## 5. Docs

- [x] 5.1 Update `docs/ml/training.md` (data through Feast, labels and windows, pipeline, single-GPU training, Katib, registry), `docs/ml/serving.md` (Triton, binary tensor requests, deploy and rollback), and the roadmap tables in `docs/Home.md` (drift and retraining, and Go Fraud scoring, as planned). Verify that the links resolve.

## 5a. Rolling retraining and the notebook

- [x] 5a.1 Add `risk_model/windows.py` (`rolling`, the newest replayed stream day, the skip guard), `RegistryClient.latest_cutoff`, and the `plan_windows` step with a `windows.json` run file.
- [x] 5a.2 Make `plan_windows` the pipeline's first step, run the rest under `dsl.If(run)`, take the schedule and creation times as input parameters, and recompile `files/dev` and `files/prod`.
- [x] 5a.3 Add `risk_model/schedule.py` and the `ensure_schedule` step, and the chart's PostSync hook Job and `schedule` values (on in dev with 36, 9 and 9 days, off in prod).
- [x] 5a.4 Add `hypothesis` properties for the windows, and unit tests for the guard and the recurring-run request.
- [x] 5a.5 Rewrite the notebook around a five-model comparison, operating thresholds, test evaluation, calibration, importance and the pipeline's gate; add `matplotlib` to the `notebook` extra.
- [x] 5a.6 Update `docs/ml/training.md` for the schedule and the notebook.

## 5b. Fraud model scoring

- [x] 5b.1 Add the `fraud-decision` delta spec: model scoring, the missing-feature policy, the evidence and the response fields.
- [x] 5b.2 Extend `contracts/fraud/v1/evidence.proto` with the model evidence and regenerate the Go bindings.
- [x] 5b.3 Add the pure decision policy in `internal/domain`, with `rapid` properties.
- [x] 5b.4 Add the KServe adapter in `internal/adapters/kserve`, with `httptest` cases for every failure.
- [x] 5b.5 Wire the model call, its deadline and the evidence into `internal/application`, `internal/transport/grpc` and `cmd/fraud`, keeping the evidence hash of old assessments unchanged.
- [x] 5b.6 Add the chart values and environment for dev and prod, and the network allowance to the predictor.
- [x] 5b.7 Update the fraud decision, serving and observability docs.

## 5c. Triton and GPU scoring

- [x] 5c.1 Spike on dev: Triton 26.10 with the FIL backend loads the model on the RTX 5060 Ti, matches CPU XGBoost within 6e-8 including NaN rows, and shares the GPU with a training pod through one claim.
- [x] 5c.2 Add `risk_model/triton.py` (repository packaging, v2 client, probe), `risk_model/objects.py`, and package the repository in `record_version`; `roll_out` probes through Triton.
- [x] 5c.3 Move gate and notebook scoring to GPU jobs: `worker gate` and `worker experiments`, a depth-1 XGBoost baseline, scoring on `device=cuda`.
- [x] 5c.4 Add the chart's shared `ResourceClaim`, `TrainingRuntime` and Triton `ServingRuntime` for dev and prod.
- [x] 5c.5 Rewrite the Go adapter for the v2 protocol with binary tensors, a cached feature order and version-checked scores.
- [x] 5c.6 On dev: roll `v1` out on Triton, check the fraud service's evidence, and run the gate, the model comparison and a 14-fold feature ablation through GPU jobs on the 54-day replay; the numbers are in `docs/ml/training.md`.

## 5d. Autoscaling and dashboard

- [x] 5d.1 Enable Triton's metrics on port 8002 with named container ports, and scrape them with a `VMPodScrape`.
- [x] 5d.2 Set `autoScaling` and `maxReplicas` in the chart's predictor values (KEDA, Triton queue time), and have `kube.inference_service` set the KEDA autoscaler class on a new and an existing service.
- [x] 5d.3 Add the ML serving dashboard to the platform's Grafana, and allow the `observability` namespace in the Argo CD project because the sidecar only watches fixed namespaces.
- [x] 5d.4 On dev: scale a second predictor replica under load on the shared GPU, and load the dashboard's panels.

## 5e. Drift and drift-triggered retraining (decided 2026-11-03: Go-side bins, a new FastAPI image, drift is the only trigger, live simulator overlay)

- [x] 5e.1 Add `risk_model/drift.py`: weighted-quantile `profile`, smoothed `psi`, the missing-share shift, the breach rule and the trigger policy as pure functions, with property-based tests (PSI of a sample against itself is 0, bin proportions sum to 1, the policy is monotone in the thresholds).
- [x] 5e.2 Compute the reference profile in `record_version`, store it as the registry property `reference` and as a `config.pbtxt` parameter, and record `trigger` and the drift summary on a version a drift run registers.
- [x] 5e.3 Count scored features into bins in Go Fraud (`fraud.model.feature_bin` by feature, bin and model version), reading the profile from the served model's config with the existing version check. The per-request cost is not measured yet (task 7.9).
- [x] 5e.4 Add the drift service (`GET /v1/drift`, `/v1/reference`, probes, `/metrics`) with a Helm Deployment, a KEDA ScaledObject and a `VMPodScrape`, and its image entry in `service-images.json` if decision 2 picks a new image.
- [x] 5e.5 Add the hourly `payment-risk-drift-check` CronJob: the policy, the KFP run creation through `risk_model.schedule.Pipelines`, deduplication, cooldown, the weekly bound and the age ceiling.
- [x] 5e.6 Remove the recurring run: the `ensure_schedule` step and `Pipelines.ensure_schedule`, the PostSync hook Job, the `schedule` values and their tests; keep the `Pipelines` REST client (now `risk_model.runs`) and `plan_windows`; add the 14-day age ceiling to the check's policy. Update `docs/ml/training.md`.
- [x] 5e.7 Add the live simulator `drift` steps (validated, deterministic, world digest unchanged) with tests, and an example step in the chart values. A step overrides `foreign_ip_probability`, `familiar_context_probability` or `amount_scale`.
- [x] 5e.8 Add the drift panels to the `ml-serving` dashboard.
- [x] 5e.9 Update `docs/ml/drift.md` and the roadmap rows; proof and pending verification stay in separate sentences.

## 5f. Model A/B experiment (decided 2026-11-03: enforced, no shadow, KEDA and Standard mode stay; a waypoint HTTPRoute splits, Argo Rollouts analysis gates; the InferenceGraph and Knative were tried and dropped)

- [x] 5f.0 Platform: a `MutatingAdmissionPolicy` for the waypoint pod's seccomp profile in `ml` (talos-proxmox #116 and #117, merged; running on dev, applied to prod only after its Manual Provision run).
- [x] 5f.1 Rework the Go client to keep a layout per model version (feature order, bin edges and `decline_threshold` from that version's config), fetch an unseen version's config with a few retries, score each answer under its own version, and add `model_version` and `declined` to `fraud.model.calls`; `record_version` packages `decline_threshold` into `config.pbtxt`.
- [x] 5f.2 Add `role=challenger` to `roll_out` (second InferenceService, `challenger` registry stage, deletion) and the `experiment` run parameter.
- [x] 5f.3 Chart: the waypoint `Gateway`, the `HTTPRoute` at weights 100/0 with Argo CD ignoring the weights, the `istio.io/use-waypoint` label on the champion's Service (KServe does not copy labels from the InferenceService but keeps ones set on the Service, so `roll_out` sets it when the chart has a waypoint), a scrape of both services, the `AnalysisTemplate`, and the Role for `analysisruns`.
- [x] 5f.4 Add the `payment-risk-promote` pipeline: the stepwise weights capped at 20 percent with an `AnalysisRun` per step, promotion or abort.
- [x] 5f.5 Add the per-version panels to `ml-serving`. A by-version breakdown in the ClickHouse `fraud-outcomes` dashboard is not built.
- [x] 5f.6 Update `docs/ml/serving.md` (A/B) and the roadmap rows.

## 6. Local checks and hand-off

- [x] 6.1 Run local non-test checks only: `ruff`, treefmt, `helm lint` and `helm template`, KFP compilation, `openspec validate train-fraud-model --strict`, and `git diff --check`. Commit locally on `feat/train-fraud-model`.
- [x] 6.2 Push and open the PR only with the user's agreement. Push with `git push origin HEAD:refs/heads/<branch>`, and confirm with `git ls-remote origin <branch>`. Report every CI check honestly.

## 7. Dev runs and evidence (run directly on dev, with the user's approval)

- [x] 7.1 Sync and verify read-only on dev: `ml-training` is healthy, the secrets are Ready, the PipelineVersions are listed, and the Notebook is stopped.
- [x] 7.2 Explore a training-set build in the notebook, run as a Job on the GPU. Record counts, the fraud rate, null shares and provenance. Score every feature set walk-forward and record the PR-AUC change per candidate.
- [x] 7.3 Run `payment-risk-training` with `tune_trials` above 0. Record the KFP graph, the Katib trials, the single-GPU TrainJob (GPU, versions, wall time, exit 0), the test metrics against the baseline and production, the registry version (`production`, with the snapshot IDs of the daily, stream and label tables, and the feature service), and the InferenceService.
- [x] 7.4 Call the model with a binary request directly, including missing features, run a `payment-risk-rollout` rollback, then run a second training run.
- [x] 7.5 Retire the first model: archive `v1` in the registry, which has no delete call, and remove its model files. Set the fraud service's model threshold from the new model's validated decline threshold.
- [x] 7.6 Write the evidence into `docs/ml/training.md` and `docs/ml/serving.md` as numbers and tables. Results that miss expectations are recorded as observed.
- [ ] 7.8 Run `payment-risk-training` without a snapshot on a cluster with room for the 8 GB staging pod, and record the incremental training-set snapshot summary.
- [x] 7.9 Approved by the user on 2026-11-03, on dev only: run the live simulator with a drift step, show the drift state and the dashboard, and show one triggered run, the repeated check starting none, and the gate outcome. Done on 2026-11-04 (see `docs/ml/drift.md`): the counters and dashboard, the shift on `amount_minor`, one triggered run and `active` on the second check, the gate rejecting a retrain that equals production, and the counters' cost (25 µs per request). Not shown: a drift-started run on newer replayed days that passes the gate, and a clean `ok` baseline on dev.
- [x] 7.10 Approved by the user on 2026-11-03, on dev only: deploy a challenger at a small percentage, show both versions side by side, an abort by guard, and a promotion. Done on 2026-11-04 (see `docs/ml/serving.md`); the stable cohort was dropped because the split is random per request.

## 8. Archive

- [ ] 8.1 After the evidence is recorded, archive the change. Verify with `openspec validate --strict --all`.
