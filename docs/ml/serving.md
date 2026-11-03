# Serving

Go Fraud owns the decision. It validates the attempt, applies the versioned rules policy, retrieves features from the Feast feature server, and, where a model endpoint is configured, calls the Triton model server directly with the features and applies policy to the score. There is no Python coordinator between Go, Feast and Triton. Code: [`src/payment-gateway/fraud`](../../src/payment-gateway/fraud).

## Today

- Rules decide, and the model can add a decline. Where the online store is enabled, Go Fraud reads the features and calls the model. A score at or above `FRAUD_MODEL_THRESHOLD` adds a decline (reason `MODEL_RISK_HIGH`, policy `rules-v1+model`), but only when the predictor answered and all five feature views were available; otherwise the rules decision stands. The score never approves what the rules decline. The feature read has a 250 ms deadline and the model call its own 150 ms, both inside the 1 s assessment deadline, and a predictor failure is recorded as `timeout` or `error`.
- Dev and prod run the same configuration: the threshold is 0.512, the decline threshold (0.1% FPR on validation) the registry holds for the served version. Where the stream views have no fresh rows the rules decision stands, and the evidence records why.
- Payments records the decision before any processor effect; fraud errors fail closed.
- No feature-read latency or freshness evidence exists for the v2 features yet, so no ML latency guarantee is claimed.

## Model service

The `payment-risk` InferenceService runs NVIDIA Triton with the FIL backend on the GPU; nothing scores on a CPU. The chart owns the `ServingRuntime` (the pinned Triton image, the shared GPU claim, the GPU node selector and the `burst.talos.dev/gpu-training` toleration); the `roll_out` step creates and patches the service, and Argo CD does not own it. It is a Standard-mode Deployment with one replica and is cluster-local.

- **GPU sharing.** Triton, the fit and Katib TrainJobs, and the gate and experiment jobs all reference the chart's one `ResourceClaim` (`ml-gpu`), so they run together on the single GPU (dev RTX 5060 Ti, prod L4). The chart's `TrainingRuntime` carries the claim, because a TrainJob cannot patch `resourceClaims`. The instance group is `KIND_GPU`, never `KIND_AUTO`, which would fall back to the CPU; without a GPU the predictor stays Pending and the fraud service records `timeout` or `error`.
- **Model loading.** `roll_out` sets the storage URI to `model-registry://payment-risk/<version>`. KServe resolves the registry version to the model directory in object storage and mounts it at `/mnt/models`. `record_version` writes the repository entry there: `payment-risk/config.pbtxt` and `payment-risk/<N>/xgboost.json`, where `N` is the version's number, so every answer names the version that made it. The config carries the ordered feature list and the registry version as parameters.
- **Request:** Open Inference Protocol v2, `POST /v2/models/payment-risk/infer`, with one FP32 row in the packaged feature order sent as binary tensor data (`Inference-Header-Content-Length`). A missing feature or an unavailable view is NaN, which XGBoost treats as missing and JSON cannot carry. Go reads the order from `GET /v2/models/payment-risk/config` and caches it.
- **Response:** the fraud class's probability is the second of two FP32 values, and `model_version` is the version number. If that number differs from the version the cached order belongs to, Go discards the score and reads the order again on the next call, so a rollout never scores a row laid out for another version.
- **Check against CPU XGBoost.** On dev, 215 real rows with 9% missing values scored within 6e-8 of CPU XGBoost, and all-missing and half-missing rows matched its default directions. A single-row call took 1.2 ms median and 4.7 ms at p99 over loopback.
- **Image.** Triton `26.10-py3` pinned by digest. It runs on the dev driver through CUDA forward compatibility. The node pulls from `nvcr.io`, whose DNS answers are occasionally stale on the dev LAN; a failed pull is retried with `talosctl image pull`.
- **Rubric gap.** The model is not baked into the image. A model image needs a registry write credential or a platform-run registry; the version stays traceable through the Model Registry instead.

## Deploy and rollback

`roll_out` is the last step of `payment-risk-training` for a passing model. It patches the storage URI, waits until the Deployment has rolled out the new storage URI, sends an all-missing row and fails unless Triton answers with a probability from the new version, then marks the version `production` and the previous one `archived`. If the check fails, it patches the previous version back. The `payment-risk-rollout` pipeline reruns the step for any earlier version that passed its gate, for rollback.

On dev, `payment-risk-rollout` with `version=v1` served the earlier version again in 1 min 47 s (3 of 3 steps), and the next training run rolled `v3` out in 20 min 30 s. A binary request for `v2` with all 50 features missing, and one with only `amount_minor` and `has_device` set, both returned `model_version` 2 and a probability of 0.8937. The Kubeflow endpoints page shows the service ready on the `payment-risk-triton` runtime with the registry URI of the live version; the model's registry entry is on [training](training.md#versions).

![KServe endpoint payment-risk: ready, Triton runtime, storage URI model-registry://payment-risk/v3](images/kserve-endpoint.png)

## Planned

| Capability | Plan |
|---|---|
| Feature-retrieval API | Typed, validated lookups by customer and method ID with health checks, deployed by Helm with rolling update and automatic rollback |
| Autoscaling | Request-driven autoscaling for the retrieval API and inference |
| A/B testing | Two inference versions with a traffic split, deployed through CI/CD, with version-specific dashboards |
| Load test | Locust run against the retrieval API, recording throughput, latency percentiles and errors as an HTML report |

Every decision records the policy, model and feature versions it used. The model call never triggers payment effects; only the decision does.
