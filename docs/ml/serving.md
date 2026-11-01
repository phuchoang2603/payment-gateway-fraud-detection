# Serving

Go Fraud owns the decision. It validates the attempt, applies the versioned rules policy, and will retrieve features from the Feast feature server once the feature-store change lands. A later model adapter calls KServe directly and applies policy to the score. There is no custom Python coordinator between Go, Feast and KServe. Code: [`src/payment-gateway/fraud`](../../src/payment-gateway/fraud).

## Today

- Rules decide; Feast feature retrieval runs in shadow mode where the online store is enabled.
- Payments records the decision before any processor effect; fraud errors fail closed.
- No feature-read latency or freshness evidence exists for the v2 features yet, so no ML latency guarantee is claimed.

## Model service

The `payment-risk` InferenceService runs a custom KServe predictor ([`src/ml/predictor`](../../src/ml/predictor)). The chart owns the `ServingRuntime` that names the predictor image; the `roll_out` step creates and patches the service, and Argo CD does not own it. It is Serverless, `minScale: 1`, on spot CPU, and cluster-local.

- **Model loading.** `roll_out` sets the storage URI to `model-registry://payment-risk/<version>`. KServe resolves the registry version to the model directory in object storage and mounts it for the predictor. The pipeline writes each run's directory once.
- **Request:** `POST /v1/models/payment-risk:predict` with `{"instances": [{"feature": value, ...}]}`. Features are named, so callers never depend on column order. A missing or null feature is NaN, which XGBoost treats as missing, and an unknown key is ignored.
- **Response:** `{"predictions": [...], "model_version": "v7"}`.
- **Feature list:** the trained booster stores the ordered `payment_risk_v1` feature list as its `feature_order` attribute, and the registry version records it. Callers reach the service through `knative-local-gateway` with the predictor host as `Host`.
- **Why custom.** The stock `kserve-xgbserver` builds an unnamed matrix, which a named booster rejects, and it cannot take named or null features.
- **Rubric gap.** The model is not baked into the image. A model image needs a registry write credential or a platform-run registry; the version stays traceable through the Model Registry instead.

## Deploy and rollback

`roll_out` is the last step of `payment-risk-training` for a passing model. It patches the storage URI, waits for the new revision, sends an empty instance and fails unless the answer is a score from the new version, then marks the version `production` and the previous one `archived`. If the check fails, it patches the previous version back. The `payment-risk-rollout` pipeline reruns the step for any earlier version that passed its gate, for rollback.

## Planned

| Capability | Plan |
|---|---|
| Feature-retrieval API | Typed, validated lookups by customer and method ID with health checks, deployed by Helm with rolling update and automatic rollback |
| Autoscaling | Request-driven autoscaling for the retrieval API and inference |
| A/B testing | Two inference versions with a traffic split, deployed through CI/CD, with version-specific dashboards |
| Load test | Locust run against the retrieval API, recording throughput, latency percentiles and errors as an HTML report |

Every decision records the policy, model and feature versions it used. Shadow inference never triggers payment effects.
