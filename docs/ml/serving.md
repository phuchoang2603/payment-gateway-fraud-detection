# Serving

Go Fraud owns the decision. It validates the attempt, applies the versioned rules policy, and will retrieve features from the Feast feature server once the feature-store change lands. A later model adapter calls KServe directly and applies policy to the score. There is no custom Python coordinator between Go, Feast and KServe. Code: [`src/payment-gateway/fraud`](../../src/payment-gateway/fraud).

## Today

- Rules decide; shadow feature retrieval is disabled in every environment until the feature-store change, and authorization counters are observe-only.
- Payments records the decision before any processor effect; fraud errors fail closed.
- Bounded live verification observed cold-start timeouts and stale warm snapshots on feature reads, so no ML latency guarantee is claimed yet.

## Planned

| Capability | Plan |
|---|---|
| Feature-retrieval API | Typed, validated lookups by customer and method ID with health checks, deployed by Helm with rolling update and automatic rollback |
| Inference | KServe predictor loading the approved model version from the registry |
| Autoscaling | Request-driven autoscaling for the retrieval API and inference |
| A/B testing | Two inference versions with a traffic split, deployed through CI/CD, with version-specific dashboards |
| Load test | Locust run against the retrieval API, recording throughput, latency percentiles and errors as an HTML report |

Every decision records the policy, model and feature versions it used. Shadow inference never triggers payment effects.
