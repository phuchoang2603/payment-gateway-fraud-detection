## Why

The deployment diagram includes progressive delivery and operational visibility that the application manifests do not yet provide. Existing Istio security, GitOps releases and Go telemetry need canary release gates and usable Grafana dashboards, alerts and missing workload metric collection.

## What Changes

- Add Argo Rollouts canaries for the public Edge checkout route through the existing Istio Gateway API HTTPRoute, with candidate-specific analysis, promotion and automatic traffic rollback.
- **BREAKING**: replace the Edge Deployment with a Rollout through a staged migration; retain the internal `edge` Service and existing checkout hostname, path and security boundaries.
- Correct environment/revision telemetry identity; add application-owned operational dashboards and alerts using the shared VictoriaMetrics, VictoriaLogs, VictoriaTraces and Grafana services.
- Complete missing Kafka and Flink metric collection; expose enabled workload health and retain existing RAG/LLM scrapes and ClickHouse analytics dashboards.
- Record platform prerequisites: Rollouts controller/CRDs and Gateway API plugin, Grafana dashboard discovery in `payment-gateway`, and shared scrape/rule/query access. Do not install shared infrastructure from this repository.

## Capabilities

### New Capabilities

- `progressive-delivery`: guarded Edge checkout canaries with traffic rollback and explicit GitOps ownership.
- `application-observability`: environment-aware operational dashboards, workload signal collection and actionable alerts.

### Modified Capabilities

- `go-service-observability`: use the current Victoria/Grafana platform for correlated telemetry and permit application dashboards.
- `talos-gitops-deployment`: permit application-owned dashboard, scrape and alert definitions while preserving shared infrastructure ownership.

## Impact

Application work affects `infra/charts/payment-gateway`, `infra/charts/kafka`, `infra/charts/data-pipelines`, environment Argo CD values and existing deployment documentation. Go observation helpers change only if current export labels cannot support revision-specific analysis. Public API and event contracts remain unchanged.

Platform prerequisites belong to `talos-proxmox`; this proposal does not authorize editing that repository. Terraform, Doppler/External Secrets, KEDA, Cluster Autoscaler and Istio ambient security already have manifests and are reused. Internal service canaries, model releases/Triton, new production workload enablement and replacement telemetry stores are excluded. Runtime readiness has not been verified against a live cluster.
