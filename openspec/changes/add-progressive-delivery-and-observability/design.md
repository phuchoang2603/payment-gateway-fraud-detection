## Context

See [proposal.md](proposal.md) for motivation. The audit covered checked-in application and read-only `../talos-proxmox` manifests; it did not establish live cluster readiness.

| Area | Present | Gap addressed here |
| --- | --- | --- |
| Delivery | Commit-tagged releases, Argo CD prune/self-heal, Istio Gateway API ingress | Six Go Deployments; no Edge Rollout, analysis or controller in inspected platform manifests |
| Security | Ambient STRICT mTLS, principal policies, network policies, Doppler/External Secrets | Preserve these boundaries across stable/candidate pods |
| Go telemetry | OTLP metrics/traces, structured logs, bounded exporters, revision metadata | Chart hardcodes `prod-talos`/`prod` for dev; no operational dashboards or canary gates |
| Data telemetry | ClickHouse analytics dashboards; LLM/RAG scrapes and KEDA queries | No Kafka exporter wiring or Flink Prometheus reporter/scrape found |
| Shared platform | Victoria metrics/logs/traces, Grafana data sources, vmalert/Alertmanager | Grafana dashboard discovery excludes `payment-gateway`; Rollouts prerequisites absent from inspected manifests |

Existing `go-service-observability` and `talos-gitops-deployment` requirements forbid application dashboards/alerts and mention HyperDX. The deltas replace those constraints while retaining platform ownership. Unrelated environment-enablement drift and the active `feast-feature-store` change are outside this proposal.

## Goals / Non-Goals

**Goals:** make the public checkout release measurable and reversible; present enabled workload health through existing platform services; keep controller and application ownership explicit.

**Non-Goals:** internal gRPC or model canaries, full-release atomicity across six services, executor-wide tracing, a replacement monitoring stack, production simulator enablement, new cloud infrastructure or secret migration. The Edge canary does not prove compatibility of concurrently updated Payments/fraud services; existing API compatibility remains required.

## Decisions

### Keep the existing Gateway API route

Use Argo Rollouts with the Gateway API traffic-router plugin and the existing Istio-managed HTTPRoute. The canonical `edge` Service selects stable pods; add `edge-canary` for candidate pods and explicit backend weights on the existing checkout rule. Preserve hostnames, `/checkout/`, `X-Forwarded-Proto: https`, timeouts, service account, probes and mesh/network policies. Both Services target the same application port. Other services remain Deployments.

This avoids a competing Istio VirtualService and keeps internal callers on stable. Replica-only canaries cannot guarantee the requested public traffic weights. No sidecar or waypoint is introduced merely to route this ingress.

Reuse the app-of-apps `ignoreDifferences` support with namespace/name-specific entries: only the Edge HTTPRoute backend weights and the two Services' controller-added `rollouts-pod-template-hash` selector field are ignored, with `RespectIgnoreDifferences=true`. Host/path/filter/timeout, base selectors and other resources stay Git-owned. Do not ignore entire specs or all Services.

### Gate on fresh candidate observations

Use an AnalysisTemplate querying the shared VictoriaMetrics Prometheus-compatible API. Existing Go OTLP HTTP server counters/histograms are the first source; validate their exported names, revision/resource label mapping, route and status dimensions before enabling analysis. Preserve stable service identity and expose environment, namespace and `service.version` as queryable bounded dimensions. Add only a missing observation boundary in the shared Go helper if the existing export cannot isolate checkout revision traffic; do not create another application Prometheus listener.

Use candidate revision, checkout route and observation-window filters so stable traffic, probes and old samples cannot satisfy a gate. Count 5xx/transport errors separately from successful business declines. Proposed configurable starting steps are 10%, 25%, 50%, then promotion, with five-minute observation windows, at least 100 candidate requests per window, error rate at most 1%, and p95 at most one second. These are initial acceptance settings, not measured SLOs; calibrate against the current prod baseline before adoption. Require multiple passing observations per step after a five-minute warm-up. The rollout has a 30-minute progress deadline.

Analysis starts only after candidate readiness and an exporter flush/window warm-up. Empty/stale samples, non-finite results and query errors never count as success; bounded query retries end in abort. Insufficient traffic holds during warm-up, then aborts on the first failed measurement. Three consecutive query errors abort the analysis; empty, stale or non-finite results fail immediately. Abort restores stable traffic while retaining stable capacity. Low-traffic prod must not promote merely because the live simulator is disabled. The user authorized direct live prod acceptance; dev receives configuration only. Unit/integration tests and image builds remain CI-owned. Application CI never contacts Kubernetes; operators inspect controller and endpoint state read-only. No production simulator is enabled by this change.

### Extend the existing telemetry paths

Go keeps OTLP to vmagent/vtagent and JSON stdout collected by the platform. Correct cluster/environment attributes in chart values; retain pod UID and source revision. Use existing Grafana datasource UIDs `VictoriaMetrics`, `VictoriaLogs`, `VictoriaTraces` and `clickhouse-marts`. Application charts own labeled dashboard ConfigMaps, VMPodScrape/VMServiceScrape resources and VMRule resources in their workload namespaces. Platform collectors, retention, data sources and alert routing remain shared.

| Workload | Collection and view |
| --- | --- |
| Six Go services | Existing OTLP RED/business/queue measurements, trace/log links; candidate/stable comparison |
| Rollout | Platform controller metrics and Kubernetes state, AnalysisRun failures and traffic progress |
| Kafka | Strimzi-supported JMX exporter configuration plus Kafka exporter consumer lag; namespace-local scrape and failure/lag panels |
| Flink live/replay | Prometheus reporter included/configured in streaming image, scoped reporter port/scrape; job health, checkpoints and backpressure |
| Spark/Argo | Existing shared operator/controller lifecycle metrics and pod logs; failed/pending/completed workflow/job views, not new executor instrumentation |
| Feast/writer and storage dependencies | Existing Kubernetes availability/restarts and logs; show no application metric where none exists rather than inventing feature-quality/latency panels |
| LLM/RAG | Reuse existing scrapes/dashboards/KEDA queries only where enabled; no new model-serving activation |

Retain ClickHouse analytics dashboards; add operational views rather than replacing analytics. Use bounded topic/job labels, exporter resource limits, and narrowly scoped network/mesh access from the platform vmagent identity. Rollout analysis needs query access to VictoriaMetrics, not application credentials or a new ingress.

VMRule alerts cover sustained gateway errors/latency, notifier backlog, job failure or consumer lag, rollout failure and required-signal absence. Scope by environment and enabled workload, include diagnostic links, and make no-data visible. Platform Alertmanager handles routing. Avoid paging on disabled workloads and avoid duplicating shared node/storage/controller alerts.

### Require platform readiness before application adoption

The platform owner must supply a pinned compatible Rollouts controller/CRDs and Gateway API plugin, appropriate controller permissions for `payment-gateway` and the named HTTPRoute, reachable metrics query API, and observable controller state. Verify installed Istio/Gateway API/plugin compatibility before enabling prod weights. Expand Grafana dashboard discovery to `payment-gateway` and verify shared VM scrape/rule discovery and alert routing in the application namespaces. These are dependencies to coordinate separately, not resources to add to this repository or reasons to grant app CI cluster-admin.

## Risks / Trade-offs

- [Deployment handover can lose endpoints] → stage adoption before pruning and verify ready stable endpoints at every boundary.
- [Argo CD can overwrite controller mutations] → narrow field exclusions and exercise reconciliation during a weighted step.
- [Resource labels or metric windows can mix revisions] → inspect actual exported series, require candidate identity, freshness and minimum observations before enabling promotion.
- [Quiet prod stalls analysis] → bounded hold/abort; traffic generation remains explicitly controlled.
- [Canary surge consumes trial capacity] → budget stable plus candidate resources and schedule onto existing capacity; Cluster Autoscaler behavior is unchanged.
- [Exporter cardinality or scrape access increases load/exposure] → bounded allowlists, resource limits and vmagent-only access; sensitive payloads remain excluded.
- [Dashboard claims exceed available signals] → show unavailable/disabled state explicitly and record live verification gaps.

## Migration Plan

1. Coordinate platform prerequisites; implement environment identity, scrape definitions, operational dashboards and alert rules first. Observe real prod series and calibrate analysis thresholds.
2. Publish a staged prod adoption release using the documented temporary Rollout `workloadRef` migration with `scaleDown: never`. Keep the existing Deployment and stable route serving while Rollout-owned replicas become ready; reserve capacity for both. Do not let the route select an unready or mixed candidate.
3. After ready Rollout stable endpoints are verified, allow the controller to narrow stable/canary Service selectors and attach the weighted route at 100% stable. Scale the old Deployment down only after the new stable endpoints serve successfully. Publish the final inline Rollout pod template and remove `workloadRef` plus the old Deployment in a separate reconciled release. Keep no permanent compatibility mode.
4. Through authorized direct prod acceptance, demonstrate successful promotion, forced candidate failure, no-data/low-traffic hold and abort, unchanged internal routing/security, and Argo CD reconciliation during a weighted step. Verify dashboards, trace/log links and an alert's firing/resolution. Store concise results in the existing deployment guide/evidence convention; do not generate a second evidence index.
5. Keep dev configuration-only. Require platform readiness before prod adoption. During an ordinary failed release, abort traffic first and revert the source image/chart through the existing release branch flow. To undo controller migration, first restore ready Deployment pods and a stable-only HTTPRoute/Service, then remove the Rollout and its ignore rules after endpoints are verified. Abort does not roll back database schemas or other service releases.

## References

- [Argo Rollouts migration](https://argo-rollouts.readthedocs.io/en/stable/migrating/) and [analysis semantics](https://argo-rollouts.readthedocs.io/en/stable/features/analysis/).
- [Gateway API plugin](https://github.com/argoproj-labs/rollouts-plugin-trafficrouter-gatewayapi), consulted through Context7 for HTTPRoute ownership and weighting.
- Read-only FSDS coursework proposal and RecSys-MLops A/B example informed revision comparison and proof requirements; its model router and separate monitoring topology are not adopted.
