## 1. Platform dependencies

- [x] 1.1 Record and coordinate the platform-owned Rollouts controller/CRDs, pinned Gateway API plugin, scoped permissions and metrics query access in the existing deployment guide; verify delivered versions and prod readiness from platform manifests/status without editing `talos-proxmox`.
- [x] 1.2 Coordinate Grafana discovery of `payment-gateway`, VM scrape/rule discovery and alert routing; verify application-namespace dashboard discovery and scrape/rule selection before enabling dependent resources.

## 2. Telemetry identity and collection

- [x] 2.1 Replace hardcoded prod resource attributes with dev/prod values while retaining stable service names, pod UID and revision; verify both environment renders and actual VictoriaMetrics series isolate environment and candidate revision.
- [x] 2.2 Configure Strimzi Kafka JMX metrics, consumer-lag exporter and scoped VM scrapes/network access; verify Helm rendering/schema checks locally and authorized prod acceptance shows broker and lag signals without sensitive or unbounded labels.
- [x] 2.3 Configure the Flink Prometheus reporter in the streaming image and both live/replay jobs, including scoped scrapes and resource budgets; verify the pinned image already bundles the reporter, existing CI image delivery, local chart renders and authorized prod acceptance showing job/checkpoint/backpressure signals.
- [x] 2.4 Connect existing Spark/Argo lifecycle and Kubernetes workload health signals to the operational views; verify available series against enabled workloads and explicitly mark unavailable application metrics rather than adding speculative instrumentation.

## 3. Operational dashboards and alerts

- [x] 3.1 Add namespace-local Grafana operational dashboard ConfigMaps for gateway, rollout and data-path health using existing datasource UIDs and trace/log links; verify JSON/chart validation and prod Grafana discovery while retaining existing analytics/LLM dashboards.
- [x] 3.2 Add scoped VMRule definitions for sustained gateway failures/latency, notifier backlog, job failure or consumer lag, rollout failure and required-signal absence; verify rule syntax locally and authorized prod acceptance demonstrates firing/resolution without alerting on disabled workloads.

## 4. Edge progressive delivery

- [x] 4.1 Add the Edge Rollout and canary Service with staged Deployment adoption, preserving pod security, probes, resources and stable internal routing; verify dev/prod Helm renders and CRD schemas, leaving the other five Go services as Deployments.
- [x] 4.2 Add weighted backends and plugin configuration to the existing checkout HTTPRoute plus resource/field-specific Argo CD ignore rules; verify rendered host/path/header/timeout remain intact and authorized prod acceptance shows reconciliation preserves a weighted step.
- [x] 4.3 Add candidate-specific AnalysisTemplate queries and configurable steps, traffic minimums, warm-up, error/latency thresholds and deadlines; verify actual exported series satisfy revision/freshness filters and authorized prod acceptance blocks missing/stale/non-finite/error or insufficient-traffic results.
- [x] 4.4 Complete prod handover through separately reconciled releases, then remove the temporary workloadRef and legacy Edge Deployment; verify ready stable endpoints throughout and a final single Edge workload controller. Keep dev configuration-only; require platform readiness before prod handover.

## 5. Verification and documentation

- [x] 5.1 Reuse existing observation/transport coverage; add focused behavioral coverage only for any new telemetry logic or uncovered rollout safety boundary, executed through existing CI checks. Verify CI results and image builds; run no tests locally and add no config-only test suite.
- [x] 5.2 Run local non-test validation in devenv: Helm lint/template for each changed chart with dev/prod values, CRD/rule/dashboard schema validation, strict OpenSpec validation and diff checks; verify all pass and report unavailable validators honestly.
- [x] 5.3 Run authorized live prod acceptance for healthy promotion, failed-candidate traffic rollback, bounded no-data/low-traffic abort, stable internal API routing and unchanged mesh/access restrictions; verify HTTP/telemetry observations directly in prod and inspect AnalysisRuns, endpoints and Argo CD reconciliation read-only as an operator. Application CI must not contact Kubernetes or gain deployment credentials.
- [x] 5.4 Update the existing deployment/operations guide with adoption/abort/source-revert steps, calibrated thresholds, dashboard links and concise acceptance results; remove superseded live HyperDX/dashboard restrictions in affected guidance, preserve historical evidence, and report pending platform/prod checks without a duplicate evidence map.
