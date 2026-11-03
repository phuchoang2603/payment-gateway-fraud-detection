## RENAMED Requirements

- FROM: `### Requirement: Shared-platform integration without application dashboards`
- TO: `### Requirement: Shared-platform telemetry integration`

## MODIFIED Requirements

### Requirement: Correlated transport diagnostics

HTTP and gRPC requests across Go services SHALL propagate valid W3C trace context and generate new context where absent or invalid. Request logs SHALL carry stable service identity, request correlation and actual active trace/span identifiers. Startup logs SHALL NOT fabricate span identifiers. Logs SHALL remain single-line JSON on stdout using the platform's parsed correlation fields. Ordinary health probes SHALL NOT generate request telemetry volume.

#### Scenario: HTTP to gRPC request

- **WHEN** a sampled edge request invokes payments, Go fraud and a Go simulator
- **THEN** the exported spans form a connected request trace with route/RPC names and service identities
- **AND** request-path logs expose matching trace identifiers through the shared Grafana trace/log data sources

### Requirement: Shared-platform telemetry integration

Workload charts SHALL configure all six Go services to use the shared OTLP metric and trace endpoints, with annotations and environment/revision metadata matching exported service identities. Verification SHALL demonstrate fresh logs, metrics, traces and trace-to-log navigation using the shared VictoriaMetrics, VictoriaLogs, VictoriaTraces and Grafana services. Applications SHALL install no telemetry store or collector; namespace-local dashboard, scrape and alert definitions SHALL be permitted. Training, Feast/KServe serving and autoscaling deployment changes SHALL remain outside this capability.

#### Scenario: Deployment acceptance

- **WHEN** the charts are enabled and a bounded synthetic workflow exercises all six Go services
- **THEN** each service's produced signals are readable from the shared platform with matching Kubernetes/service/environment/revision identities
- **AND** trace-to-log navigation works from the application operational dashboards without a second telemetry pipeline
