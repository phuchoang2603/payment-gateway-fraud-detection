## Purpose

Provide correlated, resource-bounded telemetry for the Go payment gateway services through the shared observability platform without coupling payment availability to telemetry delivery.

## ADDED Requirements

### Requirement: Explicit service telemetry lifecycle

All six gateway services (edge, payments, fraud, notifier, vault-simulator and processor-simulator) SHALL export enabled traces and metrics over OTLP with stable service identities. Fraud SHALL use `fraud-service` across replicas/revisions, with instance/revision metadata separate. Backend delivery failure SHALL NOT alone fail readiness, business requests or Kafka processing. Export buffering/retries and shutdown flushing SHALL have bounded resource/time budgets.

#### Scenario: Exporter outage

- **WHEN** a correctly configured telemetry backend becomes unreachable
- **THEN** valid business requests and Kafka processing continue and readiness stays healthy
- **AND** buffering/retries and shutdown flushing stay within configured budgets independently of cancelled request contexts

### Requirement: Explicit export configuration

Trace export SHALL default to disabled and require explicit enablement and an endpoint. Disabled export SHALL require no backend. Enabled export with missing or invalid configuration SHALL fail before readiness.

#### Scenario: Explicit configuration

- **WHEN** trace export is enabled without an endpoint
- **THEN** startup rejects the configuration before reporting ready

#### Scenario: Disabled export

- **WHEN** export is disabled without a configured backend
- **THEN** startup and valid requests succeed without contacting an exporter

### Requirement: Correlated transport diagnostics

HTTP and gRPC requests across Go services SHALL propagate valid W3C trace context and generate new context where absent or invalid. Request logs SHALL carry stable service identity, request correlation and actual active trace/span identifiers. Startup logs SHALL NOT fabricate span identifiers. Logs SHALL remain single-line JSON on stdout using the platform's parsed correlation fields. Ordinary health probes SHALL NOT generate request telemetry volume.

#### Scenario: HTTP to gRPC request

- **WHEN** a sampled edge request invokes payments, Go fraud and a Go simulator
- **THEN** the exported spans form a connected request trace with route/RPC names and service identities
- **AND** request-path logs expose matching trace identifiers in HyperDX

### Requirement: Durable asynchronous context without business mutation

Committed payment events SHALL carry optional trace context as transport metadata independent of serialized event bytes. Kafka consumers and persisted notification retries SHALL preserve causality across process restarts, with a distinct span for each delivery attempt. Missing or malformed context SHALL NOT reject a valid business event. Telemetry SHALL NOT change event ordering, idempotency, delivery signing or acknowledgement rules.

#### Scenario: Retry after restart

- **WHEN** notifier restarts after persisting a delivery whose event carried trace context
- **THEN** its next delivery attempt retains that causal relationship and creates a distinct attempt span
- **AND** the serialized business event and webhook signing behavior are unchanged

#### Scenario: Historical event without trace context

- **WHEN** a valid historical or replayed event has no usable trace metadata
- **THEN** notifier processes it normally and exposes event correlation without fabricating a remote parent

### Requirement: Bounded application metrics and safe diagnostics

Go services SHALL export HTTP/gRPC RED and business outcome, recovery, queue and notification measurements over one OTLP application metric path. Metric dimensions SHALL exclude unbounded customer, request, event, transaction and payment identifiers. Metric intervals and root-trace sampling SHALL be configurable with bounded production defaults.

#### Scenario: Payment and delivery activity

- **WHEN** accepted/rejected payments and retried webhook deliveries are exercised
- **THEN** request/outcome/latency and delivery/backlog measurements are queryable by service and bounded outcome dimensions
- **AND** per-payment metric labels are absent

### Requirement: Fraud decision measurements

Go fraud SHALL record each decision once by outcome and reason matches separately, with policy latency separate from full RPC latency. A successful decline SHALL NOT count as an RPC failure. The replaced Python Prometheus endpoint SHALL NOT remain as a second application metric path.

#### Scenario: Fraud decline with multiple reasons

- **WHEN** a valid fraud request produces a decline matching two policy reasons
- **THEN** the outcome measurement records one declined decision and the reason measurement records both matches
- **AND** the RPC succeeds, policy and RPC durations are measurable, and request logs correlate to the active fraud span

### Requirement: Safe diagnostic content

Exported diagnostics SHALL exclude credentials, authentication/signature headers, card data, checkout tokens/URLs and raw payment or webhook bodies.

#### Scenario: Sensitive request inputs

- **WHEN** a synthetic payment or webhook request contains sensitive values
- **THEN** its exported logs, spans and metric attributes exclude those values
- **AND** safe request correlation remains available

### Requirement: Shared-platform integration without application dashboards

Workload charts SHALL configure all six Go services to use the shared node agent, with annotations matching exported service identities. Verification SHALL demonstrate fresh logs, metrics, traces and trace-to-log navigation using shared HyperDX sources/presets. Applications SHALL install no telemetry store, collector, custom dashboard or scrape deployment. Training, Feast/KServe serving and autoscaling deployments SHALL remain outside this change.

#### Scenario: Deployment acceptance

- **WHEN** the charts are enabled and a bounded synthetic workflow exercises all six Go services
- **THEN** each service's produced signals are readable from the shared store with matching Kubernetes/service identities
- **AND** trace-to-log navigation works without an application-owned dashboard
