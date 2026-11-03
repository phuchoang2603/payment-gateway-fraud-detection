# application-observability Specification

## Purpose

Make enabled payment and data workloads operable through the shared Grafana and Victoria services with trustworthy identities, actionable signals and bounded collection.

## Requirements

### Requirement: Environment and release attribution

Telemetry SHALL identify its actual environment, cluster, namespace, stable service and deployed revision. Operators SHALL be able to separate candidate from stable activity and dev from prod. Dimensions SHALL remain bounded and SHALL NOT include payment identifiers, credentials, card data or raw payloads.

#### Scenario: Compare environments and revisions

- **WHEN** dev and prod or two Edge revisions emit telemetry
- **THEN** queries can isolate their signals without dev samples appearing as prod or stable samples satisfying candidate gates

### Requirement: Operational visibility for enabled workloads

Application-owned Grafana dashboards SHALL use shared data sources to show gateway request/error/latency, payment outcomes, notification retries/backlog, rollout state and enabled data-path health. Collection SHALL include Kafka health and lag, Flink job/checkpoint/backpressure signals, and available Spark/Argo lifecycle signals. Existing analytics and LLM/RAG dashboards and scrapes SHALL be reused. Disabled workloads SHALL be distinguished from missing telemetry; unavailable signals SHALL NOT be represented as healthy zero values.

#### Scenario: Inspect a failing data path

- **WHEN** an enabled Flink job fails or a Kafka consumer accumulates lag
- **THEN** operators can identify the affected workload and correlate its operational signals with shared logs

#### Scenario: Disabled application

- **WHEN** a workload is disabled in the selected environment
- **THEN** dashboards identify its disabled or absent state and workload-specific alerts do not report an outage for it

### Requirement: Actionable alerts and correlated diagnostics

Enabled workloads SHALL have sustained-condition alerts for request failures/latency, notification backlog, job failure or consumer lag, failed rollout analysis and missing required telemetry. Alerts SHALL identify environment and workload and link to relevant dashboards or operational guidance. Sampled request diagnostics SHALL support trace-to-log navigation through the shared Grafana data sources.

#### Scenario: Sustained degradation

- **WHEN** an enabled workload breaches a configured operational threshold for its evaluation duration
- **THEN** the shared alerting pipeline receives a scoped alert with a diagnostic link
- **AND** a valid business decline alone does not count as a transport outage

### Requirement: Shared and bounded collection

Application resources SHALL reuse platform-owned metric, log, trace, dashboard and alerting services. Collection SHALL use native exporters or existing OTLP paths with scoped network access and bounded resource/cardinality budgets. Telemetry outages SHALL NOT alone interrupt payment processing. Application removal SHALL remove its monitoring definitions without removing shared services or retained telemetry.

#### Scenario: Telemetry backend outage or application removal

- **WHEN** the telemetry backend is unavailable or application monitoring resources are deleted
- **THEN** payment processing does not acquire a dependency on that backend
- **AND** shared telemetry infrastructure and its retained data remain platform-owned
