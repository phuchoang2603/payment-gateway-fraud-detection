## Purpose

Establish independently operable Go and Python service boundaries with predictable readiness, lifecycle, error and contract behavior.

## Requirements

### Requirement: Independent service ownership

Services SHALL build and run without starting another service or accessing its database. Go SHALL own edge, Payments, fraud decision serving, notifier, vault simulator and processor simulator. Training, Feast transformations and custom model runtimes MAY use Python. Go fraud SHALL call separately deployed Feast/KServe serving APIs directly when those dependencies are introduced, without a custom Python coordinator. Shared artifacts SHALL contain technical helpers or versioned wire contracts, not shared mutable domain models. The conventions SHALL document composition, transport/domain/persistence boundaries, service-owned migrations and independently deployable units.

#### Scenario: Build a service independently

- **WHEN** the edge or fraud service is built from a clean checkout using its documented command
- **THEN** its runtime artifact is produced without starting sibling services or relying on the marketplace checkout
- **AND** the artifact can be started with its own configuration

#### Scenario: Review a persistence integration

- **WHEN** a service adds persistence or a migration
- **THEN** its documented ownership restricts reads, writes and migrations to its own state
- **AND** communication with another service uses a versioned external contract

### Requirement: Liveness and readiness probes

The edge SHALL expose HTTP `/health` for liveness and `/ready` for readiness (200 when ready, 503 otherwise). Fraud SHALL expose standard gRPC health checks with distinct `liveness` and `readiness` service names; readiness SHALL report SERVING only when startup has completed, the decision policy is active and shutdown has not started, and NOT_SERVING otherwise. An unavailable telemetry backend SHALL NOT alone make a service unready.

#### Scenario: Shutdown started

- **WHEN** fraud shutdown begins
- **THEN** gRPC readiness reports NOT_SERVING while liveness remains SERVING for as long as the server accepts health checks

#### Scenario: Service ready

- **WHEN** required startup resources are available and shutdown has not started
- **THEN** the service readiness check reports ready

### Requirement: Bounded resource lifecycle

Service startup SHALL validate configuration before reporting ready. Shutdown SHALL withdraw readiness, stop accepting new work, allow active work a configured bounded drain period, and release resources with bounded telemetry flush. Imports and isolated application construction SHALL NOT open listeners or start telemetry.

#### Scenario: Invalid configuration

- **WHEN** startup receives an invalid port or shutdown timeout
- **THEN** startup fails with a useful sanitized diagnostic and nonzero exit status
- **AND** the service does not report ready

#### Scenario: Termination with active work

- **WHEN** termination is requested during an active request
- **THEN** the service stops accepting new work and allows the request to finish within the drain budget
- **AND** work exceeding the budget cannot prevent bounded process exit

#### Scenario: Isolated instances

- **WHEN** the application is imported and multiple isolated instances are constructed
- **THEN** no metrics-port collision occurs until the respective lifecycle is started
- **AND** closing one instance does not change another instance's readiness

### Requirement: Versioned contracts and safe errors

The supported fraud decision request/response schemas, policy thresholds, validation semantics and operational endpoints SHALL be explicit in the versioned contract and the documentation; decision rules and thresholds SHALL be protected by behavior tests. Legacy compatibility SHALL NOT constrain modernization; intentional changes SHALL update implementation, contracts and tests together. Internal synchronous business contracts SHALL use versioned protobuf services with generated bindings for actual consumers; legacy internal JSON APIs SHALL be removed. Vendor Feast/KServe HTTP or gRPC protocols SHALL be isolated behind adapters and SHALL NOT introduce parallel internal business APIs. The replaced Python fraud runtime and unused bindings SHALL be removed without a compatibility wrapper. Contract conventions SHALL state ownership, compatibility rules and breaking-version handling. Public errors SHALL NOT expose stack traces, credentials or storage internals; new contracts SHALL use stable error categories mapped at the transport boundary.

#### Scenario: Supported decision caller

- **WHEN** a request matching the supported decision schema is submitted after a refactor or complete implementation rewrite
- **THEN** its status, response schema, outcome and reason codes match the documented current contract

#### Scenario: Unexpected application failure

- **WHEN** an unhandled adapter failure reaches an API boundary
- **THEN** the caller receives a sanitized failure response and operators can correlate its structured diagnostic

### Requirement: Consistent operational identity

All gateway services SHALL emit structured logs with a consistent service identity and real trace/span identifiers when available. Request correlation SHALL be propagated or generated when absent. Metric dimensions SHALL avoid unbounded request, customer, payment or transaction identifiers. Fraud SHALL export decision outcomes, separate reason matches, policy latency and RPC latency over OTLP. The old Python Prometheus endpoint SHALL be removed. One decision matching multiple reasons SHALL increment the decision total once. Declines SHALL NOT count as RPC errors. Trace export SHALL be disabled by default and SHALL require explicit enablement and a configured OTLP endpoint; disabled tracing SHALL NOT require a trace backend for service startup or requests. Application dashboards and alerts are deferred.

#### Scenario: Request without an incoming correlation identifier

- **WHEN** a request arrives without a request identifier
- **THEN** its response and structured request diagnostic share a generated identifier
- **AND** a startup log without an active span does not fabricate trace identifiers

#### Scenario: Unconfigured trace export

- **WHEN** the fraud service starts without trace export enabled or an endpoint configured
- **THEN** requests succeed without attempting to contact a trace backend

#### Scenario: Explicit trace export

- **WHEN** trace export is enabled with an explicit OTLP endpoint
- **THEN** spans use the configured service identity and export to that endpoint

#### Scenario: Trace export enabled without endpoint

- **WHEN** tracing is explicitly enabled without an OTLP endpoint
- **THEN** startup rejects the invalid configuration rather than selecting a backend

### Requirement: Internal Go service operation

Payments, fraud, notifier, vault simulator and processor simulator SHALL follow the edge's composition-root shape with transport, application, domain and adapter packages.

#### Scenario: Complete internal go service operation contract

- **WHEN** the `Internal Go service operation` capability is configured or exercised
- **THEN** Payments, fraud, notifier, vault simulator and processor simulator SHALL follow the edge's composition-root shape with transport, application, domain and adapter packages. Each SHALL expose standard gRPC health checks with `liveness` and `readiness` service names. For services with a database, readiness SHALL require their own database to be reachable with migrations applied, and SHALL NOT require any other service or Redis to be reachable. Each SHALL apply only its own migrations, bound shutdown like the edge, and emit structured logs, traces and metrics with a consistent service identity. Internal gRPC SHALL rely on the service mesh for transport encryption and caller identity rather than application TLS or bearer credentials; the vault SHALL separate tokenization/metadata from processing resolution on distinct ports authorized per workload identity. Probes SHALL use the native gRPC health check.

#### Scenario: Fraud unavailable at Payments startup

- **WHEN** Payments starts while fraud is down but its database is ready
- **THEN** Payments reports ready, and attempts fail closed as specified

#### Scenario: Database unreachable

- **WHEN** a service's database is unreachable
- **THEN** liveness stays SERVING and readiness reports NOT_SERVING

#### Scenario: Redis unavailable at Payments startup

- **WHEN** Payments starts with its database ready but Redis unavailable
- **THEN** it reports ready and uses durable database idempotency until Redis returns
