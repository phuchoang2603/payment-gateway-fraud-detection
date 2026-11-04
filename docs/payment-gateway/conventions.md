# Service conventions

Edge, Payments, Fraud, Notifier, Vault simulator and Processor simulator are independent Go modules under [`src/payment-gateway`](../../src/payment-gateway). Each owns its configuration, lifecycle, contracts, database access and migrations. Python handles data pipelines, Feast and model/agent workloads separately.

## Layout and dependency direction

| Location (where used) | Responsibility |
|---|---|
| `cmd/<service>/main.go` | Validate configuration and assemble dependencies; no business decisions |
| `internal/config` | Parse and validate environment settings with sanitized errors |
| `internal/domain` | Business values, rules and stable errors; no transport or database imports |
| `internal/application` | Coordinate domain operations through adapter interfaces |
| `internal/adapters` | Postgres, Redis, gRPC/HTTP clients, Kafka or webhook implementations as needed |
| `internal/transport` | Validate protocol inputs and map application results/errors |
| `db` | Service-owned migrations and SQL queries, where persistence is required |

Transport calls application code; application code coordinates domain rules and adapter interfaces. Composition roots provide concrete adapters. Keep only layers the service uses, and never import a sibling service's `internal` packages or read its database.

The [Fraud application](../../src/payment-gateway/fraud/internal/application/assessment.go) coordinates durable assessments and its [Feast adapter](../../src/payment-gateway/fraud/internal/adapters/feast/client.go) calls the feature server directly; its model adapter calls Triton with the Open Inference Protocol v2.

## Contracts and persistence

- Internal business APIs use versioned protobuf services in [`contracts/`](../../contracts). Public HTTP/JSON belongs at Edge; Feast and model APIs use their supported vendor protocols behind adapters.
- Update callers, generated bindings and tests together when changing a contract. A breaking semantic change needs an explicit compatibility/version decision.
- Each service owns its migrations and runtime database role. Payments commits state, ledger events and idempotency atomically; Debezium publishes committed events.
- Shared code in [`shared/`](../../src/payment-gateway/shared) contains wire bindings, test helpers, telemetry and security primitives, not mutable domain models or cross-service repositories.

## Runtime behavior

Startup validates configuration and required resources before becoming ready. Edge uses HTTP `/health` and `/ready`; gRPC services expose health probes. Shutdown withdraws readiness and drains work within a bounded timeout before closing dependencies.

Logs are structured and carry real trace/span IDs when available. Errors returned to callers omit credentials, stack traces and storage internals. Metric labels exclude request, payment and customer IDs. Telemetry export uses the shared OTLP agents and does not determine payment readiness.

Domain tests cover invariants and boundary cases; application tests cover orchestration and failure handling; adapter integration tests verify persistence and protocol behavior. Tests/builds run in CI.

## Build and deployment

The shared [Go Dockerfile](../../infra/docker/go-service.Dockerfile) builds each module with `GOWORK=off` and produces a static binary on non-root distroless. [Image definitions](../../.github/service-images.json) drive affected-image builds through the reusable [build workflow](../../.github/workflows/build-image.yml).

The [gateway chart](../../infra/charts/payment-gateway) deploys Edge as an Argo Rollout and the other five services as Deployments, each with its own Service and service account. Nonsecret settings live in chart values; Doppler credentials reach workloads through External Secrets.
