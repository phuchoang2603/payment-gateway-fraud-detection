# Service conventions

Each service is an independently buildable and deployable unit that owns its runtime, contracts and dependencies. Runtime behavior (liveness/readiness, bounded lifecycle, safe errors, operational identity, edge translation) is specified in [`openspec/specs/service-conventions`](../../openspec/specs/service-conventions/spec.md); this page records layout, dependency direction and ownership. The [refurbished marketplace](https://github.com/phuchoang2603/refurbished-marketplace/blob/main/docs/architecture.md) is the reference for composition roots, Go modules, versioned contracts and small shared runtime helpers.

## Layout

The layout below includes the independent Go fraud service. The [active change](../../openspec/changes/go-service-observability/proposal.md) tracks common telemetry acceptance; Feast/KServe deployment follows separately.

```text
contracts/
  payments/v1/                   # payment types, lifecycle events, labels, PaymentService
  fraud/v1/                      # internal decision RPC
  vault/v1/                      # synthetic vault
  processor/v1/                  # synthetic processor
src/
  payment-gateway/
    edge/                        # Go module; cmd/edge is the composition root
      cmd/edge/main.go
      internal/config/           # validated environment configuration
      internal/transport/        # checkout HTTP routes, hosted page, health and correlation
    payments/
    notifier/
    vault-simulator/
    processor-simulator/
    fraud/                       # Go module; cmd/fraud is the composition root
      internal/domain/           # pure decision values and policy
      internal/transport/        # gRPC mapping and validation
    shared/                      # one Go module: proto, testutil, observe and security
  simulator/                     # historical data generation
```

Both languages keep business rules separate from transport. Edge now has checkout validation/fingerprinting domain values, application coordination and gRPC adapters. Payments, notifier, vault-simulator and processor-simulator each own a Go module, composition root, domain/application packages and persistence adapters. Go fraud follows the same module/composition pattern, keeps policy in `internal/domain` and uses shared Go telemetry. Add feature/model adapters and a use-case coordinator when it calls Feast and KServe; their Python runtimes stay outside gateway orchestration. Add an application/use-case layer only when a service coordinates multiple domain or I/O operations, and add storage adapters only when it persists data. Do not create empty layers for symmetry.

## Dependency direction

```text
transport -> domain
transport -> telemetry
```

Composition roots validate configuration and assemble the service; they contain no business decisions. Transport handlers map protocol input and errors. Domain code owns decisions and stable errors without importing gRPC, HTTP frameworks, SQL drivers or broker clients. When persistence or multi-step use cases arrive, add a storage adapter and a use-case coordinator around the domain rather than putting I/O in the policy. A service never imports another service's `internal` package or reads another service's database.

## Persistence and contracts

Each service owns its schema, migrations and persistence adapter; a deployment or migration job for one service cannot modify another service's state. Domain events, integration messages and projections remain separate concepts.

Internal synchronous business APIs use versioned protobuf services under `contracts/<capability>/vN/` with generated bindings for actual consumers; no parallel internal REST/JSON API is maintained. The fresh Go fraud replacement removes unused Python bindings and wiring without a compatibility wrapper or dual-runtime migration. Keep `fraud.v1` where its semantics remain useful; a runtime language change alone needs no new wire version. Coordinate intentional contract changes with callers. Feast and KServe adapters use their supported vendor HTTP/gRPC protocols rather than introducing another internal business API. Public HTTP/JSON belongs only at the Go edge. Asynchronous events may use protobuf payloads but need their own durable transport with an outbox or equivalent boundary.

The versioned protobufs in `contracts/` are the shared wire contracts. Extract technical helpers only when multiple services in the same language actually reuse them; Go and Python cannot share runtime code directly. Do not share mutable domain models or cross-service repositories. No database, broker, event store or application framework is chosen by these conventions; Payments uses versioned state rows with an append-only Postgres ledger, Redis-first idempotency and Debezium publication.

## Images and reference material

The edge image is built from its own module with `GOWORK=off` using the reusable [`infra/docker/go-service.Dockerfile`](../../infra/docker/go-service.Dockerfile); fraud uses the same image pattern from `src/payment-gateway/fraud`. All six Go services use the `infra/charts/payment-gateway` services map, with independent Deployments and Services. Seven explicit image jobs share one workflow (see [CONTRIBUTING](../../CONTRIBUTING.md)).

In the marketplace, `services/orders/cmd/orders/main.go` demonstrates a composition root, `shared/runtime/http.go` bounded HTTP shutdown, `shared/err/grpcerr/map.go` transport error mapping and `docs/development/code-generation.md` workspace/module conventions. This repository adopts those patterns selectively and keeps gateway domain ownership separate.
