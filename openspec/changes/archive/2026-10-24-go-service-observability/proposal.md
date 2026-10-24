## Why

The gateway services lack a complete, consistently configured telemetry lifecycle and trace propagation. Replace the rules-only Python fraud runtime with Go so all six gateway services share the same operational contract and operators can follow payment decisions and notifications in HyperDX.

## What Changes

- Replace `src/fraud-service` with an independently deployable Go service at `src/payment-gateway/fraud`, owning decision validation and pure policy. Remove the Python runtime and obsolete build, code-generation, CI and deployment wiring without a compatibility wrapper or parallel service.
- Add reusable Go-only telemetry helpers under `src/payment-gateway/shared/observe`, wired by the composition roots of edge, payments, fraud, notifier, vault-simulator and processor-simulator.
- Export traces and application metrics over OTLP/gRPC to the existing node agent; retain single-line JSON stdout logs with active trace/span correlation, stable service identity and bounded metric attributes.
- Instrument public HTTP routes, internal gRPC clients/servers and notifier HTTP delivery; carry trace context through committed event publication and Kafka consumption without changing business event bytes or payment ordering.
- **BREAKING**: replace application Prometheus exports, including the old Python fraud endpoint, with OTLP instruments. Preserve business measurements, distinguish fraud decision totals from reason matches, and document metric-name/unit changes. Preserve health/readiness behavior.
- Set the later ML boundary: Go fraud calls Feast's feature server and KServe's predictor directly, applies policy and returns versioned decisions. Python remains in training, Feast transformations and model runtimes where needed; no custom Python coordinator is required. Deploying these ML components and their autoscaling is outside this change.
- Consolidate all six Go services into the gateway chart services map; remove the dedicated fraud chart and Argo CD child, and share the gateway production image pin. Configure Helm opt-in, service-name annotations and bounded sampling/export intervals. Use the shared HyperDX preset dashboards and explorers; install no application collector or custom dashboards.
- Verify accepted/rejected payment requests, asynchronous notifications, telemetry backend outages and sensitive-data exclusion with focused existing test coverage and live readback.

## Capabilities

### New Capabilities

- `go-service-observability`: Correlated Go logs, distributed traces, bounded application metrics and fail-independent delivery to the shared telemetry platform.

### Modified Capabilities

- `talos-gitops-deployment`: replace the fraud metrics-listener requirement with application OTLP export while preserving internal gRPC, health and platform ownership.
- `service-conventions`: replace Python fraud ownership and its required application metrics endpoint with Go fraud ownership and the common OTLP producer contract. Trace opt-in, bounded shutdown and telemetry-independent readiness remain in force. The delta is captured in `specs/service-conventions/spec.md`.

## Impact

Targets the six Go service modules, removal of the Python fraud service, shared Go helpers, payment event storage/CDC transport metadata, notifier delivery adapters, fraud and gateway image/chart/CI wiring, and affected architecture, roadmap and observability documentation. Retain the existing `fraud.v1` decision semantics and payment invariants; a language change alone does not require a new wire version. No compatibility layer for the replaced runtime is required. Feast/KServe deployment, Spark jobs and data-pipeline instrumentation remain later work. The independently owned platform replaces Grafana with HyperDX and must collect `payment-gateway` and `ecommerce`; marketplace applications retain their current producer contract.
