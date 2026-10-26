# Shared observability

The shared platform owns Victoria agents, Victoria stores and Grafana. The six Go services (edge, payments, fraud, notifier, vault-simulator and processor-simulator) use `shared/observe` to export traces and application metrics over OTLP/HTTP. Logs remain single-line JSON stdout with lowercase `level`, `msg`, `service`, and actual active `trace_id`/`span_id` fields. Startup logs carry no fabricated span IDs. Fraud retains the stable `fraud-service` identity; `service.instance.id` and `service.version` carry pod UID and image revision separately.

Both exports default to disabled in the binaries. Enable them explicitly with `TRACING_ENABLED=true` and `METRICS_ENABLED=true`, plus the corresponding full HTTP URLs in `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`. The chart points traces to `http://vtagent-vmks.observability.svc.cluster.local:10429/insert/opentelemetry/v1/traces` and metrics to `http://vmagent-vmks.observability.svc.cluster.local:8429/opentelemetry/v1/metrics`, sets cluster/environment resource attributes and uses the SDK’s default cumulative metric temporality, and uses a 60-second metric interval and parent-based 0.1 trace sampling. Incoming sampling decisions are retained. Chart `telemetry.metricsEndpoint` and `telemetry.tracesEndpoint` control the destinations. The agents buffer remote writes on bounded persistent queues separate from the stores; no OpenTelemetry collector is used.

Export queues hold at most 512 spans in batches of 128. Each export has a 2 s timeout; each provider has an independent 3 s shutdown budget that does not inherit cancelled request contexts. Readiness, payment processing and Kafka acknowledgements do not depend on exporter availability. OTLP uses HTTP/protobuf to the cluster-local Victoria agents, consistent with existing internal transport. No OTLP log exporter, Prometheus listener on 8010, scrape deployment, application collector or custom dashboard is installed.

HTTP spans use route templates and exclude query strings, paths containing tokens, bodies and headers. gRPC stats handlers instrument clients and servers, suppressing health probes. Fraud policy evaluation has its own child span. Webhook delivery records a client span without capturing the destination URL, signatures or body; its W3C headers propagate the active context. Arbitrary baggage is neither extracted nor injected.

Payment events store nullable `tracingspancontext` beside immutable protobuf bytes. The carrier is W3C `traceparent`/`tracestate` serialized as Java Properties. EventRouter forwards this field explicitly as the `tracingspancontext` Kafka header, including when Connect has no active tracing SDK. Notifier also accepts native W3C headers, starts a consumer span, and persists the resulting context with the delivery. Each retry restores that parent and starts a distinct attempt span. Missing or malformed metadata leaves valid business events consumable; bodies, signatures, ordering and acknowledgement rules are unchanged. This does not configure Kafka Connect to export Java spans.

The intentional metric migration is:

| Previous measurement | OTLP instrument | Unit | Attributes |
| --- | --- | --- | --- |
| `payments_attempt_duration_seconds` | `payments.attempt.duration` | `s` | none |
| `payments_attempt_failures_total` | `payments.attempt.failures` | `{attempt}` | `reason` |
| `payments_pending_outcome_age_seconds` | `payments.pending.outcome.age` | `s` | none |
| `payments_redis_idempotency_hits_total` | `payments.redis.idempotency.hits` | `{lookup}` | none |
| `payments_redis_idempotency_misses_total` | `payments.redis.idempotency.misses` | `{lookup}` | none |
| `payments_redis_idempotency_errors_total` | `payments.redis.idempotency.errors` | `{operation}` | none |
| `notifier_deliveries_total` | `notifier.deliveries` | `{attempt}` | `outcome` |
| `notifier_deliveries_pending` | `notifier.deliveries.pending` | `{delivery}` | none |
| `notifier_oldest_pending_age_seconds` | `notifier.oldest.pending.age` | `s` | none |
| `notifier_consumed_events_total` | `notifier.consumed.events` | `{event}` | `result` |
| `notifier_consumer_errors_total` | `notifier.consumer.errors` | `{error}` | `stage` |
| `notifier_consumer_last_event_age_seconds` | `notifier.consumer.last.event.age` | `s` | none |
| `notifier_consumer_last_commit_timestamp_seconds` | `notifier.consumer.last.commit.timestamp` | `s` (Unix epoch) | none |
| `processor_sale_outcomes_total` | `processor.sale.outcomes` | `{sale}` | `outcome` |
| `processor_requests_total`, `vault_requests_total` | `rpc.server.call.duration` histogram count and duration | `s` | standard RPC method/status attributes |
| `fraud_decisions_total{outcome,reason}` | `fraud.decisions` (one per decision); `fraud.reason.matches` (one per matched reason) | `{decision}`; `{match}` | `outcome`, `policy.version`; `reason`, `policy.version` |
| `fraud_decision_latency_seconds` | `fraud.policy.duration` | `s` | `outcome`, `policy.version` |
| HTTP requests | `http.server.request.duration`, `http.client.request.duration` histogram count and duration | `s` | bounded method, route (server only), status/error |

RPC full-call duration is separate from fraud policy duration. Successful declines return gRPC OK and count as successful RPCs. RPC instruments are supplied by the pinned `otelgrpc` version; confirm their actual storage names during acceptance. Per-payment/customer/request/event identifiers are excluded from metric labels. Platform container metrics cover resource capacity; redundant Go/process Prometheus collectors have been removed.

Local verification is restricted to formatting, lint, schema validation and chart rendering. Go lint passed in all seven modules, Helm lint/render passed for all chart value configurations, and strict OpenSpec validation passed; no tests or image builds ran locally. All tests and image builds run in CI. PR #87 passed all 28 checks for `026dfb5`, including Go behavior/integration tests and all seven image builds. On 2026-10-24, the prod root and children were switched to `t3/go-observability-single-replica` using that run’s published PR images. All six Go services became ready with one replica, both trace-context migrations applied, and Debezium 3.7.0.Final connector/task reported RUNNING. Fresh logs from all six identities and Payments/notifier application metrics reached ClickHouse. Live readback found missing notifier egress to the node agent; chart values now allow TCP 4317 to `otel-agent` pods in `observability`. Application pods used 1–4 mCPU and 3–5 MiB at idle; node agents used 70–159 mCPU and 103–122 MiB. Subsequent configuration changes have fresh CI checks. Deployed acceptance on 2026-10-24: a sampled approved and declined flow produced connected traces across all six service identities, including fraud spans under Payments and notifier processing and delivery spans in the same traces. A valid flow with the exporter unreachable succeeded, and all six pods exited 0 within 2–3 s of termination; `fraud-service` logged the expected `telemetry shutdown incomplete` warning. Restore the Argo root to `main` and replace temporary PR image pins before merging. Source/configuration changes alone do not establish deployed acceptance. Roll back using matching prior application images and charts; nullable metadata columns may remain.

Later Go fraud adapters will call Feast feature serving and KServe inference directly using their supported protocols and instrumentation. Their rollout must verify feature/model version diagnostics, deadlines, cold starts and trace propagation. No custom Python coordinator is required.
