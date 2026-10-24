Verification policy: do not run any tests locally. CI owns unit, integration, smoke, end-to-end and acceptance test execution, including tests against deployed environments. Local checks may include lint, formatting, schema validation and diff review. Inspect CI results and leave pending or failed verification unchecked.

## 0. Go fraud replacement and planning prerequisite

- [x] 0.1 Complete the `service-conventions` delta: Go fraud ownership, OTLP metrics instead of the Python endpoint, and vendor Feast/KServe protocols behind adapters.
- [x] 0.2 Replace `src/fraud-service` with an independently deployable Go module at `src/payment-gateway/fraud`, following the gateway composition-root/domain/transport shape. Reuse `fraud.v1` and existing decision fixtures; verify validation, deterministic policy/version reporting, multiple reason matches, decline-as-OK, health, bounded shutdown and Payments fail-closed behavior.
- [x] 0.3 Wire the Go fraud image, workspace/module, common gateway chart services map, CI and code generation; remove the dedicated fraud chart/Argo CD child and use the shared gateway production image pin; remove the Python fraud runtime, Dockerfile, unused bindings/dependencies and obsolete scripts/jobs/ports. Preserve unrelated Python tooling, update stale language/path context in `openspec/config.yaml`, and add no wrapper, dual implementation or legacy endpoint. Keep Feast/KServe clients/deployment and autoscaling for their later change.

## 1. Shared Go telemetry lifecycle

- [x] 1.1 Add small shared logging/tracing/metrics helpers following marketplace `shared/observe`; pin compatible existing SDK versions, require explicit export configuration, and wire bounded provider startup/shutdown in the six composition roots without domain dependencies.
- [x] 1.2 Extend focused lifecycle/logging coverage for invalid enabled configuration, disabled export, unreachable backends, cancelled shutdown contexts, actual trace/log correlation and sensitive-field exclusion; avoid duplicate per-service helper tests.

## 2. Transport and application measurements

- [x] 2.1 Instrument edge route templates, all Go gRPC clients/servers including Payments-to-fraud, fraud policy evaluation and notifier HTTP delivery; reuse existing handlers and preserve request IDs, suppress health probes and avoid capturing payloads, sensitive headers or arbitrary baggage.
- [x] 2.2 Map every existing business/RPC metric, including the old Python fraud measurements, to its OTLP name, unit and bounded attributes. Record one fraud decision per outcome, count reason matches separately, distinguish policy from full RPC latency, and keep declines out of RPC failure counts. Remove superseded Prometheus registries/listeners, ports and stale references while preserving health endpoints.

## 3. Durable event and retry context

- [x] 3.1 Add nullable transport context for newly appended payment events and persisted notifier deliveries. Carry it through the pinned Debezium EventRouter and Kafka headers using the marketplace Properties/W3C pattern, with no changes to business protobuf bytes, event order, existing immutable rows or webhook signatures.
- [x] 3.2 Extend current event-publication/notifier integration coverage to prove valid parentage across replay/restart, distinct delivery attempt spans and normal processing when context is missing/malformed. Verify exact committed event bytes and acknowledgements remain unchanged.

## 4. Deployment and acceptance

- [x] 4.1 Configure the six Go workload identities/annotations, explicit enablement, node-agent OTLP endpoint, 60-second metrics and initial parent-based 10% root sampling. Keep `fraud-service` as the stable fraud telemetry identity across replicas/revisions, with instance/revision metadata separate. Remove obsolete metric scraping configuration and retain generic chart CI; deploy no application dashboards/collectors.
- [x] 4.2 Run relevant non-test Go lint/format and chart lint/render checks locally; let CI run all tests, including fraud behavior and event-publication/notifier integration coverage, and image builds. Inspect CI results. Update only the affected shared-observability/deployment sections with final config, metric mapping and actual verification limits.
- [x] 4.3 After the independent HyperDX platform rollout is healthy, use CI to exercise a bounded synthetic payment/notification flow against the deployed environment covering all six Go services with deterministic sampling, including approved and declined fraud decisions. Read back fresh logs, metrics and traces, verify connected Payments/fraud spans, trace-to-log and asynchronous causality, inspect for sensitive values and record collector/application resource usage.
- [x] 4.4 Through CI, repeat a valid business flow with the exporter unreachable and demonstrate readiness, event processing and bounded shutdown. Keep live acceptance unchecked until these checks succeed; rollout must preserve the existing application image/chart rollback path.
- [x] 4.5 Refactor the shared gateway chart to derive images, database/migration wiring, Secret mounts and probe/policy ports from shared values; remove redundant overrides and unused options. Validate default/prod manifest equivalence and document stateless application pods versus database storage.

Implementation status: PR #87 passed all 28 checks at `b8cfbaf`, including Go tests and all seven image builds. Live acceptance on 2026-10-24 ran against the prod Argo root and children on `t3/go-observability-single-replica` with `pr-87-765d9f14b2ab5a2cc88a0150df657f4342aed89a` images. A sampled approved and declined (`high_amount`) payment flow produced connected traces across all six Go identities, including `fraud.v1.FraudService/Decide` and `fraud.policy.evaluate` under Payments, and notifier `payments.events.v1 process` and `webhook.delivery.attempt` spans in the same traces. A valid flow with the exporter unreachable succeeded, and all six pods exited 0 within 2–3 s of termination, inside the 3 s shutdown budget; `fraud-service` logged the expected `telemetry shutdown incomplete` warning. Restore the root to `main` and replace temporary PR image pins before merging.

Local non-test checks passed on 2026-10-24: golangci-lint in all seven Go modules, gofumpt, all Helm chart lint/render configurations, strict OpenSpec validation, Nix/shell/JSON syntax and git diff whitespace checks. No tests or image builds were run locally.
