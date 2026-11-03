# Observability

The platform owns VictoriaMetrics, VictoriaLogs, VictoriaTraces and Grafana. Application charts own telemetry settings, dashboards and alert rules; no separate application monitoring stack is deployed.

## Collection and signals

Six Go services export metrics and traces through [`shared/observe`](../../src/payment-gateway/shared/observe) over OTLP/HTTP. JSON stdout logs carry active `trace_id`/`span_id` fields for correlation. [Chart telemetry values](../../infra/charts/payment-gateway/values.yaml) configure the shared agents, export intervals and sampling; the binaries default to disabled.

| Signal | What it measures |
|---|---|
| HTTP / gRPC | Request rate, failures and latency; business declines are successful RPCs |
| Payments / Fraud / processor | Attempt duration, pending outcomes, Redis fallback, decisions and policy duration |
| Notifier | Delivery outcomes, pending age and consumer progress |
| Kafka / Flink | Broker replication, consumer lag, running jobs and checkpoint failures through native exporters |
| Workflow / Spark / Rollout | Shared controller signals and pod state; no executor-level Spark instrumentation |

Trace context crosses gRPC and the Debezium event header into notifier retries. Bounded export queues/timeouts keep payment readiness and processing independent of telemetry availability. Spans exclude bodies, query strings, token-bearing paths and sensitive headers; metric labels exclude payment/customer/event IDs.

## Dashboards and alerts

[Gateway dashboards](../../infra/charts/payment-gateway/dashboards) and [monitoring rules](../../infra/charts/payment-gateway/templates/monitoring.yaml) cover service operations and canary health. [Data dashboards](../../infra/charts/data-pipelines/dashboards) cover Kafka/Flink and job state. Grafana discovers their labelled ConfigMaps; VMAgent/VMAlert select application namespaces.

Select the environment before comparing results. VictoriaMetrics preserves dotted OTLP names and resource labels; canary queries also select `rollout.revision`. Missing series remain **No data**, never a healthy zero. The canary thresholds are acceptance settings, not measured SLOs.

Namespace-local rules cover sustained errors/latency, notification age, lag, failed jobs, checkpoint failures and missing signals. Shared Alertmanager currently routes to `blackhole`: alerts evaluate and are visible, but no external notifications are delivered. LLM-specific telemetry remains in [LLM observability](../llm/observability.md).

## Verification

Deployed acceptance on 2026-10-24 (change [`go-service-observability`](../../openspec/changes/archive/2026-10-24-go-service-observability/proposal.md)): a sampled approved and declined flow produced connected traces across all six service identities, including fraud spans under Payments and notifier delivery spans in the same trace. With the exporter unreachable, payments still succeeded and every pod exited within 3 s of termination. Application pods used 1–4 mCPU and 3–5 MiB at idle.

Fraud retrieves online features directly through its [Feast adapter](../../src/payment-gateway/fraud/internal/adapters/feast/client.go). Feature/model diagnostics, serving deadlines and cold starts require separate verification from Edge checkout canary analysis.

Prod verification on 2026-11-03 confirmed Kafka and Flink scrape targets, both operational dashboards in Grafana, native Workflow/Spark signals, candidate-specific OTLP series, a connected Edge/Payments trace and matching request logs. The rollout failure alert fired after an aborted candidate and resolved on retry. External alert delivery remains disabled by the shared `blackhole` receiver.

The gateway dashboard captures the fault spike and healthy retry over 16:45–18:20 UTC on 2026-11-03. Repeated `edge` legends represent separate series; the [AnalysisRun captures](cd.md#edge-progressive-delivery) identify the candidate hash and guard results. Checkout GET load verifies this route and its Payments dependency; it does not verify fraud decisions or webhook delivery.

![Gateway request rate, errors and HTTP/RPC latency during acceptance](images/gateway-operations-2026-11-03.png)

Over the same window, Kafka reports zero under-replicated partitions and Flink reports one running job. The cumulative failed-checkpoint counter remains at six, with no increase in this window. Negative consumer-lag values at the start reflect uncommitted offsets; they are not evidence of a healthy backlog.

![Kafka and Flink metrics during the acceptance window](images/data-operations-2026-11-03.png)

Grafana Explore retains firing samples for `EdgeRolloutDegraded` during the aborted attempts (16:55–18:05 UTC; this Explore capture displays UTC−05:00). Gaps mean no firing series, not a measured zero. The [completed Rollout](cd.md#edge-progressive-delivery) corroborates recovery; external notifications remain disabled.

![Historical firing samples for the Edge rollout failure alert](images/edge-rollout-alert-2026-11-03.png)

A sampled checkout GET at 18:15:21 UTC connects the Edge HTTP span, Edge gRPC client span and Payments server span in trace `ff6be5d585c2a57c11a65627699f03ec` (9.25 ms, HTTP 200). Explore displays UTC−05:00. This read-only checkout trace covers two services, not the full payment and notifier lifecycle.

![Connected Edge and Payments checkout trace in Grafana](images/checkout-trace-2026-11-03.png)

Filtering VictoriaLogs by that exact trace ID returns two request log entries. The separate log-volume panel has no data; the log results below it establish correlation.

![Two request logs matching the checkout trace ID](images/checkout-trace-logs-2026-11-03.png)
