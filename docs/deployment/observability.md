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

Fraud's [Feast adapter](../../src/payment-gateway/fraud/internal/adapters/feast/client.go) retrieves features; Triton calls record `fraud.model.calls`, `fraud.model.status` and `fraud.model.enforced`, with the model version and score attached to the assessment.

Trace context crosses gRPC and the Debezium event header into notifier retries. Bounded export queues/timeouts keep payment readiness and processing independent of telemetry availability. Spans exclude bodies, query strings, token-bearing paths and sensitive headers; metric labels exclude payment/customer/event IDs.

A sampled checkout GET at 18:15:21 UTC connects the Edge HTTP span, Edge gRPC client span and Payments server span in trace `ff6be5d585c2a57c11a65627699f03ec` (9.25 ms, HTTP 200). Explore displays UTC−05:00.

![Connected Edge and Payments checkout trace in Grafana](images/checkout-trace-2026-11-03.png)

Filtering VictoriaLogs by that exact trace ID returns two request log entries. The separate log-volume panel has no data; the log results below it establish correlation.

![Two request logs matching the checkout trace ID](images/checkout-trace-logs-2026-11-03.png)

## Dashboards and alerts

[Gateway dashboards](../../infra/charts/payment-gateway/dashboards) and [monitoring rules](../../infra/charts/payment-gateway/templates/monitoring.yaml) cover service operations and canary health. [Data dashboards](../../infra/charts/data-pipelines/dashboards) cover Kafka/Flink and job state. Grafana discovers their labelled ConfigMaps; VMAgent/VMAlert select application namespaces.

The gateway dashboard captures the fault spike and healthy retry over 16:45–18:20 UTC on 2026-11-03. Repeated `edge` legends represent separate series; candidate `787688ff9d` was rejected at 7.2% errors, then passed all three canary stages on retry.

![Gateway request rate, errors and HTTP/RPC latency during acceptance](images/gateway-operations-2026-11-03.png)

Over the same window, Kafka reports zero under-replicated partitions and Flink reports one running job. The cumulative failed-checkpoint counter remains at six, with no increase in this window. Negative consumer-lag values at the start reflect uncommitted offsets.

![Kafka and Flink metrics during the acceptance window](images/data-operations-2026-11-03.png)

Select the environment before comparing results. VictoriaMetrics preserves dotted OTLP names and resource labels; canary queries also select `rollout.revision`. Missing series remain **No data**, never a healthy zero.

Namespace-local rules cover sustained errors/latency, notification age, lag, failed jobs, checkpoint failures and missing signals. Shared Alertmanager currently routes to `blackhole`: alerts evaluate and are visible, but no external notifications are delivered. LLM routes also export request/token metrics; agent traces and prompt versions go to Langfuse.

Grafana Explore retains firing samples for `EdgeRolloutDegraded` during the aborted attempts (16:55–18:05 UTC; this Explore capture displays UTC−05:00). Gaps mean no firing series, not a measured zero. The rollout completed at 18:13 UTC.

![Historical firing samples for the Edge rollout failure alert](images/edge-rollout-alert-2026-11-03.png)
