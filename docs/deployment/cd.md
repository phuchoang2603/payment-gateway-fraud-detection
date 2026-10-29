# Continuous delivery

GitOps deploys the same application charts to dev and prod. CI publishes a complete release to a deployment branch; Argo CD in each cluster reconciles it. CI never contacts Kubernetes or holds cluster credentials, and it never commits to `main`.

| Environment | Release branch | Published by | Gateway hostname |
|---|---|---|---|
| Dev | `deploy/dev` | Trusted pull requests | `pay-gateway-dev.phuchoang.sbs` |
| Prod | `deploy/prod` | `main` | `pay-gateway.phuchoang.sbs` |

## Releases

- The roots are `infra/argocd/dev/root.yaml` and `infra/argocd/prod/root.yaml`. Each release records one chart source revision and per-service image tags in `infra/argocd/app-of-apps/release.yaml`.
- Images carry `pr-<number>-<full-sha>` or `main-<full-sha>` tags. The release planner (`scripts/release.py`) compares each image's build paths (`.github/service-images.json`) against its last deployed commit and rebuilds only changed images; unchanged images keep their tags, and an unreachable recorded commit counts as changed. Chart-only releases reuse every tag.
- Publication waits for all affected image builds and skips a superseded source revision. There are no image-pin commits or mutable `latest` tags.
- Bootstrap: create the `payment-gateway` namespace and its Doppler token, then apply the AppProject and the environment root once.

## Applications

| Chart | Namespace | Contents |
|---|---|---|
| `payment-gateway` | `payment-gateway` | The six Go services, their CNPG databases with separate owner and runtime roles, three Redis instances (idempotency, authorization observations, Feast), secrets, routing and policies |
| `kafka` | `payment-gateway` | Kafka, Debezium Connect, Karapace and topic declarations |
| `fraud-features` | `data` | Flink job, Feast writer and server, Feast Redis |
| `live-traffic` | `data` | Live simulator |
| `data-pipelines` | `data` | Generator and pipeline WorkflowTemplates |
| `metadata-integrations` | `data` | Catalog connections and discovery |
| `lakehouse-integration` | `data` | Polaris registration, StarRocks external catalog and reader, Iceberg storage secret |

Prod currently runs the gateway services with their databases, Redis and Kafka, plus the `data-pipelines` chart for the generator only. Live traffic, Flink/Feast, metadata integrations and the Bronze/Silver pipelines are disabled there. Application pods are stateless; Postgres and the authorization and Feast Redis instances own the persistent volumes.

## Runtime configuration

Python services take settings from configuration files and environment variables, never command-line flags. Every service validates its configuration at startup and exits non-zero with a sanitized diagnostic on invalid values; booleans accept only `true` or `false`.

| Service | Variable | Default | Purpose |
|---|---|---|---|
| fraud | `GRPC_ADDR`, `SHUTDOWN_TIMEOUT` | `:8000`, `10s` | Internal gRPC listener and drain (max 60s) |
| fraud | `FRAUD_DATABASE_URL`, `FRAUD_MIGRATE_DATABASE_URL` | required | Runtime and migration identities |
| fraud | `FRAUD_ASSESSMENT_DEADLINE`, `FRAUD_ASSESSMENT_LEASE`, `FRAUD_ASSESSMENT_RETRY_HORIZON`, `FRAUD_ASSESSMENT_ERROR_BACKOFF` | `1s`, `2s`, `5m`, `250ms` | Fenced completion and bounded recovery |
| fraud | `FRAUD_AUTH_REDIS_ADDR`, `FRAUD_AUTH_REDIS_PASSWORD`, `FRAUD_VELOCITY_EPOCH` | chart values | Authorization observation store and epoch |
| fraud | `FRAUD_GUARD_TIMEOUT`, `FRAUD_VELOCITY_THRESHOLD_10S`, `FRAUD_VELOCITY_THRESHOLD_60S` | `100ms`, `10`, `30` | Observe-only thresholds (unmeasured) |
| fraud | `FRAUD_FEAST_URL`, `FRAUD_FEATURE_EPOCH`, `FRAUD_FEATURE_DEFINITION_SHA256`, `FRAUD_FEATURE_MAX_ANCHOR_AGE`, `FRAUD_FEATURE_MAX_PROGRESS_AGE` | unset unless features enabled; `30s` budgets | Shadow feature retrieval |
| payments | `PAYMENTS_DATABASE_URL`, `PAYMENTS_MIGRATE_DATABASE_URL` | required | Runtime and migration identities |
| payments | `PAYMENTS_REDIS_ADDR`, `PAYMENTS_REDIS_PASSWORD`, `PAYMENTS_IDEMPOTENCY_TTL` | chart values, `24h` | Redis-first idempotency with a 50 ms deadline before database fallback |
| payments | `PAYMENTS_ATTEMPT_BUDGET`, `PAYMENTS_FRAUD_TIMEOUT`, `PAYMENTS_PROCESSOR_TIMEOUT` | `5s`, `1s`, `3s` | Attempt and dependency budgets |
| payments | `PAYMENTS_ASSESSMENT_RETRY_HORIZON`, `PAYMENTS_ASSESSMENT_BACKOFF` | `5m`, `1s` | Same-attempt assessment recovery |
| edge | `EDGE_HTTP_ADDR`, `EDGE_SHUTDOWN_TIMEOUT` | `:8080`, `10s` | Public listener and drain |
| edge | `EDGE_INTEGRATIONS_JSON`, `EDGE_PAYMENTS_ADDR`, `EDGE_VAULT_ADDRESS` | required, chart values | Integration configuration and dependencies |
| notifier | `NOTIFIER_DATABASE_URL`, `NOTIFIER_INTEGRATIONS_JSON`, `NOTIFIER_KAFKA_BROKERS` | required, chart values | Delivery state, subscriptions, event feed |
| vault | `VAULT_DATABASE_URL`, `VAULT_ENCRYPTION_KEY`, `VAULT_PROCESSING_ADDR` | required, required, `:8001` | Vault store, key and processor-only listener |
| processor | `PROCESSOR_DATABASE_URL`, `VAULT_ADDRESS` | required | Processor store and vault processing listener |
| processor | `PROCESSOR_LATENCY_MIN`, `PROCESSOR_LATENCY_MAX`, `PROCESSOR_TIMEOUT_RATE` | prod `20ms`, `150ms`, `0` | Simulated latency and injected lost responses |
| all Go | `METRICS_ENABLED`, `TRACING_ENABLED`, `OTEL_EXPORTER_OTLP_*_ENDPOINT` | disabled outside Helm | Telemetry export; see [observability](observability.md) |
| generator | `SIMULATOR_CONFIG`, `GENERATOR_MODE`, `SHARD_INDEX`, `SHARD_COUNT`, `GENERATOR_WORKERS` | set by the workflow | See [offline generator](../data/offline-generator.md) |
| live simulator | `SIMULATOR_CONFIG` | `/etc/simulator/live.yaml` | See [online generator](../data/online-generator.md) |

The gateway chart sets `terminationGracePeriodSeconds: 30` to cover the 10 s drain, two bounded 3 s telemetry shutdowns and scheduling margin.

## Gateway verification

The 2026-10-23 acceptance run against prod is recorded in the [gateway verification evidence](../../openspec/changes/archive/2026-10-23-build-minimum-payment-gateway/verification.md): synthetic tokenization; approve, risk-decline, processor-decline, timeout-then-recovery and expiry paths; decline-then-approve through the hosted page with signed webhooks; Redis fallback; Connect restart and Kafka outage catch-up; byte-equal topic payloads; and the ledger lifecycle check. A short smoke run does not establish the monthly availability objective or the nominal load target.
