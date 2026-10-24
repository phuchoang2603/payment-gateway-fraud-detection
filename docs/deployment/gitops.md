# GitOps deployment

GitOps manages the prod-only Kubeflow workspace and the minimum payment gateway. [talos-proxmox](https://github.com/phuchoang2603/talos-proxmox/blob/main/docs/operations/kubeflow.md) owns the shared Talos clusters, Argo CD, Kubeflow controllers, Dex SSO, External Secrets, Argo Workflows, and ingress. This repository owns its Profile, gateway workloads and service policies.

The Argo root is [`infra/argocd/prod/root.yaml`](../../infra/argocd/prod/root.yaml). Its children target the prod cluster. Gateway workloads use `payment-gateway`; Profile resources use `argo-cd` as their Argo destination so Argo does not create the Profile-owned `fraud-ml-prod` namespace first.

| Environment | Root                        | Enabled child Application                   | Helm release         | Configuration                                       |
| ----------- | --------------------------- | ------------------------------------------- | -------------------- | --------------------------------------------------- |
| Prod        | `payment-gateway-prod-root` | `payment-gateway-prod-secret-store`         | `secret-store`       | `values-prod.yaml`: shared Doppler `prd` ClusterSecretStore, token in `operators` |
| Prod        | `payment-gateway-prod-root` | `payment-gateway-prod-kubeflow-workspace`   | `kubeflow-workspace` | Profile and namespace resources in `fraud-ml-prod`  |

Gateway children are `payment-gateway-prod-{payment-gateway,kafka}`. The `infra/charts/payment-gateway` application chart owns all six Go services, including fraud, through one `services` map plus the gateway database, the operator-managed Redis resource, secrets, routing and policies. The CNPG cluster owns four databases with separate roles; Payments uses an owner for migrations, `payments_app` at runtime, and a separate replication principal restricted to the event ledger. Redis is one operator-managed standalone Valkey CR with noeviction and a password; only Payments is admitted. Kafka/Connect publish ledger bytes to the internal `payments.events.v1` topic. Only Edge `/checkout/*` is publicly routed; JSON APIs stay ClusterIP-only.

The child template renders each `valuesFile` entry separately using its `$values/` repository reference and explicitly sets the Helm release name. The fraud-service internal Service is `fraud-service` in the target cluster's `payment-gateway` namespace. Fraud uses the same deployment, service, telemetry, health and security templates as the other Go services.

Gateway service entries inherit `defaults`; `imageRepository` and `imageTag` supply shared image coordinates. A service's `database` selects its database, URL secrets and migration init container; `database.databases` defines owner and additional roles, including Payments' separate runtime role. Service ports also supply probe, route and policy ports, and mesh callers supply network ingress selectors. The chart uses one `externalSecrets` store reference and refresh interval.

The Go application pods are stateless: they use no PVCs or host-path storage. Their read-only Secret volumes hold integration configuration and encryption/signing keys. Each service ExternalSecret's `mountPath` and data-entry `env` generate the volume, mount and file-path environment variable together. Postgres owns durable application data and its persistent volume; Redis disables disk persistence and remains a disposable idempotency cache.

The workspace chart owns the `fraud-ml-prod` Kubeflow Profile (with the same email identity as platform Dex), the namespace-scoped Hub Registry and its database ExternalSecret, RustFS artifact credentials via ExternalSecret, a KFP launcher, and an Argo artifact repository. The generic `secret-store` chart is disabled by default and enabled only through its prod values file. Its token has read-only access to the `payment-gateway-fraud-detection` Doppler project's `prd` config, which holds `KUBEFLOW_REGISTRY_DB_PASSWORD`, `KUBEFLOW_RUSTFS_ACCESS_KEY`, and `KUBEFLOW_RUSTFS_SECRET_KEY`. No secret values are committed to Git. Reconcile the platform first: its Profile controller creates the namespace with ambient mesh enabled. The prod store and workspace Applications retry while the platform CRDs and Profile namespace become available. Kubernetes-native Pipelines are experimental, and the Hub Registry database uses local storage without HA; do not use this workspace with valuable production data until end-to-end validation, backup, and restore are in place.

Provision Kubernetes Secret `fraud-doppler-token` with key `dopplerToken` in the `operators` namespace, using `ESO_DOPPLER_TOKEN` from the consumer Doppler `prd` config. Do not commit or print the token. The ClusterSecretStore and the gateway and workspace ExternalSecrets can then reconcile; re-provision only on token rotation. Delete any `fraud-doppler-token` left in `fraud-ml-prod`. The RustFS access key currently matches the shared platform's bucket-scoped identity, so separate Doppler ownership does not yet imply separate S3 access; provision a distinct RustFS identity before requiring storage isolation. Verify Profile ownership, ExternalSecret readiness, authenticated Dashboard access, and a real pipeline artifact upload and download before calling the workspace operational.

## Revisions and branch rollouts

Each root uses an anchored `targetRevision` (normally `main`) and reuses `*revision` in `spec.source.helm.valuesObject.global.targetRevision`. For a branch rollout, change the anchored value in the root for that environment to the branch or commit SHA. YAML resolves the same revision for the root catalog and child chart/value sources. Commit the chart changes on that branch, then submit the updated root to that environment's Argo CD using the normal GitOps process. Changing only the live root's `targetRevision` through the Argo UI does not update the embedded child revision: update both fields together or use the anchored source file. The tracked gateway root targets `main`; a temporary branch rollout must be restored to `main` before merge.

Every PR builds all service images. Same-repository PRs publish `pr-<number>-<full-sha>` tags using the built merge commit; fork PRs build without publication because their tokens cannot write packages. Main pushes publish `main-<full-sha>` and `latest`. After all main image builds succeed, one CI job pins the gateway and Kafka production charts to `main-<full-sha>` and commits the values with `[skip ci]`. The `[skip ci]` marker prevents the automated pin commit from triggering rebuilds; manual production-values changes trigger image builds normally. Feature branches do not publish on push or change production pins. There is no manual release dispatch. After merging, retarget the live Argo root and its children to `main` and verify the rollout. PR closure cancels that PR's active image build and deletes GHCR versions from the shared publishing matrix carrying a matching `pr-<number>-<full-sha>` tag, including versions with additional tags. Versions without a matching PR tag are untouched. Publishing and cleanup share `.github/service-images.json`; cleanup verifies package repository ownership and never reads production charts. Failed cleanup can be retried with the cleanup workflow’s manual input for a closed PR. The new cleanup policy takes effect once its workflow and script are on the default branch.

## Runtime configuration

All services validate configuration at startup and exit non-zero with a sanitized diagnostic on invalid values. Boolean switches accept only `true` or `false` (case-insensitive).

| Service | Variable                           | Default                                                  | Purpose                                                          |
| ------- | ---------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------------- |
| fraud   | `GRPC_ADDR`                        | `:8000`                                                   | Internal gRPC listener                                           |
| fraud   | `SHUTDOWN_TIMEOUT` | `10s` | Bounded RPC drain (maximum 60s) |
| all Go | `METRICS_ENABLED` | `false` outside Helm | Explicit OTLP metric export |
| fraud   | `TRACING_ENABLED`                  | `false`                                                  | Opt-in instance-owned OpenTelemetry provider                     |
| fraud   | `OTEL_SERVICE_NAME`                | `fraud-service`                                          | Identity for logs, metrics and traces                            |
| fraud   | `OTEL_EXPORTER_OTLP_ENDPOINT`      | unset                                                    | Required for either enabled signal; OTLP/gRPC host:port |
| edge    | `EDGE_HTTP_ADDR`                   | `:8080`                                                  | Public HTTP listener                                             |
| edge    | `EDGE_SHUTDOWN_TIMEOUT`            | `10s`                                                    | HTTP drain budget                                                |
| edge | `EDGE_INTEGRATIONS_FILE`, `EDGE_PAYMENTS_ADDR`, `EDGE_VAULT_ADDRESS` | required / chart values | Hashed integration config and internal dependencies |
| payments | `PAYMENTS_DATABASE_URL`, `PAYMENTS_MIGRATE_DATABASE_URL` | required / owner fallback | Separate runtime and migration identities |
| payments | `PAYMENTS_REDIS_ADDR`, `PAYMENTS_REDIS_PASSWORD_FILE`, `PAYMENTS_IDEMPOTENCY_TTL` | chart values / `24h` | First idempotency check; DB fallback within a 50 ms Redis deadline |
| payments | `PAYMENTS_ATTEMPT_BUDGET`, `PAYMENTS_FRAUD_TIMEOUT`, `PAYMENTS_PROCESSOR_TIMEOUT` | `5s`, `1s`, `3s` | Bounded attempt and dependency budgets |
| notifier | `NOTIFIER_DATABASE_URL`, `NOTIFIER_INTEGRATIONS_FILE`, `NOTIFIER_KAFKA_BROKERS` | required / chart values | Durable delivery state, webhook subscriptions and internal event feed |
| vault | `VAULT_DATABASE_URL`, `VAULT_ENCRYPTION_KEY_FILE`, `VAULT_PROCESSING_ADDR` | required / required / `:8001` | Vault-owned storage/key and the separate listener for processor-only fixture resolution |
| processor | `PROCESSOR_DATABASE_URL`, `VAULT_ADDRESS` | required | Processor-owned store and the vault processing listener (port 8001) |
| processor | `PROCESSOR_LATENCY_MIN`, `PROCESSOR_LATENCY_MAX`, `PROCESSOR_TIMEOUT_RATE` | prod `20ms`, `150ms`, `0` | Nominal simulator latency and optional injected lost responses |

The common gateway chart sets `terminationGracePeriodSeconds: 30` to cover the 10 s drain, two independently bounded 3 s telemetry shutdowns and scheduling margin.

## Internal access and rollout prerequisites

Choose the intended workload cluster context and inspect the service with:

```bash
kubectl --context <workload-context> -n payment-gateway port-forward svc/fraud-service 8000:8000
```

The gateway chart creates a `ClusterIP` for fraud exposing gRPC 8000. Kubernetes 1.27+ probes the standard gRPC health service using the `liveness` and `readiness` names. Fraud has no Gateway, HTTPRoute or public ingress. Production rollout acceptance remains outstanding; a later serving increment replaces this layout with a cluster-local Knative Service and a KServe predictor (Knative and KServe serving). Public HTTP traffic belongs at Edge. Gateway Go/fraud pods enroll individually in the existing Istio ambient mesh with strict mTLS and service-account caller policies; the vault serves Edge methods on port 8000 and fixture resolution on port 8001, and its `AuthorizationPolicy` admits only Edge to 8000 and only the processor to 8001. Services speak plaintext gRPC (h2c) inside the mesh; there is no application TLS. Use matching published `main-<full-sha>` image tags for a real rollout, replacing the development `latest` defaults.

The [shared observability guide](shared-observability.md) describes OTLP configuration, metric names, stdout correlation and outstanding deployed acceptance. No application dashboard or alert is installed; telemetry integration is not a rollout prerequisite. Image publication is defined by the [release workflow](../../.github/workflows/release.yml).

## Gateway secrets and local checks

Gateway and workspace ExternalSecrets use the `fraud-doppler` ClusterSecretStore, reading Doppler `payment-gateway-fraud-detection/prd`. Its `secretStore.namespaces` allow-list in `infra/charts/secret-store/values-prod.yaml` covers `fraud-ml-prod`, `fraud-data-prod` and `payment-gateway`, and its bootstrap Secret `fraud-doppler-token` (key `dopplerToken`, from `ESO_DOPPLER_TOKEN`) lives in `operators`, so it survives Profile namespace recreation. The `secret-store` child must sync before the gateway child. Do not hand-create application secrets. Required Doppler keys are:

- `GATEWAY_DB_PAYMENTS_PASSWORD`, `GATEWAY_DB_PAYMENTS_APP_PASSWORD`, `GATEWAY_DB_PAYMENTS_CDC_PASSWORD`, `GATEWAY_DB_NOTIFIER_PASSWORD`, `GATEWAY_DB_VAULT_PASSWORD`, `GATEWAY_DB_PROCESSOR_PASSWORD`.
- `GATEWAY_REDIS_PASSWORD`, `GATEWAY_PAYMENTS_ENCRYPTION_KEY`, `GATEWAY_CHECKOUT_TOKEN_KEY`, `GATEWAY_VAULT_ENCRYPTION_KEY`, `GATEWAY_FINGERPRINT_KEY`, `GATEWAY_CHECKOUT_COOKIE_KEY`.
- `GATEWAY_INTEGRATIONS_JSON`.

`GATEWAY_INTEGRATIONS_JSON` is an array containing `integration_id` (UUID), `name`, `api_key_sha256` (hex SHA-256), `synthetic`, and optional `webhook: {url, secret, events}`. The synthetic ID must match the historical dataset; `EDGE_SYNTHETIC_FINGERPRINT_KEY=synthetic-fingerprint-v1` is non-secret. Real integrations use the secret fingerprint key. The marketplace receiver is `http://web.ecommerce.svc.cluster.local:8080/webhooks/payment-gateway`, with subscriptions `attempt.declined`, `payment.succeeded`, `payment.expired`. Keep notifier egress destinations in its chart values aligned with configured webhook URLs; HTTPS endpoints require corresponding explicit destination rules.

Enter `devenv shell`, which generates bindings and sqlc queries. For a relevant module, use `cd src/payment-gateway/<module>; GOWORK=off go test -short ./...; GOWORK=off golangci-lint run ./...`. Full `go test -race -count=1 ./...` starts shared testcontainers-go dependencies for PostgreSQL, Valkey and Kafka as needed, both locally and in CI. Docker is required unless `-short` is set; there are no `TEST_*` overrides, silent integration skips or CI service containers. The single `src/payment-gateway/shared` Go module groups protobuf bindings, test utilities and common runtime/security packages; services use `replace ../shared` and build with `GOWORK=off`. The optional local workspace is `src/payment-gateway/go.work`; run workspace commands from that directory or a service directory. `bash scripts/ci-checks.sh go` discovers and checks every service and the shared module; CI owns the full check/image sweep. End-to-end smoke and access checks run only on the prod Kubernetes cluster after GitOps rollout, without manually running services or databases in Docker.

## Gateway verification

The 2026-10-23 acceptance run against the prod cluster is recorded in the [gateway verification evidence](../../openspec/changes/archive/2026-10-23-build-minimum-payment-gateway/verification.md). It covers synthetic tokenization and the approve, risk-decline, processor-decline, timeout-then-recovery and expiry paths, a decline-then-approve purchase through the hosted page with a real integration and signed webhooks, Redis fallback, Connect restart and Kafka outage catch-up, byte-equal topic payloads, and the ledger lifecycle check. Access evidence for vault resolution, database roles and Redis is in the same file.

CI runs the contracts, Python, Helm and Go job suites on each PR. Every PR builds all images, and same-repository PRs publish disposable PR tags. Main pushes publish durable main tags. After all main image builds succeed, CI pins their shared main tag in the production values and Argo CD syncs it without manual image edits. Local development uses `devenv shell` and the short or testcontainer-backed test commands above; end-to-end smoke and access checks run only on the prod cluster, as recorded in the evidence file.

The [task checklist](../../openspec/changes/archive/2026-10-23-build-minimum-payment-gateway/tasks.md) retains two incomplete release-verification tasks for this archived change. A short smoke run does not establish the monthly availability objective or the nominal load target, and none of it establishes PCI DSS compliance or bank settlement behavior.
