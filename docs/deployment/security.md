# Security

## Secrets

All credentials live in Doppler and reach Kubernetes through External Secrets; nothing secret is in Git or rendered manifests.

- Application ExternalSecrets use the `gateway-doppler` store, selecting the `payment-gateway-fraud-detection/dev` or `prd` config. The platform owns the store and its read-only token Secret (`kube-system/gateway-doppler-token`), provisioned from Terraform; this repo declares no store or token.
- Services read secrets as environment variables through `secretKeyRef`; there are no secret file mounts. Restart the affected Deployment after rotation.
- Required gateway keys:
  - databases: `GATEWAY_DB_PAYMENTS_PASSWORD`, `GATEWAY_DB_PAYMENTS_APP_PASSWORD`, `GATEWAY_DB_PAYMENTS_CDC_PASSWORD`, `GATEWAY_DB_FRAUD_PASSWORD`, `GATEWAY_DB_FRAUD_APP_PASSWORD`, `GATEWAY_DB_NOTIFIER_PASSWORD`, `GATEWAY_DB_VAULT_PASSWORD`, `GATEWAY_DB_PROCESSOR_PASSWORD`;
  - Redis and keys: `GATEWAY_REDIS_PASSWORD`, `GATEWAY_FEAST_REDIS_PASSWORD`, `GATEWAY_PAYMENTS_ENCRYPTION_KEY`, `GATEWAY_CHECKOUT_TOKEN_KEY`, `GATEWAY_VAULT_ENCRYPTION_KEY`, `GATEWAY_FINGERPRINT_KEY`, `GATEWAY_CHECKOUT_COOKIE_KEY`;
  - integrations: `GATEWAY_INTEGRATIONS_JSON`, `OPENMETADATA_TOKEN`, and `GATEWAY_SYNTHETIC_API_KEY` when simulation is enabled;
  - storage and catalog: `OBJECT_GATEWAY_ACCESS_KEY_ID`, `OBJECT_GATEWAY_SECRET_ACCESS_KEY`, `CLICKHOUSE_ADMIN_PASSWORD`, `CLICKHOUSE_OPENMETADATA_PASSWORD`, `CLICKHOUSE_MARTS_PASSWORD`, `ICEBERG_POSTGRES_PASSWORD`, `ICEBERG_CLIENT_SECRET`, `ICEBERG_TOKEN_PRIVATE_KEY`, `ICEBERG_TOKEN_PUBLIC_KEY`;
  - LLM gateway clients: `LLM_GATEWAY_AGENTS_API_KEY`, `LLM_GATEWAY_BENCHMARK_API_KEY`, `LLM_GATEWAY_RAG_INGEST_API_KEY`, `LLM_GATEWAY_RAG_API_KEY`, `LLM_GATEWAY_CATALOG_API_KEY`, and the A2A signing key `KAGENT_GATEWAY_SIGNING_KEY`. The gateway reads them from one labelled Secret with a client id per key; each client also gets its own key Secret in the namespace it runs in. The models are self-hosted, so there is no model-provider key.
  - RAG knowledge store: `RAG_DB_WRITER_PASSWORD` (the pipeline, which owns the schema) and `RAG_DB_READER_PASSWORD` (the knowledge API, read-only).
  - Langfuse: `LANGFUSE_SALT`, `LANGFUSE_ENCRYPTION_KEY`, `LANGFUSE_NEXTAUTH_SECRET`, `LANGFUSE_POSTGRES_PASSWORD`, `LANGFUSE_REDIS_PASSWORD`, `LANGFUSE_INIT_PUBLIC_KEY`, `LANGFUSE_INIT_SECRET_KEY`, `LANGFUSE_INIT_USER_PASSWORD`. Its ClickHouse password is `CLICKHOUSE_LANGFUSE_PASSWORD`. Langfuse uses its own built-in email/password login (seeded by the init user); it is not a client of the platform Dex. The object-storage identity is mirrored by the platform into this project and read through the `gateway-doppler` store.
  - Platform Grafana datasource: `CLICKHOUSE_GRAFANA_PASSWORD` is mirrored by the platform into this project. The `clickhouse` chart ships the `ClickHouse marts` datasource definition as a ConfigMap discovered by the platform Grafana sidecar; the platform Grafana pod resolves the password from its own env.
- One gateway storage identity per environment is shared by the generator, Flink, lakehouse runtime and metadata connections, scoped to `datasets/payment-gateway/`, `runs/payment-gateway/` and `lakehouse/`.

`GATEWAY_INTEGRATIONS_JSON` is an array of `integration_id`, `name`, `api_key_sha256`, `synthetic` and an optional `webhook: {url, secret, events}`. The synthetic integration ID must match the historical dataset.

## Service-to-service authorization

- Gateway pods enroll in the Istio ambient mesh with strict mTLS and service-account `AuthorizationPolicy` rules. Services speak plaintext gRPC inside the mesh.
- The vault serves Edge on port 8000 and processor-only fixture resolution on port 8001; its policy admits only Edge to 8000 and only the processor to 8001.
- Each service owns its own database with separate migration and runtime roles; Payments has a replication principal limited to the event ledger.
- Fraud and all JSON APIs are ClusterIP-only. Only Edge `/checkout/*` is publicly routed, over HTTPS.

## Card data

Card data is synthetic. Only Edge tokenizes allow-listed fictional cards; stored fixtures are encrypted with a vault-only key; tokens, raw card fields and fixtures never appear in logs, events, Fraud, Redis or the lake. This demonstrates a PCI DSS-oriented boundary, not compliance.
