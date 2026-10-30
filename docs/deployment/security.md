# Security

## Secrets

All credentials live in Doppler and reach Kubernetes through External Secrets; nothing secret is in Git or rendered manifests.

- Application ExternalSecrets use the `gateway-doppler` store, selecting the `payment-gateway-fraud-detection/dev` or `prd` config. Its bootstrap Secret is `payment-gateway/doppler-token` (key `dopplerToken`), created from a read-only token for that config.
- Services read secrets as environment variables through `secretKeyRef`; there are no secret file mounts. Restart the affected Deployment after rotation.
- Required gateway keys:
  - databases: `GATEWAY_DB_PAYMENTS_PASSWORD`, `GATEWAY_DB_PAYMENTS_APP_PASSWORD`, `GATEWAY_DB_PAYMENTS_CDC_PASSWORD`, `GATEWAY_DB_FRAUD_PASSWORD`, `GATEWAY_DB_FRAUD_APP_PASSWORD`, `GATEWAY_DB_NOTIFIER_PASSWORD`, `GATEWAY_DB_VAULT_PASSWORD`, `GATEWAY_DB_PROCESSOR_PASSWORD`;
  - Redis and keys: `GATEWAY_REDIS_PASSWORD`, `GATEWAY_AUTHORIZATION_REDIS_PASSWORD`, `GATEWAY_FEAST_REDIS_PASSWORD`, `GATEWAY_PAYMENTS_ENCRYPTION_KEY`, `GATEWAY_CHECKOUT_TOKEN_KEY`, `GATEWAY_VAULT_ENCRYPTION_KEY`, `GATEWAY_FINGERPRINT_KEY`, `GATEWAY_CHECKOUT_COOKIE_KEY`;
  - integrations: `GATEWAY_INTEGRATIONS_JSON`, `OPENMETADATA_TOKEN`, and `GATEWAY_SYNTHETIC_API_KEY` when simulation is enabled;
  - storage and catalog: `OBJECT_GATEWAY_ACCESS_KEY_ID`, `OBJECT_GATEWAY_SECRET_ACCESS_KEY`, `CLICKHOUSE_ADMIN_PASSWORD`, `CLICKHOUSE_OPENMETADATA_PASSWORD`.
- One gateway storage identity per environment is shared by the generator, Flink, lakehouse runtime and metadata connections, scoped to `datasets/payment-gateway/`, `runs/payment-gateway/` and `lakehouse/`.

`GATEWAY_INTEGRATIONS_JSON` is an array of `integration_id`, `name`, `api_key_sha256`, `synthetic` and an optional `webhook: {url, secret, events}`. The synthetic integration ID must match the historical dataset.

## Service-to-service authorization

- Gateway pods enroll in the Istio ambient mesh with strict mTLS and service-account `AuthorizationPolicy` rules. Services speak plaintext gRPC inside the mesh.
- The vault serves Edge on port 8000 and processor-only fixture resolution on port 8001; its policy admits only Edge to 8000 and only the processor to 8001.
- Each service owns its own database with separate migration and runtime roles; Payments has a replication principal limited to the event ledger.
- Fraud and all JSON APIs are ClusterIP-only. Only Edge `/checkout/*` is publicly routed, over HTTPS.

## Card data

Card data is synthetic. Only Edge tokenizes allow-listed fictional cards; stored fixtures are encrypted with a vault-only key; tokens, raw card fields and fixtures never appear in logs, events, Fraud, Redis or the lake. This demonstrates a PCI DSS-oriented boundary, not compliance.
