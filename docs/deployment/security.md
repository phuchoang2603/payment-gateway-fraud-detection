# Security and secrets

## Central secret delivery

Doppler → External Secrets → Kubernetes Secret → workload `secretKeyRef`. Credentials stay in Doppler; endpoints, bucket names and other nonsecret settings stay in [chart values](../../infra/charts).

| Configuration | Source |
|---|---|
| `gateway-doppler` store, project `payment-gateway-fraud-detection`, configs `dev` / `prd` | [SecretStore template](../../infra/charts/secret-store/templates/secret-store.yaml), [prod values](../../infra/charts/secret-store/values-prod.yaml) |
| Database, Redis, encryption, integration and object-storage key mappings | [Gateway ExternalSecrets](../../infra/charts/payment-gateway/templates/external-secrets.yaml), [database secrets](../../infra/charts/payment-gateway/templates/database-external-secrets.yaml), [service values](../../infra/charts/payment-gateway/values.yaml) |
| LLM/RAG client keys and backing-service credentials | [LLM chart](../../infra/charts/llm-platform), [RAG chart](../../infra/charts/rag-pipeline) |

Bootstrap `payment-gateway/doppler-token` (`dopplerToken`) from a read-only token for the environment. The store restricts eligible namespaces. After rotation, restart affected workloads so environment variables refresh, using the [canary flow](cd.md#edge-progressive-delivery) for Edge.

One storage identity per environment serves the generator, Flink, lakehouse and metadata jobs. Prod uses GCS HMAC keys for the shared bucket; `datasets/`, `runs/` and `lakehouse/` prefixes are conventions, not authorization boundaries.

## Access boundaries

- [Istio policies](../../infra/charts/payment-gateway/templates/mesh.yaml) enforce strict mTLS and service-account authorization. Vault admits Edge on port 8000 and processor resolution on 8001.
- [Network policies](../../infra/charts/payment-gateway/templates/network-policy.yaml) restrict pod traffic; [database policies](../../infra/charts/payment-gateway/templates/database-network-policy.yaml) isolate database access.
- Each service owns a database with separate migration/runtime roles; Payments CDC has a replication identity.
- Public exposure and authentication are described in [routing and gateway](routing.md).

Card data is synthetic. The vault encrypts fictional fixtures and issues opaque tokens; raw card data stays out of logs, events, Fraud, Redis and the lake. This demonstrates PCI DSS-oriented isolation, not compliance.

## Verification

[Recorded gateway acceptance](ci.md#deployed-verification) covers synthetic tokenization, authenticated checkout and signed webhook delivery. Central secret delivery is configured in the linked manifests; a sanitized ExternalSecret readiness capture is still needed for the rubric's visual proof.
