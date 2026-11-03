# Security, secrets and routing

## Central secret delivery

Doppler → External Secrets → Kubernetes Secret → workload `secretKeyRef`. Credentials stay in Doppler; endpoints, bucket names and other nonsecret settings stay in [chart values](../../infra/charts).

| Configuration | Source |
|---|---|
| `gateway-doppler` store, project `payment-gateway-fraud-detection`, configs `dev` / `prd` | [SecretStore template](../../infra/charts/secret-store/templates/secret-store.yaml), [prod values](../../infra/charts/secret-store/values-prod.yaml) |
| Database, Redis, encryption, integration and object-storage key mappings | [Gateway ExternalSecrets](../../infra/charts/payment-gateway/templates/external-secrets.yaml), [database secrets](../../infra/charts/payment-gateway/templates/database-external-secrets.yaml), [service values](../../infra/charts/payment-gateway/values.yaml) |
| LLM/RAG client keys and backing-service credentials | [LLM chart](../../infra/charts/llm-platform), [RAG chart](../../infra/charts/rag-pipeline) |

Bootstrap `payment-gateway/doppler-token` (`dopplerToken`) from a read-only token for the environment. The store restricts eligible namespaces. After rotation, restart affected workloads so environment variables refresh, using a new Edge Rollout revision when its environment changes.

One storage identity per environment serves the generator, Flink, lakehouse and metadata jobs. Prod uses GCS HMAC keys for the shared bucket; `datasets/`, `runs/` and `lakehouse/` prefixes are conventions, not authorization boundaries.

## Access boundaries

- [Istio policies](../../infra/charts/payment-gateway/templates/mesh.yaml) enforce strict mTLS and service-account authorization. Vault admits Edge on port 8000 and processor resolution on 8001.
- [Network policies](../../infra/charts/payment-gateway/templates/network-policy.yaml) restrict pod traffic; [database policies](../../infra/charts/payment-gateway/templates/database-network-policy.yaml) isolate database access.
- Each service owns a database with separate migration/runtime roles; Payments CDC has a replication identity.

Card data is synthetic. The vault encrypts fictional fixtures and issues opaque tokens; raw card data stays out of logs, events, Fraud, Redis and the lake. This demonstrates PCI DSS-oriented isolation, not compliance.

## Secret-delivery verification

Gateway acceptance on 2026-10-23 covers synthetic tokenization, authenticated checkout and signed webhook delivery. Central secret delivery is configured in the linked manifests; a sanitized ExternalSecret readiness capture is still needed for the rubric's visual proof.

## Routing and gateway

Istio Gateway API is the platform equivalent of the rubric's NGINX ingress. The app owns its Gateway and HTTPRoute; the platform owns the controller and Cloudflare tunnel.

### Public checkout

`Client HTTPS → Cloudflare → tunnel → Istio Gateway → edge / edge-canary`

| Setting | Configuration |
|---|---|
| Dev / prod hostname | `pay-gateway-dev.phuchoang.sbs` / `pay-gateway.phuchoang.sbs` in [environment values](../../infra/charts/payment-gateway) |
| Tunnel and ClusterIP Gateway | [`ingress.yaml`](../../infra/charts/payment-gateway/templates/ingress.yaml) |
| `/checkout/` prefix, hostname, HTTPS forwarding header and timeouts | [`http-route.yaml`](../../infra/charts/payment-gateway/templates/http-route.yaml) |
| Stable/canary weights | [Rollouts strategy](../../infra/charts/payment-gateway/templates/_rollout.tpl); prod acceptance on 2026-11-03: 10/25/50% stages, then promotion |

TLS terminates at Cloudflare; the tunnel targets the internal HTTP listener. Public `/v1/payments` has no route and returned HTTP 404 during prod acceptance. Internal callers use the stable Edge Service; Fraud and the other JSON/gRPC APIs are ClusterIP-only under strict mTLS and service-account authorization.

### Authentication and other gateways

Hosted checkout uses bounded checkout credentials; internal merchant calls use integration API keys hashed with SHA-256. This is separate from the rubric's username/password gateway example.

| Surface | Authentication and policy |
|---|---|
| Models, MCP tools and agents | agentgateway: per-client API keys, rate/token limits and mesh-only backend access; dev checks returned 401 without a key and 20 × 429 in a 160-request burst |
| Grafana and other platform UIs | Platform-owned ingress and access configuration in [`talos-proxmox/apps`](https://github.com/phuchoang2603/talos-proxmox/tree/main/apps) |

UI access-control screenshots and a dedicated feature-API gateway rate-limit acceptance remain missing.
