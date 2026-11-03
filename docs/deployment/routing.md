# Routing and gateway

Istio Gateway API is the platform equivalent of the rubric's NGINX ingress. The app owns its Gateway and HTTPRoute; the platform owns the controller and Cloudflare tunnel.

## Public checkout

`Client HTTPS → Cloudflare → tunnel → Istio Gateway → edge / edge-canary`

| Setting | Configuration |
|---|---|
| Dev / prod hostname | `pay-gateway-dev.phuchoang.sbs` / `pay-gateway.phuchoang.sbs` in [environment values](../../infra/charts/payment-gateway) |
| Tunnel and ClusterIP Gateway | [`ingress.yaml`](../../infra/charts/payment-gateway/templates/ingress.yaml) |
| `/checkout/` prefix, hostname, HTTPS forwarding header and timeouts | [`http-route.yaml`](../../infra/charts/payment-gateway/templates/http-route.yaml) |
| Stable/canary weights | [Rollouts strategy](../../infra/charts/payment-gateway/templates/_rollout.tpl); [acceptance evidence](cd.md#edge-progressive-delivery) |

TLS terminates at Cloudflare; the tunnel targets the internal HTTP listener. Public `/v1/payments` has no route and returned HTTP 404 during prod acceptance. Internal callers use the stable Edge Service; Fraud and the other JSON/gRPC APIs are ClusterIP-only under [mesh authorization](security.md#access-boundaries).

## Authentication and other gateways

Hosted checkout uses bounded checkout credentials; internal merchant calls use integration keys ([payment flows](../payment-gateway/flows.md)). This is separate from the rubric's username/password gateway example.

| Surface | Authentication and policy |
|---|---|
| Models, MCP tools and agents | [Agent gateway](../llm/gateway.md): per-client API keys, rate/token limits and mesh-only backend access; dev checks are recorded there |
| Grafana and other platform UIs | Platform-owned ingress and access configuration in [`talos-proxmox/apps`](https://github.com/phuchoang2603/talos-proxmox/tree/main/apps) |

The [Grafana captures](observability.md#verification) prove telemetry visibility, not an authentication or rate-limit test. UI access-control screenshots and a dedicated feature-API gateway rate-limit acceptance remain missing.
