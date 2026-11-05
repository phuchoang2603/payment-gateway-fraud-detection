## Why

`risk-agent` has no tools: it tells analysts it cannot retrieve a payment's features or the fraud model's score. The online feature store (`payment_risk_v1`) and the Triton-served `payment-risk` model now exist, so the agent can be given a tool that scores a payment the way Go Fraud does and explains what stands out.

## What Changes

- **New MCP server `risk`**, its own Python package `src/llm/risk_serving` and image, deployed in `llm` by the `llm-platform` chart and reached by agents through the gateway at `/mcp/risk`, like `knowledge`.
- **Two read-only tools:**
  - `get_risk_features`: the five `payment_risk_v1` views for one customer and payment method, with each view's status (`available`, `missing`, `expired`, `future`, `error`) and values.
  - `score_payment_risk`: reads the same views, lays the row out in the served model's `feature_order` (a view that is not `available` becomes NaN, as in Go Fraud), calls Triton over Open Inference Protocol v2 binary, and returns the score, the model version, the decline threshold, and the features whose values stand out against the reference profile.
- **Input is the request fields, not a payment ID.** The agent passes the customer, the payment-method fingerprint and the attempt's request fields, so the score is a what-if re-score, never the stored decision. Evidence is a protobuf `bytea` in Fraud's database with no read API, so a lookup by payment ID would need a new Go endpoint.
- **Drivers are not attributions.** Triton FIL returns no SHAP values, so a driver is a feature's gain in the model times how unusual the value is against the training reference profile. The tool description and the prompt say so.
- **`risk-agent` in both environments** gets `mcpServers: [risk]`; its prompt seed drops the "cannot retrieve" wording, and the agent catalog version moves to 0.2.0.
- **KEDA for both MCP servers:** keep the knowledge API's combined REST and MCP scaling and add risk tool-call scaling, each between two and four replicas with a disruption budget. No risk Locust test is added; the existing knowledge load test stays.
- **Blue-green rollouts for both MCP servers:** active Services keep the gateway addresses stable, preview Services receive bounded pre-promotion MCP checks, and failed checks leave the active revision serving. KEDA targets the Rollouts, and Argo CD respects controller-owned replica counts and revision selectors.
- **Access:** mesh authorization admits the gateway, metrics scraping and bounded rollout-analysis calls to the `risk` pod, and Feast's NetworkPolicy admits the `risk` pod next to Go Fraud. The predictor and the drift API in `ml` already admit in-mesh callers, so no policy changes there.
- **Not in this change:** a payment-ID evidence lookup or any new Go endpoint, SHAP or any other model change, a step-up threshold (the served model packages only the decline threshold), tools that change anything, and live production deployment.

## Capabilities

### New Capabilities

- `risk-scoring-tool`: the `risk` MCP server, its two tools, how it builds the row from Feast and calls Triton, what it returns, how it stays consistent with the served model version, and its metrics.

### Modified Capabilities

- `agent-runtime`: the Risk agent requirement, autoscaling for both MCP servers, and their blue-green promotion behavior. The agent now has the scoring and feature tools and reports what they return.
- `agent-gateway`: a `/mcp/risk` route with the same key, rate limit and tool-call metrics as the knowledge route, and mesh authorization for gateway calls, metrics scraping and bounded rollout analysis. Phone masking uses word-boundary patterns instead of the `PhoneNumber` built-in, which altered digit runs inside payment-method fingerprints; it applies in every environment.

## Impact

- **Code:** new `src/llm/risk_serving` (a member of the `src/llm` uv workspace, with `uv.lock`), its Dockerfile `infra/docker/risk-serving.Dockerfile`.
- **Charts:** `infra/charts/llm-platform` (`risk.yaml` for the workload, Services, mesh policy, KEDA and scrape; server settings, prompt seed and registry catalog); `infra/charts/rag-pipeline` (knowledge API Rollout, preview Service, analysis and KEDA target); both charts' pre-promotion Jobs and their scoped identities; `infra/charts/data-pipelines` (Feast serve NetworkPolicy admits the `risk` pod). Both environments use the same risk tools, blue-green strategy, native Feast HTTP probes and controller ownership rules; only resource sizing and environment addresses differ.
- **CI and deploy:** `.github/service-images.json`, the `imageTags` in `infra/argocd/app-of-apps/values.yaml`, and shared Argo CD ignore rules for controller-owned fields and rollout adoption.
- **Docs:** `docs/llm/agents.md` (agents table, status and risk scaling), `docs/llm/gateway-and-jev-policy.md` (the MCP route), `docs/deployment/observability.md` (tool counters), `docs/llm/rag.md` (knowledge rollout and KEDA target), and `docs/ml/serving.md` (a one-line pointer).
- **Dependencies:** the `llm-platform` application, which is off on dev (`enabled: false` since 2026-11-01). Dev values enable both `llm-platform` and `rag-pipeline` for proof, with the platform-owned ML and LLM GPU claims referenced by name; model-server capacity remains outside this change.
- **Sequencing:** one replacement PR from `feat/risk-agent-scoring-review`; render both environments and deploy only dev. Production configuration adopts parity, but production deployment requires separate authorization.
