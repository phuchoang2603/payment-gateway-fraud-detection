## Why

The only entry agent, `coordinator`, serves analysts, and every agent shares one gateway identity, so the gateway and mesh cannot tell which agent or which kind of user is calling. Merchants need their own assistant, and they must never see fraud scores, features or drivers, which would teach them to evade the model. Rules such as "never invent a customer id" live only in prompts. Splitting the system along trust boundaries gives each kind of user its own entry agent and identity, lets the specialists that share a conversation stay local, and puts a real agent-to-agent call through the A2A gateway where sensitive data crosses a boundary.

## What Changes

- **BREAKING:** `coordinator` becomes `analyst-assistant` at `/a2a/analyst-assistant`. `/a2a/coordinator` is removed, along with the coordinator's Langfuse prompt and catalog entry. Its local sub-agents stay `risk_agent` and `evidence_agent`.
- **New `merchant-assistant`** at `/a2a/merchant-assistant`, for merchants:
  - Local sub-agent: `evidence_agent` with merchant-audience knowledge only.
  - Tool: `get_payment_status` for the merchant's own payments.
  - Remote A2A tool: `review_decline`, which calls `decline-reviewer` through the gateway.
- **`review_decline` crosses the trust boundary.** `decline-reviewer` receives a merchant's payment id, then:
  - Reads the attempt's risk inputs, re-scores a risk decline and looks up the reason's merchant wording.
  - Returns only `{payment_id, reason_code, merchant_wording, review_outcome, retry_advice}`.
  - Never returns a score, feature value, driver or model detail. The runtime rejects any other reply shape.
- **Verified caller context:**
  - Each external client gets its own gateway key. Entry agents use separate native egress origins with caller-token passthrough.
  - Key-restricted gateway routes sign short-lived client JWTs; kagent propagates the verified bearer credential and principal natively.
  - A configured merchant client has an `integration_id` in its signed credential.
  - The runtime selects the agent's policy profile from that context, never from the model.
- **Runtime policy plugin**, alongside the Langfuse plugin. Each policy profile, rendered from chart values, sets:
  - which tools may run;
  - which arguments the runtime fills from verified context instead of the model;
  - which identifiers must appear in the user's messages;
  - per-turn call limits;
  - output contracts.
  
  A denial is a tool error and a span attribute in Langfuse.
- **Payments read tools:** the payments service gains a read-only MCP transport, reached by the gateway at `/mcp/payments`. Each tool answers only for the integration the caller context names.
  - `get_payment_status`: status, version and attempt summaries (status, decline reason, assessment status) of one payment.
  - `get_attempt_risk_inputs`: analyst-only. It rebuilds a declined attempt's risk-tool inputs from the checkout snapshot and the attempt's recorded `AttemptStarted` signals, so `risk_agent` can re-score a payment by id. Today the risk tools need the analyst to type every field.
- **Audience-enforced knowledge:**
  - `/mcp/knowledge-merchant` sets the audience to `merchant`.
  - `rag-api` then filters searches to merchant and shared chunks, and refuses analyst-only chunks by id, whatever arguments the model sends.
- **Not in this change:**
  - Refunds and disputes. Refunds stay on the roadmap and will be a later skill on the same `decline-reviewer` A2A route.
  - Any write tool.
  - Real merchant sign-in. Merchants are gateway clients with keys.
  - Live production deployment.

## Capabilities

### New Capabilities
- `agent-policy`: runtime policy profiles per agent and caller. Covers tool allow-lists, verified caller context, arguments bound from that context, identifier provenance, call limits, output contracts and denial tracing.

### Modified Capabilities
- `agent-runtime`:
  - The runtime adds the policy plugin and an A2A client for remote agents through the gateway.
  - `coordinator` is replaced by the `analyst-assistant` and `merchant-assistant` entry agents.
  - Sub-agent templates are shared across entry agents.
  - The registry catalog lists both entry agents and the remote A2A dependency.
- `agent-gateway`:
  - Per-client entry keys, per-agent listener access and native signed caller credentials.
  - Entry routes for both assistants.
  - An internal A2A route admitting only `merchant-assistant` to `decline-reviewer`, whose model route has no request screen.
  - `/mcp/payments` and `/mcp/knowledge-merchant` routes.
  - The removal of `/a2a/coordinator`.
- `rag-knowledge-pipeline`: the knowledge MCP tools enforce the audience set by the gateway route rather than an argument from the model.
- `payment-processing`: the payment status query is also served to agents as a read-only MCP tool, scoped to the integration in the verified caller context.

## Impact

- **Code:**
  - `src/llm/agent_runtime`: policy plugin, native authenticated caller context and kagent remote A2A tool.
  - `src/payment-gateway/payments`: MCP transport on its own port.
  - `src/llm/rag_serving`: audience from the route.
  - Tests for each.
- **Charts:**
  - `infra/charts/llm-platform`: agents and templates from one entry-agent loop, policy profiles, RemoteMCPServers per agent and server, gateway routes and policies, signed client claims, native token passthrough, mesh rules, catalog.
  - `infra/charts/payment-gateway`: MCP port, Service and mesh rule admitting only agentgateway.
- **Secrets:**
  - Gateway keys in Doppler for the demo merchant integration and analyst UI client; superseded per-agent keys are removed.
  - The shared `agents` key stays for model calls only, through the global model config.
- **Langfuse:** prompts `analyst-assistant-system-prompt` and `merchant-assistant-system-prompt` replace `coordinator-system-prompt`.
- **Docs:** `docs/llm/agents.md`, `gateway-and-jev-policy.md`, `custom-go-adk-runtime.md`, `docs/deployment/networking.md` and `Home.md`, updated with dev evidence (screenshots and numbers).
