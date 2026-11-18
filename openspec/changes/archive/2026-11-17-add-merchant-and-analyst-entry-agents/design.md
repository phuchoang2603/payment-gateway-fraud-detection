## Context

See `proposal.md` for the motivation. Current state that shapes the approach:

- **One entry agent.** `coordinator` has `evidence_agent` and `risk_agent` compiled in as kagent Shared sub-agents. Analysts reach it at `/a2a/coordinator`.
- **Shared credentials.**
  - Every model, MCP and A2A call leaves the actor through Substrate's egress gateway with the shared `agents` key.
  - Gateway keys are JSON `{key, ...}` values in `llm-gateway-keys`, synced from Doppler.
  - The gateway therefore cannot tell agents apart.
- **The runtime** (`src/llm/agent_runtime`) is kagent's Go ADK entrypoint plus one ADK plugin, the Langfuse prompt selector. `cmd/agent_runtime/main.go` composes the A2A server, so the project controls the HTTP handler in front of it. ADK Go v2.6.0 plugins expose `BeforeAgent`, `BeforeModel`, `BeforeTool`, `AfterTool`, `AfterAgent` and `OnEvent`. A `BeforeTool` that returns a result or an error skips the tool. ADK Go also ships an A2A remote agent client (`agent/remoteagent`).
- **Payments.**
  - `GetPayment` (gRPC) returns status, version and attempt summaries with `decline_reason`, the first reason code. It returns no score or signals.
  - The append-only ledger keeps every `PaymentEvent`, including `AttemptStarted` with the attempt's `AttemptSignals`: payment-method fingerprint, IP country, device.
  - The checkout snapshot holds the amount, items, shipping country and both account creation times.
  - Together these are exactly the `RiskRequest` fields that `risk-serving` takes.
- **Knowledge.**
  - Chunks carry `audience` (`analyst`, `merchant`, `both`).
  - `search_knowledge` accepts an optional `audience` argument that the model chooses.
  - `get_knowledge_chunk` does not filter by audience.
- **ADK guidance** (adk-docs `a2a/intro.md`, `safety/index.md`):
  - Use local sub-agents for same-team, shared-context, low-latency specialists.
  - Use A2A for independent services and formal contracts.
  - Put guardrails in tools (developer-set context), in callbacks and plugins, and in identity, in that order.

## Goals / Non-Goals

**Goals:**
- Split agents by audience, with one identity per entry agent and per external client.
- Keep the specialists local inside each entry agent.
- Use A2A only where sensitive data crosses a trust boundary: merchant to analyst.
- Enforce rules in three layers, so that none of them has to be trusted alone:
  - the gateway (who can reach which route and tool);
  - the runtime (profiles, bound arguments, provenance, limits, output contracts);
  - the tools (integration scope, audience).
- Avoid duplication: one runtime image, one chart loop over entry agents, specialists defined once and rendered per audience, tools defined once in their owning service.

**Non-Goals:**
- Refunds, disputes or any write tool.
- Merchant sign-in or OAuth. Merchants are gateway clients with keys bound to an integration.
- A fraud-evidence read API (scores as recorded by Fraud).
- Changes to Jev screening thresholds.
- Prod rollout.

## Decisions

### D1. Split by audience, not by specialist
- Two entry agents: `analyst-assistant` (renamed from `coordinator`) and `merchant-assistant`, plus the internal `decline-reviewer`, which only `merchant-assistant` calls.
- Specialists stay kagent Shared sub-agents, compiled into each entry agent that names them.
- One agent-to-agent call: `merchant-assistant` → `review_decline` → `decline-reviewer`.
- The request screen runs on the model route of agents that receive human input. `decline-reviewer` receives only the bare review object, which the decision model scores 0.88 as a jailbreak, so it has its own listener and model route without the screen. The route follows from the agent's `humanInput: false`, not from the message content.
- *Alternatives considered:*
  - Separate A2A agents per specialist. This adds a hop and a pod, and loses the shared conversation, with no new trust boundary.
  - Merchant tools reading risk data directly. This puts scores in a merchant's agent's context, where a prompt injection could leak them.
- The A2A call is the only place where the merchant side needs a result computed from data it must not see.

### D2. Native authenticated caller context
- External clients keep their API keys. Each configured client has a key-restricted gateway route rule and an A2A backend that signs a short-lived JWT with static `iss`, `sub`, `aud`, `client`, `user_id` and, for merchants, `integration_id` claims. Client selection is authenticated by the gateway, never by a supplied identity header.
- The gateway signs as its own trusted subject, `agentgateway`, which the platform waypoint already admits after signature, issuer and audience verification; no application client name appears in the waypoint, the gateway CEL or the runtime. The caller travels in the `client` and `integration_id` claims. kagent's controller takes its session owner from the `user_id` claim (`server.auth.userIdClaim`, the native setting), signed as the client, or `client:integration_id` for a merchant, so callers do not share sessions; tokens without the claim, such as Dex's, fall back to `sub`. A shared `sub` alone would have put every gateway caller on one principal.
- Merchant sessions are owned per client and integration, not per end user: a merchant key identifies one integration, and the gateway sees no end user behind it, so every conversation on that key shares one session owner. That is acceptable because a session holds no secret the key holder does not already have: its tools are read-only and scoped to the key's integration through `integration_id`, so any holder of the key can already read everything a session could show. Per-end-user isolation would need the merchant's own user identity in the credential, which the API-key client model does not carry.
- kagent's native trusted-proxy authentication forwards the verified bearer credential and authenticated user identity to the actor over its private gRPC transport. The policy plugin reads those native values. It ignores `x-fraud-*`, message metadata and model-supplied identity.
- The internal review route is mounted only on the merchant assistant listener, which only that agent can reach through its native Substrate egress allowlist. It validates the forwarded merchant JWT and passes it through unchanged. The analyst runtime selects its merchant-review profile from the verified `client` claim. The route requires an `integration_id` and a client that the values configure with an integration, so no client is named in the policy.
- Static per-client signing is sufficient for the configured coursework demo. The pinned gateway's `jwtSign.claims` does not evaluate CEL. Additional clients require a configured rule/backend; a future identity provider can replace static client signing with JWT validation and passthrough.
- Native UI calls use their verified Dex credential and principal, rather than a missing-header `local` fallback. Missing or unrecognised credentials are refused.
- *Verified source constraints:* kagent v1.0.0-rc1's `core/internal/a2agateway/runtime.go` forwards credentials rather than arbitrary HTTP headers; `core/pkg/auth/credential.go` forwards the bearer and derives `x-user-id`; `adk/pkg/a2a/executor.go` exposes that identity and bearer to the invocation. The prior header spike established only the gateway rewrite, not controller-to-actor propagation.
- The runtime accepts a gateway credential only when kagent's authenticated principal equals the issuer and the `user_id` claim (or `sub` when absent), configured as `issuers.userIdClaim`. The internal review route's remote URL is checked against the gateway origin of the entry agent's listener, rendered from values, rather than a fixed host and path.
- Payments MCP requests carry an explicit `x-fraud-scope`: `integration` with the bound integration for merchants, `all` only on the analyst route for a token without an `integration_id`. A request without the header is refused, so scoping no longer depends on a header being absent.

### D3. Policy profiles rendered from chart values
- Each entry agent's values hold `profiles`, keyed by caller identity. Each profile holds:
  - `tools` and `transfers` allow-lists;
  - `remoteSkills`;
  - `bind` (argument → context key, e.g. `integration_id: integration`);
  - `fromUser` (arguments that must appear in user messages or earlier tool results);
  - `limits` (`toolCalls`, `remoteCalls` per turn);
  - optional `input` and `output` contracts (JSON schema and forbidden keys).
- The chart renders the profiles as JSON into the `AGENT_POLICY` environment variable of the entry agent's Harness (`spec.env`), which `main.go` reads with `os.Getenv` like `LANGFUSE_HOST`.
  - *Verified (task 1.3, dev):* the live `coordinator` Harness delivers all of its runtime settings this way; its `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` exists only in `spec.env`, and Langfuse holds recent `generate_content` generations from that runtime.
  - The Harness CRD's `env` items are plain `name`/`value` pairs with no value size limit.
  - The controller-generated `/config/config.json` has no free-form field for it.
  - Each entry agent has its own Harness, so each gets its own profiles, and its sub-agents share them in one runtime.
- The policy plugin loads profiles once at start-up and selects one per invocation from the caller context.
- *Alternative considered:* OPA or Cedar. That adds a dependency and a sidecar for rules that are a few allow-lists and a schema.

### D4. Policy enforcement points in the runtime
Each rule maps to an ADK callback:

| Rule | Callback | Behaviour |
|---|---|---|
| Unknown caller | Native executor boundary and `BeforeRun` | Refuse before agent execution |
| Tool allow-list, call limits, bound arguments, provenance | `BeforeTool` | Overwrite bound arguments in place, then check the rest |
| Transfer allow-list | `BeforeTool` on `transfer_to_agent` | Check the target agent |
| Input contract | Review executor boundary | Validate the request text against its JSON schema before execution; refuse with a fixed contract error |
| Output contract | `OnEvent` and review executor boundary | Capture the final root result, validate its JSON schema and forbidden keys, publish only that result |

- **Provenance** scans the session's user events and earlier tool responses for the exact string value.
- **Refusals** are returned as tool errors so the model can recover, recorded as a span event (`policy.denied` with profile, rule and tool), and counted in a metric.
- This is the ADK-recommended "plugins for reusable security policy" pattern. It complements the gateway checks; it does not replace them.

### D5. Native kagent remote A2A tool
- Use the pinned runtime's `tools.NewKAgentRemoteA2ATool` for `review_decline`, with bearer propagation enabled and isolated review sessions. It provides W3C tracing, conversation lineage, task handling and native A2A result mapping.
- Populate the compiled runtime's `RemoteAgents` from the policy's configured remote skills before `CreateRunnerConfig`; do not implement a second A2A client or HTTP-header middleware. The AgentTemplate CRD exposes only MCP and local sub-agent bindings at this release, so this small composition step is necessary.
- The call targets the gateway's internal analyst route. The merchant assistant listener on port 8081 creates an origin separate from model traffic on port 80 and the analyst assistant listener on port 8082. Its EgressPolicy grants access without injecting credentials; native MCP token passthrough reserves the origin for the caller bearer.
- Card discovery uses a listener-restricted GET rule with a short-lived gateway service credential; POST calls require the forwarded verified merchant credential. No integration is taken from the card request or from tool arguments.
- Policy profiles allow or deny the native remote tool just like other tools. The merchant review request contains a payment identifier, and its provenance is checked before delegation.

### D6. Payments MCP transport in the payments service
- A new transport adapter in `src/payment-gateway/payments` serves MCP (Go `modelcontextprotocol/go-sdk`, streamable HTTP, stateless) on its own port, beside gRPC.
- Two tools call application queries:
  - `get_payment_status`: the existing status query.
  - `get_attempt_risk_inputs`: a new query that reads the payment state and that attempt's `AttemptStarted` event from the ledger, and maps them to the risk-tool input shape.
- Domain code is unchanged. The adapter maps tool input and output only.
- *Alternatives considered:*
  - A separate Go service. That's an extra image and chart, and duplicates the store access.
  - Python tools in `risk-serving`. There are no Python gRPC stubs, and it would read another service's data.
- Payments already owns this data and the owning-integration rule, so the tools live there.

### D7. Native per-agent egress origins
- The pinned controller forbids gateway credentials on any caller-token passthrough origin, even on different headers. Per-agent API keys are therefore removed from MCP and internal A2A traffic.
- Each entry agent has a dedicated listener on the same gateway: merchant on 8081, analyst on 8082. The native compiled MCP configuration grants only that agent's origin. An explicit credential-free EgressPolicy grants the merchant origin for its native remote A2A call.
- MCP routes attach only to the permitted entry agent's listener. The merchant listener exposes merchant knowledge, payment status and internal decline review; it has no risk or analyst knowledge route. The analyst listener exposes analyst knowledge, both payments tools and risk. Port 80 carries model/data APIs and key-authenticated external A2A entry routes, with no MCP or internal review routes.
- The mesh admits only Substrate's egress gateway and the kagent controller on agent listeners. Substrate enforces each actor's exact allowed origins; a merchant actor cannot select the analyst listener. External client keys alone cannot reach MCP or the internal review route.
- Tool discovery may initialize and list without a caller JWT on these restricted listeners. Every tool call requires a valid JWT for a configured profile. Merchant payments discovery lists only `get_payment_status`, and calls to risk-input reconstruction remain forbidden. Native token propagation supplies the actual caller credential at invocation time.
- Model calls retain the shared `agents` model key on port 80; Langfuse retains its existing credential origin. No long-lived agent key enters an actor.
- Existing knowledge and risk REST routes explicitly admit only configured analyst/data clients. Merchant client keys and the actor's shared model key are refused, preventing the model origin from bypassing audience-scoped MCP access.
- *Verified on dev, 2026-11-15:* an isolated temporary Agent with bearer-only MCP passthrough, shared model credentials and the existing Langfuse grant reached `Compatible=True`. The previous dual-credential configuration had `Compatible=False`. Temporary spike resources were removed. Full routing and conversation evidence remains required after rollout.

### D8. Audience at the knowledge server
- `/mcp/knowledge-merchant` uses the same backend as `/mcp/knowledge`, and its policy sets `x-knowledge-audience: merchant`.
- `rag-api` reads the header in its MCP tools:
  - `search_knowledge` uses it in place of the model's `audience` argument, which is removed from the tool schema.
  - `get_knowledge_chunk` reports an analyst-only chunk as not found.
- The public REST route keeps full visibility; `/mcp/knowledge` removes any client-supplied audience header and keeps full visibility.

### D9. Native UI authentication and protected review output
- Analysts use their configured API-key entry route or kagent UI's native Dex authentication. Both resolve to explicit policy profiles; an absent credential never grants analyst access.
- The merchant entry agent accepts only the configured merchant client principal.
- ADK and kagent normally expose tool and sub-agent events as A2A artifacts. A final-answer schema alone cannot protect a merchant from intermediate risk data. The review executor therefore keeps native tool/session execution internal and publishes only the validated review result. Partial text, tool artifacts, raw failures and task history are excluded from merchant-facing review events. Denial traces remain visible to analysts in Langfuse.
- Use kagent's native output-schema validation where its root-agent contract is applicable. Caller-specific review contracts additionally need a per-call boundary because the same analyst entry agent also serves unrestricted analyst conversations.

## Risks / Trade-offs

- **[Jev screening blocks the bare review request as a jailbreak]** → Measured at 0.88. `decline-reviewer` uses a model route without the screen, so human text is still screened where it enters and the review is not rewritten by content.
- **[Delegated credential exposure]** → Native propagation carries a short-lived user credential into the actor. The internal route validates it and is exposed only on the merchant assistant origin. Actors hold no long-lived signing or gateway credentials.
- **[A2A adds latency to merchant decline questions]** → One hop plus `decline-reviewer`'s own model calls. Measure on dev and record p50 and p95 in the agents doc.
- **[More agent pods on dev, already at ~75% memory on dev-server1]** → Both entry agents share the Substrate worker pool, so idle conversations cost no pods. Watch the pool's KEDA scaling during the demo.
- **[An output contract makes `decline-reviewer` brittle under a 4B model]** → The skill prompt asks for JSON only. A contract error tells `merchant-assistant` to answer with the decline reason alone. Validation failures are counted.
- **[Renaming `coordinator` breaks existing evidence links and the load test target]** → Update docs, Locust targets and dashboards in the same change. Old screenshots stay as historical evidence with their dates.

## Migration Plan

1. Merge the payments MCP transport first; it is additive. Then the knowledge audience header, which is additive while `/mcp/knowledge` is unchanged.
2. Add the keys in Doppler: `analyst-ui`, `merchant-demo` (bound to a dev integration id).
3. Deploy the runtime with the policy plugin, the chart loop and the gateway routes on dev, all in one PR.
4. Create the Langfuse prompts `analyst-assistant-system-prompt`, `merchant-assistant-system-prompt` and `decline-reviewer-system-prompt` before the rollout, because the runtime fails a model call without a prompt.
5. Rollback: revert the PR. The payments and knowledge additions can stay.
