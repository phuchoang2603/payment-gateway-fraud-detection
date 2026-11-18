## 1. Verify platform assumptions

- [x] 1.1 Verify native caller propagation: key-restricted per-client gateway JWT signing, kagent's forwarded bearer and authenticated principal, and internal A2A JWT validation/passthrough. Check the pinned CRDs and source, render the routes, verify the platform waypoint permits distinct configured client subjects, and verify a native bearer-only MCP origin is Compatible on dev. Record the chosen form in design.md D2.
- [x] 1.2 Verify the pinned native `NewKAgentRemoteA2ATool`, its bearer forwarding, card discovery, trace propagation and runtime composition. Check egress credential separation and record the call pattern in design.md D5. Verify with source review and `go build`.
- [x] 1.3 Check how the kagent v1.0.0-rc1 runtime receives per-agent settings (env or config file from the Agent/Harness spec) for the policy profiles. Record it in design.md D3, verified against the live `coordinator` actor spec on dev.

## 2. Payments read tools

- [x] 2.1 Add a `get_attempt_risk_inputs` application query in `src/payment-gateway/payments`:
  - load the payment state for the integration, and the given or latest attempt's `AttemptStarted` event from the ledger;
  - map them to the risk input fields plus decline source and reason;
  - report another integration's payment as not found.
  
  Add focused unit tests for scoping and for a risk-declined attempt. Verify with `golangci-lint` and CI's Go job.
- [x] 2.2 Add the MCP transport adapter (go-sdk, streamable HTTP, stateless) on its own port with `get_payment_status` and `get_attempt_risk_inputs`, wired in the composition root. Keep it read-only and keep domain code free of MCP types. Add one transport test per tool for the input/output mapping. Verify with lint and CI.
- [x] 2.3 Expose the MCP port in `infra/charts/payment-gateway`: a Service port, plus a mesh AuthorizationPolicy admitting only the agentgateway identity to that port. Verify with `./scripts/ci-checks.sh helm` and, on dev, that a direct call from another pod is refused.

## 3. Knowledge audience from the route

- [x] 3.1 In `src/llm/rag_serving`:
  - read `x-knowledge-audience` in the MCP tools;
  - remove `audience` from `search_knowledge`'s arguments;
  - filter search to that audience plus `both`;
  - make `get_knowledge_chunk` report analyst-only chunks as not found under `merchant`.
  
  Update its tests and keep the coverage floor. Verify with ruff and CI.

## 4. Runtime policy and A2A client

- [x] 4.1 Read the caller from kagent's native authenticated invocation and forwarded bearer claims in a small policy context package. Ignore custom identity headers and message metadata. Unit test missing credentials, authenticated principal matching, signed-client claim selection and forged identity-header rejection. Verify with `go vet` and CI.
- [x] 4.2 Add `internal/policy`: profile loading and selection, plus the native ADK `BeforeRun`, `BeforeAgent`, `BeforeTool` (allow-list, transfers, bound arguments, provenance, limits) callbacks and a protected review-result boundary (output contract, no intermediate artifact/history disclosure). Reuse native output-schema validation where applicable. Record denials on the span and in a metric. Add tests per rule: unknown caller, tool not allowed, bound argument overwrite, invented identifier, limit exceeded, contract violation. Verify with CI.
- [x] 4.3 Configure kagent's native remote A2A tool from `remoteSkills` before runner creation, using native bearer and trace propagation. Keep it on the merchant listener origin with a credential-free native egress grant. Test delegation context and profile denial. Verify with CI.
- [x] 4.4 Register the policy plugin next to the Langfuse plugin and pin the new runtime image digest in the Harness values. Verify that the dev actor starts and that its logs show both plugins loaded.
  The runtime logs no plugin names, so both were verified by behaviour on dev: the policy plugin overwrote a forged `integration_id` and refused with `missing_bound_argument`, and the Langfuse plugin served prompt version 2 to the analyst.

## 5. Chart: entry agents, keys, routes

- [x] 5.1 In `infra/charts/llm-platform/values.yaml`:
  - replace `coordinator` with an `entryAgents` list (`analyst-assistant`, `merchant-assistant`, `decline-reviewer`), each with sub-agent templates, MCP servers, remote skills and `profiles` (analyst API and native Dex principals; merchant-review delegated merchant principal; merchant `merchant-demo`);
  - define specialists once, with an audience parameter.
  
  Verify with `helm template` that no `coordinator` resource remains.
- [x] 5.2 Render from the loop:
  - one Agent per entry agent, and evidence-agent templates per audience;
  - `RemoteMCPServer`s per (entry agent, server) on that agent's bearer-only listener origin, and `EgressPolicy` gateway destinations for A2A;
  - the profiles into each agent's runtime config.
  
  Verify with `./scripts/ci-checks.sh helm`, then review the rendered diff.
- [x] 5.3 Gateway:
  - routes `/a2a/analyst-assistant`, `/a2a/merchant-assistant`, `/a2a/internal/decline-reviewer`, `/mcp/payments`, `/mcp/knowledge-merchant`, with one policy each;
  - per-client static signed claims, internal validated bearer passthrough and separate agent listeners;
  - per-client external entry rules and listener-scoped tool authorization, including per-tool rules on `/mcp/payments`, and `/mcp/risk` limited to `analyst-assistant` and `decline-reviewer`;
  - `/a2a/coordinator` removed.
  - REST client allowlists exclude merchant keys and the shared model key, closing the alternate path to analyst data.
  
  Verify with Helm lint and on dev with the gateway checks in 7.1.
- [x] 5.4 Keep the external `analyst-ui` and `merchant-demo` keys (with a dev `integration_id`) and their ExternalSecret mapping. Remove superseded per-agent keys and mappings without printing any value. Verify that the synced Secrets have the expected key names only.
- [x] 5.5 Update the registry catalog: both entry agents, their sub-agents, MCP servers and the `review_decline` remote skill, with versions bumped. Verify that the dev catalog job succeeds and the registry UI lists both.
- [x] 5.6 Create Langfuse prompts `analyst-assistant-system-prompt`, `decline-reviewer-system-prompt` (the `review_decline` JSON-only instructions) and `merchant-assistant-system-prompt` before deploying. Remove `coordinator-system-prompt` after the rollout. Verify both exist with the `production` label.

## 6. Deploy to dev

- [x] 6.1 Open one PR with sections 2–5. Let CI run, deploy to dev via `deploy/dev`, and wait for all Argo apps to be Synced and Healthy. Report any failed or pending checks honestly.

## 7. Dev evidence and docs

- [x] 7.1 Gateway checks from outside the agents, with external keys from Secrets and native agent-listener configuration. Never print credentials. Record each as one screenshot:
  - merchant key on `/a2a/analyst-assistant` → 403;
  - forged identity headers and bearer ignored on external entry, and cross-client session access refused;
  - the merchant listener lists only `get_payment_status` on `/mcp/payments`, and refuses `get_attempt_risk_inputs`; verify the merchant actor cannot reach the analyst origin;
  - `/mcp/knowledge-merchant` refuses an analyst chunk;
  - merchant and shared-model keys cannot read analyst data through the existing REST routes.
- [x] 7.2 Conversations on dev:
  - an analyst risk question through `analyst-assistant`, transferring to `risk_agent`;
  - a merchant decline question through `merchant-assistant`, calling `get_payment_status` then `review_decline` over A2A, with the answer containing no score.
  
  Capture kagent UI or Langfuse screenshots of the linked traces, and the p50/p95 latency of 10 merchant decline questions.
- [x] 7.3 Policy denials: one invented-identifier refusal and one output-contract refusal visible in Langfuse. Add a merchant review prompt to the Jev screening check and record whether it passes.
- [x] 7.4 Update docs:
  - `docs/llm/agents.md`: architecture mermaid, entry agents, `review_decline`, evidence;
  - `gateway-and-jev-policy.md`: routes, keys, native signed identity propagation;
  - `custom-go-adk-runtime.md`: policy plugin and A2A client, with ADK guidance as the rationale;
  - `docs/deployment/networking.md`: new routes and payments MCP port;
  - `Home.md`: rubric rows for the A2A Gateway and Coordinator.
  
  Mark old `coordinator` captures as historical. Verify that the docs link check passes.

## 8. Review follow-ups

- [x] 8.1 Caller identity from claims: the gateway signs as `agentgateway` with `client`, `integration_id` and `user_id` claims; `caller.go` reads the claims and checks kagent's principal against the configured user claim; the review authorization, MCP routes and Dex mapping take client names from values; kagent owns sessions by `user_id` (talos-proxmox#137, replacing #136); the remote skill URL is validated against the configured gateway origin.
- [x] 8.2 Record on dev, from real merchant and analyst requests through the gateway: the merchant `tools/list` shows only `get_payment_status`, `/mcp/knowledge-merchant` refuses an analyst-only chunk, and an analyst-only payments tool is refused on the merchant route. Source: gateway access logs and Langfuse traces; remove the "not exercised" row.
- [x] 8.3 Find why merchant reviews fail from their traces, fix the cause and re-run the 10-question measurement; update the agents.md table.
- [x] 8.4 Smaller fixes: nil guard in `OnEvent`; explicit `x-fraud-scope` on payments MCP; policy profiles as YAML rendered with `toJson`; require a valid token for MCP `tools/list`.
- [x] 8.5 State in the PR description that the Kafka exporter off-mesh label and the drift deadline are dev-health fixes outside this change.
