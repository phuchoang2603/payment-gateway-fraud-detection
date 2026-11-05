## Context

See proposal.md for the motivation. What shapes the design, checked against the code:

- **Go Fraud is the reference.** `adapters/feast` posts `payment_risk_v1` to the feature server's `/get-online-features` with the entities `integration_id`, `customer_id`, `payment_method_fingerprint`, `scored_at` and the request fields (`amount_minor`, both account-creation times, `shipping_country`, `ip_country`, `item_count`, `has_device`). Feast computes `request_features` from them. `adapters/kserve` reads `feature_order`, `registry_version`, `decline_threshold` and `bin_edges` from Triton's `/v2/models/payment-risk/config`, lays one FP32 row out in that order (NaN for a view that is not `available`), and scores it over v2 binary (`input__0`, output `output__0`, the fraud probability is the second of two values).
- **The evidence is not readable.** Fraud stores each assessment as a protobuf `bytea` in a table only its own role reads, and `FraudService` has only `Decide`.
- **The model's reference profile is served.** `payment-risk-drift` in `ml` answers `GET /v1/reference` with the serving version, and per feature its edges, the share of training rows per bin (missing last) and its gain. Triton's config carries edges but neither shares nor gain, and only the decline threshold, not step-up.
- **Access today.** `ml` has no policy on the predictor or the drift API, and `llm` is ambient, so the `risk` pod reaches both. `feast-server` is outside the mesh, and its NetworkPolicy admits only Go Fraud.
- **Knowledge pattern.** `rag-api` is a FastAPI app with an `MCPServer` mounted at `/`, stateless streamable HTTP with JSON responses, a counter per tool call, deployed by the `rag-pipeline` chart; the `llm-platform` chart only wires the gateway, the kagent `RemoteMCPServer`, the registry entry and the mesh policy for each entry in `gateway.mcpServers`.

## Goals / Non-Goals

**Goals:**

- The risk agent scores a payment through the tool and says which features stand out, with nothing invented.
- The tool behaves like Go Fraud on views and model versions, so a score here means what a score there means.

**Non-Goals:**

- Reading the stored decision, any new Go endpoint or database role, SHAP, a step-up threshold, or caching features.

## Decisions

### 1. Input is the request fields (a what-if score)

The agent passes `integration_id`, `customer_id`, `payment_method_fingerprint` and the request fields; the tool sets `scored_at` to now. Alternatives: look up the stored decision evidence by payment ID and only re-score it. That needs a read path that does not exist (a new gRPC method or a database reader role in Go Fraud), for a result that would only repeat what Fraud already decided. The what-if score also answers "what if the amount were higher", which a stored lookup cannot. The cost is that analysts must supply the fingerprint, which Edge computes; the prompt asks for it. A payment-ID lookup is a later change if the analysts need it.

### 2. Drivers: gain times distance from the reference

For each feature, `distance` is how far the value sits from the middle of the training distribution: for a value in bin `b`, the cumulative share below the bin plus half the bin's share gives its position `p` in (0, 1), and `distance = |p - 0.5| * 2`; a missing value has `distance = 1 - share_missing`, so a feature that is usually missing is not unusual when missing. The ranking score is `gain_share * distance`, where `gain_share` is the feature's gain over the total. The tool returns the top five with value, position `p`, and score share, and says in its description and result that this is a ranking of unusual, influential features, not an attribution of this score. Alternatives: SHAP needs a second CPU model or Triton's Shapley support, which FIL here does not expose; raw gain alone would show the same features for every payment.

The reference comes from the drift API's `/v1/reference`, not the Model Registry, so the tool needs no registry access and no new image dependency. It is cached per version, and used only when its version equals the version that scored; otherwise the tool returns no drivers and the reason.

### 3. Its own package, `src/llm/risk_serving`

`rag_serving` pulls psycopg, Feast and the embedder, and loads Feast for about a minute. The risk server needs `httpx`, FastAPI, `mcp` and `prometheus-client`, starts in seconds, and has a different failure profile. It joins the `src/llm` uv workspace (lock shared with `rag_serving`) for pinned versions and one lint config, and does not import `fraud_rag_common` or the training package `risk_model`. The v2 binary client is a small file of its own.

### 4. Module layout

Transport and logic stay separate: `app.py` (FastAPI, MCP tools, counters, `/healthz`, `/readyz`, `/metrics`), `features.py` (Feast request, view status rules, the same TTLs as Go: 31 d 12 h for daily views, 24 h 5 m for stream views), `model.py` (Triton config and v2 binary inference, version consistency), `drivers.py` (reference profile and ranking). `features.py` and `drivers.py` hold plain functions over plain data so that they are tested without a network.

### 5. Row layout and version consistency

Feast field names arrive as `view__field`, so a name in `feature_order` maps to `(view, field)` by splitting on `__`, as in Go. A value is a float, a boolean is 0 or 1, and anything else is NaN. The tool keeps the last configuration it read. After inference, if the answer's `model_version` (`"4"`, prefixed `v`) differs from the cached `registry_version`, it re-reads the config and rebuilds the row once, then scores again; a second mismatch is an error. This costs one extra call during a rollout or while a challenger takes a share of traffic, and never mixes layouts.

### 6. Deadlines and errors

Gateway route timeout is 60 s. Feast read: 5 s (a cold feature server is slow; Go uses 250 ms inside a 1 s assessment, which the tool does not need). Model call: 3 s, configuration: 3 s, reference: 3 s. Any failure raises a tool error that names the failing system and returns no score and no partial row, so the agent reports what failed and does not guess.

### 7. Deployment

The risk server, its MCP route and the 0.2.0 agent binding are shared chart defaults. There is one scoring prompt and no toolless environment fallback.

The `risk` workload, its active and preview Services, ServiceAccount, `VMPodScrape`, disruption budget and mesh policy live in `infra/charts/llm-platform/templates/risk.yaml`. The active Service name and port join `gateway.mcpServers`, so the existing templates produce the route, backend, policy, kagent `RemoteMCPServer` and registry entry. The image comes from `risk.image` with the release tag. Addresses (`feast-server.data`, `payment-risk-predictor.ml`, `payment-risk-drift.ml`) are chart values.

Both `risk` and `rag-api` use Argo Rollouts blue-green in every environment, with the knowledge workload still owned by `rag-pipeline`. The gateway keeps using the active Service's existing DNS name. No environment selects an alternate Deployment strategy. Production is rendered but not deployed by this change. Both servers retain their pod security and readiness boundaries. Mesh admits the gateway and metrics agent, plus a dedicated analysis ServiceAccount restricted to the release namespace and tool port; the knowledge API also keeps its existing load-test identity. There is no risk load-test identity or Locust Job.

### 8. Prompt, catalog and prompt seeding

`risk-agent` goes to 0.2.0. The seed no longer says the tools are missing, and tells the agent to ask for missing identifiers, report the score with its version and threshold, name any view that was not available, show the drivers as unusual influential features, and say a tool error means no score. Langfuse only seeds a prompt it lacks, so an environment that already has `risk-agent-system-prompt` needs a new `production` version published there; the seed covers a fresh Langfuse.

### 9. Metrics

`risk_mcp_tool_calls_total{tool,outcome}`, `risk_tool_latency_seconds{tool}` (histogram) and `risk_view_status_total{view,status}`. The gateway already counts MCP calls per route and tool; these show failures that the gateway sees as successful tool results and the view statuses behind a score.

### 10. KEDA for both servers

Both servers have a `ScaledObject` and a `PodDisruptionBudget` with `minAvailable: 1`. KEDA targets `argoproj.io/v1alpha1`, kind `Rollout`, in blue-green mode and owns `/spec/replicas`; the chart leaves replicas unset. Knowledge keeps two to four replicas and its combined REST and MCP rate target of 10 requests/s per replica. Risk uses two to four replicas and `sum(rate(risk_mcp_tool_calls_total[1m]))`, scoped to its namespace and scrape job, with an initial configurable target of 5 calls/s per replica. This is a starting setting, not a measured capacity claim: each score reads Feast and Triton. Count all outcomes so upstream failures do not hide demand. A failed metrics query must not be treated as zero demand. Keep the existing knowledge Locust test; no risk Locust file, Job or load-test report is planned.

### 11. Blue-green promotion and controller ownership

Each Rollout has `activeService`, `previewService`, `previewReplicaCount: 1`, automatic promotion after successful `prePromotionAnalysis`, and a 30-second scale-down delay for old endpoints. Do not set `autoPromotionSeconds`: a timer must not bypass the analysis gate. The preview runs at one replica while KEDA scales active capacity; before switching the active Service, the candidate grows to the desired replica count and becomes ready. Promotion can therefore temporarily require both revisions at full desired capacity, despite the single preview replica during analysis. No Gateway API traffic-splitting plugin is needed because the fixed MCP backend points to the active Service.

Each server has an `AnalysisTemplate` with a bounded Job that initializes MCP, lists tools, and calls a real read-only tool through its preview Service. Risk scores a shared synthetic request, asserting no MCP tool error, a finite probability in [0, 1], model version, threshold and available request features. Daily and stream views may be missing, expired or future under the normal scoring rules, but an invalid view, error or timeout fails analysis. The gate verifies a real Feast read and Triton inference without requiring a particular environment’s seeded customer. Full-history scoring remains separately covered by service tests and recorded dev evidence. Knowledge calls its retrieval tool against the seeded corpus and checks the expected result contract. Health checks, a successful HTTP status with `isError`, missing request features or knowledge passages, and dependency failures are not a passing analysis. Set Job and analysis deadlines and failure limits; any failure or timeout keeps active traffic on the previous revision. A new preview pod becomes an endpoint of its Service in the mesh a moment after it is Ready, so the client waits up to 150 seconds for the MCP handshake to be answered (the Rollout starts the analysis in the same second it scales the preview from 0 to 1, and the knowledge API alone takes up to a minute to import Feast and become ready), retrying only connection failures and 5xx answers; once the preview has answered, every later failure, tool error or invalid result fails at once. The Job's own deadline is 300 seconds, because it counts the image pull (17 to 22 seconds on dev for a new tag) as well as the client's 200-second bound; at 60 seconds both dev analyses of a healthy preview were killed with `DeadlineExceeded`, one of them after its check had already logged a success. Dev showed why: three successive analyses of a healthy preview failed with `ReadError` because ztunnel still reported no healthy upstream for the preview Service. These are deployment gates, separate from throughput testing. Use the service's existing image for its analysis client where practical, with request arguments supplied as shared chart values and the dedicated analysis identity admitted by mesh.

Argo CD ignores only the Rollouts' replica counts and the active/preview Services' controller-added revision hash selectors, with `RespectIgnoreDifferences=true`; it still reconciles base selectors, identities and security. Retarget knowledge's existing KEDA object and remove the superseded Deployment after a controlled adoption with ready active endpoints. Sequence adoption so two controllers never own the same pods, using Rollouts' workload adoption support where appropriate. Verify the initial-install analysis behavior separately: pre-promotion analysis on an update cannot serve as proof that an initial deployment was gated. Failed candidate analysis and a successful update are distinct dev proof items for both servers.

### 12. GPU claims and dev placement

The platform owns the `ml/ml-gpu` and `llm/llm-gpu` claims, their device selection and the GPU tolerations; the charts reference the claims by name only and render no `ResourceClaim`, selector or GPU taint setting. Dev keeps `ml-gpu` on the GTX 1650 and `llm-gpu` on the RTX 5060 Ti through the platform's claims, and the application never deletes or recreates them. The platform has verified adoption and its admission policy in both environments, so the app charts set no GPU `nodeSelector` or toleration: the claim's allocation places the pod and admission adds the toleration. Removing them changes the generated pod templates, so merging rolls the GPU workloads in every environment that runs these charts. On dev the CPU-only chat router requests 250m CPU and 128 MiB with a 512 MiB memory limit: its preset reserved 1 core and 2 GiB while observed usage was 158m and 36 MiB, leaving too little requested CPU for the 500m ML predictor. Whether FIL, XGBoost training and the LLM runtimes fit the GTX 1650's 4 GiB and its driver is platform and ML work, not part of this change.

### 13. Identifiers and numbers through the gateway's PII masking

The first dev chat showed the gateway altering the tool's inputs: its `PhoneNumber` built-in masked a 7-digit run inside the 64-character hex payment-method fingerprint, so the agent called the tool with a corrupted key. The chart now masks phones with its own patterns in `gateway.piiPatterns` (separators or a leading plus, or a bare 10 to 11 digit number, all at word boundaries), which leave digit runs inside hex identifiers and UUIDs alone and still mask the usual phone shapes; the built-in is dropped from both the request and the response rule. The change applies to every environment. It narrows a privacy control, which the user approved, and a number in an unusual format may no longer be masked. The card pattern already masks any 13 to 19 digit run, which includes the decimals of a full-precision float, so the tool rounds every float in its result to four decimals and reports `at_or_above_decline_threshold`, computed on the unrounded values as Go Fraud does, so that the agent never compares rounded numbers.

### 14. Rollout gating and the dashboard

A candidate whose pods never become ready never starts its analysis, so both Rollouts set `progressDeadlineAbort: true` as the Edge Rollout does, and abort at the 300-second progress deadline instead of staying `Progressing`; the field exists only on Rollouts, so it is rendered only where blue-green is enabled. The agents dashboard follows the other components' pattern: the ConfigMap's `grafana_folder` annotation comes from `dashboards.folder` (`LLM`), its MCP panels cover both servers, and a Risk tool row shows latency, feature view statuses and the servers' KEDA replicas.

## Risks / Trade-offs

- [A score from a view that is not `available` looks like any other score] → the result carries every view's status and the prompt requires the agent to name a missing one.
- [Drivers read as a cause] → the wording is in the tool description, the result and the prompt; the score is described as a ranking.
- [Prompt in Langfuse stays old] → the release notes say to publish a new `production` version where the prompt already exists.
- [Feast's network policy is the only guard in front of the feature server, and now admits a second caller] → it selects the exact pod label in `llm`; the feature server is outside the mesh, so identity cannot be used.
- [Blue-green promotion briefly needs both revisions at full desired capacity] → keep one preview replica during analysis, sequence the two server updates and record any pending capacity; never force promotion past readiness or failed analysis.
- [The risk scaling target has not been load-tested] → label 5 calls/s per replica as provisional and record observed call rate, replicas and latency without claiming a Locust benchmark.
- [Reconciliation resets promotion or scaling] → limit Argo CD ignore rules to controller-owned replica counts and revision selectors and verify reconciliation during an update.
- [Kubelet HTTP probes were previously dropped on dev] → both environments use the native `/health` HTTP probe. Diagnose CNI host-to-pod connectivity instead of adding exec probes or node-IP exceptions; dev readiness must be checked after deployment.
- [A preview Job bypasses the gateway] → admit only its dedicated analysis identity at the tool port; ordinary callers still use the gateway.
- [The LLM applications are off on dev] → dev app-of-apps values enable `llm-platform` and `rag-pipeline`, as authorized. The GPU claims are platform-owned; unavailable capacity leaves chat proof pending and is not worked around by changing the ML claim.

### 15. Environment parity and shared analysis ownership

Risk tools, agent bindings, blue-green workloads, preview identities, KEDA targets and Argo CD ignore rules are shared defaults. Remove `risk.enabled`, rollout strategy toggles, the alternate prompt file and Feast probe overrides rather than maintaining parallel environment paths. Keep hostnames, object stores, storage classes and capacity-driven GPU/CPU/memory sizing in environment values. Do not deploy production as part of this change.

The shared analysis module and behavior tests live in `src/llm/common`. Both images copy `fraud_rag_common/mcp_analysis.py` as a standalone release command; risk does not install the Feast/Postgres common package. The analysis is common deployment tooling, not risk runtime logic.
