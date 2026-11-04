## 1. Package and image

- [x] 1.1 Add `src/llm/risk_serving` (`fraud_risk_serving`, `pyproject.toml`) to the `src/llm` uv workspace with pinned `fastapi`, `uvicorn`, `mcp`, `httpx` and `prometheus-client`, refresh `uv.lock`, and verify `uv lock --check` and `ruff check` pass in `devenv shell`
- [x] 1.2 Add `infra/docker/risk-serving.Dockerfile` modelled on `rag-serving.Dockerfile` and its entry in `.github/service-images.json`, and verify the JSON is valid and the image job appears in CI
- [x] 1.3 Add the image to `imageTags` in `infra/argocd/app-of-apps/values.yaml` and verify `helm template` of the app-of-apps renders it

## 2. Feature read

- [x] 2.1 Implement `features.py`: the `payment_risk_v1` request with the entities Go sends, the view statuses (`available`, `missing`, `future`, `expired`, `error`, `timeout`) with the daily and stream TTLs, and values only for `available` views; verify with behavior tests for missing, expired, future and unreachable cases that CI runs
- [x] 2.2 Implement `get_risk_features` over it and verify a CI test lists each view with its status and values

## 3. Model call

- [x] 3.1 Implement `model.py`: read `feature_order`, `registry_version`, `decline_threshold` from Triton's config, lay out one FP32 row (NaN for unavailable views, booleans 0 or 1), send it as v2 binary and parse the answer; verify with CI tests against a fake Triton for the layout, NaN rules and a malformed answer
- [x] 3.2 Implement the version rule: when the answering version differs from the cached one, re-read the config, rebuild the row and retry once, then error; verify with a CI test that a changed version scores under its own layout and a second mismatch errors
- [x] 3.3 Implement `score_payment_risk` returning probability, model version, decline threshold and every view's status, with no score on any upstream error; verify with CI tests for the all-available, one-view-missing and model-down cases

## 4. Drivers

- [x] 4.1 Implement `drivers.py`: fetch and cache `/v1/reference` per version from the drift API, compute `distance` and `gain_share * distance`, return the top five with value, position and share, and no drivers with a reason when the reference version differs; verify with CI tests (including a property test that the ranking is stable under feature order and that a median value ranks below an extreme one)
- [x] 4.2 Put "ranking of unusual, influential features, not an attribution" in the tool description and the result, and verify a CI test asserts the result carries it

## 5. MCP server

- [x] 5.1 Implement `app.py` after `fraud_rag_serving.app`: FastAPI, `MCPServer` mounted at `/`, stateless streamable HTTP with JSON, `/healthz`, `/readyz`, `/metrics`, tool-call counter, latency histogram and view-status counter; verify a CI test lists both tools, calls one successfully and one with the model down, and reads both outcomes from the counters
- [x] 5.2 Add the `__main__` entry point the Dockerfile runs and verify the CI image job builds the image

## 6. Charts

- [x] 6.1 Add `risk` to `gateway.mcpServers` in `infra/charts/llm-platform/values.yaml` with the risk settings (image, upstream addresses, resources), and verify `helm template` renders its backend, route, policy, `RemoteMCPServer` and catalog entry
- [x] 6.2 Add `templates/risk.yaml`: ServiceAccount, workload, active and preview Services, `VMPodScrape` and an `AuthorizationPolicy` admitting the gateway, metrics agent and dedicated rollout-analysis identity at the appropriate ports, restricted pod security like `rag-api`; verify `helm lint` and `helm template` pass
- [x] 6.3 Set `mcpServers: [risk]` on `risk-agent`, move it to 0.2.0, rewrite `files/prompts/risk-agent.md` for the tool, and verify the rendered AgentTemplate lists the `risk` server and the catalog publishes 0.2.0
- [x] 6.4 Admit the `risk` pod to the feature server's NetworkPolicy in `infra/charts/data-pipelines/templates/feature-serving.yaml` (namespace `llm`, pod label `app.kubernetes.io/name: risk-serving`) and verify `helm template` shows both callers and no other
- [x] 6.5 Verify every chart renders under `helm lint` in CI, and that the dev and prod values render

- [x] 6.6 Add risk KEDA scaling between two and four replicas on its tool-call counter, with an initial configurable target of 5 calls/s per replica, a `PodDisruptionBudget` (`minAvailable: 1`) and no chart-owned replicas; retain knowledge's existing combined-rate target and existing Locust test, and add no risk Locust file or Job
- [x] 6.7 Add dev-enabled blue-green Rollouts for risk and knowledge with active/preview Services, one preview replica, pre-promotion analysis and delayed old-revision scale-down; retain production's existing workload behavior, remove superseded dev Deployment configuration and verify dev and prod render differences with `helm template`
- [x] 6.8 Retarget knowledge's existing KEDA object and risk's new object to `argoproj.io/v1alpha1` `Rollout` on dev, and add dev-scoped Argo CD ignores for `/spec/replicas` and controller-owned Service revision hash selectors with `RespectIgnoreDifferences=true`; verify base selectors and security fields remain reconciled
- [x] 6.9 Add bounded `AnalysisTemplate` Jobs for both preview Services using dedicated identities and configured dev fixtures; initialize MCP, list tools and call scoring or retrieval, failing on MCP errors, malformed results, missing fixtures and deadlines; leave `autoPromotionSeconds` unset so it cannot bypass analysis, wait up to 150 seconds for the preview to answer the MCP handshake and fail at once on any later error, and let CI run behavioral coverage for the analysis clients
- [x] 6.10 Plan and verify knowledge Deployment adoption with ready active endpoints and no competing pod controllers; check AppProject permissions, sync ordering and cleanup so the completed dev migration has no superseded Deployment

## 7. Docs and spec

- [x] 7.1 Update `docs/llm/agents.md` (agents table: `risk-agent` 0.2.0 with the `risk` server, status line), `docs/llm/gateway.md` (the `/mcp/risk` route and its mesh rule), `docs/llm/observability.md` (the three metrics and risk scaling target), `docs/llm/rag.md` (knowledge blue-green release and KEDA target), and add a one-line pointer in `docs/ml/serving.md`; keep edits to the affected sections and identify the risk scaling target as provisional without a load-test claim
- [x] 7.2 Run `openspec validate risk-agent-scoring --strict` in `devenv shell` and verify it passes

## 8. Checks and dev proof

- [x] 8.1 Run `treefmt` and the non-test linters through `devenv shell`, push with an explicit refspec, open the PR, and verify the CI jobs (Python tests, Helm lint, image build) pass, reporting any failure honestly
- [x] 8.2 With the `llm-platform` application on dev and the model servers running, call `score_payment_risk` through `/mcp/risk` with a dev customer and fingerprint, and record the score, model version, decline threshold, call counters and latency in `docs/llm/agents.md` as numbers
- [x] 8.3 Run a kagent chat where the coordinator transfers to `risk_agent`, which calls the tool through the gateway; capture screenshots of the transfer, the tool call and the answer, and one trace, and record proof and pending items in separate sentences
- [ ] 8.4 If the Langfuse prompt for `risk-agent` already exists on dev, publish the new `production` version and verify the next conversation uses it

- [ ] 8.5 On dev, record each server's successful pre-promotion AnalysisRun and active revision switch, then demonstrate a failed candidate analysis retaining the previous active revision; record the bounded Job result, active/preview revisions and replica counts as numbers or screenshots, verify Argo CD reconciliation preserves controller-owned fields, and distinguish first-install behavior from an analyzed update
- [x] 8.6 Record risk's observed tool-call rate, KEDA desired/current replica counts and latency alongside the chat proof; report scaling that was not observed as pending rather than claiming a load-test result, retain the existing knowledge Locust test without adding a risk benchmark, and do not run tests locally

## 9. GPU claims and dev placement

- [x] 9.1 Render no `ResourceClaim` and no claim selector from `llm-platform` or `ml-training` in dev or prod while workloads still reference `ml-gpu` and `llm-gpu` by name; verify with `helm template` that no claim renders and that production workload placement is unchanged
- [x] 9.2 Set dev chat-router requests to fit the shared node and preserve production renders; verify on dev that the router scheduler and the ML predictor are both Ready

## 10. PII masking and identifiers

- [x] 10.1 Replace the gateway's `PhoneNumber` built-in with word-boundary phone patterns in `gateway.piiPatterns`, in both masking rules, and verify `helm template` renders them and no `PhoneNumber` for dev and prod
- [x] 10.2 Round every float in the risk tool's result to four decimals and return `at_or_above_decline_threshold` from the unrounded values; verify the CI service test asserts the rounded score and threshold and the flag
- [x] 10.3 On dev, after the release, send the chat question again with the real fingerprint and verify the tool receives it unchanged and the answer carries a score

