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
- [x] 6.7 Add blue-green Rollouts shared by both environments for risk and knowledge with active/preview Services, one preview replica, pre-promotion analysis and delayed old-revision scale-down; remove alternate Deployment configuration and verify dev and prod render the same strategy with `helm template`
- [x] 6.8 Retarget knowledge's existing KEDA object and risk's new object to `argoproj.io/v1alpha1` `Rollout` in both environments, and add shared Argo CD ignores for `/spec/replicas` and controller-owned Service revision hash selectors with `RespectIgnoreDifferences=true`; verify base selectors and security fields remain reconciled
- [x] 6.9 Add bounded `AnalysisTemplate` Jobs for both preview Services using dedicated identities and configured shared arguments; initialize MCP, list tools and call scoring or retrieval, failing on MCP errors, malformed results, missing request features or knowledge passages and deadlines; leave `autoPromotionSeconds` unset so it cannot bypass analysis, wait up to 150 seconds for the preview to answer the MCP handshake and fail at once on any later error, and let CI run behavioral coverage for the analysis clients
- [x] 6.10 Plan and verify knowledge Deployment adoption with ready active endpoints and no competing pod controllers; check AppProject permissions, sync ordering and cleanup so the completed dev migration has no superseded Deployment

## 7. Docs and spec

- [x] 7.1 Update `docs/llm/agents.md` (agents table: `risk-agent` 0.2.0 with the `risk` server, status line), `docs/llm/gateway-and-jev-policy.md` (the `/mcp/risk` route and its mesh rule), `docs/deployment/observability.md` (the three metrics), `docs/llm/scaling.md` (risk scaling target), `docs/llm/rag.md` (knowledge blue-green release and KEDA target), and add a one-line pointer in `docs/ml/serving.md`; keep edits to the affected sections and identify the risk scaling target as provisional without a load-test claim
- [x] 7.2 Run `openspec validate risk-agent-scoring --strict` in `devenv shell` and verify it passes

## 8. Checks and dev proof

- [x] 8.1 Run `treefmt` and the non-test linters through `devenv shell`, push with an explicit refspec, open the PR, and verify the CI jobs (Python tests, Helm lint, image build) pass, reporting any failure honestly
- [x] 8.2 With the `llm-platform` application on dev and the model servers running, call `score_payment_risk` through `/mcp/risk` with a dev customer and fingerprint, and record the score, model version, decline threshold, call counters and latency in `docs/llm/agents.md` as numbers
- [x] 8.3 Run a kagent chat where the coordinator transfers to `risk_agent`, which calls the tool through the gateway; capture screenshots of the transfer, the tool call and the answer, and one trace, and record proof and pending items in separate sentences
- [x] 8.4 If the Langfuse prompt for `risk-agent` already exists on dev, publish the new `production` version and verify the next conversation uses it

- [x] 8.5 On dev, record each server's successful pre-promotion AnalysisRun and active revision switch, then demonstrate a failed candidate analysis retaining the previous active revision; record the bounded Job result, active/preview revisions and replica counts as numbers or screenshots, verify Argo CD reconciliation preserves controller-owned fields, and distinguish first-install behavior from an analyzed update
- [x] 8.6 Record risk's observed tool-call rate, KEDA desired/current replica counts and latency alongside the chat proof; report scaling that was not observed as pending rather than claiming a load-test result, retain the existing knowledge Locust test without adding a risk benchmark, and do not run tests locally

## 9. GPU claims and dev placement

- [x] 9.1 Render no `ResourceClaim` and no claim selector from `llm-platform` or `ml-training` in dev or prod while workloads still reference `ml-gpu` and `llm-gpu` by name; verify with `helm template` that no claim renders and that production workload placement is unchanged
- [x] 9.2 Set dev chat-router requests to fit the shared node and retain capacity-specific production sizing; verify on dev that the router scheduler and the ML predictor are both Ready

## 10. PII masking and identifiers

- [x] 10.1 Replace the gateway's `PhoneNumber` built-in with word-boundary phone patterns in `gateway.piiPatterns`, in both masking rules, and verify `helm template` renders them and no `PhoneNumber` for dev and prod
- [x] 10.2 Round every float in the risk tool's result to four decimals and return `at_or_above_decline_threshold` from the unrounded values; verify the CI service test asserts the rounded score and threshold and the flag
- [x] 10.3 On dev, after the release, send the chat question again with the real fingerprint and verify the tool receives it unchanged and the answer carries a score

## 11. Review follow-ups

- [x] 11.1 Run a faulty candidate for each server on dev with Argo CD's reconcile paused for the app, record the analysis result and that the active revision kept serving, then restore the spec; verify the Rollouts return to Healthy with the original template
- [x] 11.2 Set `progressDeadlineAbort: true` on both Rollouts; verify `helm template` renders it in both environments
- [x] 11.3 Move the agents dashboard to the `LLM` folder through `dashboards.folder`, cover both MCP servers in its tool panels and add the Risk tool row; verify the rendered ConfigMap carries the annotation and valid JSON
- [x] 11.4 Remove the GPU `nodeSelector` and tolerations from the `llm-platform` and `ml-training` charts and the benchmark manifests; verify no claim, selector or toleration renders and that workloads still reference their claims by name

## 12. Scaling evidence

- [x] 12.1 Give the risk pod a startup probe like `rag-api`'s, so replicas started by KEDA on a busy node are not killed by the liveness probe; verify with `helm template` that the probe renders and `helm lint` passes
- [x] 12.2 Lower the Locust Job's CPU request so it schedules on the dev nodes, rerun it against `rag-api`, and record the report, the service level it checks and KEDA's 2 to 4 scale-out
- [x] 12.3 Record the risk scale-out and scale-down on the dashboard's replicas panel, which also charts the agent worker pool, and gather the load test, KEDA runs and blue-green releases in `docs/llm/scaling.md`; record that the worker pool's third worker stayed not Ready

## 13. Merge preparation

- [x] 13.1 Move the shared MCP release-analysis client and its tests under LLM common; have both serving images copy it and remove service-specific coupling from image selection
- [x] 13.2 Replace the original dev Feast probe-IP ingress exception with container-local health probes, verify dev readiness and preserve production's existing probes (superseded by task 14.4)
- [x] 13.3 Capture the blue-green preview, retained active revision and successful analysis in the Rollouts dashboard
- [x] 13.4 Rebase on current main, refresh the PR description with verified proof and remaining limits, and pass formatting, lint, Helm renders, strict OpenSpec validation and CI

## 14. Shared environment behavior

- [x] 14.1 Use the same risk tools, prompt, blue-green workloads, preview identities, Feast callers and Argo controller ownership in dev/prod; remove alternate paths and render both environments
- [x] 14.2 Move analysis and behavior tests into common, update image change detection and copies without adding Feast to the risk image
- [x] 14.3 Make the scoring gate independent of a seeded dev customer, while rejecting malformed results, missing request features and upstream failures; let CI run behavior tests
- [x] 14.4 Remove the Feast exec probe override, render native HTTP probes for both environments, and diagnose dev probe connectivity after deployment without adding source-IP exceptions
- [x] 14.5 Update affected docs and validate lint, formatting, lock consistency, chart rendering and OpenSpec; record CI and deployment results separately
