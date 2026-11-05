# Risk agent: scoring tool

`risk-agent` re-scores a payment with the served fraud model and explains what is unusual about it. It reaches the model through the `risk` MCP server (`risk-serving`), which the agent gateway exposes at `/mcp/risk`. Dev enables the server and `risk-agent` 0.2.0; production keeps the toolless 0.1.0 agent and has no risk server. The agent runtime, registry and prompts are in [Agents](agents.md); the knowledge tools are in [RAG](rag.md).

## Tool

The `risk` server accepts a `request` object with `integration_id`, `customer_id`, `payment_method_fingerprint`, `amount_minor`, both account-creation timestamps, `shipping_country`, `ip_country`, `item_count` and `has_device`. The tool sets the scoring time, so the result is a what-if re-score, not a stored decision.

| Tool | Returns |
|---|---|
| `get_risk_features` | The five `payment_risk_v1` views, each with its status (`available`, `missing`, `future`, `expired`, `error`, `timeout`) and values only when `available` |
| `score_payment_risk` | Probability, model version, decline threshold, `at_or_above_decline_threshold`, every view's status and the drivers |

Unavailable views contribute NaN to Triton, and an upstream error returns no score. Drivers rank feature gain times distance from the training reference distribution; they are not per-prediction attributions, and a reference from another model version yields no drivers. Numbers in a result carry at most four decimals, because the gateway masks any run of 13 or more digits as a card number.

## Releases and scaling

On dev, `risk-serving` is a blue-green Rollout and KEDA (2 to 4 replicas, target 5 calls/s per replica) targets its scale subresource. A dedicated analysis Job initializes MCP, lists the tools and calls the preview before promotion; a tool error or timeout blocks promotion and keeps the active revision serving. The knowledge server's release is in [RAG](rag.md#dev-releases).

| Release | Pre-promotion analysis | Outcome |
|---|---|---|
| `risk-serving` 756dbb8686 | Successful | Promoted |
| `risk-serving` 5dcd89f99f, faulty: `MODEL_URL` points at a model server that does not exist | Failed in 27 s: `preview returned an MCP tool error` | Rollout aborted; the active Service kept selecting 756dbb8686, and the gateway scored 0.1967 during the rollout and after it |

The faulty candidate was made by pausing Argo CD's reconcile of the app with the `argocd.argoproj.io/skip-reconcile` annotation, patching one environment variable on the Rollout, and restoring it afterwards. Argo Rollouts skips the analysis on a first install, which has no previous revision to protect; that case was not run.

The 5 calls/s target is above what one dev client generates. To see the scaling, the target was lowered to 0.5 calls/s per replica for one run and set back to 5 afterwards; a client sent 240 tool calls through the gateway at an average of 0.97 calls/s (233 succeeded, 7 failed, none rate-limited; the failures were not investigated):

| Time (UTC) | Metric / target | Replicas (desired) |
|---|---|---|
| 02:17:40 | 0 / 0.5 | 2 (2) |
| 02:19:15 | 0.725 / 0.5 | 2 (3) |
| 02:20:01 | 0.430 / 0.5 | 4 (4) |

## Proof on dev

A call to `/mcp/risk` through the gateway returned 401 without a key, listed both tools, and scored a dev customer with all five views `available` (748 ms cold, 256 ms warm).

At 02:12 UTC on 2026-11-05 a kagent chat sent the coordinator an analyst question with a dev customer and fingerprint. The coordinator transferred to `risk_agent`, which called `get_risk_features` and `score_payment_risk` with the fingerprint unchanged:

![kagent chat: transfer_to_agent risk_agent, then get_risk_features with the analyst's identifiers](images/risk-chat-transfer.png)

The result is model v5, a score of 0.0006 against the decline threshold 0.8597, `at_or_above_decline_threshold` false, and five drivers led by `customer_snapshot_attempt_count_1h` (34.57% of the ranking) and `amount_minor` (31.92%). The v4 reference had recorded no feature gains because the trainer did not name its features; a dev retrain after the fix registered v5 with gains for 48 of its 50 features.

![The score_payment_risk result: model v5, score, decline threshold, at_or_above_decline_threshold false and the drivers](images/risk-chat-tool-result.png)

The answer states the probability, model version, threshold and top drivers, with the caveat that they are not per-prediction explanations:

![The answer: probability 0.0006, model version 5, decline threshold 0.8597, drivers and the caveat](images/risk-chat-answer.png)

One Langfuse trace covers the conversation, tagged `risk-agent:production`, with both tool spans between the model calls:

![Langfuse trace: invoke_agent coordinator, generate_content, transfer_to_agent, invoke_agent risk_agent](images/risk-trace-agents.png)

![The same trace further down: the get_risk_features and score_payment_risk tool spans](images/risk-trace-tools.png)

The agents dashboard, in the `LLM` Grafana folder, covers both MCP servers and has a Risk tool row; `score_payment_risk` read 1.04 s mean and 4.63 s p95 over the 3 hours shown, every feature view `available`, and 2 replicas:

![Grafana, LLM folder: LLM agents with the Risk tool row, latency, feature views by status and MCP server replicas](images/risk-dashboard.png)

## Prompt

`risk-agent-system-prompt` in Langfuse was published as `production` version 3 and the `variant-b` A/B version as version 4. The catalog seeds only missing prompts, so an existing prompt needs new versions in Langfuse rather than a release.

## Not verified

- A fresh conversation on prompt version 3: the kagent controller was in `CreateContainerError` and the A2A route answered 503, so the chat above predates the publish.
- A first install of either Rollout, KEDA scale-down, and the 5 calls/s target itself.
