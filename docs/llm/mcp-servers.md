# MCP servers: releases and scaling

The knowledge server (`rag-api`, [RAG](rag.md)) and the risk server (`risk-serving`, [Agents](agents.md#risk-agent)) sit behind the agent gateway. On dev both are blue-green Rollouts and KEDA targets their scale subresources; production retains the Deployment. The active Service keeps its name and a `-preview` Service selects the candidate. A bounded pre-promotion Job with its own identity initializes MCP, lists the tools and makes one call (`rag-api`: a configured knowledge chunk; `risk-serving`: a scoring call). A tool error, missing result or timeout blocks promotion and keeps the active revision serving. The preview starts at one replica and grows to KEDA's desired count before the Service switches; Argo CD leaves replica counts and revision hash selectors to their controllers. `progressDeadlineAbort` aborts a candidate that never becomes ready.

| Release | Pre-promotion analysis | Outcome |
|---|---|---|
| `rag-api` 7fcf4cbc | Successful in 73 s, waiting for a cold pod | Promoted; one Rollout owns every ReplicaSet and no Deployment remains |
| `risk-serving` 756dbb8686 | Successful | Promoted |
| `rag-api` 69f45b78c4, faulty: `RAG_DB_HOST` points at a missing database | None: the preview pod failed readiness with 503 for more than 5 min | Never promoted; the active Service kept selecting 7fcf4cbc. The Rollout stayed `Progressing` until the spec was restored, which is why `progressDeadlineAbort` is set |
| `risk-serving` 5dcd89f99f, faulty: `MODEL_URL` points at a model server that does not exist | Failed in 27 s: `preview returned an MCP tool error` | Rollout aborted; the active Service kept selecting 756dbb8686, and the gateway scored 0.1967 during the rollout and after it |

Each faulty candidate was made by pausing Argo CD's reconcile of the app with the `argocd.argoproj.io/skip-reconcile` annotation, patching one environment variable on the Rollout, and restoring it afterwards; both Rollouts returned to Healthy on their original revisions. Argo Rollouts skips the analysis on a first install, which has no previous revision to protect; that case was not run.

KEDA scales `rag-api` from 2 to 4 replicas on its combined REST and MCP request rate at 10 req/s per replica, with a Locust run in [RAG](rag.md#load-test-and-autoscaling), and `risk-serving` from 2 to 4 on its tool-call rate at an initial 5 calls/s per replica, which has no Locust test. Risk's 5 calls/s target is above what one dev client generates, so the target was lowered to 0.5 calls/s per replica for one run and set back to 5 afterwards. A client sent 240 tool calls through the gateway at an average of 0.97 calls/s (233 succeeded, 7 failed, none rate-limited; the failures were not investigated):

| Time (UTC) | Metric / target | Replicas (desired) |
|---|---|---|
| 02:17:40 | 0 / 0.5 | 2 (2) |
| 02:19:15 | 0.725 / 0.5 | 2 (3) |
| 02:20:01 | 0.430 / 0.5 | 4 (4) |

Scale-down, the 5 calls/s target itself and why the fourth replica did not become ready were not observed.

