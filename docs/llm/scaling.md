# Autoscaling, load test and releases

Three workloads scale on KEDA and two of them release blue-green on dev:

| Workload | Range | Metric and target | Evidence |
|---|---|---|---|
| Knowledge API (`rag-api`) | 2 to 4 replicas | Combined REST and MCP request rate, 10 req/s per replica | [Locust run and scale-out](#knowledge-api-rag-api) |
| Risk MCP (`risk-serving`) | 2 to 4 replicas | Tool-call rate, initial 5 calls/s per replica | [Scale-out](#risk-mcp-risk-serving) |
| Agent workers (`llm-agents` WorkerPool) | 2 to 6 workers | Partly or fully booked workers, 0.7 per worker | [Twelve conversations](#agent-worker-pool) |

Model servers are not autoscaled: each environment has one GPU ([Inference](inference.md)). Agent conversations are Substrate actors, created on demand and suspended to zero when idle ([Agents](agents.md#sandbox-warm-start-and-autoscaling)).

## Load test and service level

[`src/llm/rag_serving/loadtest/locustfile.py`](../../src/llm/rag_serving/loadtest/locustfile.py) requests chunks by id (19 of 20 requests real ids, 1 an unknown id that must return 404). It runs in the cluster as an on-demand Job, [`infra/charts/rag-pipeline/loadtest/job.yaml`](../../infra/charts/rag-pipeline/loadtest/job.yaml), against `rag-api` directly as the `llm-benchmark` identity, so it measures the API rather than the gateway's per-client rate limit. Run on 2026-10-30 with 60 users for 5 minutes; the full report is [`loadtest/knowledge-api-locust.html`](loadtest/knowledge-api-locust.html).

```text
Name                     # reqs   # fails |  Avg  Min   Max  Med |  req/s
GET /v1/chunks/{chunk_id}  39822   0 (0%) |  116   11  2520   84 | 133.21
GET /v1/chunks/{unknown}    2123   0 (0%) |  113   12  1751   81 |   7.10
Aggregated                 41945   0 (0%) |  116   11  2520   84 | 140.31

Percentiles (ms)   50%  75%  90%  95%  99%  99.9%  100%
Aggregated          84  140  230  310  560   1300  2500
```

Service level for the chunk endpoint: p95 at or below 500 ms and no failed requests at 60 users. The run met it, with a p95 of 310 ms, 0 failures in 41,945 requests and 140 req/s:

![Locust report for rag-api: 41,945 requests, 0 failures, p95 310 ms, 140 req/s, 60 users](images/rag-locust-report.png)

## KEDA scale-out

### Knowledge API (`rag-api`)

KEDA scales `rag-api` from 2 to 4 replicas on its combined REST and MCP request rate (`knowledge_api_requests_total` plus `knowledge_mcp_tool_calls_total` in VictoriaMetrics, target 10 req/s per replica); the chart leaves `replicas` to KEDA. During the run:

```text
19:26:53  metric 0/10       ready replicas 2
19:29:25  metric 41/10      ready replicas 2   # HPA: "New size: 4; reason: external metric s0-prometheus above target"
19:30:11  metric 41/10      ready replicas 3
19:30:55  metric 41/10      ready replicas 4
19:34:21  metric 0/10       ready replicas 4   # load finished
19:39:11  metric 0/10       ready replicas 2   # HPA: "New size: 2; reason: All metrics below target" after the 5-minute stabilisation window
```

### Risk MCP (`risk-serving`)

Risk's 5 calls/s target is above what one dev client generates, so the target was lowered to 0.5 calls/s per replica for one run and set back to 5 afterwards. A client sent 240 tool calls through the gateway at an average of 0.97 calls/s (233 succeeded, 7 failed, none rate-limited; the failures were not investigated):

| Time (UTC) | Metric / target | Replicas (desired) |
|---|---|---|
| 02:17:40 | 0 / 0.5 | 2 (2) |
| 02:19:15 | 0.725 / 0.5 | 2 (3) |
| 02:20:01 | 0.430 / 0.5 | 4 (4) |

Scale-down, the 5 calls/s target itself and why the fourth replica did not become ready were not observed.

### Agent worker pool

KEDA scales the WorkerPool from 2 to 6 workers on Substrate's own metric, the number of workers that are partly or fully booked (`ate_workerpool_workers{ate_worker_state=~"partial|at_capacity"}`), 0.7 per worker. Argo CD leaves the pool's `replicas` to KEDA.

Twelve conversations sent at once to the two-worker pool (2026-10-31):

| | |
|---|---|
| Workers | 2 → KEDA asked for 3 at 56 s, Ready at 78 s |
| Conversations accepted on the first message | 12 of 12 |
| Time to the full answer | 79 to 123 s, median 112 s (the shared GPU is the limit) |

## Blue-green releases

The knowledge server (`rag-api`, [RAG](rag.md)) and the risk server (`risk-serving`, [Agents](agents.md#risk-agent)) sit behind the agent gateway. On dev both are blue-green Rollouts and KEDA targets their scale subresources; production retains the Deployment. The active Service keeps its name and a `-preview` Service selects the candidate. A bounded pre-promotion Job with its own identity initializes MCP, lists the tools and makes one call (`rag-api`: a configured knowledge chunk; `risk-serving`: a scoring call). A tool error, missing result or timeout blocks promotion and keeps the active revision serving. The preview starts at one replica and grows to KEDA's desired count before the Service switches; Argo CD leaves replica counts and revision hash selectors to their controllers. `progressDeadlineAbort` aborts a candidate that never becomes ready.

| Release | Pre-promotion analysis | Outcome |
|---|---|---|
| `rag-api` 7fcf4cbc | Successful in 73 s, waiting for a cold pod | Promoted; one Rollout owns every ReplicaSet and no Deployment remains |
| `risk-serving` 756dbb8686 | Successful | Promoted |
| `rag-api` 69f45b78c4, faulty: `RAG_DB_HOST` points at a missing database | None: the preview pod failed readiness with 503 for more than 5 min | Never promoted; the active Service kept selecting 7fcf4cbc. The Rollout stayed `Progressing` until the spec was restored, which is why `progressDeadlineAbort` is set |
| `risk-serving` 5dcd89f99f, faulty: `MODEL_URL` points at a model server that does not exist | Failed in 27 s: `preview returned an MCP tool error` | Rollout aborted; the active Service kept selecting 756dbb8686, and the gateway scored 0.1967 during the rollout and after it |

Each faulty candidate was made by pausing Argo CD's reconcile of the app with the `argocd.argoproj.io/skip-reconcile` annotation, patching one environment variable on the Rollout, and restoring it afterwards; both Rollouts returned to Healthy on their original revisions. Argo Rollouts skips the analysis on a first install, which has no previous revision to protect; that case was not run.
