# Autoscaling and load test

Three workloads scale on KEDA. The two MCP servers are the agents' tools and the knowledge Web API; the worker pool runs the agents themselves.

| Workload | Range | Metric and target | Proof |
|---|---|---|---|
| Knowledge API (`rag-api`) | 2 to 4 replicas | Combined REST and MCP request rate, 10 req/s per replica | [Locust run](#load-test-and-service-level), [scale-out](#knowledge-api-rag-api) |
| Risk MCP (`risk-serving`) | 2 to 4 replicas | Tool-call rate, 5 calls/s per replica | [Scale-out](#risk-mcp-risk-serving) |
| Agent workers (`llm-agents` WorkerPool) | 2 to 6 workers | Partly or fully booked workers, 0.7 per worker | [Twelve conversations](#agent-worker-pool) |

Model servers are not autoscaled: each environment has one GPU ([Inference](inference.md)). Agent conversations are Substrate actors, created on demand and suspended to zero when idle ([Agents](agents.md#sandbox-warm-start-and-autoscaling)). The coordinator and its sub-agents therefore scale as workers, not as per-agent replicas; the MCP server captures below show the KEDA mechanism on the tools they call. Releases of the two MCP servers are on [MCP releases](mcp-releases.md).

## Load test and service level

[`src/llm/rag_serving/loadtest/locustfile.py`](../../src/llm/rag_serving/loadtest/locustfile.py) requests chunks by id: 19 of 20 requests use real ids and 1 an unknown id that must return 404. It runs in the cluster as an on-demand Job, [`infra/charts/rag-pipeline/loadtest/job.yaml`](../../infra/charts/rag-pipeline/loadtest/job.yaml), against `rag-api` directly as the `llm-benchmark` identity, so it measures the API rather than the gateway's per-client rate limit. The Job asks for 250m CPU and may burst; at 1 CPU it did not schedule on the full dev nodes. The run is 60 users for 5 minutes on 2026-11-05 at 05:36 UTC over 40 chunk ids; the full report is [`loadtest/knowledge-api-locust.html`](loadtest/knowledge-api-locust.html).

```text
Name                       # reqs   # fails |  Avg  Min   Max  Med |  req/s
GET /v1/chunks/{chunk_id}   51738   0 (0%) |   26    4  1463   14 | 172.64
GET /v1/chunks/{unknown}     2715   0 (0%) |   25    5   801   13 |   9.06
Aggregated                  54453   0 (0%) |   26    4  1463   14 | 181.70

Percentiles (ms)   50%  75%  90%  95%  99%  99.9%  100%
Aggregated          14   24   47   86  240   600   1500
```

The service level for the chunk endpoint is p95 at or below 500 ms and no failed requests at 60 users. The run met it with a p95 of 86 ms and 0 failures in 54,453 requests at 181.7 req/s, while KEDA took `rag-api` from 2 to 4 replicas:

![Locust report for rag-api: 54,453 requests, 0 failures, p95 86 ms, 181.7 req/s, 60 users](images/rag-locust-report.png)

## KEDA scale-out

The "Replicas: MCP servers and agent workers" panel in the LLM agents dashboard (Autoscaling row) shows desired and current replicas for `rag-api`, `risk-serving` and the agent worker pool. The risk run (05:29 to 05:35 UTC on 2026-11-05) and the Locust run (05:36 to 05:42) are both on it; the gaps are scrape gaps of the metrics store.

![Grafana, Autoscaling row: risk-serving rises from 2 to 4 replicas at 05:32 and falls back, rag-api rises from 2 to 4 at 05:39 and falls back at about 05:49, and the agent worker pool stays at 3](images/scale-keda-replicas.png)

### Knowledge API (`rag-api`)

KEDA scales `rag-api` from 2 to 4 replicas on its combined REST and MCP request rate (`knowledge_api_requests_total` plus `knowledge_mcp_tool_calls_total` in VictoriaMetrics); the chart leaves `replicas` to KEDA. During the Locust run:

| Time (UTC) | Metric / target (req/s per replica) | Replicas desired (ready) |
|---|---|---|
| 05:36:16 | 0 / 10 | 2 (2) |
| 05:39:21 | 20.4 / 10 | 4 (2) |
| 05:39:52 | 46.3 / 10 | 4 (4) |
| 05:41:52 | 46.3 / 10 | 4 (4), load finished |

### Risk MCP (`risk-serving`)

Risk's 5 calls/s target is above what one dev client generates, so the target was lowered to 0.5 calls/s per replica for one run and set back to 5 afterwards. A client sent 528 `get_risk_features` calls through the gateway in 300 s at 1.76 calls/s; all returned HTTP 200 and none was rate-limited. The server's own counters record 330 successes and 190 tool failures (`feature store read failed`), all between 05:31 and 05:33 while Feast was being rolled out by the same pull request's dev deploy. KEDA counts calls whatever their outcome, so the scale-out does not depend on them:

| Time (UTC) | Metric / target (calls/s per replica) | Replicas desired (ready) |
|---|---|---|
| 05:30:42 | 0.159 / 0.5 | 2 (2) |
| 05:32:03 | 0.595 / 0.5 | 3 (2) |
| 05:32:24 | 0.446 / 0.5 | 4 (2) |
| 05:33:05 | 0.430 / 0.5 | 4 (3) |
| 05:34:06 | 0.442 / 0.5 | 4 (4) |

The risk server was back to 2 replicas by 05:43. The panel before the worker pool was added shows this run alone:

![Grafana: risk-serving desired and current replicas rise from 2 to 3 and 4 at 05:32 and stay at 4](images/scale-risk-serving.png)

While the new pods started on the full node, two of them failed their liveness probe, which had no startup grace, and were restarted; the chart now gives the pod a startup probe like `rag-api`'s.

### Agent worker pool

KEDA scales the WorkerPool from 2 to 6 workers on Substrate's own metric, the number of workers that are partly or fully booked (`ate_workerpool_workers{ate_worker_state=~"partial|at_capacity"}`), 0.7 per worker. Argo CD leaves the pool's `replicas` to KEDA. Twelve conversations sent at once to the two-worker pool (2026-10-31):

| | |
|---|---|
| Workers | 2 → KEDA asked for 3 at 56 s, Ready at 78 s |
| Conversations accepted on the first message | 12 of 12 |
| Time to the full answer | 79 to 123 s, median 112 s (the shared GPU is the limit) |

## Limits

- The agent worker scale-out has numbers only. The 2026-10-31 run has no Grafana capture, and the 2026-11-05 attempt read 667m of the 700m target and asked for a third worker, but that worker (on `dev-server3`) stayed not Ready for 26 minutes because its credential bundle was issued late and its readiness probe timed out. The dashboard capture above shows the pool at 3 workers, not a scale-out. A clean capture needs a dev window with spare node capacity and a signed-in Grafana session.
- The risk run demonstrates scaling, not a failure-free service level: the server's counters account for 520 of the 528 client calls, and the remaining eight outcomes were not reconciled. The 5 calls/s target itself was not reached.
- The Locust run is one run on dev, directly against `rag-api`; the Feast feature server has no load test and no autoscaler ([serving limits](../ml/serving.md#limits)).
