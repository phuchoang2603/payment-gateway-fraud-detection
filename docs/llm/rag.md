# RAG: knowledge pipeline, Web API and MCP tool

The agents answer from a small knowledge base about the payment gateway: decline codes, risk policy, the payment lifecycle, the checkout API, fraud features and support guidance. A pipeline turns documents into embedded chunks in a feature store, and one service serves them as a Web API (by id) and as MCP tools (search and read). It never reads payment data.

- Code: [`src/llm`](../../src/llm), a uv workspace: [`common`](../../src/llm/common) (Feast store, gateway embedding client), [`rag_pipeline`](../../src/llm/rag_pipeline) (corpus, chunking, validation, steps, OpenMetadata) and [`rag_serving`](../../src/llm/rag_serving) (FastAPI + MCP).
- Deployment: [`infra/charts/rag-pipeline`](../../infra/charts/rag-pipeline), application `rag-pipeline` (workflow in `data`, store and API in `llm`). Images `rag-pipeline` and `rag-serving`.
- Contract: [`contracts/data-products/knowledge-base`](../../contracts/data-products/knowledge-base).

## Pipeline

```text
publish → chunk → embed → validate → write → verify → checks → (on exit) catalog
```

| Step | What it does |
|---|---|
| publish | Renders the corpus from the repository (specs, contracts, docs) plus six hand-written documents labelled synthetic, computes the knowledge version (a hash of the documents, chunker settings and embedding model revision) and stores it under an immutable prefix. If that version is already stored, every following step is skipped. |
| chunk | Splits by section heading, at most 280 words with 40 words of overlap; each chunk gets a stable `chunk_id` (`<doc>#<section-slug>`). |
| embed | Embeds every chunk through the gateway's `/v1/embeddings` with the pipeline's own client key. |
| validate | The quality gate, before anything is written: document coverage, embedding shape, no card number or e-mail in the text, synthetic documents flagged, and recall@3 on a golden question set. |
| write | Appends the version to the Iceberg table `knowledge.chunks` (the offline history, partitioned by version) and writes it through Feast to the pgvector online store, then types the vector column and builds an HNSW index (Feast does not). |
| verify | The online store equals the offline version (same hashes, no extra ids), and the search plan uses the HNSW index. |
| checks | The shared OpenMetadata checks job runs the ODCS contract's six rules on `knowledge.chunks` and records the results in the catalog. |
| catalog | Records the run status, the corpus container and the lineage container → `rag-knowledge` → `knowledge.chunks` in OpenMetadata. |

Each new `rag-pipeline` image starts one run, rendered as a `Workflow` named after the image tag (an ordinary resource, not a sync hook, so a failed run never blocks a sync). A run on an unchanged corpus exits early.

### Stores

| Store | What | Why |
|---|---|---|
| Iceberg `knowledge.chunks` (Polaris catalog, MinIO) | Every version's chunks and embeddings | History and the contract checks, next to the other data products |
| Feast feature view `knowledge_chunks` on CNPG Postgres + pgvector (`rag-pgvector`) | The current version, HNSW-indexed | Low-latency lookups by id and vector search for the API and the tool |

### A complete run on dev

Run `rag-knowledge-07ae38319a`, 2026-10-30 19:12–19:25 UTC, every step succeeded in 13 minutes:

![Argo Workflows: rag-knowledge-07ae38319a, every step succeeded](images/rag-argo-workflow.png)


```text
publish-corpus  {"knowledge_version": "0aa3e93777236677", "documents": 17, "skip": false}
chunk           {"chunks": 161}
embed           {"embedded": 161}
validate        coverage ok, embedding shape ok, no card or e-mail, synthetic flagged, golden recall@3 = 0.9
write           {"written": 161}
verify          online matches offline, no extra ids (161), search uses the HNSW index
checks          all 6 checks succeeded
catalog         {"status": "Successful", "lineage_edges": 1}
```

## Governance

The run's lineage and checks are in OpenMetadata, the same as the Bronze and Silver pipelines:

![OpenMetadata lineage from the corpus container through the rag-knowledge pipeline to knowledge.chunks](images/rag-openmetadata-lineage.png)

The contract's six SQL rules (row count, unique chunk id, text bounds, embedding shape, no card or e-mail, synthetic flagged) as data-quality results on `knowledge.chunks`:

![OpenMetadata data quality: six knowledge-base tests, all successful](images/rag-openmetadata-checks.png)

## Web API and MCP tool (`rag-api`)

One FastAPI process serves both, so there is one deployment, one reader identity and no extra hop.

| Interface | Endpoint | Purpose |
|---|---|---|
| REST (`/v1`, routed as `/knowledge/v1` at the gateway) | `GET /v1/chunks/{chunk_id}` | The data-pull Web API: retrieval by id, with pydantic models and path validation; later also customer features by id |
| MCP (`/mcp`, routed as `/mcp/knowledge`) | `search_knowledge(query, top_k, audience)`, `get_knowledge_chunk(chunk_id)` | The agents' tools. Search embeds the question through the gateway and runs the HNSW query; it exists only as a tool because its callers are agents |
| Health | `/healthz` (liveness), `/readyz` (database reachable) | Kubernetes probes; readiness does not wait for a knowledge version, so the first deploy is not blocked by the pipeline |
| Metrics | `/metrics` | REST requests by path and status, MCP tool calls by tool and outcome |

- Handlers are async; Feast and Postgres calls run in worker threads. The Feast store loads in the background at start-up, so the first tool call after a rollout does not pay for it.
- The MCP server runs stateless with JSON responses, so any replica answers any request.
- Tools are discovered, not listed: kagent and the catalog job read them from `tools/list`, so a new tool is only a code change.

### Deployment, rollout and fallback

Both environments render an Argo Rollout with active and preview Services, readiness gating and a bounded pre-promotion MCP check. A PodDisruptionBudget keeps one replica available, and KEDA owns the desired count. Callers retain mesh identity through the fixed active Service.

[`app.py`](../../src/llm/rag_serving/fraud_rag_serving/app.py) implements async MCP tools with a shared `httpx.AsyncClient`, thread-offloaded Feast calls and locked lazy initialization. [`api.yaml`](../../infra/charts/rag-pipeline/templates/api.yaml) owns blue-green release/readiness; [`mcp.yaml`](../../infra/charts/llm-platform/templates/mcp.yaml) owns gateway routing. A failed candidate analysis retains the active revision; the shared blue-green gate and dev results are in [MCP servers](scaling.md#blue-green-releases). Alternate-backend failover is not implemented. The load test below measures request handling/scaling, not failed-rollout recovery.

### Checks through the gateway

```text
GET /knowledge/v1/chunks/decline-codes%23new-account-geo-mismatch  →  200 {"chunk_id": "decline-codes#new-account-geo-mismatch", "title": "Decline and reason-code glossary", ...}
GET /knowledge/v1/chunks/decline-codes%23missing                   →  404 {"detail":"unknown chunk_id"}
GET /knowledge/v1/chunks/not-a-chunk-id                            →  422 string_pattern_mismatch on path.chunk_id
no key                                                             →  401
160 requests at once with one client's key                         →  140 × 200, 20 × 429
```

MCP discovery, authentication and mesh results are in the [gateway checks](gateway-and-jev-policy.md#checks-on-dev).

### Load test, scaling and releases

The Locust run, its service level, KEDA scale-out and blue-green dev captures of `rag-api` are in [Scaling](scaling.md).

### Tests

[`src/llm/rag_serving/tests/test_service.py`](../../src/llm/rag_serving/tests/test_service.py) uses pytest fixtures and mocks for the store and the embedding client (a fake embedder counts gateway calls, the store functions are monkeypatched), and covers the REST paths (200, 404, 422), the MCP tools, validation before any gateway call, readiness, lazy loading, metrics and the app lifespan. The serving member enforces `--cov-fail-under=90`; in CI (Python CI run 41112821278, job `src/llm/rag_serving`, commit 724831f): 14 passed, 98.52% coverage.

## CI/CD

The `rag-pipeline` and `rag-serving` images use the shared [CI](../deployment/ci.md) and [release flow](../deployment/cd.md#release-flow). Argo CD reconciles the chart; Argo Workflows runs the knowledge pipeline ([recorded run](#a-complete-run-on-dev)).
