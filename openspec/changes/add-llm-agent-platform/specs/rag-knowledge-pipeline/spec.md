## ADDED Requirements

### Requirement: Versioned document corpus
The RAG corpus SHALL contain only documents about gateway behaviour, never payment, label or customer data, and never simulator configuration or calibration.
- Documents derived from specs, contracts or docs SHALL be rendered from those sources into the corpus carried by the `rag-pipeline` image, which is built from `src/llm/rag_pipeline/`.
- Hand-written documents SHALL state in their text that they are synthetic.
- CI tests SHALL fail when a code-bearing document lists codes, thresholds or the policy version differently from the code and protos.

No CI job SHALL write to the bucket. The first step of every run SHALL write the corpus from the image to `datasets/payment-gateway/knowledge/<knowledge_version>/` with a `manifest.json` of per-document SHA-256 hashes, and SHALL never rewrite a prefix whose manifest exists. `knowledge_version` SHALL change whenever a document, the chunker settings or the embedding model or its revision changes.

#### Scenario: Reason code added in code only
- **WHEN** a new risk reason code is added to the Fraud policy but not to the `decline-codes` document
- **THEN** the CI sync test fails

#### Scenario: Unchanged corpus
- **WHEN** a run starts and the offline table and the online store already hold the current `knowledge_version`
- **THEN** the run ends successfully without chunking, embedding or writing

#### Scenario: Embedding model changes
- **WHEN** the embedding model revision changes and the documents do not
- **THEN** a new `knowledge_version` is produced and every chunk is embedded again

### Requirement: Chunk, embed and write workflow
An Argo `WorkflowTemplate` `rag-knowledge` in the `rag-pipeline` chart, in namespace `data`, SHALL run these steps in order: publish corpus, chunk, embed, validate, write, verify, contract checks. No store SHALL be written before validation passes. On exit it SHALL publish the run status and lineage. Steps SHALL hand each other object paths, not data, and SHALL run without Spark.

The chart SHALL render one `Workflow` per `rag-pipeline` image tag, so each new pipeline image starts exactly one run. A failed run SHALL NOT fail or retry the application's sync. Runs SHALL have a deadline, manual submission from the template SHALL remain possible, and there SHALL be no schedule.
- Chunks SHALL follow the document structure (requirement, clause, section or FAQ entry), split longer sections on paragraph boundaries with overlap, and start with the document title and section.
- `chunk_id` SHALL be `<doc_id>#<section-slug>[-n]`, and each chunk SHALL carry a `content_sha256`.
- Embedding SHALL use the platform's embedding model through the agent gateway's `/v1/embeddings` route with the `ragingest` client key, never by calling the model server directly. It SHALL produce 1024-dimension vectors, and documents SHALL be embedded without a query instruction.

The workflow SHALL be enabled only where the LLM platform is enabled.

#### Scenario: Release with a changed document
- **WHEN** a release publishes a new `rag-pipeline` image because a corpus source changed
- **THEN** the application's sync starts a `rag-knowledge` run that writes a new `knowledge_version`

#### Scenario: Failed run
- **WHEN** a `rag-knowledge` run fails
- **THEN** the application's sync still completes, the next release still syncs, and the failure is visible in Argo Workflows and as a failed run in OpenMetadata

#### Scenario: Embedding through the gateway
- **WHEN** the embed step runs
- **THEN** its requests carry the `ragingest` key, pass the gateway's policy, and are refused without the key

#### Scenario: Gateway unavailable
- **WHEN** the embedding route returns errors after the step's retries
- **THEN** the run fails before anything is written to either store

### Requirement: Offline history and pgvector online store
The write step SHALL append the version's chunks, with their embeddings, `knowledge_version`, document metadata and `content_sha256`, to the Iceberg table `knowledge.chunks` in the app-owned Polaris catalog.

It SHALL write the same chunks through Feast, project `knowledge_base`, feature view `knowledge_chunks`, entity `chunk_id`, to a Postgres online store with `vector_enabled` and cosine search. The store SHALL run on the app-owned CNPG cluster `rag-pgvector` in `llm`, using a pgvector-capable image, with the `vector` extension managed declaratively and a Feast SQL registry in the same database.
- The pipeline SHALL use a writer role that owns the schema.
- Readers SHALL get a separate SELECT-only role.
- Both passwords SHALL come from the app's Doppler config through External Secrets.

A new version SHALL replace the online contents completely, so chunks removed from the corpus are not retrievable. Every version SHALL stay in the offline table.

After each refresh, the write step SHALL type the Feast table's vector column to 1024 dimensions and build an HNSW cosine index on it. The service SHALL search with the Feast query that this index serves.

#### Scenario: Index after refresh
- **WHEN** the online store has been refreshed for a new version
- **THEN** with sequential scans disabled, the plan for the service's search query uses the HNSW index

#### Scenario: Section deleted from a document
- **WHEN** a section is removed and the next version is written
- **THEN** its `chunk_id` is no longer returned by the online store, and the offline table still holds it under the earlier version

#### Scenario: Reader role
- **WHEN** a consumer connects with the reader role and attempts DDL or a write
- **THEN** Postgres rejects it

### Requirement: Knowledge run validation
A run SHALL fail when any check fails. Every quality check SHALL pass on the staged, embedded chunks before anything is written. The same contract rules SHALL then run on the current version of `knowledge.chunks` through the catalog's test runner, as for the other data products. The rules are:
- row count within the expected range;
- unique `chunk_id`;
- no empty text, and text length within bounds;
- 1024 finite embedding values with unit norm;
- synthetic documents flagged;
- no card-number or e-mail patterns in the text.

Before the write, the gate SHALL also verify that:
- every manifest document has at least one chunk;
- a golden question set, ranked against the staged vectors, reaches the configured recall@3 against the expected documents.

After the write, the verify step SHALL check that every current chunk is readable from the online store by id with the same `content_sha256`, and that no other ids are online.

Each run SHALL keep a report under `runs/payment-gateway/knowledge/<run>/`.

#### Scenario: Stale online chunk
- **WHEN** the online store holds a `chunk_id` that is not in the current version
- **THEN** the verify step fails the run and names the extra id

#### Scenario: Retrieval regression
- **WHEN** a corpus or chunker change makes the golden recall@3 fall below its threshold
- **THEN** the run fails before either store is written

### Requirement: Knowledge Web API and MCP tool in one service
The `rag-api` service SHALL be one FastAPI application that serves both REST endpoints and MCP tools. It SHALL be the only reader of the knowledge online store, and SHALL connect with the reader role.

REST is retrieval by id: pydantic response models, async handlers, and liveness and readiness endpoints used by its Kubernetes probes. It SHALL provide:
- chunk retrieval by `chunk_id`;
- customer feature retrieval by `customer_id` from the online feature store, once customer features are online.

MCP: the streamable-HTTP tools at `/mcp`, in stateless mode:
- `search_knowledge`, which embeds the query through the gateway with the embedding model's query instruction and its own client key, runs the indexed Feast vector search, and reads the chunks back by id;
- `get_knowledge_chunk`, backed by the same function as the REST chunk endpoint.

Search is offered only as the MCP tool, because its callers are agents; REST serves lookups by id.

Deployment:
- It SHALL run as a Deployment in `llm` whose rolling update keeps the previous version serving until a new version is ready.
- Its readiness SHALL depend on the database only, not on a loaded knowledge version.
- It SHALL be load-tested for throughput and latency.
- Agents SHALL reach the MCP tools only through the gateway route `/mcp/knowledge`.
- It SHALL be published to the agent registry as an MCP server.

`evidence-agent` SHALL use these tools through the gateway.

#### Scenario: Unknown chunk id
- **WHEN** a client requests a `chunk_id` that is not in the current version
- **THEN** the API returns 404 with a validated error body

#### Scenario: Invalid search request
- **WHEN** a `search_knowledge` call has an empty query or `top_k` outside the allowed range
- **THEN** the tool returns a validation error without calling the gateway or the store

#### Scenario: Agent cites a chunk
- **WHEN** an analyst asks `evidence-agent` what `NEW_ACCOUNT_GEO_MISMATCH` means
- **THEN** the agent calls `search_knowledge` through the gateway and answers citing the returned `chunk_id`

#### Scenario: Failed revision
- **WHEN** a new `rag-api` revision fails its readiness check
- **THEN** traffic stays on the last ready revision
