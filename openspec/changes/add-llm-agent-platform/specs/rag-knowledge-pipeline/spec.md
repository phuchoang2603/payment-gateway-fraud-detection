## ADDED Requirements

### Requirement: Versioned document corpus
The RAG corpus SHALL contain only documents about gateway behaviour, never payment, label or customer data, and never simulator configuration or calibration.
- Documents derived from specs, contracts or docs SHALL be rendered from those sources into the corpus carried by the `rag` image, which is built from `src/rag/`.
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
An Argo `WorkflowTemplate` `rag-knowledge` in the `rag-pipeline` chart, in namespace `llm`, SHALL run these steps in order: publish corpus, chunk, embed, write, validate, contract checks. On exit it SHALL publish the run status and lineage. Steps SHALL hand each other object paths, not data, and SHALL run without Spark.

An Argo CD PostSync hook SHALL start a run whenever the `rag-pipeline` application syncs a change, and the hook SHALL have a deadline. Manual submission from the template SHALL remain possible. There SHALL be no schedule.
- Chunks SHALL follow the document structure (requirement, clause, section or FAQ entry), split longer sections on paragraph boundaries with overlap, and start with the document title and section.
- `chunk_id` SHALL be `<doc_id>#<section-slug>[-n]`, and each chunk SHALL carry a `content_sha256`.
- Embedding SHALL use the platform's embedding model through the agent gateway's `/v1/embeddings` route with the `rag-ingest` client key, never by calling the model server directly. It SHALL produce 1024-dimension vectors, and documents SHALL be embedded without a query instruction.

The workflow SHALL be enabled only where the LLM platform is enabled.

#### Scenario: Release with a changed document
- **WHEN** a release publishes a new `rag` image because a corpus source changed
- **THEN** the application's sync starts a `rag-knowledge` run that writes a new `knowledge_version`

#### Scenario: Embedding through the gateway
- **WHEN** the embed step runs
- **THEN** its requests carry the `rag-ingest` key, pass the gateway's policy, and are refused without the key

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

#### Scenario: Section deleted from a document
- **WHEN** a section is removed and the next version is written
- **THEN** its `chunk_id` is no longer returned by the online store, and the offline table still holds it under the earlier version

#### Scenario: Reader role
- **WHEN** a consumer connects with the reader role and attempts DDL or a write
- **THEN** Postgres rejects it

### Requirement: Knowledge run validation
A run SHALL fail when any check fails. The contract checks SHALL run on the current version through the catalog's test runner, as the other data products do:
- row count within the expected range;
- unique `chunk_id`;
- no empty text, and text length within bounds;
- 1024 finite embedding values with unit norm;
- synthetic documents flagged;
- no card-number or e-mail patterns in the text.

The pipeline checks SHALL verify that:
- every manifest document has at least one chunk;
- every current chunk is readable from the online store by id with the same `content_sha256`, and no other ids are online;
- a golden question set reaches the configured recall@3 against the expected documents.

Each run SHALL keep a report under `runs/payment-gateway/knowledge/<run>/`.

#### Scenario: Stale online chunk
- **WHEN** the online store holds a `chunk_id` that is not in the current version
- **THEN** the validate step fails the run and names the extra id

#### Scenario: Retrieval regression
- **WHEN** a corpus or chunker change makes the golden recall@3 fall below its threshold
- **THEN** the run fails before the change is reported as successful

### Requirement: Knowledge Web API
The `rag-api` service SHALL be a FastAPI application with pydantic request and response models, async handlers, and liveness and readiness endpoints used by its Kubernetes probes. It SHALL be the only reader of the knowledge online store, and SHALL connect with the reader role. It SHALL provide:
- chunk retrieval by `chunk_id`;
- search that embeds the query through the gateway with the embedding model's query instruction and its own client key, takes candidate ids from Feast vector search, then reads the chunks by id and ranks them by distance;
- customer feature retrieval by `customer_id` from the online feature store, once customer features are online.

It SHALL run as a Knative Service in `llm`, and SHALL be load-tested for throughput and latency.

#### Scenario: Unknown chunk id
- **WHEN** a client requests a `chunk_id` that is not in the current version
- **THEN** the API returns 404 with a validated error body

#### Scenario: Invalid search request
- **WHEN** a search request has an empty query or `top_k` outside the allowed range
- **THEN** the API returns 422 without calling the gateway or the store

### Requirement: Knowledge MCP tool
The `rag-mcp` server SHALL expose the streamable-HTTP MCP tools `search_knowledge` and `get_knowledge_chunk`, implemented by calling the Web API. It SHALL hold no database credentials.
- It SHALL run as a Knative Service with revision-based rolling updates and automatic fallback to the last ready revision.
- It SHALL be reachable by agents only through the gateway route `/mcp/knowledge`.
- It SHALL be published to the agent registry as an MCP server.

`evidence-agent` SHALL use these tools through the gateway, as version 0.2.0.

#### Scenario: Agent cites a chunk
- **WHEN** an analyst asks `evidence-agent` what `NEW_ACCOUNT_GEO_MISMATCH` means
- **THEN** the agent calls `search_knowledge` through the gateway and answers citing the returned `chunk_id`

#### Scenario: Failed revision
- **WHEN** a new `rag-mcp` revision fails its readiness check
- **THEN** traffic stays on the last ready revision
