## ADDED Requirements

### Requirement: Versioned document corpus
The RAG corpus SHALL contain only documents about gateway behaviour, never payment, label or customer data, and never simulator configuration or calibration.
- Documents derived from specs, contracts or docs SHALL be rendered from those sources when the `knowledge` image is built.
- Hand-written documents SHALL state in their text that they are synthetic.
- CI tests SHALL fail when a code-bearing document lists codes, thresholds or the policy version differently from the code and protos.

The first step of every run SHALL write the corpus to `datasets/payment-gateway/knowledge/<knowledge_version>/` with a `manifest.json` of per-document SHA-256 hashes, and SHALL never rewrite a prefix whose manifest exists. `knowledge_version` SHALL change whenever a document, the chunker settings or the embedding model or its revision changes.

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
An Argo `WorkflowTemplate` `rag-knowledge` in the `data-pipelines` chart SHALL run these steps in order: publish corpus, chunk, embed, write, validate, contract checks. On exit it SHALL publish the run status and lineage. Steps SHALL hand each other object paths, not data.
- Chunks SHALL follow the document structure (requirement, clause, section or FAQ entry), split longer sections on paragraph boundaries with overlap, and start with the document title and section.
- `chunk_id` SHALL be `<doc_id>#<section-slug>[-n]`, and each chunk SHALL carry a `content_sha256`.
- Embedding SHALL use the platform's embedding model through the agent gateway's `/v1/embeddings` route with the `rag` client key, never by calling the model server directly. It SHALL produce 1024-dimension vectors, and documents SHALL be embedded without a query instruction.

The workflow SHALL be enabled only where the LLM platform is enabled.

#### Scenario: Embedding through the gateway
- **WHEN** the embed step runs
- **THEN** its requests carry the `rag` key, pass the gateway's policy, and are refused without the key

#### Scenario: Gateway unavailable
- **WHEN** the embedding route returns errors after the step's retries
- **THEN** the run fails before anything is written to either store

### Requirement: Offline history and pgvector online store
The write step SHALL append the version's chunks, with their embeddings, `knowledge_version`, document metadata and `content_sha256`, to the Iceberg table `knowledge.chunks` in the app-owned Polaris catalog.

It SHALL write the same chunks through Feast, project `knowledge_base`, feature view `knowledge_chunks`, entity `chunk_id`, to a Postgres online store with `vector_enabled` and cosine search. The store SHALL run on the app-owned CNPG cluster `knowledge-pgvector` in `data`, using a pgvector-capable image, with the `vector` extension managed declaratively and a Feast SQL registry in the same database.
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
