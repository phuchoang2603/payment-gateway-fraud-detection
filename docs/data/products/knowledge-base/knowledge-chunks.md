# urn:datacontract:fraud:knowledge-chunks
## Info
*One row per knowledge-base chunk per knowledge version, with its text, provenance and embedding, as written by the rag-knowledge pipeline.*
- **name:** RAG knowledge chunks
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Agents read the current version through the knowledge API and MCP tools, never this table directly. Use this table for history, lineage and quality checks. The current version is the one with the latest event_timestamp.

### Purpose
One row per knowledge-base chunk per knowledge version, with its text, provenance and embedding, as written by the rag-knowledge pipeline.

### Limitations
Documents describe gateway behaviour only; they hold no payment, label or customer data. Hand-written documents are synthetic and say so. Embeddings come from Qwen3-Embedding-0.6B through the agent gateway and change whenever the model or chunker changes.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Iceberg table chunks in schema knowledge, read through the ClickHouse lakehouse database. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** lakehouse<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### chunks
*Append-only history of knowledge chunks; each knowledge version is written once.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  chunk_id | string | *Stable chunk identity, doc_id#section-slug with an optional -n part suffix.*<br />• `required` |
|  doc_id | string | *Source document identity.*<br />• `required` |
|  title | string | *Document title.*<br />• `required` |
|  section | string | *Heading path of the chunk within its document.*<br />• `required` |
|  audience | string | *analyst, merchant or both.*<br />• `required` |
|  synthetic | boolean | *True for hand-written synthetic documents, false for documents rendered from repository sources.*<br />• `required` |
|  source_path | string | *Repository sources the document was rendered from or must agree with, separated by semicolons.*<br />• `required` |
|  text | string | *Chunk text, prefixed with the document title and section.*<br />• `required` |
|  content_sha256 | string | *SHA-256 of the chunk text.*<br />• `required` |
|  embedding | array | *Unit-norm 1024-dimension document embedding.*<br />• `required` |
| &#x2007;&#x21b3; items | number | *No description.* |
|  knowledge_version | string | *Hash of the documents, chunker settings and embedding model with its revision.*<br />• `required` |
|  event_timestamp | timestamp | *Time the version was chunked.*<br />• `required` |

## SLA Properties
