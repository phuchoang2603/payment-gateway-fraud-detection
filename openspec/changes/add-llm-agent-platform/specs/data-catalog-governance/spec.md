## ADDED Requirements

### Requirement: Knowledge-base product in the catalog
The RAG knowledge pipeline SHALL be governed like the other data pipelines:
- an ODPS product and ODCS contract owned by the `rag-pipeline` chart, whose SQL quality rules the shared checks job runs against `knowledge.chunks`;
- the Argo pipeline `rag-knowledge` with its run status;
- lineage from the corpus version container through `rag-knowledge` to `knowledge.chunks`.

#### Scenario: Successful knowledge run
- **WHEN** a `rag-knowledge` run completes
- **THEN** OpenMetadata shows the run status, the passing contract test results on `knowledge.chunks`, and lineage from the corpus version container to that table
