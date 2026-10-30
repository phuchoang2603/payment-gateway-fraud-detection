from fraud_rag_common import store
from fraud_rag_common.gateway import DIMENSION


def test_feast_table_name_is_the_one_the_index_step_alters():
    _, view = store.definitions("s3://bucket/run/embedded.parquet")

    assert store.table_name(view) == "knowledge_base_knowledge_chunks"


def test_feature_view_exposes_every_field_and_a_cosine_vector():
    chunk, view = store.definitions("s3://bucket/run/embedded.parquet")
    fields = {field.name: field for field in view.schema}

    assert chunk.join_key == "chunk_id"
    assert set(store.FIELDS) | {"embedding"} <= set(fields)
    assert fields["embedding"].vector_index and fields["embedding"].vector_length == DIMENSION
    assert fields["embedding"].vector_search_metric == "COSINE"
    assert view.ttl.total_seconds() == 0


def test_repo_config_validates_with_a_vector_enabled_postgres_store(monkeypatch):
    for name, value in {
        "RAG_DB_HOST": "rag-pgvector-rw",
        "RAG_DB_NAME": "rag",
        "RAG_DB_USER": "rag_reader",
        "RAG_DB_PASSWORD": "p@ss:word",
    }.items():
        monkeypatch.setenv(name, value)

    config = store.config()

    assert config.online_store.vector_enabled
    assert config.project == store.PROJECT
    assert "p%40ss%3Aword" in config.registry.path
