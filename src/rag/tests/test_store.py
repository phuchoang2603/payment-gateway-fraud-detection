from rag import store
from rag.gateway import DIMENSION


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
