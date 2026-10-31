import os
from datetime import timedelta
from urllib.parse import quote

import psycopg
from feast import Entity, FeatureStore, FeatureView, Field, FileSource, ValueType
from feast.infra.key_encoding_utils import deserialize_entity_key
from feast.infra.online_stores.helpers import compute_table_id
from feast.repo_config import RepoConfig
from feast.types import Array, Bool, Float32, String
from psycopg import sql

from fraud_rag_common.gateway import DIMENSION

PROJECT = "knowledge_base"
VIEW = "knowledge_chunks"
FIELDS = (
    "doc_id",
    "title",
    "section",
    "audience",
    "synthetic",
    "source_path",
    "text",
    "knowledge_version",
    "content_sha256",
)


def connection() -> dict:
    return {
        "host": os.environ["RAG_DB_HOST"],
        "port": int(os.environ.get("RAG_DB_PORT", "5432")),
        "dbname": os.environ["RAG_DB_NAME"],
        "user": os.environ["RAG_DB_USER"],
        "password": os.environ["RAG_DB_PASSWORD"],
        "sslmode": "require",
    }


def config() -> RepoConfig:
    db = connection()
    url = (
        f"postgresql+psycopg://{quote(db['user'], safe='')}:{quote(db['password'], safe='')}"
        f"@{db['host']}:{db['port']}/{db['dbname']}?sslmode=require"
    )
    return RepoConfig(
        project=PROJECT,
        provider="local",
        registry={"registry_type": "sql", "path": url, "cache_ttl_seconds": 60},
        online_store={
            "type": "postgres",
            "host": db["host"],
            "port": db["port"],
            "database": db["dbname"],
            "user": db["user"],
            "password": db["password"],
            "sslmode": "require",
            "vector_enabled": True,
        },
        entity_key_serialization_version=3,
    )


def store() -> FeatureStore:
    return FeatureStore(config=config())


def definitions(source_path: str) -> tuple[Entity, FeatureView]:
    chunk = Entity(name="chunk", join_keys=["chunk_id"], value_type=ValueType.STRING)
    source = FileSource(name=f"{VIEW}_source", path=source_path, timestamp_field="event_timestamp")
    schema = [Field(name=name, dtype=Bool if name == "synthetic" else String) for name in FIELDS]
    schema.append(
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=DIMENSION,
            vector_search_metric="COSINE",
        )
    )
    view = FeatureView(name=VIEW, entities=[chunk], ttl=timedelta(0), schema=schema, source=source, online=True)
    return chunk, view


def table_name(view: FeatureView) -> str:
    return compute_table_id(PROJECT, view, False)


# Feast creates `vector_value` without a dimension and never indexes it; pgvector can only index a
# dimensioned column, and only the v1 ORDER BY ... LIMIT query (not v2's GROUP BY) can use the index.
def index_statements(table: str) -> list[sql.Composed]:
    return [
        sql.SQL("ALTER TABLE {} ALTER COLUMN vector_value TYPE vector({}) USING vector_value::vector({})").format(
            sql.Identifier(table), sql.Literal(DIMENSION), sql.Literal(DIMENSION)
        ),
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING hnsw (vector_value vector_cosine_ops)").format(
            sql.Identifier(f"{table}_hnsw"), sql.Identifier(table)
        ),
    ]


def refresh(feature_store: FeatureStore, frame, source_path: str) -> None:
    chunk, view = definitions(source_path)
    existing = {item.name for item in feature_store.list_feature_views()}
    if VIEW in existing:
        feature_store.apply([chunk], objects_to_delete=[view], partial=False)
    feature_store.apply([chunk, view])
    with psycopg.connect(**connection()) as conn:
        for statement in index_statements(table_name(view)):
            conn.execute(statement)
    feature_store.write_to_online_store(VIEW, frame)


def online_entities(view: FeatureView) -> int:
    with psycopg.connect(**connection()) as conn:
        query = sql.SQL("SELECT count(DISTINCT entity_key) FROM {}").format(sql.Identifier(table_name(view)))
        return conn.execute(query).fetchone()[0]


def vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(repr(float(value)) for value in vector) + "]"


# The search and the index check share this query, so the index check proves what the search does.
def nearest_query(table: str) -> sql.Composed:
    return sql.SQL(
        "SELECT entity_key, vector_value <=> %(q)s::vector AS distance FROM {} "
        "ORDER BY vector_value <=> %(q)s::vector LIMIT %(k)s"
    ).format(sql.Identifier(table))


def chunk_id_of(entity_key: bytes) -> str:
    return deserialize_entity_key(bytes(entity_key), entity_key_serialization_version=3).entity_values[0].string_val


def uses_index(view: FeatureView) -> bool:
    table = table_name(view)
    probe = vector_literal([0.0] * (DIMENSION - 1) + [1.0])
    with psycopg.connect(**connection()) as conn:
        conn.execute("SET enable_seqscan = off")
        rows = conn.execute(sql.SQL("EXPLAIN ") + nearest_query(table), {"q": probe, "k": 5}).fetchall()
    return any(f"{table}_hnsw" in row[0] for row in rows)


def read(feature_store: FeatureStore, chunk_ids: list[str]) -> list[dict | None]:
    if not chunk_ids:
        return []
    response = feature_store.get_online_features(
        features=[f"{VIEW}:{name}" for name in FIELDS],
        entity_rows=[{"chunk_id": chunk_id} for chunk_id in chunk_ids],
    ).to_dict()
    rows = []
    for index, chunk_id in enumerate(response["chunk_id"]):
        row = {name: response[name][index] for name in FIELDS}
        rows.append({"chunk_id": chunk_id, **row} if row["text"] is not None else None)
    return rows


# Feast 0.67 cannot return ids with distances on Postgres: v1 retrieval returns only the embedding column and v2
# can pair ids with the wrong rows. The nearest-neighbour query runs on Feast's own table, then `read` fetches the
# features through Feast.
def search(feature_store: FeatureStore, vector: list[float], top_k: int) -> list[tuple[str, float]]:
    _, view = definitions("unused")
    with psycopg.connect(**connection()) as conn:
        rows = conn.execute(nearest_query(table_name(view)), {"q": vector_literal(vector), "k": top_k}).fetchall()
    return [(chunk_id_of(key), float(distance)) for key, distance in rows if distance is not None]
