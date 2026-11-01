import os

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table

NAMESPACE = "knowledge"
TABLE = "chunks"
SCHEMA = pa.schema(
    [
        pa.field("chunk_id", pa.string()),
        pa.field("doc_id", pa.string()),
        pa.field("title", pa.string()),
        pa.field("section", pa.string()),
        pa.field("audience", pa.string()),
        pa.field("synthetic", pa.bool_()),
        pa.field("source_path", pa.string()),
        pa.field("text", pa.string()),
        pa.field("content_sha256", pa.string()),
        pa.field("embedding", pa.list_(pa.float32())),
        pa.field("knowledge_version", pa.string()),
        pa.field("event_timestamp", pa.timestamp("us", tz="UTC")),
    ]
)


def catalog() -> Catalog:
    lakehouse = load_catalog(
        "lakehouse",
        **{
            "type": "rest",
            "uri": os.environ["ICEBERG_REST_URI"],
            "warehouse": os.environ["ICEBERG_WAREHOUSE"],
            "credential": f"{os.environ['ICEBERG_CLIENT_ID']}:{os.environ['ICEBERG_CLIENT_SECRET']}",
            "oauth2-server-uri": os.environ["ICEBERG_OAUTH2_URI"],
            "scope": "PRINCIPAL_ROLE:ALL",
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.region": os.environ["AWS_REGION"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.force-virtual-addressing": "false" if os.environ.get("S3_PATH_STYLE", "true") == "true" else "true",
        },
    )
    # Polaris runs with STS disabled and rejects PyIceberg's default request for vended credentials; the writer
    # brings its own storage credentials, so the request is dropped. PyIceberg has no public switch for it.
    lakehouse._session.headers.pop("X-Iceberg-Access-Delegation", None)
    return lakehouse


def table(lakehouse: Catalog) -> Table:
    try:
        lakehouse.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    identifier = (NAMESPACE, TABLE)
    if lakehouse.table_exists(identifier):
        return lakehouse.load_table(identifier)
    created = lakehouse.create_table(identifier, schema=SCHEMA)
    with created.update_spec() as spec:
        spec.add_identity("knowledge_version")
    return lakehouse.load_table(identifier)


def rows(chunks: Table, version: str) -> pa.Table:
    return chunks.scan(row_filter=EqualTo("knowledge_version", version)).to_arrow()


def append(chunks: Table, frame: pa.Table) -> None:
    chunks.append(frame.select(SCHEMA.names).cast(SCHEMA))
