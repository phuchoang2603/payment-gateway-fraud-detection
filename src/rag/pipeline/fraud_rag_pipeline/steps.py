import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from fraud_rag_common import store
from fraud_rag_common.gateway import Embedder
from pyarrow import fs

from fraud_rag_pipeline import corpus, offline, validation
from fraud_rag_pipeline.chunking import chunk
from fraud_rag_pipeline.openmetadata import client as catalog

OUTPUTS = Path(os.environ.get("STEP_OUTPUTS", "/tmp/outputs"))


def filesystem() -> fs.S3FileSystem:
    endpoint = urlparse(os.environ["S3_ENDPOINT"])
    return fs.S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region=os.environ["AWS_REGION"],
        endpoint_override=endpoint.netloc,
        scheme=endpoint.scheme or "https",
        force_virtual_addressing=os.environ.get("S3_PATH_STYLE", "true") != "true",
    )


def key(uri: str) -> str:
    return uri.removeprefix("s3://")


def write_text(storage: fs.S3FileSystem, uri: str, text: str) -> None:
    with storage.open_output_stream(key(uri)) as stream:
        stream.write(text.encode())


def read_text(storage: fs.S3FileSystem, uri: str) -> str:
    with storage.open_input_stream(key(uri)) as stream:
        return stream.read().decode()


def exists(storage: fs.S3FileSystem, uri: str) -> bool:
    return storage.get_file_info(key(uri)).type != fs.FileType.NotFound


def corpus_uri(version: str) -> str:
    return f"{os.environ['KNOWLEDGE_ROOT'].rstrip('/')}/{version}"


def run_uri(name: str) -> str:
    return f"{os.environ['RUN_ROOT'].rstrip('/')}/{name}"


def embedding() -> tuple[str, str]:
    return os.environ["EMBEDDING_MODEL"], os.environ["EMBEDDING_REVISION"]


def output(name: str, value: str) -> None:
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    (OUTPUTS / name).write_text(value)


def report(name: str, checks: dict) -> None:
    write_text(filesystem(), run_uri(f"{name}.json"), json.dumps(checks, indent=2, sort_keys=True))
    print(json.dumps(checks, sort_keys=True))
    if not validation.passed(checks):
        failed = sorted(check for check, value in checks.items() if not value["ok"])
        raise SystemExit(f"{name} failed: {', '.join(failed)}")


def chunk_rows(documents: list[corpus.Document], version: str) -> list[dict]:
    settings = corpus.settings()["chunker"]
    now = datetime.now(UTC)
    return [
        {
            "chunk_id": item.chunk_id,
            "doc_id": item.doc_id,
            "title": item.title,
            "section": item.section,
            "audience": item.audience,
            "synthetic": item.synthetic,
            "source_path": item.source_path,
            "text": item.text,
            "content_sha256": item.content_sha256,
            "knowledge_version": version,
            "event_timestamp": now,
        }
        for document in documents
        for item in chunk(document, settings["max_words"], settings["overlap_words"])
    ]


def already_stored(version: str, expected: int) -> bool:
    if offline.rows(offline.table(offline.catalog()), version).num_rows != expected:
        return False
    _, view = store.definitions(run_uri("embedded.parquet"))
    try:
        return store.online_entities(view) == expected
    except Exception as error:
        print(f"online store not readable yet: {error}")
        return False


def publish() -> None:
    documents = corpus.load(Path(os.environ.get("SOURCES_ROOT", "/app/sources")))
    settings = corpus.settings()["chunker"]
    model, revision = embedding()
    version = corpus.knowledge_version(documents, settings, model, revision)
    storage = filesystem()
    prefix = corpus_uri(version)
    if not exists(storage, f"{prefix}/manifest.json"):
        for document in documents:
            write_text(storage, f"{prefix}/documents/{document.doc_id}.md", document.text)
        manifest = corpus.manifest(documents, version, settings, model, revision)
        write_text(storage, f"{prefix}/manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
    skip = already_stored(version, len(chunk_rows(documents, version)))
    print(json.dumps({"knowledge_version": version, "documents": len(documents), "skip": skip}))
    output("version", version)
    output("skip", "true" if skip else "false")


def stage_chunks() -> None:
    version = os.environ["KNOWLEDGE_VERSION"]
    storage = filesystem()
    prefix = corpus_uri(version)
    manifest = json.loads(read_text(storage, f"{prefix}/manifest.json"))
    documents = [
        corpus.Document(
            doc_id=entry["doc_id"],
            title=entry["title"],
            audience=entry["audience"],
            synthetic=entry["synthetic"],
            sources=tuple(entry["sources"]),
            text=read_text(storage, f"{prefix}/documents/{entry['doc_id']}.md"),
        )
        for entry in manifest["documents"]
    ]
    for document, entry in zip(documents, manifest["documents"]):
        if document.sha256 != entry["sha256"]:
            raise SystemExit(f"{document.doc_id}: stored document does not match its manifest hash")
    rows = chunk_rows(documents, version)
    schema = pa.schema([field for field in offline.SCHEMA if field.name != "embedding"])
    with storage.open_output_stream(key(run_uri("chunks.parquet"))) as stream:
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), stream)
    print(json.dumps({"knowledge_version": version, "chunks": len(rows)}))


def read_table(name: str) -> pa.Table:
    with filesystem().open_input_file(key(run_uri(name))) as stream:
        return pq.read_table(stream)


def embed() -> None:
    chunks = read_table("chunks.parquet")
    vectors = Embedder.from_env().documents(chunks.column("text").to_pylist())
    embedded = chunks.append_column("embedding", pa.array(vectors, type=pa.list_(pa.float32())))
    with filesystem().open_output_stream(key(run_uri("embedded.parquet"))) as stream:
        pq.write_table(embedded.select(offline.SCHEMA.names).cast(offline.SCHEMA), stream)
    print(json.dumps({"embedded": embedded.num_rows}))


def validate() -> None:
    rows = read_table("embedded.parquet").to_pylist()
    version = os.environ["KNOWLEDGE_VERSION"]
    manifest = json.loads(read_text(filesystem(), f"{corpus_uri(version)}/manifest.json"))
    questions = [item["question"] for item in validation.golden()["questions"]]
    checks = {
        **validation.contract_rules(rows),
        **validation.coverage(rows, manifest),
        **validation.retrieval(rows, Embedder.from_env().queries(questions)),
    }
    report("validate", checks)


def write() -> None:
    embedded = read_table("embedded.parquet")
    version = os.environ["KNOWLEDGE_VERSION"]
    chunks = offline.table(offline.catalog())
    if offline.rows(chunks, version).num_rows == 0:
        offline.append(chunks, embedded)
    frame = embedded.to_pandas()
    frame["embedding"] = frame["embedding"].map(lambda vector: [float(value) for value in vector])
    store.refresh(store.store(), frame, run_uri("embedded.parquet"))
    print(json.dumps({"knowledge_version": version, "written": embedded.num_rows}))


def verify() -> None:
    version = os.environ["KNOWLEDGE_VERSION"]
    expected = offline.rows(offline.table(offline.catalog()), version).to_pylist()
    feature_store = store.store()
    _, view = store.definitions(run_uri("embedded.parquet"))
    online = store.read(feature_store, [row["chunk_id"] for row in expected])
    report(
        "verify",
        validation.store_matches(expected, online, store.online_entities(view), store.uses_index(view)),
    )


def publish_catalog() -> None:
    summary = catalog.publish(
        catalog.Catalog.from_env(),
        os.environ["WORKFLOW_STATUS"],
        os.environ["RUN_ID"],
        os.environ.get("KNOWLEDGE_VERSION", ""),
        json.loads(os.environ.get("AI_ASSETS", "null")),
    )
    print(json.dumps(summary, sort_keys=True))


STEPS = {
    "publish": publish,
    "chunk": stage_chunks,
    "embed": embed,
    "validate": validate,
    "write": write,
    "verify": verify,
    "catalog": publish_catalog,
}


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in STEPS:
        raise SystemExit(f"usage: python -m fraud_rag_pipeline {{{','.join(STEPS)}}}")
    STEPS[sys.argv[1]]()
