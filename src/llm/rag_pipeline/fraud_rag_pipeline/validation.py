import math
import re
from pathlib import Path

import yaml
from fraud_rag_common.gateway import DIMENSION

MIN_CHUNKS = 50
MAX_CHUNKS = 5000
MIN_TEXT = 40
MAX_TEXT = 6000
CARD_NUMBER = re.compile(r"(?:\d[ -]?){12,18}\d")
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
DISCLAIMER = "Synthetic document"
GOLDEN = Path(__file__).with_name("golden.yaml")


def result(ok: bool, observed) -> dict:
    return {"ok": bool(ok), "observed": observed}


def norm(vector) -> float:
    return math.sqrt(sum(float(value) * float(value) for value in vector))


# The ids match the quality rules in contracts/data-products/knowledge-base/knowledge-chunks.odcs.yaml.
def contract_rules(rows: list[dict]) -> dict:
    ids = [row["chunk_id"] for row in rows]
    duplicates = sorted({chunk_id for chunk_id in ids if ids.count(chunk_id) > 1})
    bad_text = [row["chunk_id"] for row in rows if not row["text"] or not MIN_TEXT <= len(row["text"]) <= MAX_TEXT]
    bad_vectors = [
        row["chunk_id"]
        for row in rows
        if len(row["embedding"]) != DIMENSION
        or not all(math.isfinite(float(value)) for value in row["embedding"])
        or abs(norm(row["embedding"]) - 1.0) > 1e-3
    ]
    flags: dict[str, set] = {}
    disclaimed: set[str] = set()
    for row in rows:
        flags.setdefault(row["doc_id"], set()).add(bool(row["synthetic"]))
        if DISCLAIMER in row["text"]:
            disclaimed.add(row["doc_id"])
    unflagged = sorted(
        doc_id for doc_id, values in flags.items() if len(values) > 1 or (True in values and doc_id not in disclaimed)
    )
    sensitive = [row["chunk_id"] for row in rows if CARD_NUMBER.search(row["text"]) or EMAIL.search(row["text"])]
    return {
        "knowledge_row_count": result(MIN_CHUNKS <= len(rows) <= MAX_CHUNKS, len(rows)),
        "knowledge_unique_chunk_id": result(not duplicates, duplicates),
        "knowledge_text_bounds": result(not bad_text, bad_text),
        "knowledge_embedding_shape": result(not bad_vectors, bad_vectors),
        "knowledge_synthetic_flagged": result(not unflagged, unflagged),
        "knowledge_no_card_or_email": result(not sensitive, sensitive),
    }


def coverage(rows: list[dict], manifest: dict) -> dict:
    chunked = {row["doc_id"] for row in rows}
    missing = sorted(document["doc_id"] for document in manifest["documents"] if document["doc_id"] not in chunked)
    return {"knowledge_document_coverage": result(not missing, missing)}


def golden() -> dict:
    return yaml.safe_load(GOLDEN.read_text())


def recall_at_3(rows: list[dict], questions: list[dict], vectors: list[list[float]]) -> float:
    hits = 0
    for question, vector in zip(questions, vectors):
        scored = sorted(rows, key=lambda row: -sum(a * b for a, b in zip(vector, row["embedding"])))
        if {row["doc_id"] for row in scored[:3]} & set(question["doc_ids"]):
            hits += 1
    return hits / len(questions)


def retrieval(rows: list[dict], vectors: list[list[float]]) -> dict:
    spec = golden()
    score = recall_at_3(rows, spec["questions"], vectors)
    return {"knowledge_golden_recall_at_3": result(score >= spec["recall_at_3"], round(score, 3))}


def store_matches(expected: list[dict], online: list[dict | None], online_count: int, indexed: bool) -> dict:
    mismatched = [
        row["chunk_id"]
        for row, stored in zip(expected, online)
        if stored is None
        or stored["content_sha256"] != row["content_sha256"]
        or stored["knowledge_version"] != row["knowledge_version"]
    ]
    return {
        "knowledge_online_matches_offline": result(not mismatched, mismatched),
        "knowledge_online_has_no_extra_ids": result(online_count == len(expected), online_count),
        "knowledge_search_uses_hnsw": result(indexed, indexed),
    }


def passed(report: dict) -> bool:
    return all(check["ok"] for check in report.values())
