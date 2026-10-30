import math

import yaml

from rag import validation
from rag.gateway import DIMENSION, normalize


def unit(index):
    vector = [0.0] * DIMENSION
    vector[index] = 1.0
    return vector


def row(chunk_id="d#a", doc_id="d", text="A passage long enough to pass the text bounds check.", **extra):
    base = {
        "chunk_id": chunk_id,
        "doc_id": doc_id,
        "text": text,
        "synthetic": False,
        "embedding": unit(0),
        "content_sha256": "h",
        "knowledge_version": "v",
    }
    return {**base, **extra}


def test_gate_rules_match_the_contract_rule_ids(repo):
    contract = yaml.safe_load((repo / "contracts/data-products/knowledge-base/knowledge-chunks.odcs.yaml").read_text())
    contract_ids = {rule["id"] for table in contract["schema"] for rule in table["quality"]}

    assert set(validation.contract_rules([row()])) == contract_ids


def test_duplicate_ids_bad_vectors_and_card_numbers_fail():
    rows = [
        row(),
        row(),
        row(chunk_id="d#b", embedding=[math.nan] * DIMENSION),
        row(chunk_id="d#c", text="Card 4111 1111 1111 1111 appears in this passage text."),
    ]

    checks = validation.contract_rules(rows)

    assert checks["knowledge_unique_chunk_id"] == {"ok": False, "observed": ["d#a"]}
    assert checks["knowledge_embedding_shape"]["observed"] == ["d#b"]
    assert checks["knowledge_no_card_or_email"]["observed"] == ["d#c"]
    assert not checks["knowledge_row_count"]["ok"]


def test_synthetic_documents_need_the_disclaimer():
    flagged = [row(doc_id="s", synthetic=True)]
    disclaimed = [row(doc_id="s", synthetic=True, text="Synthetic document for the demo; not a real policy.")]

    assert validation.contract_rules(flagged)["knowledge_synthetic_flagged"]["observed"] == ["s"]
    assert validation.contract_rules(disclaimed)["knowledge_synthetic_flagged"]["ok"]


def test_coverage_names_documents_without_chunks():
    manifest = {"documents": [{"doc_id": "d"}, {"doc_id": "e"}]}

    assert validation.coverage([row()], manifest) == {"knowledge_document_coverage": {"ok": False, "observed": ["e"]}}


def test_recall_counts_a_hit_when_an_expected_document_is_in_the_top_three():
    rows = [row(chunk_id=f"d{i}#a", doc_id=f"d{i}", embedding=unit(i)) for i in range(5)]
    questions = [{"doc_ids": ["d4"]}, {"doc_ids": ["d4"]}]
    vectors = [normalize([1.0 if i == 4 else 0.0 for i in range(DIMENSION)]), unit(3)]

    assert validation.recall_at_3(rows, questions, vectors) == 0.5


def test_store_check_catches_stale_and_extra_rows():
    expected = [row(), row(chunk_id="d#b")]
    online = [dict(expected[0]), {**expected[1], "knowledge_version": "old"}]

    checks = validation.store_matches(expected, online, online_count=3, indexed=True)

    assert checks["knowledge_online_matches_offline"]["observed"] == ["d#b"]
    assert checks["knowledge_online_has_no_extra_ids"] == {"ok": False, "observed": 3}
    assert checks["knowledge_search_uses_hnsw"]["ok"]
