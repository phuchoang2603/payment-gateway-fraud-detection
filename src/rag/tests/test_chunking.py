from rag import corpus
from rag.chunking import chunk, sections
from rag.corpus import Document

SETTINGS = corpus.settings()["chunker"]


def doc(text, doc_id="sample"):
    return Document(doc_id=doc_id, title="Sample", audience="analyst", synthetic=False, sources=("x",), text=text)


def test_corpus_chunk_ids_are_unique_and_stable(repo):
    documents = corpus.load(repo)
    first = [item.chunk_id for document in documents for item in chunk(document, **SETTINGS)]
    second = [item.chunk_id for document in documents for item in chunk(document, **SETTINGS)]

    assert first == second
    assert len(first) == len(set(first))


def test_scenarios_stay_inside_their_requirement():
    text = "### Requirement: Idempotent attempts\nBody.\n#### Scenario: Retry\n- WHEN x\n- THEN y\n"

    found = sections(text)

    assert found == [("Requirement: Idempotent attempts", "Body.\n#### Scenario: Retry\n- WHEN x\n- THEN y")]


def test_nested_headings_build_a_section_path():
    found = sections("## Schema\nIntro.\n### attempts\nRows.\n## Terms\nUse.\n")

    assert [name for name, _ in found] == ["Schema", "Schema › attempts", "Terms"]


def test_headings_inside_code_fences_are_text():
    found = sections("## Example\n```bash\n# not a heading\n```\n")

    assert len(found) == 1 and "# not a heading" in found[0][1]


def test_long_sections_split_with_overlap_and_numbered_ids():
    body = "\n\n".join(" ".join(f"w{p}x{i}" for i in range(100)) for p in range(5))
    chunks = chunk(doc(f"## Long\n{body}\n"), max_words=280, overlap_words=40)

    assert [item.chunk_id for item in chunks] == ["sample#long-1", "sample#long-2", "sample#long-3"]
    tail = chunks[0].text.split()[-40:]
    assert chunks[1].text.split("\n\n", 1)[1].split()[:40] == tail


def test_repeated_headings_get_distinct_ids_and_text_carries_context():
    chunks = chunk(doc("## Notes\na\n## Notes\nb\n"), **SETTINGS)

    assert [item.chunk_id for item in chunks] == ["sample#notes", "sample#notes-2"]
    assert chunks[0].text.startswith("Sample › Notes\n\n")
