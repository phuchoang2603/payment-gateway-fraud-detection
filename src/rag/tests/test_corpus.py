from rag import corpus


def test_every_document_renders_with_a_unique_id(repo):
    documents = corpus.load(repo)

    assert len(documents) == len({document.doc_id for document in documents})
    assert {document.doc_id for document in documents} >= {"decline-codes", "payment-processing", "fraud-features"}
    assert all(document.text.strip() for document in documents)


def test_hand_written_documents_say_they_are_synthetic(repo):
    synthetic = [document for document in corpus.load(repo) if document.synthetic]

    assert len(synthetic) == 7
    assert all(document.text.startswith(corpus.DISCLAIMER) for document in synthetic)


def test_rendered_documents_are_not_flagged_synthetic(repo):
    rendered = {entry["doc_id"] for entry in corpus.settings()["documents"]}

    assert all(not document.synthetic for document in corpus.load(repo) if document.doc_id in rendered)


def test_simulator_configuration_is_never_a_source(repo):
    sources = [source for document in corpus.load(repo) for source in document.sources]

    assert not [source for source in sources if "simulator/configs" in source or "calibration" in source]


def test_proto_rendering_keeps_each_message_with_its_comment(repo):
    events = next(document for document in corpus.load(repo) if document.doc_id == "payment-events")

    assert "## AttemptDeclined" in events.text
    assert "## PaymentExpired" in events.text
    assert "Not legal while a processor outcome remains unresolved." in events.text


def test_markdown_rendering_drops_listed_sections_and_mermaid():
    entry = {"drop_sections": ["Purpose"]}
    text = "# Title\n## Purpose\nskip me\n## Requirements\nkeep\n```mermaid\ngraph\n```\n"

    rendered = corpus.markdown(text, entry)

    assert "skip me" not in rendered and "graph" not in rendered and "# Title" not in rendered
    assert "keep" in rendered


def test_version_changes_with_documents_chunker_and_model(repo):
    documents = corpus.load(repo)
    chunker = corpus.settings()["chunker"]
    base = corpus.knowledge_version(documents, chunker, "qwen3-embedding", "a")

    assert base == corpus.knowledge_version(documents, chunker, "qwen3-embedding", "a")
    assert base != corpus.knowledge_version(documents, chunker, "qwen3-embedding", "b")
    assert base != corpus.knowledge_version(documents, {**chunker, "max_words": 1}, "qwen3-embedding", "a")
    assert base != corpus.knowledge_version(documents[1:], chunker, "qwen3-embedding", "a")


def test_manifest_lists_hashes_without_text(repo):
    documents = corpus.load(repo)
    manifest = corpus.manifest(documents, "v", {}, "m", "r")

    assert [entry["doc_id"] for entry in manifest["documents"]] == [document.doc_id for document in documents]
    assert all("text" not in entry and len(entry["sha256"]) == 64 for entry in manifest["documents"])


def test_image_build_paths_cover_every_corpus_source(repo):
    import json

    images = json.loads((repo / ".github/service-images.json").read_text())["include"]
    paths = next(image["paths"] for image in images if image["service"] == "rag")
    dockerfile = (repo / "infra/docker/rag.Dockerfile").read_text()

    for entry in corpus.settings()["documents"]:
        for source in entry["sources"]:
            covering = [path for path in paths if source == path or source.startswith(path.rstrip("/") + "/")]
            assert covering, f"{source} is not a build path of the rag image"
            assert any(f"COPY {path} " in dockerfile for path in covering), f"{source} is not copied into the image"
