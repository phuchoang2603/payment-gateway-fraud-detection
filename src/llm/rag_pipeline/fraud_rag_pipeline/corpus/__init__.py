import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

PACKAGE = Path(__file__).parent
DOCUMENTS = PACKAGE / "documents"
AUDIENCES = {"analyst", "merchant", "both"}
DISCLAIMER = "Synthetic document for the payment-gateway-fraud-detection demo"
FRONT_MATTER = re.compile(r"\A---\n(.*?)\n---\n", re.S)
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*$")
MERMAID = re.compile(r"^```mermaid\n.*?^```\n?", re.S | re.M)


@dataclass(frozen=True)
class Document:
    doc_id: str
    title: str
    audience: str
    synthetic: bool
    sources: tuple[str, ...]
    text: str

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.text.encode()).hexdigest()


def settings() -> dict:
    return yaml.safe_load((PACKAGE / "sources.yaml").read_text())


def load(sources_root: Path) -> list[Document]:
    spec = settings()
    documents = [rendered(entry, sources_root) for entry in spec["documents"]]
    documents += [synthetic(path) for path in sorted(DOCUMENTS.glob("*.md"))]
    ids = [document.doc_id for document in documents]
    duplicates = sorted({doc_id for doc_id in ids if ids.count(doc_id) > 1})
    if duplicates:
        raise ValueError(f"duplicate doc_id: {duplicates}")
    for document in documents:
        if document.audience not in AUDIENCES:
            raise ValueError(f"{document.doc_id}: audience must be one of {sorted(AUDIENCES)}")
        if not document.text.strip():
            raise ValueError(f"{document.doc_id}: empty document")
    return sorted(documents, key=lambda document: document.doc_id)


def synthetic(path: Path) -> Document:
    raw = path.read_text()
    match = FRONT_MATTER.match(raw)
    if not match:
        raise ValueError(f"{path.name}: missing front matter")
    meta = yaml.safe_load(match.group(1))
    if meta.get("synthetic") is not True:
        raise ValueError(f"{path.name}: hand-written documents must declare synthetic: true")
    text = raw[match.end() :].strip() + "\n"
    if not text.startswith(DISCLAIMER):
        raise ValueError(f"{path.name}: hand-written documents must open with the synthetic disclaimer")
    return Document(
        doc_id=meta["doc_id"],
        title=meta["title"],
        audience=meta["audience"],
        synthetic=True,
        sources=tuple(meta.get("consistent_with", [])),
        text=text,
    )


def rendered(entry: dict, sources_root: Path) -> Document:
    render = RENDERERS[entry["render"]]
    parts = [render((sources_root / source).read_text(), entry) for source in entry["sources"]]
    return Document(
        doc_id=entry["doc_id"],
        title=entry["title"],
        audience=entry["audience"],
        synthetic=False,
        sources=tuple(entry["sources"]),
        text="\n\n".join(part.strip() for part in parts) + "\n",
    )


def markdown(text: str, entry: dict) -> str:
    drop = set(entry.get("drop_sections", []))
    text = MERMAID.sub("", text)
    kept, skipping_level, fenced = [], None, False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
        heading = None if fenced else HEADING.match(line)
        if heading:
            level = len(heading.group(1))
            if skipping_level is not None and level <= skipping_level:
                skipping_level = None
            if heading.group(2) in drop:
                skipping_level = level
                continue
            if level == 1:
                continue
        if skipping_level is None:
            kept.append(line)
    return "\n".join(kept)


def proto(text: str, entry: dict) -> str:
    sections, comments, block, depth = [], [], [], 0
    for line in text.splitlines():
        stripped = line.strip()
        if depth == 0:
            if stripped.startswith("//"):
                comments.append(stripped.lstrip("/ ").strip())
                continue
            definition = re.match(r"(message|enum)\s+(\w+)\s*\{", stripped)
            if not definition:
                comments = []
                continue
            block = [line]
            depth = line.count("{") - line.count("}")
            name = definition.group(2)
            if depth == 0:
                sections.append(proto_section(definition.group(1), name, comments, block))
                comments = []
            continue
        block.append(line)
        depth += line.count("{") - line.count("}")
        if depth == 0:
            sections.append(proto_section(definition.group(1), name, comments, block))
            comments = []
    return "\n\n".join(sections)


def proto_section(kind: str, name: str, comments: list[str], block: list[str]) -> str:
    lines = [f"## {name}", f"Protobuf {kind} `{name}`."]
    if comments:
        lines.append(" ".join(comments))
    lines += ["", "```proto", *block, "```"]
    return "\n".join(lines)


def features(text: str, entry: dict) -> str:
    contract = yaml.safe_load(text)
    lines = [
        "## Contract",
        f"Feature contract `{contract['id']}`, version {contract['version']}.",
        "Entity keys: "
        + "; ".join(f"{view}: {', '.join(keys)}" for view, keys in contract["entity_keys"].items())
        + ".",
        "",
        "## Time and freshness rules",
    ]
    lines += [f"- {name}: {rule}" for name, rule in contract["time"].items()]
    for feature in contract["features"]:
        window = feature.get("window_seconds")
        lines += [
            "",
            f"## Feature {feature['name']}",
            f"- Type: {feature['type']}; units: {feature['units']}; scope: {feature['scope']}.",
            f"- Formula: {feature['formula']}.",
            f"- Window: {f'{window} seconds' if window else 'none (request-time value)'}.",
            f"- Missing values: {feature['missing']}.",
        ]
    return "\n".join(lines)


RENDERERS = {"markdown": markdown, "proto": proto, "features": features}


def knowledge_version(documents: list[Document], chunker: dict, model: str, revision: str) -> str:
    payload = {
        "documents": [[document.doc_id, document.sha256] for document in documents],
        "chunker": chunker,
        "embedding": [model, revision],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


def manifest(documents: list[Document], version: str, chunker: dict, model: str, revision: str) -> dict:
    return {
        "knowledge_version": version,
        "chunker": chunker,
        "embedding": {"model": model, "revision": revision},
        "documents": [
            {**{k: v for k, v in asdict(document).items() if k != "text"}, "sha256": document.sha256}
            for document in documents
        ],
    }
