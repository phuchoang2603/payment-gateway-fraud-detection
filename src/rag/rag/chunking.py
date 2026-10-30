import hashlib
import re
from dataclasses import dataclass

from rag.corpus import HEADING, Document

SPLIT_LEVEL = 3


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    doc_id: str
    title: str
    section: str
    audience: str
    synthetic: bool
    source_path: str
    text: str

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.text.encode()).hexdigest()


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:80] or "section"


def sections(text: str) -> list[tuple[str, str]]:
    found, path, body = [], [], []

    def flush():
        content = "\n".join(body).strip()
        if content:
            found.append((" › ".join(title for _, title in path) or "Overview", content))

    fenced = False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
        heading = None if fenced else HEADING.match(line)
        if heading and len(heading.group(1)) <= SPLIT_LEVEL:
            flush()
            body = []
            level = len(heading.group(1))
            path = [(depth, title) for depth, title in path if depth < level] + [(level, heading.group(2))]
            continue
        body.append(line)
    flush()
    return found


def paragraphs(body: str, max_words: int) -> list[str]:
    pieces = []
    for block in re.split(r"\n\s*\n", body):
        if len(block.split()) <= max_words:
            pieces.append(block)
            continue
        current = []
        for line in block.splitlines():
            if current and len(" ".join(current + [line]).split()) > max_words:
                pieces.append("\n".join(current))
                current = []
            if len(line.split()) > max_words:
                words = line.split()
                pieces += [" ".join(words[i : i + max_words]) for i in range(0, len(words), max_words)]
                continue
            current.append(line)
        if current:
            pieces.append("\n".join(current))
    return [piece for piece in pieces if piece.strip()]


def pack(body: str, max_words: int, overlap_words: int) -> list[str]:
    parts, current = [], []
    for piece in paragraphs(body, max_words):
        if current and len(" ".join(current + [piece]).split()) > max_words:
            parts.append("\n\n".join(current))
            tail = " ".join(parts[-1].split()[-overlap_words:]) if overlap_words else ""
            current = [tail] if tail else []
        current.append(piece)
    if current:
        parts.append("\n\n".join(current))
    return parts


def chunk(document: Document, max_words: int, overlap_words: int) -> list[Chunk]:
    chunks, seen = [], {}
    for section, body in sections(document.text):
        base = slug(section)
        seen[base] = seen.get(base, 0) + 1
        if seen[base] > 1:
            base = f"{base}-{seen[base]}"
        parts = pack(body, max_words, overlap_words)
        for index, part in enumerate(parts, start=1):
            suffix = f"-{index}" if len(parts) > 1 else ""
            chunks.append(
                Chunk(
                    chunk_id=f"{document.doc_id}#{base}{suffix}",
                    doc_id=document.doc_id,
                    title=document.title,
                    section=section,
                    audience=document.audience,
                    synthetic=document.synthetic,
                    source_path=";".join(document.sources),
                    text=f"{document.title} › {section}\n\n{part}",
                )
            )
    return chunks
