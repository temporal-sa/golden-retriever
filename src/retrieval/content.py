"""Process-local searchable document parsing and deterministic chunking."""

from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass


class InvalidDocumentPayloadError(ValueError):
    """A staged document cannot be decoded or parsed safely."""


@dataclass(frozen=True)
class ParsedDocument:
    title: str
    source_uri: str | None
    text: str


@dataclass(frozen=True)
class TextChunk:
    ordinal: int
    text: str
    content_hash: str


def parse_staged_document(body: bytes, *, fallback_title: str) -> ParsedDocument:
    """Decode UTF-8 and parse the small front-matter subset used by fixtures.

    Documents without front matter remain valid. Only ``title`` and ``source_uri`` are
    recognized so parsing does not require a YAML implementation in the worker.
    """

    try:
        decoded = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidDocumentPayloadError("staged document must be valid UTF-8") from exc

    normalized = unicodedata.normalize(
        "NFC",
        decoded.replace("\r\n", "\n").replace("\r", "\n"),
    )
    title = fallback_title
    source_uri: str | None = None
    text = normalized
    if normalized.startswith("---\n"):
        marker = normalized.find("\n---\n", 4)
        if marker < 0:
            raise InvalidDocumentPayloadError("front matter is missing its closing marker")
        metadata_text = normalized[4:marker]
        for line in metadata_text.splitlines():
            key, separator, value = line.partition(":")
            if not separator:
                raise InvalidDocumentPayloadError("front-matter entries must use key: value")
            key = key.strip()
            value = value.strip()
            if key == "title" and value:
                title = value
            elif key == "source_uri" and value:
                source_uri = value
        text = normalized[marker + 5 :]

    text = text.strip()
    if not text:
        raise InvalidDocumentPayloadError("staged document body must not be empty")
    return ParsedDocument(title=title.strip() or fallback_title, source_uri=source_uri, text=text)


def chunk_text(
    text: str,
    *,
    max_characters: int = 1_200,
    overlap_characters: int = 150,
) -> tuple[TextChunk, ...]:
    """Create stable paragraph-boundary chunks with a bounded text overlap."""

    if max_characters <= 0:
        raise ValueError("max_characters must be positive")
    if overlap_characters < 0 or overlap_characters >= max_characters:
        raise ValueError("overlap_characters must be non-negative and smaller than max_characters")

    paragraphs = tuple(part.strip() for part in text.split("\n\n") if part.strip())
    if not paragraphs:
        raise InvalidDocumentPayloadError("document has no searchable text")

    raw_chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        remaining = paragraph
        while remaining:
            available = max_characters - len(current) - (2 if current else 0)
            if available <= 0:
                raw_chunks.append(current)
                current = current[-overlap_characters:] if overlap_characters else ""
                continue
            if len(remaining) <= available:
                current = f"{current}\n\n{remaining}" if current else remaining
                remaining = ""
                continue

            split_at = remaining.rfind(" ", 0, available + 1)
            if split_at <= 0:
                split_at = available
            piece = remaining[:split_at].rstrip()
            current = f"{current}\n\n{piece}" if current else piece
            raw_chunks.append(current)
            overlap = current[-overlap_characters:] if overlap_characters else ""
            current = overlap.lstrip()
            remaining = remaining[split_at:].lstrip()

    if current:
        raw_chunks.append(current)

    return tuple(
        TextChunk(
            ordinal=ordinal,
            text=chunk,
            content_hash=hashlib.sha256(chunk.encode("utf-8")).hexdigest(),
        )
        for ordinal, chunk in enumerate(raw_chunks)
    )
