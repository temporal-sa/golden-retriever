"""Bounded PDF-to-text extraction for the Google Drive adapter."""

from __future__ import annotations

import unicodedata
from io import BytesIO


class PdfTextExtractionError(ValueError):
    """A PDF cannot be converted into searchable text."""


class PdfTextTooLargeError(PdfTextExtractionError):
    """Extracted PDF text exceeds the configured staging limit."""


def extract_pdf_text(body: bytes, *, max_text_bytes: int) -> str:
    """Extract page-labelled UTF-8 text without retaining PDF binary content.

    The Drive download is bounded separately. This second bound prevents a compact PDF
    from expanding into an unexpectedly large staged document.
    """

    if max_text_bytes <= 0:
        raise ValueError("max_text_bytes must be positive")
    try:
        from pypdf import PdfReader
        from pypdf.errors import PdfReadError
    except ImportError as exc:  # pragma: no cover - packaging/configuration path
        raise RuntimeError("install the google-drive extra to enable PDF text extraction") from exc

    try:
        reader = PdfReader(BytesIO(body), strict=False)
        if reader.is_encrypted and reader.decrypt("") == 0:
            raise PdfTextExtractionError("encrypted PDF requires a password")

        sections: list[str] = []
        encoded_size = 0
        for page_number, page in enumerate(reader.pages, start=1):
            extracted = page.extract_text() or ""
            normalized = unicodedata.normalize(
                "NFC",
                extracted.replace("\x00", "").replace("\r\n", "\n").replace("\r", "\n"),
            ).strip()
            if not normalized:
                continue
            section = f"Page {page_number}\n\n{normalized}"
            section_size = len(section.encode("utf-8")) + (2 if sections else 0)
            if encoded_size + section_size > max_text_bytes:
                raise PdfTextTooLargeError(
                    f"extracted PDF text exceeds the {max_text_bytes}-byte staging limit"
                )
            sections.append(section)
            encoded_size += section_size
    except PdfTextExtractionError:
        raise
    except (PdfReadError, KeyError, OSError, TypeError, ValueError) as exc:
        raise PdfTextExtractionError("PDF text extraction failed") from exc

    if not sections:
        raise PdfTextExtractionError("PDF contains no extractable text")
    return "\n\n".join(sections)


__all__ = ["PdfTextExtractionError", "PdfTextTooLargeError", "extract_pdf_text"]
