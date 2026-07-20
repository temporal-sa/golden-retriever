from __future__ import annotations

from io import BytesIO

import pytest

from retrieval.google_drive.pdf import (
    PdfTextExtractionError,
    PdfTextTooLargeError,
    extract_pdf_text,
)


def _one_page_pdf(text: str) -> bytes:
    from pypdf import PdfWriter
    from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject

    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    page[NameObject("/Resources")] = DictionaryObject(
        {NameObject("/Font"): DictionaryObject({NameObject("/F1"): font})}
    )
    stream = DecodedStreamObject()
    escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    stream.set_data(f"BT /F1 12 Tf 72 720 Td ({escaped}) Tj ET".encode("ascii"))
    page[NameObject("/Contents")] = writer._add_object(stream)
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def test_extract_pdf_text_adds_stable_page_label() -> None:
    text = extract_pdf_text(
        _one_page_pdf("FlightFactor landing checklist"),
        max_text_bytes=10_000,
    )

    assert text == "Page 1\n\nFlightFactor landing checklist"


def test_extract_pdf_text_rejects_empty_or_oversized_output() -> None:
    with pytest.raises(PdfTextExtractionError, match="no extractable text"):
        extract_pdf_text(_one_page_pdf(""), max_text_bytes=10_000)

    with pytest.raises(PdfTextTooLargeError, match="staging limit"):
        extract_pdf_text(_one_page_pdf("long searchable sentence"), max_text_bytes=8)
