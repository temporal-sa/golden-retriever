from __future__ import annotations

import pytest

from retrieval.content import InvalidDocumentPayloadError, chunk_text, parse_staged_document


def test_front_matter_is_parsed_and_line_endings_are_normalized() -> None:
    parsed = parse_staged_document(
        b"---\r\ntitle: Renewal plan\r\nsource_uri: https://example.invalid/renewal\r\n---\r\n"
        b"Northstar renews in Q3.\r\n\r\nSecurity review is required.",
        fallback_title="fallback",
    )

    assert parsed.title == "Renewal plan"
    assert parsed.source_uri == "https://example.invalid/renewal"
    assert "\r" not in parsed.text


def test_chunking_is_bounded_stable_and_overlapping() -> None:
    text = "alpha " * 260 + "\n\n" + "beta " * 260

    first = chunk_text(text, max_characters=300, overlap_characters=30)
    second = chunk_text(text, max_characters=300, overlap_characters=30)

    assert first == second
    assert len(first) > 2
    assert all(len(chunk.text) <= 300 for chunk in first)
    assert [chunk.ordinal for chunk in first] == list(range(len(first)))


@pytest.mark.parametrize(
    "body",
    [b"\xff", b"", b"---\ntitle: missing close\nbody"],
)
def test_invalid_documents_fail_closed(body: bytes) -> None:
    with pytest.raises(InvalidDocumentPayloadError):
        parse_staged_document(body, fallback_title="fallback")
