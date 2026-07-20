from __future__ import annotations

from pathlib import Path

import pytest

from retrieval.google_drive.staging import (
    GoogleDriveStagingIntegrityError,
    GoogleDriveStagingStore,
    InvalidGoogleDriveStagingUriError,
)


async def test_content_addressed_staging_round_trip_and_integrity(tmp_path: Path) -> None:
    store = GoogleDriveStagingStore(tmp_path)
    first = await store.stage(b"drive document")
    duplicate = await store.stage(b"drive document")

    assert first == duplicate
    assert await store.get(first.uri) == b"drive document"

    digest = first.content_hash
    (tmp_path / "content" / "sha256" / digest).write_bytes(b"tampered")
    with pytest.raises(GoogleDriveStagingIntegrityError):
        await store.get(first.uri)


@pytest.mark.parametrize(
    "uri",
    [
        "file:///tmp/document",
        "gdrive-stage://sha256/../secret",
        "gdrive-stage://sha256/%2e%2e",
        "gdrive-stage://sha256/not-a-digest",
        "gdrive-stage://sha256/" + "a" * 64 + "?ignored=true",
    ],
)
async def test_staging_rejects_non_content_addressed_uris(tmp_path: Path, uri: str) -> None:
    store = GoogleDriveStagingStore(tmp_path)

    with pytest.raises(InvalidGoogleDriveStagingUriError):
        await store.get(uri)
