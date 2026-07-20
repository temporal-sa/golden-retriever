from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from retrieval.google_drive.config import GoogleDriveConfig
from retrieval.google_drive.models import (
    DriveAuthenticationError,
    DriveFile,
    DriveFilesPage,
    DriveQuotaExhausted,
)
from retrieval.google_drive.pdf import PdfTextExtractionError
from retrieval.google_drive.provider import (
    GOOGLE_FOLDER_MIME_TYPE,
    GoogleDriveProviderGateway,
)
from retrieval.google_drive.staging import GoogleDriveStagingStore
from retrieval.temporal.activities.provider_api import (
    FetchResourcePageRequest,
    InvalidCredentialsError,
    ListActiveUsersRequest,
    ProviderPreflightRequest,
    ProviderQuotaExhausted,
    ProviderRequestError,
)


class FakeDriveApi:
    def __init__(self) -> None:
        self.pages: dict[tuple[str | None, str | None], DriveFilesPage] = {}
        self.bodies: dict[str, bytes] = {}
        self.list_error: Exception | None = None
        self.list_calls: list[tuple[str | None, str | None]] = []
        self.download_calls: list[tuple[str, str | None]] = []

    async def list_files(
        self,
        *,
        page_token: str | None,
        page_size: int,
        parent_id: str | None,
    ) -> DriveFilesPage:
        assert 0 < page_size <= 1_000
        self.list_calls.append((parent_id, page_token))
        if self.list_error is not None:
            raise self.list_error
        return self.pages[(parent_id, page_token)]

    async def download_file(
        self,
        file: DriveFile,
        *,
        export_mime_type: str | None,
        max_bytes: int,
    ) -> bytes:
        self.download_calls.append((file.file_id, export_mime_type))
        body = self.bodies[file.file_id]
        assert len(body) <= max_bytes
        return body


def _config(tmp_path: Path) -> GoogleDriveConfig:
    return GoogleDriveConfig(
        credential_key="workspace-primary",
        user_key="drive-user",
        root_folder_id="root-folder",
        staging_directory=tmp_path,
    )


def _request(
    sync_sequence: str,
    cursor: str | None = None,
    *,
    page_size: int = 100,
) -> FetchResourcePageRequest:
    return FetchResourcePageRequest(
        store_key="drive-store",
        lifecycle_generation=0,
        sync_sequence=sync_sequence,
        user_key="drive-user",
        resource_key="files",
        cursor=cursor,
        page_size=page_size,
        request_id=f"request-{sync_sequence}-{cursor or 'initial'}",
    )


async def _fetch_all(
    gateway: GoogleDriveProviderGateway,
    sync_sequence: str,
) -> list:
    manifests = []
    cursor = None
    while True:
        manifest = await gateway.fetch_resource_page(_request(sync_sequence, cursor))
        manifests.append(manifest)
        cursor = manifest.next_cursor
        if cursor is None:
            return manifests


async def test_provider_recurses_stages_text_and_reconciles_removed_files(tmp_path: Path) -> None:
    api = FakeDriveApi()
    api.pages = {
        ("root-folder", None): DriveFilesPage(
            files=(
                DriveFile(
                    "doc-1",
                    "Roadmap",
                    "application/vnd.google-apps.document",
                    version="10",
                    web_view_link="https://drive.google.com/document/d/doc-1/edit",
                ),
                DriveFile("nested", "Nested", GOOGLE_FOLDER_MIME_TYPE),
            ),
            next_page_token="root-page-2",
        ),
        ("root-folder", "root-page-2"): DriveFilesPage(
            files=(DriveFile("text-2", "Notes.txt", "text/plain", md5_checksum="abc"),)
        ),
        ("nested", None): DriveFilesPage(
            files=(DriveFile("binary-ignored", "Binary.zip", "application/zip"),)
        ),
    }
    api.bodies = {"doc-1": b"Q3 roadmap", "text-2": b"launch notes"}
    staging = GoogleDriveStagingStore(tmp_path)
    gateway = GoogleDriveProviderGateway(_config(tmp_path), api, staging)

    users = await gateway.list_active_users(
        ListActiveUsersRequest("drive-store", 0, None, 100, "users-request")
    )
    assert [user.user_key for user in users.users] == ["drive-user"]

    first_sync = await _fetch_all(gateway, "sync-1")
    references = [document for page in first_sync for document in page.documents]
    assert [document.document_key for document in references] == [
        "gdrive:doc-1",
        "gdrive:text-2",
    ]
    assert api.download_calls == [("doc-1", "text/plain"), ("text-2", None)]
    staged = await staging.get(references[0].staging_uri)
    assert b"title: Roadmap" in staged
    assert b"source_uri: https://drive.google.com/document/d/doc-1/edit" in staged
    assert staged.endswith(b"Q3 roadmap")
    assert first_sync[-1].deleted_document_keys == ()

    api.pages = {
        ("root-folder", None): DriveFilesPage(
            files=(
                DriveFile(
                    "doc-1",
                    "Roadmap",
                    "application/vnd.google-apps.document",
                    version="10",
                ),
            )
        )
    }
    second_listing = await gateway.fetch_resource_page(_request("sync-2"))
    assert second_listing.deleted_document_keys == ()
    assert second_listing.next_cursor is not None
    second_final = await gateway.fetch_resource_page(_request("sync-2", second_listing.next_cursor))
    assert second_final.deleted_document_keys == ("gdrive:text-2",)

    calls_before_retry = len(api.list_calls)
    retried_final = await gateway.fetch_resource_page(
        _request("sync-2", second_listing.next_cursor)
    )
    assert retried_final.deleted_document_keys == ("gdrive:text-2",)
    assert len(api.list_calls) == calls_before_retry


async def test_provider_extracts_and_stages_pdf_text(tmp_path: Path, monkeypatch) -> None:
    api = FakeDriveApi()
    api.pages = {
        ("root-folder", None): DriveFilesPage(
            files=(
                DriveFile(
                    "manual-pdf",
                    "FlightFactor manual.pdf",
                    "application/pdf",
                    size=1_024,
                    web_view_link="https://drive.google.com/file/d/manual-pdf/view",
                ),
            )
        )
    }
    api.bodies = {"manual-pdf": b"%PDF-test-body"}
    monkeypatch.setattr(
        "retrieval.google_drive.provider.extract_pdf_text",
        lambda body, *, max_text_bytes: "Page 1\n\nFlight controls and landing checklist.",
    )
    staging = GoogleDriveStagingStore(tmp_path)
    gateway = GoogleDriveProviderGateway(_config(tmp_path), api, staging)

    manifests = await _fetch_all(gateway, "pdf-sync")

    references = [document for page in manifests for document in page.documents]
    assert [document.document_key for document in references] == ["gdrive:manual-pdf"]
    assert api.download_calls == [("manual-pdf", None)]
    staged = await staging.get(references[0].staging_uri)
    assert b"title: FlightFactor manual.pdf" in staged
    assert b"Page 1\n\nFlight controls and landing checklist." in staged


async def test_provider_fails_when_configured_held_pdf_cannot_be_extracted(
    tmp_path: Path,
    monkeypatch,
) -> None:
    api = FakeDriveApi()
    api.pages = {
        ("root-folder", None): DriveFilesPage(
            files=(DriveFile("held-pdf", "Held.pdf", "application/pdf", size=100),)
        )
    }
    api.bodies = {"held-pdf": b"not-a-pdf"}

    def fail_extraction(body: bytes, *, max_text_bytes: int) -> str:
        raise PdfTextExtractionError("broken PDF")

    monkeypatch.setattr("retrieval.google_drive.provider.extract_pdf_text", fail_extraction)
    config = replace(_config(tmp_path), held_file_id="held-pdf")
    gateway = GoogleDriveProviderGateway(config, api, GoogleDriveStagingStore(tmp_path))

    with pytest.raises(ProviderRequestError) as raised:
        await gateway.fetch_resource_page(_request("held-pdf-sync"))

    assert raised.value.error_type == "GoogleDriveHeldFileUnreadable"


async def test_preflight_recurses_metadata_without_downloading_bodies(tmp_path: Path) -> None:
    api = FakeDriveApi()
    api.pages = {
        ("root-folder", None): DriveFilesPage(
            files=(
                DriveFile("nested", "Nested", GOOGLE_FOLDER_MIME_TYPE),
                DriveFile("doc-2", "Zeta", "text/plain", modified_time="2026-07-20"),
            )
        ),
        ("nested", None): DriveFilesPage(
            files=(DriveFile("doc-1", "Alpha", "application/vnd.google-apps.document"),)
        ),
    }
    gateway = GoogleDriveProviderGateway(_config(tmp_path), api, GoogleDriveStagingStore(tmp_path))

    result = await gateway.preflight(ProviderPreflightRequest("preflight-1"))

    assert [item.name for item in result.files] == ["Alpha", "Zeta"]
    assert result.files[0].held_for_demo
    assert all(item.searchable for item in result.files)
    assert result.folders_scanned == 2
    assert api.download_calls == []


async def test_provider_splits_reconciliation_tombstones_into_bounded_pages(
    tmp_path: Path,
) -> None:
    api = FakeDriveApi()
    original_files = tuple(
        DriveFile(f"file-{index}", f"File {index}.txt", "text/plain") for index in range(5)
    )
    api.pages = {("root-folder", None): DriveFilesPage(files=original_files)}
    api.bodies = {file.file_id: f"body {file.file_id}".encode() for file in original_files}
    gateway = GoogleDriveProviderGateway(
        _config(tmp_path),
        api,
        GoogleDriveStagingStore(tmp_path),
    )
    await _fetch_all(gateway, "sync-baseline")

    api.pages = {("root-folder", None): DriveFilesPage(files=())}
    manifests = []
    cursor = None
    while True:
        manifest = await gateway.fetch_resource_page(_request("sync-delete", cursor, page_size=2))
        manifests.append(manifest)
        cursor = manifest.next_cursor
        if cursor is None:
            break

    deletion_pages = [manifest for manifest in manifests if manifest.deleted_document_keys]
    assert [len(manifest.deleted_document_keys) for manifest in deletion_pages] == [2, 2, 1]
    assert {
        document_key
        for manifest in deletion_pages
        for document_key in manifest.deleted_document_keys
    } == {f"gdrive:file-{index}" for index in range(5)}


@pytest.mark.parametrize(
    ("api_error", "expected"),
    [
        (DriveAuthenticationError("bad token"), InvalidCredentialsError),
        (DriveQuotaExhausted(retry_after_seconds=7), ProviderQuotaExhausted),
    ],
)
async def test_provider_maps_drive_auth_and_quota_errors(
    tmp_path: Path,
    api_error: Exception,
    expected: type[Exception],
) -> None:
    api = FakeDriveApi()
    api.list_error = api_error
    gateway = GoogleDriveProviderGateway(
        _config(tmp_path),
        api,
        GoogleDriveStagingStore(tmp_path),
    )

    with pytest.raises(expected) as raised:
        await gateway.fetch_resource_page(_request("sync-errors"))
    if isinstance(raised.value, ProviderQuotaExhausted):
        assert raised.value.retry_after_seconds == 7


async def test_provider_rejects_wrong_user_without_calling_drive(tmp_path: Path) -> None:
    api = FakeDriveApi()
    gateway = GoogleDriveProviderGateway(
        _config(tmp_path),
        api,
        GoogleDriveStagingStore(tmp_path),
    )

    with pytest.raises(ProviderRequestError, match="unknown user key"):
        await gateway.fetch_resource_page(replace(_request("sync"), user_key="other"))
    assert api.list_calls == []
