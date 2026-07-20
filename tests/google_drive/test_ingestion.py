from __future__ import annotations

from pathlib import Path

from retrieval.google_drive.config import GoogleDriveConfig
from retrieval.google_drive.models import DriveFile, DriveFilesPage
from retrieval.google_drive.provider import GoogleDriveProviderGateway
from retrieval.google_drive.staging import GoogleDriveStagingStore
from retrieval.temporal.activities.ingestion import IngestionActivities
from retrieval.temporal.activities.provider_api import FetchResourcePageRequest
from retrieval.temporal.activities.repositories import InMemoryRetrievalRepository
from retrieval.temporal.models.documents import DocumentIngestionInput
from retrieval.temporal.models.operations import ResultStatus


class OneDocumentDriveApi:
    async def list_files(
        self,
        *,
        page_token: str | None,
        page_size: int,
        parent_id: str | None,
    ) -> DriveFilesPage:
        assert page_token is None
        assert page_size == 100
        assert parent_id is None
        return DriveFilesPage(
            files=(
                DriveFile(
                    "doc-1",
                    "Launch plan",
                    "application/vnd.google-apps.document",
                    version="42",
                    web_view_link="https://drive.google.com/document/d/doc-1/edit",
                ),
            )
        )

    async def download_file(
        self,
        file: DriveFile,
        *,
        export_mime_type: str | None,
        max_bytes: int,
    ) -> bytes:
        assert file.file_id == "doc-1"
        assert export_mime_type == "text/plain"
        return b"Launch in October.\n\nSecurity review is complete."


async def test_drive_reference_flows_through_staging_and_generation_fenced_ingestion(
    tmp_path: Path,
) -> None:
    config = GoogleDriveConfig(
        credential_key="workspace-primary",
        user_key="drive-user",
        staging_directory=tmp_path,
    )
    staging = GoogleDriveStagingStore(tmp_path)
    gateway = GoogleDriveProviderGateway(config, OneDocumentDriveApi(), staging)
    manifest = await gateway.fetch_resource_page(
        FetchResourcePageRequest(
            store_key="drive-store",
            lifecycle_generation=0,
            sync_sequence="sync-1",
            user_key="drive-user",
            resource_key="files",
            cursor=None,
            page_size=100,
            request_id="request-1",
        )
    )

    repository = InMemoryRetrievalRepository()
    await repository.ensure_store("drive-store")
    result = await IngestionActivities(repository, staging).ingest_staged_document(
        DocumentIngestionInput(
            store_key="drive-store",
            lifecycle_generation=0,
            document=manifest.documents[0],
            idempotency_key="ingest-drive-doc-1-v42",
            sync_sequence="sync-1",
            user_key="drive-user",
            resource_key="files",
        )
    )
    stored = await repository.inspect_store("drive-store")

    assert result.status is ResultStatus.SUCCEEDED
    document = stored.documents["gdrive:doc-1"]
    assert document.title == "Launch plan"
    assert document.source_uri == "https://drive.google.com/document/d/doc-1/edit"
    assert [chunk.text for chunk in document.chunks] == [
        "Launch in October.\n\nSecurity review is complete."
    ]
