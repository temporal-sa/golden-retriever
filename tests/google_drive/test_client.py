from __future__ import annotations

import httpx
import pytest

from retrieval.google_drive.client import GoogleDriveApiClient
from retrieval.google_drive.models import DriveFile, DriveQuotaExhausted


class StaticTokenProvider:
    def __init__(self) -> None:
        self.refreshes: list[bool] = []

    async def get_token(self, *, force_refresh: bool = False) -> str:
        self.refreshes.append(force_refresh)
        return "access-token"


async def test_client_lists_and_exports_with_read_only_drive_endpoints() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer access-token"
        if request.url.path == "/drive/v3/files":
            return httpx.Response(
                200,
                json={
                    "files": [
                        {
                            "id": "doc-1",
                            "name": "Plan",
                            "mimeType": "application/vnd.google-apps.document",
                            "version": "8",
                        }
                    ]
                },
            )
        assert request.url.path == "/drive/v3/files/doc-1/export"
        assert request.url.params["mimeType"] == "text/plain"
        return httpx.Response(200, content=b"plan body")

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = GoogleDriveApiClient(StaticTokenProvider(), http_client=http_client)
    page = await client.list_files(page_token=None, page_size=100, parent_id="folder-id")
    body = await client.download_file(
        page.files[0],
        export_mime_type="text/plain",
        max_bytes=1_000,
    )

    assert body == b"plan body"
    assert requests[0].url.params["q"] == "'folder-id' in parents"
    assert requests[0].url.params["supportsAllDrives"] == "true"
    await http_client.aclose()


async def test_client_turns_429_retry_after_into_structured_quota_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"Retry-After": "9"},
            json={
                "error": {
                    "message": "Rate Limit Exceeded",
                    "errors": [{"reason": "rateLimitExceeded"}],
                }
            },
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = GoogleDriveApiClient(StaticTokenProvider(), http_client=http_client)

    with pytest.raises(DriveQuotaExhausted) as raised:
        await client.download_file(
            DriveFile("doc-1", "Plan", "application/vnd.google-apps.document"),
            export_mime_type="text/plain",
            max_bytes=1_000,
        )
    assert raised.value.retry_after_seconds == 9
    await http_client.aclose()


async def test_client_forces_one_token_refresh_after_401() -> None:
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(401, json={"error": {"message": "expired"}})
        return httpx.Response(200, json={"files": []})

    tokens = StaticTokenProvider()
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = GoogleDriveApiClient(tokens, http_client=http_client)

    page = await client.list_files(page_token=None, page_size=100, parent_id=None)

    assert page.files == ()
    assert tokens.refreshes == [False, True]
    await http_client.aclose()
