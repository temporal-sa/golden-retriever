"""Minimal async Google Drive v3 client with google-auth token refresh."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Protocol
from urllib.parse import quote

from .config import GoogleDriveConfig, GoogleDriveConfigurationError
from .models import (
    DriveAuthenticationError,
    DriveFile,
    DriveFilesPage,
    DriveFileTooLargeError,
    DriveNotFoundError,
    DriveQuotaExhausted,
    DriveRequestError,
)

DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
DRIVE_API_BASE_URL = "https://www.googleapis.com/drive/v3"
_RATE_LIMIT_REASONS = {
    "dailyLimitExceeded",
    "rateLimitExceeded",
    "sharingRateLimitExceeded",
    "userRateLimitExceeded",
}


class AccessTokenProvider(Protocol):
    async def get_token(self, *, force_refresh: bool = False) -> str: ...


class GoogleAuthAccessTokenProvider:
    """Serialize refreshes for a google-auth credentials object."""

    def __init__(self, credentials: Any) -> None:
        self._credentials = credentials
        self._lock = asyncio.Lock()

    async def get_token(self, *, force_refresh: bool = False) -> str:
        async with self._lock:
            token = getattr(self._credentials, "token", None)
            valid = bool(getattr(self._credentials, "valid", False))
            if force_refresh or not valid or not token:
                await asyncio.to_thread(self._refresh)
                token = getattr(self._credentials, "token", None)
            if not token:
                raise DriveAuthenticationError("Google credentials returned no access token")
            return str(token)

    def _refresh(self) -> None:
        try:
            from google.auth.exceptions import RefreshError
            from google.auth.transport.requests import Request
        except ImportError as exc:  # pragma: no cover - packaging/configuration path
            raise GoogleDriveConfigurationError(
                "install the google-drive extra to use Google authentication"
            ) from exc
        try:
            self._credentials.refresh(Request())
        except RefreshError as exc:
            raise DriveAuthenticationError("Google credential refresh was rejected") from exc


def google_credentials(config: GoogleDriveConfig) -> Any:
    """Load read-only credentials from a key file or Application Default Credentials."""

    try:
        import google.auth
        from google.oauth2 import service_account
    except ImportError as exc:  # pragma: no cover - packaging/configuration path
        raise GoogleDriveConfigurationError(
            "install the google-drive extra to use the Google Drive adapter"
        ) from exc

    scopes = (DRIVE_READONLY_SCOPE,)
    if config.credentials_file is not None:
        if not config.credentials_file.is_file():
            raise GoogleDriveConfigurationError(
                "GOOGLE_DRIVE_CREDENTIALS_FILE does not identify a readable file"
            )
        credentials = service_account.Credentials.from_service_account_file(
            str(config.credentials_file),
            scopes=scopes,
        )
    else:
        credentials, _project_id = google.auth.default(scopes=scopes)

    if config.subject is not None:
        with_subject = getattr(credentials, "with_subject", None)
        if with_subject is None:
            raise GoogleDriveConfigurationError(
                "GOOGLE_DRIVE_SUBJECT requires service-account credentials that support delegation"
            )
        credentials = with_subject(config.subject)
    return credentials


class GoogleDriveApiClient:
    """Drive REST client limited to listing and read-only file content operations."""

    def __init__(
        self,
        token_provider: AccessTokenProvider,
        *,
        timeout_seconds: float = 60.0,
        http_client: Any | None = None,
        base_url: str = DRIVE_API_BASE_URL,
    ) -> None:
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - packaging/configuration path
            raise GoogleDriveConfigurationError(
                "install the google-drive extra to use the Google Drive adapter"
            ) from exc
        self._tokens = token_provider
        self._base_url = base_url.rstrip("/")
        self._client = http_client or httpx.AsyncClient(
            timeout=timeout_seconds,
            follow_redirects=True,
        )
        self._owns_client = http_client is None

    @classmethod
    def from_config(cls, config: GoogleDriveConfig) -> GoogleDriveApiClient:
        return cls(
            GoogleAuthAccessTokenProvider(google_credentials(config)),
            timeout_seconds=config.request_timeout_seconds,
        )

    async def list_files(
        self,
        *,
        page_token: str | None,
        page_size: int,
        parent_id: str | None,
    ) -> DriveFilesPage:
        params: dict[str, str | int] = {
            "corpora": "user",
            "fields": (
                "nextPageToken,incompleteSearch,files("
                "id,name,mimeType,modifiedTime,version,md5Checksum,size,trashed,webViewLink)"
            ),
            "includeItemsFromAllDrives": "true",
            "pageSize": max(1, min(page_size, 1_000)),
            "spaces": "drive",
            "supportsAllDrives": "true",
        }
        if page_token is not None:
            params["pageToken"] = page_token
        if parent_id is not None:
            escaped_parent = parent_id.replace("\\", "\\\\").replace("'", "\\'")
            params["q"] = f"'{escaped_parent}' in parents"

        response = await self._request("GET", "/files", params=params)
        payload = response.json()
        raw_files = payload.get("files", ())
        if not isinstance(raw_files, list):
            raise DriveRequestError(
                "Google Drive files.list returned an invalid files payload",
                status_code=502,
            )
        files: list[DriveFile] = []
        for raw in raw_files:
            if not isinstance(raw, dict):
                continue
            file_id = raw.get("id")
            name = raw.get("name")
            mime_type = raw.get("mimeType")
            if not all(isinstance(value, str) and value for value in (file_id, name, mime_type)):
                continue
            raw_size = raw.get("size")
            try:
                size = int(raw_size) if raw_size is not None else None
            except (TypeError, ValueError):
                size = None
            files.append(
                DriveFile(
                    file_id=file_id,
                    name=name,
                    mime_type=mime_type,
                    modified_time=str(raw.get("modifiedTime") or ""),
                    version=str(raw.get("version") or ""),
                    md5_checksum=str(raw.get("md5Checksum") or ""),
                    size=size,
                    trashed=bool(raw.get("trashed", False)),
                    web_view_link=(
                        str(raw["webViewLink"]) if raw.get("webViewLink") is not None else None
                    ),
                )
            )
        next_page_token = payload.get("nextPageToken")
        return DriveFilesPage(
            files=tuple(files),
            next_page_token=(str(next_page_token) if isinstance(next_page_token, str) else None),
            incomplete_search=bool(payload.get("incompleteSearch", False)),
        )

    async def download_file(
        self,
        file: DriveFile,
        *,
        export_mime_type: str | None,
        max_bytes: int,
    ) -> bytes:
        encoded_id = quote(file.file_id, safe="")
        if export_mime_type is None:
            path = f"/files/{encoded_id}"
            params = {"alt": "media", "supportsAllDrives": "true"}
        else:
            path = f"/files/{encoded_id}/export"
            params = {"mimeType": export_mime_type}
        return await self._stream_bytes(path, params=params, max_bytes=max_bytes)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str | int],
    ) -> Any:
        for auth_attempt in range(2):
            token = await self._tokens.get_token(force_refresh=auth_attempt > 0)
            response = await self._client.request(
                method,
                f"{self._base_url}{path}",
                params=params,
                headers={"Authorization": f"Bearer {token}"},
            )
            if response.status_code == 401 and auth_attempt == 0:
                continue
            if response.is_success:
                return response
            self._raise_response_error(response)
        raise DriveAuthenticationError("Google Drive rejected refreshed credentials")

    async def _stream_bytes(
        self,
        path: str,
        *,
        params: dict[str, str],
        max_bytes: int,
    ) -> bytes:
        for auth_attempt in range(2):
            token = await self._tokens.get_token(force_refresh=auth_attempt > 0)
            async with self._client.stream(
                "GET",
                f"{self._base_url}{path}",
                params=params,
                headers={"Authorization": f"Bearer {token}"},
            ) as response:
                if response.status_code == 401 and auth_attempt == 0:
                    await response.aread()
                    continue
                if not response.is_success:
                    await response.aread()
                    self._raise_response_error(response)
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > max_bytes:
                        raise DriveFileTooLargeError(
                            f"Google Drive file exceeds the {max_bytes}-byte staging limit"
                        )
                return bytes(body)
        raise DriveAuthenticationError("Google Drive rejected refreshed credentials")

    def _raise_response_error(self, response: Any) -> None:
        reason, _message = _error_details(response)
        status_code = int(response.status_code)
        if status_code == 401:
            raise DriveAuthenticationError("Google Drive rejected configured credentials")
        if status_code == 429 or (status_code == 403 and reason in _RATE_LIMIT_REASONS):
            raise DriveQuotaExhausted(
                retry_after_seconds=_retry_after_seconds(response.headers.get("Retry-After"))
            )
        if status_code == 404:
            raise DriveNotFoundError("Google Drive file is unavailable")
        if status_code in {500, 502, 503, 504}:
            raise DriveRequestError(
                f"transient Google Drive API error ({status_code})",
                status_code=status_code,
                reason=reason,
            )
        raise DriveRequestError(
            f"Google Drive API request rejected ({status_code}; reason={reason or 'unknown'})",
            status_code=status_code,
            reason=reason,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def _error_details(response: Any) -> tuple[str, str]:
    try:
        payload = response.json()
    except Exception:
        return "", ""
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return "", ""
    message = str(error.get("message") or "")
    errors = error.get("errors")
    if isinstance(errors, list):
        for item in errors:
            if isinstance(item, dict) and item.get("reason"):
                return str(item["reason"]), message
    return "", message


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())


__all__ = [
    "DRIVE_READONLY_SCOPE",
    "AccessTokenProvider",
    "GoogleAuthAccessTokenProvider",
    "GoogleDriveApiClient",
    "google_credentials",
]
