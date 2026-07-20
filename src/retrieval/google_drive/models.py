"""Small internal models for Google Drive API responses."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class DriveFile:
    file_id: str
    name: str
    mime_type: str
    modified_time: str = ""
    version: str = ""
    md5_checksum: str = ""
    size: int | None = None
    trashed: bool = False
    web_view_link: str | None = None


@dataclass(frozen=True)
class DriveFilesPage:
    files: tuple[DriveFile, ...]
    next_page_token: str | None = None
    incomplete_search: bool = False


class DriveApiError(RuntimeError):
    """Base error raised by the Drive HTTP client."""


class DriveAuthenticationError(DriveApiError):
    pass


class DriveQuotaExhausted(DriveApiError):
    def __init__(
        self,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__("Google Drive API quota exhausted")
        self.retry_after_seconds = retry_after_seconds


class DriveNotFoundError(DriveApiError):
    pass


class DriveFileTooLargeError(DriveApiError):
    pass


class DriveRequestError(DriveApiError):
    def __init__(self, message: str, *, status_code: int, reason: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.reason = reason


class DriveApiClientProtocol(Protocol):
    async def list_files(
        self,
        *,
        page_token: str | None,
        page_size: int,
        parent_id: str | None,
    ) -> DriveFilesPage: ...

    async def download_file(
        self,
        file: DriveFile,
        *,
        export_mime_type: str | None,
        max_bytes: int,
    ) -> bytes: ...


__all__ = [
    "DriveApiClientProtocol",
    "DriveApiError",
    "DriveAuthenticationError",
    "DriveFile",
    "DriveFileTooLargeError",
    "DriveFilesPage",
    "DriveNotFoundError",
    "DriveQuotaExhausted",
    "DriveRequestError",
]
