"""Environment configuration for the Google Drive retrieval adapter."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from math import isfinite
from pathlib import Path


class GoogleDriveConfigurationError(ValueError):
    """Google Drive adapter configuration is missing or unsafe."""


_FOLDER_ID = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class GoogleDriveConfig:
    """Fail-closed configuration shared by the Drive client and provider."""

    credential_key: str
    staging_directory: Path | None
    user_key: str
    credentials_file: Path | None = None
    subject: str | None = None
    root_folder_id: str | None = None
    max_file_bytes: int = 10 * 1024 * 1024
    request_timeout_seconds: float = 60.0
    staging_backend: str = "filesystem"
    held_file_id: str | None = None

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> GoogleDriveConfig:
        values = os.environ if environ is None else environ
        credential_key = _required(values, "GOOGLE_DRIVE_CREDENTIAL_KEY")
        staging_backend = (_optional(values, "RETRIEVAL_STAGING_BACKEND") or "filesystem").lower()
        if staging_backend not in {"filesystem", "lakebase"}:
            raise GoogleDriveConfigurationError(
                "RETRIEVAL_STAGING_BACKEND must be 'filesystem' or 'lakebase'"
            )
        staging_value = _optional(values, "GOOGLE_DRIVE_STAGING_DIRECTORY")
        staging_directory = None if staging_value is None else Path(staging_value)
        if staging_backend == "filesystem" and staging_directory is None:
            raise GoogleDriveConfigurationError(
                "GOOGLE_DRIVE_STAGING_DIRECTORY is required for filesystem staging"
            )
        if staging_directory is not None:
            if not staging_directory.is_absolute():
                raise GoogleDriveConfigurationError(
                    "GOOGLE_DRIVE_STAGING_DIRECTORY must be an absolute shared path"
                )
            if staging_directory == Path("/"):
                raise GoogleDriveConfigurationError(
                    "GOOGLE_DRIVE_STAGING_DIRECTORY must not be the filesystem root"
                )

        credentials_value = _optional(values, "GOOGLE_DRIVE_CREDENTIALS_FILE")
        credentials_file = Path(credentials_value) if credentials_value is not None else None
        if credentials_file is not None and not credentials_file.is_absolute():
            raise GoogleDriveConfigurationError(
                "GOOGLE_DRIVE_CREDENTIALS_FILE must be an absolute path"
            )

        root_folder_id = _optional(values, "GOOGLE_DRIVE_ROOT_FOLDER_ID")
        if root_folder_id is not None and _FOLDER_ID.fullmatch(root_folder_id) is None:
            raise GoogleDriveConfigurationError(
                "GOOGLE_DRIVE_ROOT_FOLDER_ID contains unsupported characters"
            )
        held_file_id = _optional(values, "GOOGLE_DRIVE_HELD_FILE_ID")
        if held_file_id is not None and _FOLDER_ID.fullmatch(held_file_id) is None:
            raise GoogleDriveConfigurationError(
                "GOOGLE_DRIVE_HELD_FILE_ID contains unsupported characters"
            )

        max_file_bytes = _positive_int(
            values,
            "GOOGLE_DRIVE_MAX_FILE_BYTES",
            10 * 1024 * 1024,
        )
        request_timeout_seconds = _positive_float(
            values,
            "GOOGLE_DRIVE_REQUEST_TIMEOUT",
            60.0,
        )
        return cls(
            credential_key=credential_key,
            staging_directory=staging_directory,
            user_key=_optional(values, "GOOGLE_DRIVE_USER_KEY") or credential_key,
            credentials_file=credentials_file,
            subject=_optional(values, "GOOGLE_DRIVE_SUBJECT"),
            root_folder_id=root_folder_id,
            max_file_bytes=max_file_bytes,
            request_timeout_seconds=request_timeout_seconds,
            staging_backend=staging_backend,
            held_file_id=held_file_id,
        )

    def sync_metadata(self, *, page_size: int = 100) -> dict[str, str]:
        """Return the provider metadata expected by :class:`SyncCommand`."""

        if page_size <= 0 or page_size > 1_000:
            raise ValueError("page_size must be between 1 and 1000")
        return {
            "provider": "google-drive",
            "credential_key": self.credential_key,
            "quota_class": "drive-api-v3",
            "resource_types": "files",
            "provider_page_size": str(page_size),
            "refresh_search_index": "true",
        }


def _optional(values: Mapping[str, str], name: str) -> str | None:
    value = values.get(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _required(values: Mapping[str, str], name: str) -> str:
    value = _optional(values, name)
    if value is None:
        raise GoogleDriveConfigurationError(f"{name} is required")
    return value


def _positive_int(values: Mapping[str, str], name: str, default: int) -> int:
    raw = _optional(values, name)
    try:
        value = default if raw is None else int(raw)
    except ValueError as exc:
        raise GoogleDriveConfigurationError(f"{name} must be an integer") from exc
    if value <= 0:
        raise GoogleDriveConfigurationError(f"{name} must be positive")
    return value


def _positive_float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = _optional(values, name)
    try:
        value = default if raw is None else float(raw)
    except ValueError as exc:
        raise GoogleDriveConfigurationError(f"{name} must be numeric") from exc
    if not isfinite(value) or value <= 0:
        raise GoogleDriveConfigurationError(f"{name} must be positive")
    return value


__all__ = ["GoogleDriveConfig", "GoogleDriveConfigurationError"]
