from __future__ import annotations

from pathlib import Path

import pytest

from retrieval.google_drive.config import (
    GoogleDriveConfig,
    GoogleDriveConfigurationError,
)


def test_config_requires_opaque_credential_and_absolute_staging_path(tmp_path: Path) -> None:
    config = GoogleDriveConfig.from_env(
        {
            "GOOGLE_DRIVE_CREDENTIAL_KEY": "workspace-primary",
            "GOOGLE_DRIVE_STAGING_DIRECTORY": str(tmp_path),
            "GOOGLE_DRIVE_ROOT_FOLDER_ID": "folder_123-ABC",
            "GOOGLE_DRIVE_MAX_FILE_BYTES": "2048",
            "GOOGLE_DRIVE_REQUEST_TIMEOUT": "12.5",
        }
    )

    assert config.user_key == "workspace-primary"
    assert config.root_folder_id == "folder_123-ABC"
    assert config.max_file_bytes == 2048
    assert config.request_timeout_seconds == 12.5
    assert config.sync_metadata(page_size=250) == {
        "provider": "google-drive",
        "credential_key": "workspace-primary",
        "quota_class": "drive-api-v3",
        "resource_types": "files",
        "provider_page_size": "250",
    }


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"GOOGLE_DRIVE_CREDENTIAL_KEY": "key"},
        {
            "GOOGLE_DRIVE_CREDENTIAL_KEY": "key",
            "GOOGLE_DRIVE_STAGING_DIRECTORY": "relative/path",
        },
        {
            "GOOGLE_DRIVE_CREDENTIAL_KEY": "key",
            "GOOGLE_DRIVE_STAGING_DIRECTORY": "/",
        },
        {
            "GOOGLE_DRIVE_CREDENTIAL_KEY": "key",
            "GOOGLE_DRIVE_STAGING_DIRECTORY": "/tmp/staging",
            "GOOGLE_DRIVE_ROOT_FOLDER_ID": "not/a/folder/id",
        },
        {
            "GOOGLE_DRIVE_CREDENTIAL_KEY": "key",
            "GOOGLE_DRIVE_STAGING_DIRECTORY": "/tmp/staging",
            "GOOGLE_DRIVE_REQUEST_TIMEOUT": "nan",
        },
    ],
)
def test_invalid_config_fails_closed(values: dict[str, str]) -> None:
    with pytest.raises(GoogleDriveConfigurationError):
        GoogleDriveConfig.from_env(values)
