"""Shared content-addressed staging for Google Drive document bodies."""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse


class InvalidGoogleDriveStagingUriError(ValueError):
    """A staging URI is malformed or points outside the content store."""


class GoogleDriveStagingIntegrityError(RuntimeError):
    """Staged bytes no longer match their content-addressed URI."""


@dataclass(frozen=True)
class StagedGoogleDriveObject:
    uri: str
    content_hash: str


class GoogleDriveStagingStore:
    """Stage immutable bytes beneath a shared, operator-managed directory.

    The directory must be durable and mounted at the same absolute path on every
    worker replica. Content files are immutable and safe to reuse across retries.
    """

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("Google Drive staging root must be absolute")
        self.root = root
        self.content_root = root / "content" / "sha256"
        self.state_root = root / "state"

    async def prepare(self) -> None:
        await asyncio.to_thread(self._prepare_sync)

    async def stage(self, body: bytes) -> StagedGoogleDriveObject:
        return await asyncio.to_thread(self._stage_sync, body)

    async def get(self, staging_uri: str) -> bytes:
        digest = _digest_from_uri(staging_uri)
        return await asyncio.to_thread(self._get_sync, digest)

    def _prepare_sync(self) -> None:
        self.content_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.state_root.mkdir(mode=0o700, parents=True, exist_ok=True)

    def _stage_sync(self, body: bytes) -> StagedGoogleDriveObject:
        self._prepare_sync()
        digest = hashlib.sha256(body).hexdigest()
        target = self.content_root / digest
        if target.exists():
            if target.is_symlink() or not target.is_file():
                raise GoogleDriveStagingIntegrityError("staged content path must be a regular file")
            existing = target.read_bytes()
            if hashlib.sha256(existing).hexdigest() != digest:
                raise GoogleDriveStagingIntegrityError(
                    "existing staged content does not match its SHA-256 path"
                )
        else:
            descriptor, temporary_name = tempfile.mkstemp(prefix=".stage-", dir=self.content_root)
            temporary = Path(temporary_name)
            try:
                os.fchmod(descriptor, 0o600)
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(body)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        return StagedGoogleDriveObject(
            uri=f"gdrive-stage://sha256/{digest}",
            content_hash=digest,
        )

    def _get_sync(self, digest: str) -> bytes:
        target = self.content_root / digest
        if target.is_symlink() or not target.is_file():
            raise GoogleDriveStagingIntegrityError("staged content path must be a regular file")
        body = target.read_bytes()
        if hashlib.sha256(body).hexdigest() != digest:
            raise GoogleDriveStagingIntegrityError("staged content does not match its SHA-256 URI")
        return body


def _digest_from_uri(staging_uri: str) -> str:
    parsed = urlparse(staging_uri)
    if (
        parsed.scheme != "gdrive-stage"
        or parsed.netloc != "sha256"
        or parsed.params
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
        or unquote(parsed.path) != parsed.path
    ):
        raise InvalidGoogleDriveStagingUriError(
            "only gdrive-stage://sha256/<digest> URIs are supported"
        )
    digest = parsed.path.removeprefix("/")
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise InvalidGoogleDriveStagingUriError("staging URI must contain a SHA-256 digest")
    return digest


__all__ = [
    "GoogleDriveStagingIntegrityError",
    "GoogleDriveStagingStore",
    "InvalidGoogleDriveStagingUriError",
    "StagedGoogleDriveObject",
]
