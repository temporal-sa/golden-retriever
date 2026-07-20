"""Google Drive implementation of the retrieval provider gateway."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from retrieval.temporal.activities.provider_api import (
    ActiveUsersPage,
    FetchResourcePageRequest,
    InvalidCredentialsError,
    ListActiveUsersRequest,
    ProviderQuotaExhausted,
    ProviderRequestError,
    ResourcePageManifest,
    UserDescriptor,
)
from retrieval.temporal.models.documents import DocumentRef

from .config import GoogleDriveConfig
from .models import (
    DriveApiClientProtocol,
    DriveAuthenticationError,
    DriveFile,
    DriveFilesPage,
    DriveFileTooLargeError,
    DriveNotFoundError,
    DriveQuotaExhausted,
    DriveRequestError,
)
from .staging import GoogleDriveStagingStore

GOOGLE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
GOOGLE_SHORTCUT_MIME_TYPE = "application/vnd.google-apps.shortcut"
_EXPORT_MIME_TYPES = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.presentation": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
}
_DIRECT_APPLICATION_MIME_TYPES = {
    "application/csv",
    "application/javascript",
    "application/json",
    "application/ld+json",
    "application/rtf",
    "application/sql",
    "application/toml",
    "application/x-httpd-php",
    "application/x-javascript",
    "application/x-ndjson",
    "application/x-sh",
    "application/x-yaml",
    "application/xhtml+xml",
    "application/xml",
    "application/yaml",
}


class _EmptyDocumentError(ValueError):
    pass


@dataclass(frozen=True)
class _TraversalState:
    current_folder_id: str | None
    pending_folder_ids: tuple[str, ...] = ()
    page_token: str | None = None
    tombstone_offset: int | None = None


@dataclass(frozen=True)
class _CachedPage:
    page_key: str
    documents: tuple[DocumentRef, ...]
    deleted_document_keys: tuple[str, ...]
    next_cursor: str | None
    final: bool

    def manifest(self, request_id: str) -> ResourcePageManifest:
        return ResourcePageManifest(
            request_id=request_id,
            page_key=self.page_key,
            documents=self.documents,
            deleted_document_keys=self.deleted_document_keys,
            next_cursor=self.next_cursor,
        )


class _GoogleDriveProviderState:
    """Filesystem checkpoints kept outside Temporal workflow payloads."""

    def __init__(self, root: Path, *, root_folder_id: str | None) -> None:
        self._root = root
        self._root_folder_id = root_folder_id

    async def cached_page(
        self,
        request: FetchResourcePageRequest,
    ) -> _CachedPage | None:
        return await asyncio.to_thread(self._cached_page_sync, request)

    async def traversal_state(
        self,
        request: FetchResourcePageRequest,
    ) -> _TraversalState:
        return await asyncio.to_thread(self._traversal_state_sync, request)

    async def save_cursor(
        self,
        request: FetchResourcePageRequest,
        state: _TraversalState,
    ) -> str:
        return await asyncio.to_thread(self._save_cursor_sync, request, state)

    async def save_page(
        self,
        request: FetchResourcePageRequest,
        page: _CachedPage,
    ) -> None:
        await asyncio.to_thread(self._save_page_sync, request, page)

    async def reconciled_deletions(
        self,
        request: FetchResourcePageRequest,
        current_documents: tuple[DocumentRef, ...],
    ) -> tuple[str, ...]:
        return await asyncio.to_thread(
            self._reconciled_deletions_sync,
            request,
            current_documents,
        )

    async def save_tombstones(
        self,
        request: FetchResourcePageRequest,
        document_keys: tuple[str, ...],
    ) -> None:
        await asyncio.to_thread(self._save_tombstones_sync, request, document_keys)

    async def tombstone_page(
        self,
        request: FetchResourcePageRequest,
        *,
        offset: int,
        page_size: int,
    ) -> tuple[tuple[str, ...], int]:
        return await asyncio.to_thread(
            self._tombstone_page_sync,
            request,
            offset,
            page_size,
        )

    async def promote_baseline(
        self,
        request: FetchResourcePageRequest,
        deleted_document_keys: tuple[str, ...],
    ) -> None:
        await asyncio.to_thread(
            self._promote_baseline_sync,
            request,
            deleted_document_keys,
        )

    def _scope_directory(self, request: FetchResourcePageRequest) -> Path:
        identity = _stable_digest(
            {
                "store_key": request.store_key,
                "user_key": request.user_key,
                "resource_key": request.resource_key,
                "root_folder_id": self._root_folder_id or "",
            }
        )
        return self._root / identity

    def _run_directory(self, request: FetchResourcePageRequest) -> Path:
        run_identity = _stable_digest(
            {
                "lifecycle_generation": request.lifecycle_generation,
                "sync_sequence": request.sync_sequence,
            }
        )
        return self._scope_directory(request) / "runs" / run_identity

    def _page_path(self, request: FetchResourcePageRequest) -> Path:
        cursor_key = request.cursor or "initial"
        return self._run_directory(request) / "pages" / f"{_text_digest(cursor_key)}.json"

    def _cached_page_sync(self, request: FetchResourcePageRequest) -> _CachedPage | None:
        path = self._page_path(request)
        if not path.is_file():
            return None
        return _cached_page_from_payload(_read_json(path))

    def _traversal_state_sync(self, request: FetchResourcePageRequest) -> _TraversalState:
        if request.cursor is None:
            return _TraversalState(current_folder_id=self._root_folder_id)
        prefix = "gdrive-cursor-v1:"
        if not request.cursor.startswith(prefix):
            raise ProviderRequestError(
                "Google Drive page cursor is invalid",
                error_type="InvalidProviderCursor",
            )
        digest = request.cursor.removeprefix(prefix)
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ProviderRequestError(
                "Google Drive page cursor is invalid",
                error_type="InvalidProviderCursor",
            )
        path = self._run_directory(request) / "cursors" / f"{digest}.json"
        if not path.is_file():
            raise ProviderRequestError(
                "Google Drive page cursor checkpoint is unavailable",
                error_type="InvalidProviderCursor",
            )
        payload = _read_json(path)
        current = payload.get("current_folder_id")
        pending = payload.get("pending_folder_ids", [])
        page_token = payload.get("page_token")
        tombstone_offset = payload.get("tombstone_offset")
        if (
            (current is not None and not isinstance(current, str))
            or not isinstance(pending, list)
            or any(not isinstance(value, str) for value in pending)
            or (page_token is not None and not isinstance(page_token, str))
            or (
                tombstone_offset is not None
                and (
                    isinstance(tombstone_offset, bool)
                    or not isinstance(tombstone_offset, int)
                    or tombstone_offset < 0
                )
            )
        ):
            raise ProviderRequestError(
                "Google Drive page cursor checkpoint is corrupt",
                error_type="InvalidProviderCursor",
            )
        return _TraversalState(
            current_folder_id=current,
            pending_folder_ids=tuple(pending),
            page_token=page_token,
            tombstone_offset=tombstone_offset,
        )

    def _save_cursor_sync(
        self,
        request: FetchResourcePageRequest,
        state: _TraversalState,
    ) -> str:
        payload = {
            "current_folder_id": state.current_folder_id,
            "pending_folder_ids": list(state.pending_folder_ids),
            "page_token": state.page_token,
            "tombstone_offset": state.tombstone_offset,
        }
        digest = _stable_digest(payload)
        path = self._run_directory(request) / "cursors" / f"{digest}.json"
        _write_json_atomic(path, payload)
        return f"gdrive-cursor-v1:{digest}"

    def _save_page_sync(
        self,
        request: FetchResourcePageRequest,
        page: _CachedPage,
    ) -> None:
        payload = {
            "page_key": page.page_key,
            "documents": [asdict(document) for document in page.documents],
            "deleted_document_keys": list(page.deleted_document_keys),
            "next_cursor": page.next_cursor,
            "final": page.final,
        }
        _write_json_atomic(self._page_path(request), payload)

    def _all_current_document_keys_sync(
        self,
        request: FetchResourcePageRequest,
        current_documents: tuple[DocumentRef, ...] = (),
    ) -> set[str]:
        keys = {document.document_key for document in current_documents}
        pages = self._run_directory(request) / "pages"
        if not pages.is_dir():
            return keys
        for path in pages.glob("*.json"):
            cached = _cached_page_from_payload(_read_json(path))
            keys.update(document.document_key for document in cached.documents)
        return keys

    def _baseline_sync(self, request: FetchResourcePageRequest) -> tuple[set[str], set[str]]:
        path = self._scope_directory(request) / "baseline.json"
        if not path.is_file():
            return set(), set()
        payload = _read_json(path)
        current = payload.get("current_document_keys", [])
        tombstones = payload.get("tombstone_document_keys", [])
        if (
            not isinstance(current, list)
            or any(not isinstance(value, str) for value in current)
            or not isinstance(tombstones, list)
            or any(not isinstance(value, str) for value in tombstones)
        ):
            raise ProviderRequestError(
                "Google Drive reconciliation checkpoint is corrupt",
                error_type="ProviderCheckpointCorrupt",
            )
        return set(current), set(tombstones)

    def _reconciled_deletions_sync(
        self,
        request: FetchResourcePageRequest,
        current_documents: tuple[DocumentRef, ...],
    ) -> tuple[str, ...]:
        current = self._all_current_document_keys_sync(request, current_documents)
        prior_current, prior_tombstones = self._baseline_sync(request)
        return tuple(sorted((prior_tombstones | (prior_current - current)) - current))

    def _save_tombstones_sync(
        self,
        request: FetchResourcePageRequest,
        document_keys: tuple[str, ...],
    ) -> None:
        _write_json_atomic(
            self._run_directory(request) / "tombstones.json",
            {"document_keys": list(document_keys)},
        )

    def _tombstone_page_sync(
        self,
        request: FetchResourcePageRequest,
        offset: int,
        page_size: int,
    ) -> tuple[tuple[str, ...], int]:
        path = self._run_directory(request) / "tombstones.json"
        if not path.is_file():
            raise ProviderRequestError(
                "Google Drive tombstone checkpoint is unavailable",
                error_type="ProviderCheckpointCorrupt",
            )
        values = _read_json(path).get("document_keys")
        if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
            raise ProviderRequestError(
                "Google Drive tombstone checkpoint is corrupt",
                error_type="ProviderCheckpointCorrupt",
            )
        if offset >= len(values):
            raise ProviderRequestError(
                "Google Drive tombstone cursor is out of range",
                error_type="InvalidProviderCursor",
            )
        bounded_size = max(1, min(page_size, 1_000))
        return tuple(values[offset : offset + bounded_size]), len(values)

    def _promote_baseline_sync(
        self,
        request: FetchResourcePageRequest,
        deleted_document_keys: tuple[str, ...],
    ) -> None:
        current = self._all_current_document_keys_sync(request)
        _prior_current, prior_tombstones = self._baseline_sync(request)
        run_tombstones = set(deleted_document_keys)
        tombstone_path = self._run_directory(request) / "tombstones.json"
        if tombstone_path.is_file():
            values = _read_json(tombstone_path).get("document_keys")
            if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
                raise ProviderRequestError(
                    "Google Drive tombstone checkpoint is corrupt",
                    error_type="ProviderCheckpointCorrupt",
                )
            run_tombstones.update(values)
        tombstones = (prior_tombstones | run_tombstones) - current
        _write_json_atomic(
            self._scope_directory(request) / "baseline.json",
            {
                "current_document_keys": sorted(current),
                "tombstone_document_keys": sorted(tombstones),
            },
        )


class GoogleDriveProviderGateway:
    """Read Google Drive pages, stage searchable text, and emit compact references."""

    def __init__(
        self,
        config: GoogleDriveConfig,
        api: DriveApiClientProtocol,
        staging_store: GoogleDriveStagingStore,
    ) -> None:
        self._config = config
        self._api = api
        self._staging = staging_store
        self._state = _GoogleDriveProviderState(
            staging_store.state_root,
            root_folder_id=config.root_folder_id,
        )

    async def list_active_users(self, request: ListActiveUsersRequest) -> ActiveUsersPage:
        users = ()
        if request.cursor is None:
            users = (UserDescriptor(user_key=self._config.user_key),)
        return ActiveUsersPage(request_id=request.request_id, users=users)

    async def fetch_resource_page(
        self,
        request: FetchResourcePageRequest,
    ) -> ResourcePageManifest:
        if request.user_key != self._config.user_key:
            raise ProviderRequestError(
                "Google Drive request uses an unknown user key",
                error_type="InvalidProviderUser",
            )
        if request.resource_key != "files":
            raise ProviderRequestError(
                f"Google Drive does not support resource {request.resource_key!r}",
                error_type="UnsupportedProviderResource",
            )

        await self._staging.prepare()
        cached = await self._state.cached_page(request)
        if cached is not None:
            if cached.final:
                await self._state.promote_baseline(request, cached.deleted_document_keys)
            return cached.manifest(request.request_id)

        traversal = await self._state.traversal_state(request)
        if traversal.tombstone_offset is not None:
            return await self._fetch_tombstone_page(request, traversal.tombstone_offset)
        try:
            page = await self._list_files(traversal, request.page_size)
            if page.incomplete_search:
                raise ProviderRequestError(
                    "Google Drive reported an incomplete files.list result",
                    error_type="IncompleteProviderSearch",
                )
            documents = await self._stage_documents(page.files)
        except DriveAuthenticationError as exc:
            raise InvalidCredentialsError("Google Drive rejected configured credentials") from exc
        except DriveQuotaExhausted as exc:
            raise ProviderQuotaExhausted(
                remaining=0,
                retry_after_seconds=exc.retry_after_seconds,
            ) from exc
        except DriveNotFoundError as exc:
            raise ProviderRequestError(
                "Google Drive root or listing scope is unavailable",
                error_type="GoogleDriveScopeUnavailable",
            ) from exc
        except DriveRequestError as exc:
            if exc.status_code >= 500:
                raise
            raise ProviderRequestError(
                str(exc),
                error_type="GoogleDriveRequestRejected",
            ) from exc

        next_state = _next_traversal_state(traversal, page, self._config.root_folder_id)
        if next_state is None:
            deleted_document_keys = await self._state.reconciled_deletions(request, documents)
            if deleted_document_keys:
                await self._state.save_tombstones(request, deleted_document_keys)
                next_state = _TraversalState(
                    current_folder_id=None,
                    tombstone_offset=0,
                )
        next_cursor = (
            None if next_state is None else await self._state.save_cursor(request, next_state)
        )
        final = next_cursor is None

        cached = _CachedPage(
            page_key=_page_key(request),
            documents=documents,
            deleted_document_keys=(),
            next_cursor=next_cursor,
            final=final,
        )
        await self._state.save_page(request, cached)
        if final:
            await self._state.promote_baseline(request, ())
        return cached.manifest(request.request_id)

    async def _fetch_tombstone_page(
        self,
        request: FetchResourcePageRequest,
        offset: int,
    ) -> ResourcePageManifest:
        deleted_document_keys, total = await self._state.tombstone_page(
            request,
            offset=offset,
            page_size=request.page_size,
        )
        next_offset = offset + len(deleted_document_keys)
        next_state = None
        if next_offset < total:
            next_state = _TraversalState(
                current_folder_id=None,
                tombstone_offset=next_offset,
            )
        next_cursor = (
            None if next_state is None else await self._state.save_cursor(request, next_state)
        )
        cached = _CachedPage(
            page_key=_page_key(request),
            documents=(),
            deleted_document_keys=deleted_document_keys,
            next_cursor=next_cursor,
            final=next_cursor is None,
        )
        await self._state.save_page(request, cached)
        if cached.final:
            await self._state.promote_baseline(request, deleted_document_keys)
        return cached.manifest(request.request_id)

    async def _list_files(self, state: _TraversalState, page_size: int) -> DriveFilesPage:
        try:
            return await self._api.list_files(
                page_token=state.page_token,
                page_size=page_size,
                parent_id=state.current_folder_id,
            )
        except DriveRequestError as exc:
            if state.page_token is None or exc.status_code != 400:
                raise
            # Drive documents that rejected tokens should be discarded and restarted.
            return await self._api.list_files(
                page_token=None,
                page_size=page_size,
                parent_id=state.current_folder_id,
            )

    async def _stage_documents(self, files: tuple[DriveFile, ...]) -> tuple[DocumentRef, ...]:
        documents: list[DocumentRef] = []
        for file in files:
            if file.trashed or file.mime_type in {
                GOOGLE_FOLDER_MIME_TYPE,
                GOOGLE_SHORTCUT_MIME_TYPE,
            }:
                continue
            export_mime_type = _export_mime_type(file.mime_type)
            if export_mime_type is _UNSUPPORTED:
                continue
            if file.size is not None and file.size > self._config.max_file_bytes:
                continue
            try:
                raw_body = await self._api.download_file(
                    file,
                    export_mime_type=export_mime_type,
                    max_bytes=self._config.max_file_bytes,
                )
                staged_body = _searchable_body(file, raw_body)
            except (
                DriveFileTooLargeError,
                DriveNotFoundError,
                UnicodeDecodeError,
                _EmptyDocumentError,
            ):
                continue
            staged = await self._staging.stage(staged_body)
            documents.append(
                DocumentRef(
                    document_key=_document_key(file.file_id),
                    source_version=(
                        file.version
                        or file.md5_checksum
                        or file.modified_time
                        or staged.content_hash
                    ),
                    staging_uri=staged.uri,
                    content_hash=staged.content_hash,
                )
            )
        return tuple(documents)

    async def aclose(self) -> None:
        close = getattr(self._api, "aclose", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result


_UNSUPPORTED = object()


def _export_mime_type(mime_type: str) -> str | None | object:
    if mime_type in _EXPORT_MIME_TYPES:
        return _EXPORT_MIME_TYPES[mime_type]
    if mime_type.startswith("text/") or mime_type in _DIRECT_APPLICATION_MIME_TYPES:
        return None
    return _UNSUPPORTED


def _searchable_body(file: DriveFile, raw_body: bytes) -> bytes:
    text = raw_body.decode("utf-8")
    if not text.strip():
        raise _EmptyDocumentError("Google Drive document is empty")
    title = " ".join(file.name.splitlines()).strip() or file.file_id
    source_uri = file.web_view_link or f"https://drive.google.com/open?id={file.file_id}"
    source_uri = " ".join(source_uri.splitlines()).strip()
    return f"---\ntitle: {title}\nsource_uri: {source_uri}\n---\n{text}".encode()


def _next_traversal_state(
    state: _TraversalState,
    page: DriveFilesPage,
    root_folder_id: str | None,
) -> _TraversalState | None:
    if root_folder_id is None:
        if page.next_page_token is None:
            return None
        return _TraversalState(current_folder_id=None, page_token=page.next_page_token)

    pending = list(state.pending_folder_ids)
    for file in page.files:
        if (
            file.mime_type == GOOGLE_FOLDER_MIME_TYPE
            and not file.trashed
            and file.file_id != state.current_folder_id
            and file.file_id not in pending
        ):
            pending.append(file.file_id)
    if page.next_page_token is not None:
        return _TraversalState(
            current_folder_id=state.current_folder_id,
            pending_folder_ids=tuple(pending),
            page_token=page.next_page_token,
        )
    if not pending:
        return None
    return _TraversalState(
        current_folder_id=pending[0],
        pending_folder_ids=tuple(pending[1:]),
    )


def _page_key(request: FetchResourcePageRequest) -> str:
    return f"gdrive-page-{_text_digest(request.cursor or 'initial')[:24]}"


def _document_key(file_id: str) -> str:
    return f"gdrive:{file_id}"


def _stable_digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _text_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ProviderRequestError(
            "Google Drive provider checkpoint is corrupt",
            error_type="ProviderCheckpointCorrupt",
        ) from exc
    if not isinstance(payload, dict):
        raise ProviderRequestError(
            "Google Drive provider checkpoint is corrupt",
            error_type="ProviderCheckpointCorrupt",
        )
    return payload


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".state-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _cached_page_from_payload(payload: dict[str, Any]) -> _CachedPage:
    try:
        raw_documents = payload.get("documents", [])
        documents = tuple(DocumentRef(**item) for item in raw_documents)
        deleted = tuple(str(value) for value in payload.get("deleted_document_keys", []))
        next_cursor = payload.get("next_cursor")
        if next_cursor is not None and not isinstance(next_cursor, str):
            raise TypeError("next cursor must be a string")
        return _CachedPage(
            page_key=str(payload["page_key"]),
            documents=documents,
            deleted_document_keys=deleted,
            next_cursor=next_cursor,
            final=bool(payload.get("final", False)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ProviderRequestError(
            "Google Drive provider page checkpoint is corrupt",
            error_type="ProviderCheckpointCorrupt",
        ) from exc


__all__ = ["GoogleDriveProviderGateway"]
