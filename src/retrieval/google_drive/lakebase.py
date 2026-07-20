"""Lakebase-backed Google Drive staging bodies and traversal checkpoints."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from retrieval.lakebase.repository import AsyncConnectionProvider
from retrieval.temporal.activities.provider_api import (
    FetchResourcePageRequest,
    ProviderRequestError,
)
from retrieval.temporal.models.documents import DocumentRef

from .provider import (
    _cached_page_from_payload,
    _CachedPage,
    _stable_digest,
    _text_digest,
    _TraversalState,
)
from .staging import (
    GoogleDriveStagingIntegrityError,
    StagedGoogleDriveObject,
    _digest_from_uri,
)


class GoogleDriveLakebaseStore:
    """Shared immutable content and idempotent provider state for every worker replica."""

    def __init__(
        self,
        provider: AsyncConnectionProvider,
        *,
        root_folder_id: str | None,
    ) -> None:
        self._provider = provider
        self._root_folder_id = root_folder_id

    async def prepare(self) -> None:
        # Core migrations own schema creation. Keeping prepare makes this object
        # satisfy the same staging port as the local adapter.
        return None

    async def stage(self, body: bytes) -> StagedGoogleDriveObject:
        digest = hashlib.sha256(body).hexdigest()
        async with self._provider.connection() as connection:
            await connection.execute(
                """
                INSERT INTO retrieval_connector.staged_content (content_hash, body)
                VALUES (%s, %s)
                ON CONFLICT (content_hash) DO NOTHING
                """,
                (digest, body),
            )
            cursor = await connection.execute(
                """
                SELECT body
                FROM retrieval_connector.staged_content
                WHERE content_hash = %s
                """,
                (digest,),
            )
            row = await cursor.fetchone()
        existing = bytes(_row(row, "body", 0)) if row is not None else b""
        if hashlib.sha256(existing).hexdigest() != digest:
            raise GoogleDriveStagingIntegrityError(
                "Lakebase staged content does not match its SHA-256 key"
            )
        return StagedGoogleDriveObject(
            uri=f"gdrive-stage://sha256/{digest}",
            content_hash=digest,
        )

    async def get(self, staging_uri: str) -> bytes:
        digest = _digest_from_uri(staging_uri)
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT body
                FROM retrieval_connector.staged_content
                WHERE content_hash = %s
                """,
                (digest,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise GoogleDriveStagingIntegrityError("Lakebase staged content is unavailable")
        body = bytes(_row(row, "body", 0))
        if hashlib.sha256(body).hexdigest() != digest:
            raise GoogleDriveStagingIntegrityError(
                "Lakebase staged content does not match its SHA-256 URI"
            )
        return body

    async def cached_page(self, request: FetchResourcePageRequest) -> _CachedPage | None:
        payload = await self._load(self._page_key(request))
        return None if payload is None else _cached_page_from_payload(payload)

    async def traversal_state(self, request: FetchResourcePageRequest) -> _TraversalState:
        if request.cursor is None:
            return _TraversalState(current_folder_id=self._root_folder_id)
        prefix = "gdrive-cursor-v1:"
        if not request.cursor.startswith(prefix):
            raise _invalid_cursor()
        digest = request.cursor.removeprefix(prefix)
        if len(digest) != 64 or any(value not in "0123456789abcdef" for value in digest):
            raise _invalid_cursor()
        payload = await self._load(f"{self._run_prefix(request)}/cursor/{digest}")
        if payload is None:
            raise ProviderRequestError(
                "Google Drive page cursor checkpoint is unavailable",
                error_type="InvalidProviderCursor",
            )
        return _traversal_from_payload(payload)

    async def save_cursor(
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
        await self._save(f"{self._run_prefix(request)}/cursor/{digest}", payload)
        return f"gdrive-cursor-v1:{digest}"

    async def save_page(self, request: FetchResourcePageRequest, page: _CachedPage) -> None:
        await self._save(
            self._page_key(request),
            {
                "page_key": page.page_key,
                "documents": [
                    {
                        "document_key": item.document_key,
                        "source_version": item.source_version,
                        "staging_uri": item.staging_uri,
                        "content_hash": item.content_hash,
                    }
                    for item in page.documents
                ],
                "deleted_document_keys": list(page.deleted_document_keys),
                "next_cursor": page.next_cursor,
                "final": page.final,
            },
        )

    async def reconciled_deletions(
        self,
        request: FetchResourcePageRequest,
        current_documents: tuple[DocumentRef, ...],
    ) -> tuple[str, ...]:
        current = await self._all_current_document_keys(request, current_documents)
        prior_current, prior_tombstones = await self._baseline(request)
        return tuple(sorted((prior_tombstones | (prior_current - current)) - current))

    async def save_tombstones(
        self,
        request: FetchResourcePageRequest,
        document_keys: tuple[str, ...],
    ) -> None:
        await self._save(
            f"{self._run_prefix(request)}/tombstones",
            {"document_keys": list(document_keys)},
        )

    async def tombstone_page(
        self,
        request: FetchResourcePageRequest,
        *,
        offset: int,
        page_size: int,
    ) -> tuple[tuple[str, ...], int]:
        payload = await self._load(f"{self._run_prefix(request)}/tombstones")
        values = None if payload is None else payload.get("document_keys")
        if not isinstance(values, list) or any(not isinstance(item, str) for item in values):
            raise ProviderRequestError(
                "Google Drive tombstone checkpoint is unavailable or corrupt",
                error_type="ProviderCheckpointCorrupt",
            )
        if offset >= len(values):
            raise _invalid_cursor()
        bounded_size = max(1, min(page_size, 1_000))
        return tuple(values[offset : offset + bounded_size]), len(values)

    async def promote_baseline(
        self,
        request: FetchResourcePageRequest,
        deleted_document_keys: tuple[str, ...],
    ) -> None:
        current = await self._all_current_document_keys(request)
        _prior, prior_tombstones = await self._baseline(request)
        run_payload = await self._load(f"{self._run_prefix(request)}/tombstones")
        run_tombstones = set(deleted_document_keys)
        if run_payload is not None:
            values = run_payload.get("document_keys")
            if not isinstance(values, list) or any(not isinstance(item, str) for item in values):
                raise ProviderRequestError(
                    "Google Drive tombstone checkpoint is corrupt",
                    error_type="ProviderCheckpointCorrupt",
                )
            run_tombstones.update(values)
        await self._save(
            self._baseline_key(request),
            {
                "current_document_keys": sorted(current),
                "tombstone_document_keys": sorted((prior_tombstones | run_tombstones) - current),
            },
        )

    def _scope_digest(self, request: FetchResourcePageRequest) -> str:
        return _stable_digest(
            {
                "store_key": request.store_key,
                "user_key": request.user_key,
                "resource_key": request.resource_key,
                "root_folder_id": self._root_folder_id or "",
            }
        )

    def _run_prefix(self, request: FetchResourcePageRequest) -> str:
        run = _stable_digest(
            {
                "lifecycle_generation": request.lifecycle_generation,
                "sync_sequence": request.sync_sequence,
            }
        )
        return f"scope/{self._scope_digest(request)}/run/{run}"

    def _page_key(self, request: FetchResourcePageRequest) -> str:
        return f"{self._run_prefix(request)}/page/{_text_digest(request.cursor or 'initial')}"

    def _baseline_key(self, request: FetchResourcePageRequest) -> str:
        return f"scope/{self._scope_digest(request)}/baseline"

    async def _all_current_document_keys(
        self,
        request: FetchResourcePageRequest,
        current_documents: tuple[DocumentRef, ...] = (),
    ) -> set[str]:
        keys = {item.document_key for item in current_documents}
        prefix = f"{self._run_prefix(request)}/page/%"
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT payload
                FROM retrieval_connector.checkpoints
                WHERE checkpoint_key LIKE %s
                """,
                (prefix,),
            )
            rows = await cursor.fetchall()
        for row in rows:
            cached = _cached_page_from_payload(_json_payload(_row(row, "payload", 0)))
            keys.update(item.document_key for item in cached.documents)
        return keys

    async def _baseline(
        self,
        request: FetchResourcePageRequest,
    ) -> tuple[set[str], set[str]]:
        payload = await self._load(self._baseline_key(request))
        if payload is None:
            return set(), set()
        current = payload.get("current_document_keys")
        tombstones = payload.get("tombstone_document_keys")
        if (
            not isinstance(current, list)
            or any(not isinstance(item, str) for item in current)
            or not isinstance(tombstones, list)
            or any(not isinstance(item, str) for item in tombstones)
        ):
            raise ProviderRequestError(
                "Google Drive reconciliation checkpoint is corrupt",
                error_type="ProviderCheckpointCorrupt",
            )
        return set(current), set(tombstones)

    async def _load(self, key: str) -> dict[str, Any] | None:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT payload
                FROM retrieval_connector.checkpoints
                WHERE checkpoint_key = %s
                """,
                (key,),
            )
            row = await cursor.fetchone()
        return None if row is None else _json_payload(_row(row, "payload", 0))

    async def _save(self, key: str, payload: dict[str, Any]) -> None:
        async with self._provider.connection() as connection:
            await connection.execute(
                """
                INSERT INTO retrieval_connector.checkpoints (checkpoint_key, payload)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (checkpoint_key) DO UPDATE
                SET payload = EXCLUDED.payload, updated_at = clock_timestamp()
                """,
                (key, json.dumps(payload, sort_keys=True, separators=(",", ":"))),
            )


def _traversal_from_payload(payload: dict[str, Any]) -> _TraversalState:
    current = payload.get("current_folder_id")
    pending = payload.get("pending_folder_ids", [])
    token = payload.get("page_token")
    offset = payload.get("tombstone_offset")
    if (
        (current is not None and not isinstance(current, str))
        or not isinstance(pending, list)
        or any(not isinstance(item, str) for item in pending)
        or (token is not None and not isinstance(token, str))
        or (offset is not None and (isinstance(offset, bool) or not isinstance(offset, int)))
        or (isinstance(offset, int) and offset < 0)
    ):
        raise ProviderRequestError(
            "Google Drive page cursor checkpoint is corrupt",
            error_type="InvalidProviderCursor",
        )
    return _TraversalState(current, tuple(pending), token, offset)


def _invalid_cursor() -> ProviderRequestError:
    return ProviderRequestError(
        "Google Drive page cursor is invalid",
        error_type="InvalidProviderCursor",
    )


def _json_payload(value: Any) -> dict[str, Any]:
    payload = json.loads(value) if isinstance(value, str) else value
    if not isinstance(payload, dict):
        raise ProviderRequestError(
            "Google Drive provider checkpoint is corrupt",
            error_type="ProviderCheckpointCorrupt",
        )
    return payload


def _row(row: Any, name: str, index: int) -> Any:
    return row[name] if isinstance(row, dict) else row[index]


__all__ = ["GoogleDriveLakebaseStore"]
