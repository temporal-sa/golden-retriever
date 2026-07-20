from __future__ import annotations

import json
from contextlib import asynccontextmanager

from retrieval.google_drive.lakebase import GoogleDriveLakebaseStore
from retrieval.google_drive.provider import _CachedPage, _TraversalState
from retrieval.temporal.activities.provider_api import FetchResourcePageRequest
from retrieval.temporal.models.documents import DocumentRef


class _Cursor:
    def __init__(self, rows=()) -> None:
        self.rows = list(rows)

    async def fetchone(self):
        return self.rows[0] if self.rows else None

    async def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self) -> None:
        self.content = {}
        self.checkpoints = {}

    async def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        if normalized.startswith("INSERT INTO retrieval_connector.staged_content"):
            digest, body = params
            self.content.setdefault(digest, body)
            return _Cursor()
        if "FROM retrieval_connector.staged_content" in normalized:
            body = self.content.get(params[0])
            return _Cursor([] if body is None else [{"body": body}])
        if normalized.startswith("INSERT INTO retrieval_connector.checkpoints"):
            key, payload = params
            self.checkpoints[key] = json.loads(payload)
            return _Cursor()
        if "checkpoint_key LIKE" in normalized:
            prefix = params[0].removesuffix("%")
            return _Cursor(
                {"payload": payload}
                for key, payload in self.checkpoints.items()
                if key.startswith(prefix)
            )
        if "FROM retrieval_connector.checkpoints" in normalized:
            payload = self.checkpoints.get(params[0])
            return _Cursor([] if payload is None else [{"payload": payload}])
        raise AssertionError(normalized)


class _Provider:
    def __init__(self) -> None:
        self.value = _Connection()

    @asynccontextmanager
    async def connection(self):
        yield self.value


def _request(cursor=None):
    return FetchResourcePageRequest(
        store_key="store",
        lifecycle_generation=7,
        sync_sequence="sync-1",
        user_key="drive-user",
        resource_key="files",
        cursor=cursor,
        page_size=100,
        request_id="request",
    )


async def test_lakebase_store_shares_content_and_idempotent_page_checkpoints() -> None:
    provider = _Provider()
    store = GoogleDriveLakebaseStore(provider, root_folder_id="root")

    staged = await store.stage(b"searchable body")
    assert await store.get(staged.uri) == b"searchable body"

    cursor = await store.save_cursor(
        _request(),
        _TraversalState("nested", ("later",), "drive-page", None),
    )
    restored = await store.traversal_state(_request(cursor))
    assert restored == _TraversalState("nested", ("later",), "drive-page", None)

    page = _CachedPage(
        page_key="page-1",
        documents=(DocumentRef("gdrive:1", "v1", staged.uri, staged.content_hash),),
        deleted_document_keys=(),
        next_cursor=cursor,
        final=False,
    )
    await store.save_page(_request(), page)
    assert await store.cached_page(_request()) == page
