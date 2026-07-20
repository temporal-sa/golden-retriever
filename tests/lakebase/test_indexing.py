from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from retrieval.lakebase.indexing import LakebaseHybridIndexRefresher
from retrieval.temporal.activities.repositories import StaleLifecycleGenerationError
from retrieval.temporal.activities.search_index import RefreshSearchIndexInput


class _Cursor:
    def __init__(self, row) -> None:
        self.row = row

    async def fetchone(self):
        return self.row


class _Connection:
    def __init__(self, rows) -> None:
        self.rows = list(rows)
        self.calls = []

    async def execute(self, sql, params=None, *, prepare=None):
        self.calls.append((" ".join(sql.split()), params, prepare))
        if sql.lstrip().startswith("SELECT"):
            return _Cursor(self.rows.pop(0))
        return _Cursor(None)


class _Provider:
    def __init__(self, connection) -> None:
        self.value = connection

    @asynccontextmanager
    async def connection(self):
        yield self.value


@pytest.mark.asyncio
async def test_refresh_reindexes_between_two_generation_checks() -> None:
    connection = _Connection(
        [
            {"lifecycle_state": "active", "lifecycle_generation": 7},
            {"lifecycle_state": "active", "lifecycle_generation": 7},
        ]
    )

    await LakebaseHybridIndexRefresher(_Provider(connection)).refresh(
        RefreshSearchIndexInput("store", 7, "sync-1")
    )

    assert "REINDEX INDEX retrieval.document_chunks_search_bm25_idx" in connection.calls[1][0]
    assert connection.calls[1][2] is False


@pytest.mark.asyncio
async def test_refresh_rejects_a_store_that_fenced_during_reindex() -> None:
    connection = _Connection(
        [
            {"lifecycle_state": "active", "lifecycle_generation": 7},
            {"lifecycle_state": "deactivating", "lifecycle_generation": 8},
        ]
    )

    with pytest.raises(StaleLifecycleGenerationError):
        await LakebaseHybridIndexRefresher(_Provider(connection)).refresh(
            RefreshSearchIndexInput("store", 7, "sync-1")
        )
