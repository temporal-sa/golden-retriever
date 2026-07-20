"""Lakebase Search index maintenance guarded by the store generation."""

from __future__ import annotations

from typing import Any

from retrieval.temporal.activities.repositories import (
    LifecycleStateRejectedError,
    StaleLifecycleGenerationError,
)
from retrieval.temporal.activities.search_index import RefreshSearchIndexInput

from .repository import AsyncConnectionProvider


class LakebaseHybridIndexRefresher:
    """Refresh BM25 corpus statistics before a sync becomes searchable."""

    def __init__(self, provider: AsyncConnectionProvider) -> None:
        self._provider = provider

    async def refresh(self, command: RefreshSearchIndexInput) -> None:
        async with self._provider.connection() as connection:
            await self._require_current(connection, command)
            await connection.execute(
                "REINDEX INDEX retrieval.document_chunks_search_bm25_idx",
                prepare=False,
            )
            await self._require_current(connection, command)

    @staticmethod
    async def _require_current(connection: Any, command: RefreshSearchIndexInput) -> None:
        cursor = await connection.execute(
            """
            SELECT lifecycle_state, lifecycle_generation
            FROM retrieval.stores
            WHERE store_key = %s
            """,
            (command.store_key,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise KeyError(command.store_key)
        state = str(row["lifecycle_state"] if isinstance(row, dict) else row[0])
        generation = int(row["lifecycle_generation"] if isinstance(row, dict) else row[1])
        if generation != command.lifecycle_generation:
            raise StaleLifecycleGenerationError(command.lifecycle_generation, generation)
        if state not in {"active", "syncing"}:
            raise LifecycleStateRejectedError(
                f"store {command.store_key!r} is {state}; search refresh rejected"
            )


__all__ = ["LakebaseHybridIndexRefresher"]
