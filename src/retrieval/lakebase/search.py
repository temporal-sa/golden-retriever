"""Search adapters over generation-fenced Lakebase retrieval rows."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Protocol

from .config import LakebaseConfig
from .connection import LakebaseConnectionProvider
from .repository import AsyncConnectionProvider


class UnsupportedSearchBackendError(RuntimeError):
    """A requested optional backend was not explicitly provisioned."""


@dataclass(frozen=True, slots=True)
class SearchHit:
    document_key: str
    title: str
    source_uri: str | None
    chunk_ordinal: int
    excerpt: str
    score: float
    committed_generation: int


class RetrievalSearch(Protocol):
    backend: str

    async def search(
        self,
        store_key: str,
        query: str,
        limit: int = 8,
    ) -> tuple[SearchHit, ...]: ...


_SEARCH_SQL = """
WITH search_query AS (
    SELECT websearch_to_tsquery('english', %s) AS value
)
SELECT
    d.document_key,
    d.title,
    d.source_uri,
    c.chunk_ordinal,
    ts_headline(
        'english',
        c.chunk_text,
        search_query.value,
        'StartSel=[[HIT]], StopSel=[[/HIT]], MaxWords=36, MinWords=12, MaxFragments=2'
    ) AS excerpt,
    ts_rank_cd(c.search_vector, search_query.value) AS score,
    s.lifecycle_generation AS committed_generation
FROM retrieval.document_chunks AS c
JOIN retrieval.documents AS d
  ON d.store_key = c.store_key
 AND d.document_key = c.document_key
JOIN retrieval.stores AS s
  ON s.store_key = c.store_key
CROSS JOIN search_query
WHERE s.store_key = %s
  AND s.lifecycle_state IN ('active', 'syncing')
  AND d.lifecycle_generation = s.lifecycle_generation
  AND c.lifecycle_generation = s.lifecycle_generation
  AND c.search_vector @@ search_query.value
ORDER BY score DESC, d.document_key ASC, c.chunk_ordinal ASC
LIMIT %s
"""


class PostgresTextSearch:
    """Always-available native Postgres full-text search backend."""

    backend = "postgres_text"

    def __init__(
        self,
        provider: AsyncConnectionProvider,
        *,
        owns_provider: bool = False,
    ) -> None:
        self._provider = provider
        self._owns_provider = owns_provider

    async def search(
        self,
        store_key: str,
        query: str,
        limit: int = 8,
    ) -> tuple[SearchHit, ...]:
        if not store_key or not store_key.strip():
            raise ValueError("store_key must not be empty")
        normalized_query = query.strip()
        if not normalized_query:
            return ()
        if len(normalized_query) > 1_000:
            raise ValueError("search query must not exceed 1000 characters")
        if not 1 <= limit <= 50:
            raise ValueError("search limit must be between 1 and 50")

        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                _SEARCH_SQL,
                (normalized_query, store_key, limit),
            )
            rows = await cursor.fetchall()
        return tuple(
            SearchHit(
                document_key=str(_row(row, "document_key", 0)),
                title=str(_row(row, "title", 1)),
                source_uri=(
                    None if _row(row, "source_uri", 2) is None else str(_row(row, "source_uri", 2))
                ),
                chunk_ordinal=int(_row(row, "chunk_ordinal", 3)),
                excerpt=str(_row(row, "excerpt", 4)),
                score=float(_row(row, "score", 5)),
                committed_generation=int(_row(row, "committed_generation", 6)),
            )
            for row in rows
        )

    async def aclose(self) -> None:
        if self._owns_provider:
            close = getattr(self._provider, "aclose", None)
            if close is not None:
                await close()


class LakebaseHybridSearch:
    """Reserved explicit backend; never masquerades as native text search."""

    backend = "lakebase_hybrid"

    async def search(
        self,
        store_key: str,
        query: str,
        limit: int = 8,
    ) -> tuple[SearchHit, ...]:
        raise UnsupportedSearchBackendError(
            "lakebase_hybrid requires a separately provisioned index migration and adapter"
        )


async def create_search(
    provider: AsyncConnectionProvider | None = None,
    *,
    config: LakebaseConfig | None = None,
    backend: str | None = None,
) -> RetrievalSearch:
    """Create the configured backend, opening a pool only when one was not supplied."""

    selected = (backend or os.environ.get("RETRIEVAL_SEARCH_BACKEND", "postgres_text")).strip()
    if selected == "lakebase_hybrid":
        raise UnsupportedSearchBackendError(
            "lakebase_hybrid is not enabled by the core deployment; use postgres_text"
        )
    if selected != "postgres_text":
        raise ValueError(f"unsupported RETRIEVAL_SEARCH_BACKEND {selected!r}")

    if provider is not None:
        return PostgresTextSearch(provider)
    effective_config = config or LakebaseConfig.from_env(default_pool_max_size=10)
    owned_provider = LakebaseConnectionProvider(effective_config)
    try:
        await owned_provider.open()
    except BaseException:
        await owned_provider.aclose()
        raise
    return PostgresTextSearch(owned_provider, owns_provider=True)


def _row(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        return row[name]
    return row[index]


__all__ = [
    "LakebaseHybridSearch",
    "PostgresTextSearch",
    "RetrievalSearch",
    "SearchHit",
    "UnsupportedSearchBackendError",
    "create_search",
]
