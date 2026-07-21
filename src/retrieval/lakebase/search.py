"""Search adapters over generation-fenced Lakebase retrieval rows."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Protocol

from retrieval.embeddings import EmbeddingProvider, create_embedding_provider

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
    keyword_rank: int | None = None
    vector_rank: int | None = None


class RetrievalSearch(Protocol):
    backend: str

    async def search(
        self,
        store_key: str,
        query: str,
        limit: int = 8,
    ) -> tuple[SearchHit, ...]: ...

    async def readiness(self) -> dict[str, bool]: ...


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

    async def readiness(self) -> dict[str, bool]:
        return {"search": True, "embeddings": True}


_HYBRID_SEARCH_SQL = """
WITH keyword_scored AS (
    SELECT
        c.document_key,
        c.chunk_ordinal,
        c.search_vector <@> to_bm25query(
            to_tsvector('english', %s),
            'retrieval.document_chunks_search_bm25_idx'
        ) AS distance
    FROM retrieval.document_chunks AS c
    JOIN retrieval.documents AS d
      ON d.store_key = c.store_key
     AND d.document_key = c.document_key
    JOIN retrieval.stores AS s ON s.store_key = c.store_key
    WHERE s.store_key = %s
      AND s.lifecycle_state IN ('active', 'syncing')
      AND d.lifecycle_generation = s.lifecycle_generation
      AND c.lifecycle_generation = s.lifecycle_generation
),
keyword_candidates AS (
    SELECT
        document_key,
        chunk_ordinal,
        row_number() OVER (ORDER BY distance ASC, document_key, chunk_ordinal) AS candidate_rank
    FROM keyword_scored
    ORDER BY distance ASC, document_key, chunk_ordinal
    LIMIT %s
),
vector_scored AS (
    SELECT
        c.document_key,
        c.chunk_ordinal,
        c.embedding <=> %s::vector AS distance
    FROM retrieval.document_chunks AS c
    JOIN retrieval.documents AS d
      ON d.store_key = c.store_key
     AND d.document_key = c.document_key
    JOIN retrieval.stores AS s ON s.store_key = c.store_key
    WHERE s.store_key = %s
      AND s.lifecycle_state IN ('active', 'syncing')
      AND d.lifecycle_generation = s.lifecycle_generation
      AND c.lifecycle_generation = s.lifecycle_generation
      AND c.embedding IS NOT NULL
),
vector_candidates AS (
    SELECT
        document_key,
        chunk_ordinal,
        row_number() OVER (ORDER BY distance ASC, document_key, chunk_ordinal) AS candidate_rank
    FROM vector_scored
    ORDER BY distance ASC, document_key, chunk_ordinal
    LIMIT %s
),
rank_contributions AS (
    SELECT document_key, chunk_ordinal, 'keyword' AS channel, candidate_rank
    FROM keyword_candidates
    UNION ALL
    SELECT document_key, chunk_ordinal, 'vector' AS channel, candidate_rank
    FROM vector_candidates
),
fused AS (
    SELECT
        document_key,
        chunk_ordinal,
        SUM(1.0 / (60.0 + candidate_rank)) AS rrf_score,
        MIN(candidate_rank) FILTER (WHERE channel = 'keyword') AS keyword_rank,
        MIN(candidate_rank) FILTER (WHERE channel = 'vector') AS vector_rank
    FROM rank_contributions
    GROUP BY document_key, chunk_ordinal
)
SELECT
    d.document_key,
    d.title,
    d.source_uri,
    c.chunk_ordinal,
    left(c.chunk_text, 700) AS excerpt,
    fused.rrf_score AS score,
    s.lifecycle_generation AS committed_generation,
    fused.keyword_rank,
    fused.vector_rank
FROM fused
JOIN retrieval.document_chunks AS c
  ON c.store_key = %s
 AND c.document_key = fused.document_key
 AND c.chunk_ordinal = fused.chunk_ordinal
JOIN retrieval.documents AS d
  ON d.store_key = c.store_key
 AND d.document_key = c.document_key
JOIN retrieval.stores AS s ON s.store_key = c.store_key
WHERE s.lifecycle_state IN ('active', 'syncing')
  AND d.lifecycle_generation = s.lifecycle_generation
  AND c.lifecycle_generation = s.lifecycle_generation
ORDER BY score DESC, d.document_key ASC, c.chunk_ordinal ASC
LIMIT %s
"""


class LakebaseHybridSearch:
    """Lakebase BM25 + ANN search fused with reciprocal-rank fusion."""

    backend = "lakebase_hybrid"

    def __init__(
        self,
        provider: AsyncConnectionProvider,
        embedding_provider: EmbeddingProvider,
        *,
        owns_provider: bool = False,
        candidate_limit: int = 50,
    ) -> None:
        if not 1 <= candidate_limit <= 200:
            raise ValueError("candidate_limit must be between 1 and 200")
        self._provider = provider
        self._embedding_provider = embedding_provider
        self._owns_provider = owns_provider
        self._candidate_limit = candidate_limit

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
        vectors = await self._embedding_provider.embed((normalized_query,), query=True)
        if len(vectors) != 1:
            raise RuntimeError("embedding provider returned the wrong number of query vectors")
        vector = _vector_literal(vectors[0])
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                _HYBRID_SEARCH_SQL,
                (
                    normalized_query,
                    store_key,
                    self._candidate_limit,
                    vector,
                    store_key,
                    self._candidate_limit,
                    store_key,
                    limit,
                ),
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
                keyword_rank=(
                    None
                    if _row(row, "keyword_rank", 7) is None
                    else int(_row(row, "keyword_rank", 7))
                ),
                vector_rank=(
                    None
                    if _row(row, "vector_rank", 8) is None
                    else int(_row(row, "vector_rank", 8))
                ),
            )
            for row in rows
        )

    async def aclose(self) -> None:
        if self._owns_provider:
            close = getattr(self._provider, "aclose", None)
            if close is not None:
                await close()

    async def readiness(self) -> dict[str, bool]:
        embeddings_ready = False
        search_ready = False
        try:
            vectors = await self._embedding_provider.embed(
                ("retrieval demo readiness",),
                query=True,
            )
            embeddings_ready = len(vectors) == 1
        except Exception:
            embeddings_ready = False
        try:
            async with self._provider.connection() as connection:
                cursor = await connection.execute(
                    """
                    SELECT
                        to_regclass('retrieval.document_chunks_search_bm25_idx') IS NOT NULL
                            AS bm25_ready,
                        to_regclass('retrieval.document_chunks_embedding_ann_idx') IS NOT NULL
                            AS ann_ready
                    """
                )
                row = await cursor.fetchone()
            search_ready = bool(row and _row(row, "bm25_ready", 0) and _row(row, "ann_ready", 1))
        except Exception:
            search_ready = False
        return {"search": search_ready, "embeddings": embeddings_ready}


async def create_search(
    provider: AsyncConnectionProvider | None = None,
    *,
    config: LakebaseConfig | None = None,
    backend: str | None = None,
    embedding_provider: EmbeddingProvider | None = None,
) -> RetrievalSearch:
    """Create the configured backend, opening a pool only when one was not supplied."""

    selected = (backend or os.environ.get("RETRIEVAL_SEARCH_BACKEND", "postgres_text")).strip()
    if selected not in {"postgres_text", "lakebase_hybrid"}:
        raise ValueError(f"unsupported RETRIEVAL_SEARCH_BACKEND {selected!r}")

    if provider is not None:
        if selected == "lakebase_hybrid":
            return LakebaseHybridSearch(
                provider,
                embedding_provider or create_embedding_provider(),
            )
        return PostgresTextSearch(provider)
    effective_config = config or LakebaseConfig.from_env(default_pool_max_size=10)
    owned_provider = LakebaseConnectionProvider(effective_config)
    try:
        await owned_provider.open()
    except BaseException:
        await owned_provider.aclose()
        raise
    if selected == "lakebase_hybrid":
        return LakebaseHybridSearch(
            owned_provider,
            embedding_provider or create_embedding_provider(),
            owns_provider=True,
        )
    return PostgresTextSearch(owned_provider, owns_provider=True)


def _row(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        return row[name]
    return row[index]


def _vector_literal(vector: tuple[float, ...]) -> str:
    return "[" + ",".join(format(value, ".17g") for value in vector) + "]"


__all__ = [
    "LakebaseHybridSearch",
    "PostgresTextSearch",
    "RetrievalSearch",
    "SearchHit",
    "UnsupportedSearchBackendError",
    "create_search",
]
