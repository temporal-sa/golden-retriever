from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from retrieval.lakebase.search import (
    LakebaseHybridSearch,
    PostgresTextSearch,
    create_search,
)


class Cursor:
    def __init__(self, rows) -> None:
        self.rows = rows

    async def fetchall(self):
        return self.rows

    async def fetchone(self):
        return self.rows[0] if self.rows else None


class Connection:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, params: tuple[object, ...] = ()) -> Cursor:
        self.calls.append((" ".join(sql.split()), params))
        return Cursor(self.rows)


class Provider:
    def __init__(self, connection: Connection) -> None:
        self.value = connection
        self.calls = 0

    @asynccontextmanager
    async def connection(self):
        self.calls += 1
        yield self.value


class Embeddings:
    identity = "test-embedding"
    dimension = 3

    async def embed(self, texts, *, query=False):
        assert query
        assert tuple(texts) in {("renewal risk",), ("retrieval demo readiness",)}
        return ((0.1, 0.2, 0.3),)


@pytest.mark.asyncio
async def test_text_search_is_generation_and_lifecycle_fenced_with_stable_order() -> None:
    connection = Connection(
        [
            {
                "document_key": "renewal-plan",
                "title": "Renewal plan",
                "source_uri": "https://example.test/renewal",
                "chunk_ordinal": 0,
                "excerpt": "Renewal is [[HIT]]October[[/HIT]].",
                "score": 0.75,
                "committed_generation": 7,
            },
            {
                "document_key": "support-plan",
                "title": "Support plan",
                "source_uri": None,
                "chunk_ordinal": 1,
                "excerpt": "P1 target is Friday.",
                "score": 0.5,
                "committed_generation": 7,
            },
        ]
    )
    provider = Provider(connection)

    hits = await PostgresTextSearch(provider).search("northstar", "renewal OR P1", limit=8)

    assert [hit.document_key for hit in hits] == ["renewal-plan", "support-plan"]
    assert hits[0].committed_generation == 7
    assert hits[1].source_uri is None
    sql, params = connection.calls[0]
    assert "websearch_to_tsquery" in sql
    assert "s.lifecycle_state IN ('active', 'syncing')" in sql
    assert "d.lifecycle_generation = s.lifecycle_generation" in sql
    assert "c.lifecycle_generation = s.lifecycle_generation" in sql
    assert "ORDER BY score DESC, d.document_key ASC, c.chunk_ordinal ASC" in sql
    assert params == ("renewal OR P1", "northstar", 8)


@pytest.mark.asyncio
async def test_empty_query_returns_without_acquiring_database_connection() -> None:
    provider = Provider(Connection([]))

    assert await PostgresTextSearch(provider).search("northstar", "   ") == ()
    assert provider.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, 51])
async def test_limit_is_bounded(limit: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 50"):
        await PostgresTextSearch(Provider(Connection([]))).search("northstar", "renewal", limit)


@pytest.mark.asyncio
async def test_default_factory_selects_native_text_explicitly() -> None:
    provider = Provider(Connection([]))
    search = await create_search(provider, backend="postgres_text")

    assert search.backend == "postgres_text"


@pytest.mark.asyncio
async def test_hybrid_backend_is_not_silently_labelled_or_fallbacked() -> None:
    search = await create_search(
        Provider(Connection([])),
        backend="lakebase_hybrid",
        embedding_provider=Embeddings(),
    )

    assert isinstance(search, LakebaseHybridSearch)
    assert search.backend == "lakebase_hybrid"


@pytest.mark.asyncio
async def test_hybrid_search_fuses_bm25_and_ann_after_generation_filtering() -> None:
    connection = Connection(
        [
            {
                "document_key": "plan",
                "title": "Plan",
                "source_uri": "https://drive.test/plan",
                "chunk_ordinal": 2,
                "excerpt": "Renewal risk is concentrated in onboarding.",
                "score": 0.031,
                "committed_generation": 8,
                "keyword_rank": 1,
                "vector_rank": 3,
            }
        ]
    )

    hits = await LakebaseHybridSearch(
        Provider(connection),
        Embeddings(),
        candidate_limit=25,
    ).search("store", "renewal risk", limit=4)

    assert hits[0].keyword_rank == 1
    assert hits[0].vector_rank == 3
    sql, params = connection.calls[0]
    assert "lakebase_bm25" not in sql
    assert "to_bm25query" in sql
    assert "c.embedding <=> %s::vector" in sql
    assert sql.count("d.lifecycle_generation = s.lifecycle_generation") >= 3
    assert sql.count("c.lifecycle_generation = s.lifecycle_generation") >= 3
    assert params == (
        "renewal risk",
        "store",
        25,
        "[0.10000000000000001,0.20000000000000001,0.29999999999999999]",
        "store",
        25,
        "store",
        4,
    )


@pytest.mark.asyncio
async def test_hybrid_readiness_probes_embedding_dimension_and_both_indexes() -> None:
    connection = Connection([{"bm25_ready": True, "ann_ready": True}])
    search = LakebaseHybridSearch(Provider(connection), Embeddings())

    assert await search.readiness() == {"search": True, "embeddings": True}
    assert "document_chunks_search_bm25_idx" in connection.calls[0][0]
    assert "document_chunks_embedding_ann_idx" in connection.calls[0][0]
