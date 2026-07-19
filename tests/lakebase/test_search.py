from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from retrieval.lakebase.search import (
    PostgresTextSearch,
    UnsupportedSearchBackendError,
    create_search,
)


class Cursor:
    def __init__(self, rows) -> None:
        self.rows = rows

    async def fetchall(self):
        return self.rows


class Connection:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, params: tuple[object, ...]) -> Cursor:
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
    with pytest.raises(UnsupportedSearchBackendError, match="not enabled"):
        await create_search(Provider(Connection([])), backend="lakebase_hybrid")
