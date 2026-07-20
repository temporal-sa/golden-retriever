from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from retrieval.lakebase.repository import LakebaseRetrievalRepository
from retrieval.temporal.activities.repositories import (
    CleanupIncompleteError,
    IdempotencyConflictError,
    SearchableDocument,
    SearchChunk,
    StaleLifecycleGenerationError,
    document_payload_hash,
)
from retrieval.temporal.models.documents import DocumentRef
from retrieval.temporal.models.lifecycle import StoreLifecycleState


@dataclass
class Expected:
    fragment: str
    rows: tuple[object, ...] = ()
    error: Exception | None = None


class Cursor:
    def __init__(self, rows: tuple[object, ...]) -> None:
        self.rows = rows

    async def fetchone(self):
        return self.rows[0] if self.rows else None

    async def fetchall(self):
        return list(self.rows)


class Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None


class ScriptedConnection:
    def __init__(self, expected: list[Expected]) -> None:
        self.expected = list(expected)
        self.calls: list[tuple[str, object]] = []

    def transaction(self) -> Transaction:
        return Transaction()

    async def execute(self, sql: str, params=None, **_) -> Cursor:
        normalized = " ".join(sql.split())
        self.calls.append((normalized, params))
        assert self.expected, f"unexpected SQL: {normalized}"
        expected = self.expected.pop(0)
        assert expected.fragment in normalized, (
            f"expected SQL containing {expected.fragment!r}, got {normalized!r}"
        )
        if expected.error is not None:
            raise expected.error
        return Cursor(expected.rows)

    def assert_complete(self) -> None:
        assert not self.expected, f"unexecuted SQL expectations: {self.expected}"


class Provider:
    def __init__(self, *connections: ScriptedConnection) -> None:
        self.connections = list(connections)
        self.calls = 0

    @asynccontextmanager
    async def connection(self):
        index = min(self.calls, len(self.connections) - 1)
        self.calls += 1
        yield self.connections[index]


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def document() -> SearchableDocument:
    return SearchableDocument(
        reference=DocumentRef(
            document_key="renewal-plan",
            source_version="v3",
            staging_uri="fixture://renewal-plan.md",
            content_hash=digest("complete staged object"),
        ),
        title="Northstar renewal plan",
        source_uri="https://example.test/renewal",
        body_hash=digest("parsed body"),
        chunks=(
            SearchChunk(ordinal=0, text="Renewal is in October.", content_hash=digest("c0")),
            SearchChunk(ordinal=1, text="The champion is Priya.", content_hash=digest("c1")),
        ),
    )


def repeated_chunk_document() -> SearchableDocument:
    item = document()
    repeated_hash = digest("repeated")
    return SearchableDocument(
        reference=item.reference,
        title=item.title,
        source_uri=item.source_uri,
        body_hash=item.body_hash,
        chunks=(
            SearchChunk(ordinal=0, text="Repeated section", content_hash=repeated_hash),
            SearchChunk(ordinal=1, text="Repeated section", content_hash=repeated_hash),
        ),
    )


def writable_store(generation: int = 7) -> dict[str, object]:
    return {
        "lifecycle_state": "active",
        "lifecycle_generation": generation,
        "last_lifecycle_transition": datetime(2026, 7, 18, tzinfo=UTC),
    }


@pytest.mark.asyncio
async def test_commit_locks_generation_before_receipt_and_writes_atomically() -> None:
    connection = ScriptedConnection(
        [
            Expected("FROM retrieval.stores", (writable_store(),)),
            Expected(
                "INSERT INTO retrieval.write_receipts",
                ({"idempotency_key": "commit-1"},),
            ),
            Expected("INSERT INTO retrieval.documents"),
            Expected("DELETE FROM retrieval.document_chunks"),
            Expected("INSERT INTO retrieval.document_chunks"),
        ]
    )
    repository = LakebaseRetrievalRepository(Provider(connection))

    outcome = await repository.commit_document_if_current("northstar", 7, document(), "commit-1")

    assert not outcome.duplicate
    assert outcome.chunks_written == 2
    assert "FOR SHARE" in connection.calls[0][0]
    assert "write_receipts" in connection.calls[1][0]
    assert connection.calls[0][0].find("FROM retrieval.stores") >= 0
    chunk_params = connection.calls[-1][1]
    assert chunk_params[-5] == [0, 1]
    assert chunk_params[-2] == [None, None]
    assert chunk_params[-1] == [None, None]
    connection.assert_complete()


@pytest.mark.asyncio
async def test_commit_accepts_repeated_chunk_hashes_at_distinct_ordinals() -> None:
    connection = ScriptedConnection(
        [
            Expected("FROM retrieval.stores", (writable_store(),)),
            Expected(
                "INSERT INTO retrieval.write_receipts",
                ({"idempotency_key": "commit-repeated"},),
            ),
            Expected("INSERT INTO retrieval.documents"),
            Expected("DELETE FROM retrieval.document_chunks"),
            Expected("INSERT INTO retrieval.document_chunks"),
        ]
    )

    outcome = await LakebaseRetrievalRepository(Provider(connection)).commit_document_if_current(
        "northstar",
        7,
        repeated_chunk_document(),
        "commit-repeated",
    )

    assert outcome.chunks_written == 2
    chunk_params = connection.calls[-1][1]
    assert chunk_params[-1][0] == chunk_params[-1][1]
    connection.assert_complete()


@pytest.mark.asyncio
async def test_exact_duplicate_returns_stored_outcome_only_after_current_fence() -> None:
    item = document()
    receipt_hash = document_payload_hash(item, expected_generation=7)
    connection = ScriptedConnection(
        [
            Expected("FROM retrieval.stores", (writable_store(),)),
            Expected("INSERT INTO retrieval.write_receipts"),
            Expected(
                "SELECT operation_type",
                (
                    {
                        "operation_type": "upsert_document",
                        "document_key": "renewal-plan",
                        "lifecycle_generation": 7,
                        "payload_hash": receipt_hash,
                        "result": {"chunks_written": 2},
                    },
                ),
            ),
        ]
    )

    outcome = await LakebaseRetrievalRepository(Provider(connection)).commit_document_if_current(
        "northstar", 7, item, "commit-1"
    )

    assert outcome.duplicate
    assert outcome.chunks_written == 2
    connection.assert_complete()


@pytest.mark.asyncio
async def test_stale_redelivery_is_rejected_before_historical_receipt_lookup() -> None:
    connection = ScriptedConnection(
        [Expected("FROM retrieval.stores", (writable_store(generation=8),))]
    )

    with pytest.raises(StaleLifecycleGenerationError) as raised:
        await LakebaseRetrievalRepository(Provider(connection)).commit_document_if_current(
            "northstar", 7, document(), "commit-1"
        )

    assert raised.value.actual_generation == 8
    assert len(connection.calls) == 1
    assert "write_receipts" not in connection.calls[0][0]
    connection.assert_complete()


@pytest.mark.asyncio
async def test_receipt_key_reuse_with_different_payload_fails() -> None:
    connection = ScriptedConnection(
        [
            Expected("FROM retrieval.stores", (writable_store(),)),
            Expected("INSERT INTO retrieval.write_receipts"),
            Expected(
                "SELECT operation_type",
                (
                    {
                        "operation_type": "delete_document",
                        "document_key": "another-document",
                        "lifecycle_generation": 7,
                        "payload_hash": "0" * 64,
                        "result": {"chunks_written": 0},
                    },
                ),
            ),
        ]
    )

    with pytest.raises(IdempotencyConflictError, match="different payload"):
        await LakebaseRetrievalRepository(Provider(connection)).commit_document_if_current(
            "northstar", 7, document(), "reused-key"
        )
    connection.assert_complete()


@pytest.mark.asyncio
async def test_bounded_cleanup_counts_cascaded_chunks_and_clears_state_at_end() -> None:
    connection = ScriptedConnection(
        [
            Expected(
                "FROM retrieval.stores",
                (
                    {
                        "lifecycle_state": "deactivating",
                        "lifecycle_generation": 8,
                        "last_lifecycle_transition": datetime.now(UTC),
                    },
                ),
            ),
            Expected(
                "SELECT document_key",
                ({"document_key": "a"}, {"document_key": "b"}),
            ),
            Expected("SELECT count(*) AS chunk_count", ({"chunk_count": 5},)),
            Expected("DELETE FROM retrieval.documents"),
            Expected("SELECT EXISTS", ({"remaining": False},)),
            Expected("DELETE FROM retrieval.retrieval_state"),
        ]
    )

    outcome = await LakebaseRetrievalRepository(
        Provider(connection)
    ).remove_object_batch_if_current("northstar", 8, 2)

    assert outcome.deleted_documents == 2
    assert outcome.deleted_chunks == 5
    assert not outcome.remaining
    assert connection.calls[1][1] == ("northstar", 2)
    connection.assert_complete()


@pytest.mark.asyncio
async def test_begin_deactivation_replay_does_not_increment_again() -> None:
    transitioned_at = datetime(2026, 7, 18, 12, tzinfo=UTC)
    connection = ScriptedConnection(
        [
            Expected("UPDATE retrieval.stores"),
            Expected(
                "FROM retrieval.stores",
                (
                    {
                        "lifecycle_state": "deactivating",
                        "lifecycle_generation": 8,
                        "last_lifecycle_transition": transitioned_at,
                    },
                ),
            ),
        ]
    )

    generation = await LakebaseRetrievalRepository(Provider(connection)).begin_deactivation(
        "northstar", 7
    )

    assert generation.previous_generation == 7
    assert generation.lifecycle_generation == 8
    assert generation.transitioned_at == transitioned_at
    assert sum("UPDATE retrieval.stores" in sql for sql, _ in connection.calls) == 1
    connection.assert_complete()


@pytest.mark.asyncio
async def test_mark_inactive_fails_when_any_cleanup_rows_remain() -> None:
    connection = ScriptedConnection(
        [
            Expected(
                "FROM retrieval.stores",
                (
                    {
                        "lifecycle_state": "deactivating",
                        "lifecycle_generation": 8,
                        "last_lifecycle_transition": datetime.now(UTC),
                    },
                ),
            ),
            Expected(
                "active_users_remain",
                (
                    {
                        "active_users_remain": False,
                        "documents_remain": True,
                        "chunks_remain": True,
                        "retrieval_state_remains": False,
                    },
                ),
            ),
        ]
    )

    with pytest.raises(CleanupIncompleteError):
        await LakebaseRetrievalRepository(Provider(connection)).mark_inactive("northstar", 8)
    connection.assert_complete()


@pytest.mark.asyncio
async def test_snapshot_read_is_lightweight_counts_only() -> None:
    transitioned_at = datetime.now(UTC)
    connection = ScriptedConnection(
        [
            Expected(
                "SELECT s.store_key",
                (
                    {
                        "store_key": "northstar",
                        "display_name": "Northstar",
                        "lifecycle_state": "syncing",
                        "lifecycle_generation": 7,
                        "last_lifecycle_transition": transitioned_at,
                        "active_user_count": 3,
                        "document_count": 5,
                        "chunk_count": 7,
                    },
                ),
            )
        ]
    )

    snapshot = await LakebaseRetrievalRepository(Provider(connection)).get_store("northstar")

    assert snapshot.lifecycle_state is StoreLifecycleState.SYNCING
    assert snapshot.document_count == 5
    assert snapshot.chunk_count == 7
    connection.assert_complete()


class SerializationFailure(RuntimeError):
    sqlstate = "40001"


@pytest.mark.asyncio
async def test_only_recognized_receipted_transaction_failures_are_retried() -> None:
    first = ScriptedConnection(
        [Expected("FROM retrieval.stores", error=SerializationFailure("retry"))]
    )
    second = ScriptedConnection(
        [
            Expected("FROM retrieval.stores", (writable_store(),)),
            Expected(
                "INSERT INTO retrieval.write_receipts",
                ({"idempotency_key": "commit-retry"},),
            ),
            Expected("INSERT INTO retrieval.documents"),
            Expected("DELETE FROM retrieval.document_chunks"),
            Expected("INSERT INTO retrieval.document_chunks"),
        ]
    )
    provider = Provider(first, second)

    outcome = await LakebaseRetrievalRepository(
        provider, transaction_retry_limit=1
    ).commit_document_if_current("northstar", 7, document(), "commit-retry")

    assert outcome.chunks_written == 2
    assert provider.calls == 2
    first.assert_complete()
    second.assert_complete()
