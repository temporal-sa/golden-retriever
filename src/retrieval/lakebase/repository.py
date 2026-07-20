"""Lakebase/Postgres implementation of the retrieval persistence contract."""

from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, TypeVar

from retrieval.temporal.activities.repositories import (
    CleanupBatchOutcome,
    CleanupIncompleteError,
    CommitOutcome,
    IdempotencyConflictError,
    LifecycleStateRejectedError,
    SearchableDocument,
    StaleLifecycleGenerationError,
    StoreSnapshot,
    delete_payload_hash,
    document_payload_hash,
)
from retrieval.temporal.models.lifecycle import NewGeneration, StoreLifecycleState

from .config import LakebaseConfig
from .connection import LakebaseConnectionProvider


class AsyncConnectionProvider(Protocol):
    def connection(self) -> Any: ...


_T = TypeVar("_T")
_RETRYABLE_TRANSACTION_STATES = frozenset({"40001", "40P01"})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_WRITABLE_STATES = frozenset({StoreLifecycleState.ACTIVE, StoreLifecycleState.SYNCING})
_DEACTIVATION_REPLAY_STATES = frozenset(
    {
        StoreLifecycleState.DEACTIVATING,
        StoreLifecycleState.INACTIVE,
        StoreLifecycleState.DEACTIVATION_FAILED,
    }
)

_SNAPSHOT_SQL = """
SELECT
    s.store_key,
    s.display_name,
    s.lifecycle_state,
    s.lifecycle_generation,
    s.last_lifecycle_transition,
    (
        SELECT count(*)
        FROM retrieval.store_users AS u
        WHERE u.store_key = s.store_key AND u.active
    ) AS active_user_count,
    (
        SELECT count(*)
        FROM retrieval.documents AS d
        WHERE d.store_key = s.store_key
    ) AS document_count,
    (
        SELECT count(*)
        FROM retrieval.document_chunks AS c
        WHERE c.store_key = s.store_key
    ) AS chunk_count
FROM retrieval.stores AS s
WHERE s.store_key = %s
"""


class LakebaseRetrievalRepository:
    """Generation-fenced repository backed by parameterized Postgres statements."""

    def __init__(
        self,
        provider: AsyncConnectionProvider,
        *,
        transaction_retry_limit: int = 3,
        owns_provider: bool = False,
    ) -> None:
        if transaction_retry_limit < 0:
            raise ValueError("transaction_retry_limit must be non-negative")
        self._provider = provider
        self._transaction_retry_limit = transaction_retry_limit
        self._owns_provider = owns_provider

    async def create_store(
        self,
        store_key: str,
        display_name: str,
        *,
        generation: int = 0,
        state: StoreLifecycleState = StoreLifecycleState.ACTIVE,
    ) -> StoreSnapshot:
        _require_nonempty(store_key, "store_key")
        _require_nonempty(display_name, "display_name")
        _require_generation_value(generation)
        async with self._provider.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    INSERT INTO retrieval.stores (
                        store_key,
                        display_name,
                        lifecycle_state,
                        lifecycle_generation
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (store_key) DO NOTHING
                    RETURNING store_key
                    """,
                    (store_key, display_name, state.value, generation),
                )
                inserted = await cursor.fetchone()
                if inserted is None:
                    existing_cursor = await connection.execute(
                        """
                        SELECT display_name, lifecycle_state, lifecycle_generation
                        FROM retrieval.stores
                        WHERE store_key = %s
                        FOR SHARE
                        """,
                        (store_key,),
                    )
                    existing = await existing_cursor.fetchone()
                    if existing is None:
                        raise RuntimeError("store conflict disappeared during create")
                    if (
                        str(_row(existing, "display_name", 0)) != display_name
                        or StoreLifecycleState(str(_row(existing, "lifecycle_state", 1)))
                        is not state
                        or int(_row(existing, "lifecycle_generation", 2)) != generation
                    ):
                        raise IdempotencyConflictError(
                            f"store {store_key!r} already exists with different attributes"
                        )
                return await self._fetch_snapshot(connection, store_key)

    async def get_store(self, store_key: str) -> StoreSnapshot:
        async with self._provider.connection() as connection:
            return await self._fetch_snapshot(connection, store_key)

    async def begin_deactivation(self, store_key: str, expected_generation: int) -> NewGeneration:
        _require_generation_value(expected_generation)
        async with self._provider.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    UPDATE retrieval.stores
                    SET lifecycle_generation = lifecycle_generation + 1,
                        lifecycle_state = 'deactivating',
                        last_lifecycle_transition = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE store_key = %s
                      AND lifecycle_generation = %s
                      AND lifecycle_state IN ('active', 'syncing')
                    RETURNING lifecycle_generation, last_lifecycle_transition
                    """,
                    (store_key, expected_generation),
                )
                changed = await cursor.fetchone()
                if changed is not None:
                    return NewGeneration(
                        store_key=store_key,
                        previous_generation=expected_generation,
                        lifecycle_generation=int(_row(changed, "lifecycle_generation", 0)),
                        transitioned_at=_row(changed, "last_lifecycle_transition", 1),
                    )

                current = await self._lock_store(connection, store_key, mode="SHARE")
                actual_generation = int(_row(current, "lifecycle_generation", 1))
                current_state = StoreLifecycleState(str(_row(current, "lifecycle_state", 0)))
                if (
                    actual_generation == expected_generation + 1
                    and current_state in _DEACTIVATION_REPLAY_STATES
                ):
                    return NewGeneration(
                        store_key=store_key,
                        previous_generation=expected_generation,
                        lifecycle_generation=actual_generation,
                        transitioned_at=_row(current, "last_lifecycle_transition", 2),
                    )
                if actual_generation != expected_generation:
                    raise StaleLifecycleGenerationError(expected_generation, actual_generation)
                raise LifecycleStateRejectedError(f"store {store_key!r} is {current_state.value}")

    async def resume_deactivation(self, store_key: str, lifecycle_generation: int) -> NewGeneration:
        _require_generation_value(lifecycle_generation)
        if lifecycle_generation == 0:
            raise LifecycleStateRejectedError("a deactivation generation must be positive")
        async with self._provider.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(
                    """
                    UPDATE retrieval.stores
                    SET lifecycle_state = 'deactivating',
                        last_lifecycle_transition = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE store_key = %s
                      AND lifecycle_generation = %s
                      AND lifecycle_state = 'deactivation_failed'
                    RETURNING lifecycle_generation, last_lifecycle_transition
                    """,
                    (store_key, lifecycle_generation),
                )
                changed = await cursor.fetchone()
                if changed is not None:
                    return NewGeneration(
                        store_key=store_key,
                        previous_generation=lifecycle_generation - 1,
                        lifecycle_generation=int(_row(changed, "lifecycle_generation", 0)),
                        transitioned_at=_row(changed, "last_lifecycle_transition", 1),
                    )
                current = await self._lock_store(connection, store_key, mode="SHARE")
                actual = int(_row(current, "lifecycle_generation", 1))
                if actual != lifecycle_generation:
                    raise StaleLifecycleGenerationError(lifecycle_generation, actual)
                state = StoreLifecycleState(str(_row(current, "lifecycle_state", 0)))
                if state is not StoreLifecycleState.DEACTIVATING:
                    raise LifecycleStateRejectedError(
                        "only a failed or already-resuming deactivation can be resumed"
                    )
                return NewGeneration(
                    store_key=store_key,
                    previous_generation=lifecycle_generation - 1,
                    lifecycle_generation=lifecycle_generation,
                    transitioned_at=_row(current, "last_lifecycle_transition", 2),
                )

    async def mark_inactive(self, store_key: str, expected_generation: int) -> StoreSnapshot:
        _require_generation_value(expected_generation)
        async with self._provider.connection() as connection:
            async with connection.transaction():
                current = await self._lock_store(connection, store_key, mode="UPDATE")
                self._require_generation(current, expected_generation)
                state = StoreLifecycleState(str(_row(current, "lifecycle_state", 0)))
                if state is StoreLifecycleState.INACTIVE:
                    return await self._fetch_snapshot(connection, store_key)
                if state is not StoreLifecycleState.DEACTIVATING:
                    raise LifecycleStateRejectedError("store must be deactivating before inactive")
                counts_cursor = await connection.execute(
                    """
                    SELECT
                        EXISTS (
                            SELECT 1 FROM retrieval.store_users
                            WHERE store_key = %s AND active
                        ) AS active_users_remain,
                        EXISTS (
                            SELECT 1 FROM retrieval.documents WHERE store_key = %s
                        ) AS documents_remain,
                        EXISTS (
                            SELECT 1 FROM retrieval.document_chunks WHERE store_key = %s
                        ) AS chunks_remain,
                        EXISTS (
                            SELECT 1 FROM retrieval.retrieval_state WHERE store_key = %s
                        ) AS retrieval_state_remains
                    """,
                    (store_key, store_key, store_key, store_key),
                )
                counts = await counts_cursor.fetchone()
                assert counts is not None
                if any(
                    bool(_row(counts, name, index))
                    for index, name in enumerate(
                        (
                            "active_users_remain",
                            "documents_remain",
                            "chunks_remain",
                            "retrieval_state_remains",
                        )
                    )
                ):
                    raise CleanupIncompleteError("store cleanup is incomplete")
                await connection.execute(
                    """
                    UPDATE retrieval.stores
                    SET lifecycle_state = 'inactive',
                        last_lifecycle_transition = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE store_key = %s AND lifecycle_generation = %s
                    """,
                    (store_key, expected_generation),
                )
                return await self._fetch_snapshot(connection, store_key)

    async def mark_deactivation_failed(
        self, store_key: str, expected_generation: int
    ) -> StoreSnapshot:
        _require_generation_value(expected_generation)
        async with self._provider.connection() as connection:
            async with connection.transaction():
                current = await self._lock_store(connection, store_key, mode="UPDATE")
                self._require_generation(current, expected_generation)
                state = StoreLifecycleState(str(_row(current, "lifecycle_state", 0)))
                if state is StoreLifecycleState.DEACTIVATION_FAILED:
                    return await self._fetch_snapshot(connection, store_key)
                if state is not StoreLifecycleState.DEACTIVATING:
                    raise LifecycleStateRejectedError(
                        "store must be deactivating before recording a failure"
                    )
                await connection.execute(
                    """
                    UPDATE retrieval.stores
                    SET lifecycle_state = 'deactivation_failed',
                        last_lifecycle_transition = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE store_key = %s AND lifecycle_generation = %s
                    """,
                    (store_key, expected_generation),
                )
                return await self._fetch_snapshot(connection, store_key)

    async def activate_user_if_current(
        self, store_key: str, expected_generation: int, user_key: str
    ) -> None:
        _require_nonempty(user_key, "user_key")
        async with self._provider.connection() as connection:
            async with connection.transaction():
                await self._require_writable(connection, store_key, expected_generation)
                await connection.execute(
                    """
                    INSERT INTO retrieval.store_users (
                        store_key, user_key, active, lifecycle_generation, updated_at
                    )
                    VALUES (%s, %s, true, %s, clock_timestamp())
                    ON CONFLICT (store_key, user_key) DO UPDATE
                    SET active = true,
                        lifecycle_generation = EXCLUDED.lifecycle_generation,
                        updated_at = clock_timestamp()
                    """,
                    (store_key, user_key, expected_generation),
                )

    async def mutate_retrieval_state_if_current(
        self,
        store_key: str,
        expected_generation: int,
        key: str,
        value: str,
    ) -> None:
        _require_nonempty(key, "key")
        async with self._provider.connection() as connection:
            async with connection.transaction():
                await self._require_writable(connection, store_key, expected_generation)
                await connection.execute(
                    """
                    INSERT INTO retrieval.retrieval_state (
                        store_key,
                        state_key,
                        state_value,
                        lifecycle_generation,
                        updated_at
                    )
                    VALUES (%s, %s, CAST(%s AS jsonb), %s, clock_timestamp())
                    ON CONFLICT (store_key, state_key) DO UPDATE
                    SET state_value = EXCLUDED.state_value,
                        lifecycle_generation = EXCLUDED.lifecycle_generation,
                        updated_at = clock_timestamp()
                    """,
                    (
                        store_key,
                        key,
                        json.dumps(value, ensure_ascii=False),
                        expected_generation,
                    ),
                )

    async def commit_document_if_current(
        self,
        store_key: str,
        expected_generation: int,
        document: SearchableDocument,
        idempotency_key: str,
    ) -> CommitOutcome:
        _validate_document(document)
        _require_nonempty(idempotency_key, "idempotency_key")
        payload_hash = document_payload_hash(
            document,
            expected_generation=expected_generation,
        )

        async def transaction_body() -> CommitOutcome:
            async with self._provider.connection() as connection:
                async with connection.transaction():
                    # The generation/state lock intentionally precedes receipt
                    # lookup. A late redelivery is stale even when a historical
                    # receipt with the same key still exists after cleanup.
                    await self._require_writable(connection, store_key, expected_generation)
                    outcome = CommitOutcome(chunks_written=len(document.chunks))
                    duplicate = await self._insert_or_match_receipt(
                        connection,
                        store_key=store_key,
                        idempotency_key=idempotency_key,
                        operation_type="upsert_document",
                        document_key=document.reference.document_key,
                        lifecycle_generation=expected_generation,
                        payload_hash=payload_hash,
                        outcome=outcome,
                    )
                    if duplicate is not None:
                        return duplicate

                    await connection.execute(
                        """
                        INSERT INTO retrieval.documents (
                            store_key,
                            document_key,
                            source_version,
                            staging_uri,
                            content_hash,
                            title,
                            source_uri,
                            body_hash,
                            lifecycle_generation,
                            ingested_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, clock_timestamp())
                        ON CONFLICT (store_key, document_key) DO UPDATE
                        SET source_version = EXCLUDED.source_version,
                            staging_uri = EXCLUDED.staging_uri,
                            content_hash = EXCLUDED.content_hash,
                            title = EXCLUDED.title,
                            source_uri = EXCLUDED.source_uri,
                            body_hash = EXCLUDED.body_hash,
                            lifecycle_generation = EXCLUDED.lifecycle_generation,
                            ingested_at = clock_timestamp()
                        """,
                        (
                            store_key,
                            document.reference.document_key,
                            document.reference.source_version,
                            document.reference.staging_uri,
                            document.reference.content_hash,
                            document.title,
                            document.source_uri,
                            document.body_hash,
                            expected_generation,
                        ),
                    )
                    await connection.execute(
                        """
                        DELETE FROM retrieval.document_chunks
                        WHERE store_key = %s AND document_key = %s
                        """,
                        (store_key, document.reference.document_key),
                    )
                    await connection.execute(
                        """
                        INSERT INTO retrieval.document_chunks (
                            store_key,
                            document_key,
                            chunk_ordinal,
                            chunk_text,
                            chunk_hash,
                            embedding,
                            embedding_model,
                            lifecycle_generation
                        )
                        SELECT %s, %s, chunks.ordinal, chunks.chunk_text, chunks.chunk_hash,
                               chunks.embedding_text::vector, chunks.embedding_model, %s
                        FROM unnest(
                            %s::integer[], %s::text[], %s::text[], %s::text[], %s::text[]
                        ) AS chunks(
                            ordinal, chunk_text, chunk_hash, embedding_text, embedding_model
                        )
                        """,
                        (
                            store_key,
                            document.reference.document_key,
                            expected_generation,
                            [chunk.ordinal for chunk in document.chunks],
                            [chunk.text for chunk in document.chunks],
                            [chunk.content_hash for chunk in document.chunks],
                            [_vector_literal(chunk.embedding) for chunk in document.chunks],
                            [chunk.embedding_model for chunk in document.chunks],
                        ),
                    )
                    return outcome

        return await self._retry_receipted_transaction(transaction_body)

    async def delete_document_if_current(
        self,
        store_key: str,
        expected_generation: int,
        document_key: str,
        idempotency_key: str,
    ) -> CommitOutcome:
        _require_nonempty(document_key, "document_key")
        _require_nonempty(idempotency_key, "idempotency_key")
        payload_hash = delete_payload_hash(
            document_key,
            expected_generation=expected_generation,
        )

        async def transaction_body() -> CommitOutcome:
            async with self._provider.connection() as connection:
                async with connection.transaction():
                    await self._require_writable(connection, store_key, expected_generation)
                    outcome = CommitOutcome(chunks_written=0)
                    duplicate = await self._insert_or_match_receipt(
                        connection,
                        store_key=store_key,
                        idempotency_key=idempotency_key,
                        operation_type="delete_document",
                        document_key=document_key,
                        lifecycle_generation=expected_generation,
                        payload_hash=payload_hash,
                        outcome=outcome,
                    )
                    if duplicate is not None:
                        return duplicate
                    await connection.execute(
                        """
                        DELETE FROM retrieval.documents
                        WHERE store_key = %s AND document_key = %s
                        """,
                        (store_key, document_key),
                    )
                    return outcome

        return await self._retry_receipted_transaction(transaction_body)

    async def remove_object_batch_if_current(
        self,
        store_key: str,
        expected_generation: int,
        batch_size: int,
    ) -> CleanupBatchOutcome:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        async with self._provider.connection() as connection:
            async with connection.transaction():
                await self._require_cleanup(connection, store_key, expected_generation)
                keys_cursor = await connection.execute(
                    """
                    SELECT document_key
                    FROM retrieval.documents
                    WHERE store_key = %s
                    ORDER BY document_key
                    LIMIT %s
                    FOR UPDATE
                    """,
                    (store_key, batch_size),
                )
                document_keys = [
                    str(_row(row, "document_key", 0)) for row in await keys_cursor.fetchall()
                ]
                deleted_chunks = 0
                if document_keys:
                    chunks_cursor = await connection.execute(
                        """
                        SELECT count(*) AS chunk_count
                        FROM retrieval.document_chunks
                        WHERE store_key = %s AND document_key = ANY(%s)
                        """,
                        (store_key, document_keys),
                    )
                    chunks = await chunks_cursor.fetchone()
                    assert chunks is not None
                    deleted_chunks = int(_row(chunks, "chunk_count", 0))
                    await connection.execute(
                        """
                        DELETE FROM retrieval.documents
                        WHERE store_key = %s AND document_key = ANY(%s)
                        """,
                        (store_key, document_keys),
                    )
                remaining_cursor = await connection.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM retrieval.documents WHERE store_key = %s
                    ) AS remaining
                    """,
                    (store_key,),
                )
                remaining_row = await remaining_cursor.fetchone()
                assert remaining_row is not None
                remaining = bool(_row(remaining_row, "remaining", 0))
                if not remaining:
                    await connection.execute(
                        "DELETE FROM retrieval.retrieval_state WHERE store_key = %s",
                        (store_key,),
                    )
                return CleanupBatchOutcome(
                    deleted_documents=len(document_keys),
                    deleted_chunks=deleted_chunks,
                    remaining=remaining,
                )

    async def deactivate_users_if_current(
        self,
        store_key: str,
        expected_generation: int,
        user_keys: tuple[str, ...],
    ) -> int:
        async with self._provider.connection() as connection:
            async with connection.transaction():
                await self._require_cleanup(connection, store_key, expected_generation)
                if user_keys:
                    cursor = await connection.execute(
                        """
                        UPDATE retrieval.store_users
                        SET active = false,
                            lifecycle_generation = %s,
                            updated_at = clock_timestamp()
                        WHERE store_key = %s AND active AND user_key = ANY(%s)
                        RETURNING user_key
                        """,
                        (expected_generation, store_key, list(user_keys)),
                    )
                else:
                    cursor = await connection.execute(
                        """
                        UPDATE retrieval.store_users
                        SET active = false,
                            lifecycle_generation = %s,
                            updated_at = clock_timestamp()
                        WHERE store_key = %s AND active
                        RETURNING user_key
                        """,
                        (expected_generation, store_key),
                    )
                return len(await cursor.fetchall())

    async def _insert_or_match_receipt(
        self,
        connection: Any,
        *,
        store_key: str,
        idempotency_key: str,
        operation_type: str,
        document_key: str,
        lifecycle_generation: int,
        payload_hash: str,
        outcome: CommitOutcome,
    ) -> CommitOutcome | None:
        result = {"chunks_written": outcome.chunks_written}
        cursor = await connection.execute(
            """
            INSERT INTO retrieval.write_receipts (
                store_key,
                idempotency_key,
                operation_type,
                document_key,
                lifecycle_generation,
                payload_hash,
                result
            )
            VALUES (%s, %s, %s, %s, %s, %s, CAST(%s AS jsonb))
            ON CONFLICT (store_key, idempotency_key) DO NOTHING
            RETURNING idempotency_key
            """,
            (
                store_key,
                idempotency_key,
                operation_type,
                document_key,
                lifecycle_generation,
                payload_hash,
                json.dumps(result, separators=(",", ":"), sort_keys=True),
            ),
        )
        if await cursor.fetchone() is not None:
            return None
        existing_cursor = await connection.execute(
            """
            SELECT operation_type,
                   document_key,
                   lifecycle_generation,
                   payload_hash,
                   result
            FROM retrieval.write_receipts
            WHERE store_key = %s AND idempotency_key = %s
            """,
            (store_key, idempotency_key),
        )
        receipt = await existing_cursor.fetchone()
        if receipt is None:
            raise RuntimeError("conflicting idempotency receipt disappeared")
        if (
            str(_row(receipt, "operation_type", 0)) != operation_type
            or str(_row(receipt, "document_key", 1)) != document_key
            or int(_row(receipt, "lifecycle_generation", 2)) != lifecycle_generation
            or str(_row(receipt, "payload_hash", 3)) != payload_hash
        ):
            raise IdempotencyConflictError("idempotency key was reused with a different payload")
        existing_result = _row(receipt, "result", 4)
        if isinstance(existing_result, str):
            existing_result = json.loads(existing_result)
        return CommitOutcome(
            duplicate=True,
            chunks_written=int(existing_result.get("chunks_written", 0)),
        )

    async def _retry_receipted_transaction(self, operation: Callable[[], Awaitable[_T]]) -> _T:
        for attempt in range(self._transaction_retry_limit + 1):
            try:
                return await operation()
            except Exception as exc:
                if (
                    getattr(exc, "sqlstate", None) not in _RETRYABLE_TRANSACTION_STATES
                    or attempt >= self._transaction_retry_limit
                ):
                    raise
                # Only receipt-protected document mutations use this helper.
                await asyncio.sleep(min(0.05 * (2**attempt), 0.5))
        raise AssertionError("retry loop terminated without returning or raising")

    async def _require_writable(
        self, connection: Any, store_key: str, expected_generation: int
    ) -> Any:
        _require_generation_value(expected_generation)
        current = await self._lock_store(connection, store_key, mode="SHARE")
        self._require_generation(current, expected_generation)
        state = StoreLifecycleState(str(_row(current, "lifecycle_state", 0)))
        if state not in _WRITABLE_STATES:
            raise LifecycleStateRejectedError(f"store state {state.value} rejects retrieval writes")
        return current

    async def _require_cleanup(
        self, connection: Any, store_key: str, expected_generation: int
    ) -> Any:
        _require_generation_value(expected_generation)
        current = await self._lock_store(connection, store_key, mode="UPDATE")
        self._require_generation(current, expected_generation)
        state = StoreLifecycleState(str(_row(current, "lifecycle_state", 0)))
        if state is not StoreLifecycleState.DEACTIVATING:
            raise LifecycleStateRejectedError("cleanup requires DEACTIVATING state")
        return current

    @staticmethod
    async def _lock_store(connection: Any, store_key: str, *, mode: str) -> Any:
        if mode not in {"SHARE", "UPDATE"}:
            raise ValueError("unsupported store lock mode")
        # ``mode`` is a closed internal allowlist, never caller-provided SQL.
        cursor = await connection.execute(
            f"""
            SELECT lifecycle_state,
                   lifecycle_generation,
                   last_lifecycle_transition
            FROM retrieval.stores
            WHERE store_key = %s
            FOR {mode}
            """,
            (store_key,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise KeyError(f"unknown store {store_key!r}")
        return row

    @staticmethod
    def _require_generation(row: Any, expected_generation: int) -> None:
        actual = int(_row(row, "lifecycle_generation", 1))
        if actual != expected_generation:
            raise StaleLifecycleGenerationError(expected_generation, actual)

    @staticmethod
    async def _fetch_snapshot(connection: Any, store_key: str) -> StoreSnapshot:
        cursor = await connection.execute(_SNAPSHOT_SQL, (store_key,))
        row = await cursor.fetchone()
        if row is None:
            raise KeyError(f"unknown store {store_key!r}")
        return StoreSnapshot(
            store_key=str(_row(row, "store_key", 0)),
            display_name=str(_row(row, "display_name", 1)),
            lifecycle_state=StoreLifecycleState(str(_row(row, "lifecycle_state", 2))),
            lifecycle_generation=int(_row(row, "lifecycle_generation", 3)),
            last_lifecycle_transition=_row(row, "last_lifecycle_transition", 4),
            active_user_count=int(_row(row, "active_user_count", 5)),
            document_count=int(_row(row, "document_count", 6)),
            chunk_count=int(_row(row, "chunk_count", 7)),
        )

    async def aclose(self) -> None:
        if self._owns_provider:
            close = getattr(self._provider, "aclose", None)
            if close is not None:
                await close()


async def create_repository() -> LakebaseRetrievalRepository:
    """Environment factory used by ``RETRIEVAL_REPOSITORY_FACTORY``."""

    config = LakebaseConfig.from_env(default_pool_max_size=20)
    provider = LakebaseConnectionProvider(config)
    try:
        await provider.open()
    except BaseException:
        await provider.aclose()
        raise
    return LakebaseRetrievalRepository(
        provider,
        transaction_retry_limit=config.transaction_retry_limit,
        owns_provider=True,
    )


def _row(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        return row[name]
    return row[index]


def _require_nonempty(value: str, name: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{name} must not be empty")


def _require_generation_value(generation: int) -> None:
    if generation < 0:
        raise ValueError("generation must be non-negative")


def _validate_document(document: SearchableDocument) -> None:
    _require_nonempty(document.reference.document_key, "document_key")
    _require_nonempty(document.reference.source_version, "source_version")
    _require_nonempty(document.reference.staging_uri, "staging_uri")
    _require_nonempty(document.title, "title")
    for value, label in (
        (document.reference.content_hash, "content_hash"),
        (document.body_hash, "body_hash"),
    ):
        if _SHA256.fullmatch(value) is None:
            raise ValueError(f"{label} must be lowercase SHA-256 hex")
    if not document.chunks:
        raise ValueError("a searchable document must have at least one chunk")
    ordinals: set[int] = set()
    for chunk in document.chunks:
        if chunk.ordinal < 0 or chunk.ordinal in ordinals:
            raise ValueError("chunk ordinals must be unique and non-negative")
        if not chunk.text:
            raise ValueError("chunk text must not be empty")
        if _SHA256.fullmatch(chunk.content_hash) is None:
            raise ValueError("chunk content_hash must be lowercase SHA-256 hex")
        if (chunk.embedding is None) != (chunk.embedding_model is None):
            raise ValueError("chunk embedding and embedding_model must be supplied together")
        if chunk.embedding is not None:
            if not chunk.embedding:
                raise ValueError("chunk embedding must not be empty")
            if not all(math.isfinite(value) for value in chunk.embedding):
                raise ValueError("chunk embedding values must be finite")
        ordinals.add(chunk.ordinal)
    if tuple(chunk.ordinal for chunk in document.chunks) != tuple(range(len(document.chunks))):
        raise ValueError("chunk ordinals must be contiguous and ordered from zero")


def _vector_literal(vector: tuple[float, ...] | None) -> str | None:
    if vector is None:
        return None
    return "[" + ",".join(format(value, ".17g") for value in vector) + "]"


__all__ = ["AsyncConnectionProvider", "LakebaseRetrievalRepository", "create_repository"]
