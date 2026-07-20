"""Persistence ports and a deterministic test adapter.

Production deployments must implement :class:`RetrievalRepository` with transactions in
the authoritative metadata/index database. The important contract is that generation and
status checks occur in the same transaction as each mutation.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from retrieval.temporal.models.documents import DocumentRef
from retrieval.temporal.models.lifecycle import NewGeneration, StoreLifecycleState


class StaleLifecycleGenerationError(RuntimeError):
    """The mutation's expected generation is no longer authoritative."""


class LifecycleStateRejectedError(RuntimeError):
    """The authoritative store state does not permit the requested mutation."""


@dataclass
class StoreRecord:
    store_key: str
    lifecycle_state: StoreLifecycleState = StoreLifecycleState.ACTIVE
    lifecycle_generation: int = 0
    last_lifecycle_transition: datetime = field(default_factory=lambda: datetime.now(UTC))
    active_users: set[str] = field(default_factory=set)
    retrieval_state: dict[str, str] = field(default_factory=dict)
    documents: dict[str, DocumentRef] = field(default_factory=dict)


class RetrievalRepository(Protocol):
    """Atomic store lifecycle and generation-fenced side-effect authority."""

    async def get_store(self, store_key: str) -> StoreRecord: ...

    async def begin_deactivation(
        self, store_key: str, expected_generation: int
    ) -> NewGeneration: ...

    async def resume_deactivation(
        self, store_key: str, lifecycle_generation: int
    ) -> NewGeneration: ...

    async def mark_inactive(self, store_key: str, expected_generation: int) -> StoreRecord: ...

    async def mark_deactivation_failed(
        self, store_key: str, expected_generation: int
    ) -> StoreRecord: ...

    async def activate_user_if_current(
        self, store_key: str, expected_generation: int, user_key: str
    ) -> None: ...

    async def mutate_retrieval_state_if_current(
        self, store_key: str, expected_generation: int, key: str, value: str
    ) -> None: ...

    async def upsert_document_if_current(
        self, store_key: str, expected_generation: int, document: DocumentRef
    ) -> None: ...

    async def delete_document_if_current(
        self, store_key: str, expected_generation: int, document_key: str
    ) -> None: ...

    async def remove_all_objects_if_current(
        self, store_key: str, expected_generation: int
    ) -> int: ...

    async def deactivate_users_if_current(
        self, store_key: str, expected_generation: int, user_keys: tuple[str, ...]
    ) -> int: ...


class InMemoryRetrievalRepository:
    """Single-process test adapter; not a production persistence implementation."""

    def __init__(self) -> None:
        self._records: dict[str, StoreRecord] = {}
        self._lock = asyncio.Lock()

    async def ensure_store(
        self,
        store_key: str,
        *,
        generation: int = 0,
        state: StoreLifecycleState = StoreLifecycleState.ACTIVE,
    ) -> StoreRecord:
        async with self._lock:
            record = self._records.get(store_key)
            if record is None:
                record = StoreRecord(
                    store_key=store_key,
                    lifecycle_state=state,
                    lifecycle_generation=generation,
                )
                self._records[store_key] = record
            return self._copy(record)

    async def get_store(self, store_key: str) -> StoreRecord:
        async with self._lock:
            return self._copy(self._require(store_key))

    async def begin_deactivation(self, store_key: str, expected_generation: int) -> NewGeneration:
        async with self._lock:
            record = self._require(store_key)
            # Activity delivery is at-least-once. If the transition committed but its
            # response was lost, this expected generation still identifies the same
            # stable deactivation operation and returns the original logical result.
            if (
                record.lifecycle_generation == expected_generation + 1
                and record.lifecycle_state
                in {
                    StoreLifecycleState.DEACTIVATING,
                    StoreLifecycleState.INACTIVE,
                    StoreLifecycleState.DEACTIVATION_FAILED,
                }
            ):
                return NewGeneration(
                    store_key=store_key,
                    previous_generation=expected_generation,
                    lifecycle_generation=record.lifecycle_generation,
                    transitioned_at=record.last_lifecycle_transition,
                )
            self._require_generation(record, expected_generation)
            if record.lifecycle_state not in {
                StoreLifecycleState.ACTIVE,
                StoreLifecycleState.SYNCING,
            }:
                raise LifecycleStateRejectedError(
                    f"store {store_key!r} is {record.lifecycle_state.value}"
                )
            previous = record.lifecycle_generation
            record.lifecycle_generation += 1
            record.lifecycle_state = StoreLifecycleState.DEACTIVATING
            record.last_lifecycle_transition = datetime.now(UTC)
            return NewGeneration(
                store_key=store_key,
                previous_generation=previous,
                lifecycle_generation=record.lifecycle_generation,
                transitioned_at=record.last_lifecycle_transition,
            )

    async def resume_deactivation(self, store_key: str, lifecycle_generation: int) -> NewGeneration:
        async with self._lock:
            record = self._require(store_key)
            self._require_generation(record, lifecycle_generation)
            if lifecycle_generation <= 0:
                raise LifecycleStateRejectedError("a deactivation generation must be positive")
            if record.lifecycle_state is StoreLifecycleState.DEACTIVATION_FAILED:
                record.lifecycle_state = StoreLifecycleState.DEACTIVATING
                record.last_lifecycle_transition = datetime.now(UTC)
            elif record.lifecycle_state is not StoreLifecycleState.DEACTIVATING:
                raise LifecycleStateRejectedError(
                    "only a failed or already-resuming deactivation can be resumed"
                )
            return NewGeneration(
                store_key=store_key,
                previous_generation=lifecycle_generation - 1,
                lifecycle_generation=lifecycle_generation,
                transitioned_at=record.last_lifecycle_transition,
            )

    async def mark_inactive(self, store_key: str, expected_generation: int) -> StoreRecord:
        async with self._lock:
            record = self._require(store_key)
            self._require_generation(record, expected_generation)
            if record.lifecycle_state is StoreLifecycleState.INACTIVE:
                return self._copy(record)
            if record.lifecycle_state is not StoreLifecycleState.DEACTIVATING:
                raise LifecycleStateRejectedError("store must be deactivating before inactive")
            record.lifecycle_state = StoreLifecycleState.INACTIVE
            record.last_lifecycle_transition = datetime.now(UTC)
            return self._copy(record)

    async def mark_deactivation_failed(
        self, store_key: str, expected_generation: int
    ) -> StoreRecord:
        async with self._lock:
            record = self._require(store_key)
            self._require_generation(record, expected_generation)
            if record.lifecycle_state is StoreLifecycleState.DEACTIVATION_FAILED:
                return self._copy(record)
            if record.lifecycle_state is not StoreLifecycleState.DEACTIVATING:
                raise LifecycleStateRejectedError(
                    "store must be deactivating before recording deactivation failure"
                )
            record.lifecycle_state = StoreLifecycleState.DEACTIVATION_FAILED
            record.last_lifecycle_transition = datetime.now(UTC)
            return self._copy(record)

    async def activate_user_if_current(
        self, store_key: str, expected_generation: int, user_key: str
    ) -> None:
        async with self._lock:
            record = self._require_writable(store_key, expected_generation)
            record.active_users.add(user_key)

    async def mutate_retrieval_state_if_current(
        self, store_key: str, expected_generation: int, key: str, value: str
    ) -> None:
        async with self._lock:
            record = self._require_writable(store_key, expected_generation)
            record.retrieval_state[key] = value

    async def upsert_document_if_current(
        self, store_key: str, expected_generation: int, document: DocumentRef
    ) -> None:
        async with self._lock:
            record = self._require_writable(store_key, expected_generation)
            record.documents[document.document_key] = document

    async def delete_document_if_current(
        self, store_key: str, expected_generation: int, document_key: str
    ) -> None:
        async with self._lock:
            record = self._require_writable(store_key, expected_generation)
            record.documents.pop(document_key, None)

    async def remove_all_objects_if_current(self, store_key: str, expected_generation: int) -> int:
        async with self._lock:
            record = self._require_cleanup_generation(store_key, expected_generation)
            count = len(record.documents)
            record.documents.clear()
            record.retrieval_state.clear()
            return count

    async def deactivate_users_if_current(
        self, store_key: str, expected_generation: int, user_keys: tuple[str, ...]
    ) -> int:
        async with self._lock:
            record = self._require_cleanup_generation(store_key, expected_generation)
            targets = record.active_users if not user_keys else set(user_keys)
            removed = len(record.active_users.intersection(targets))
            record.active_users.difference_update(targets)
            return removed

    def _require(self, store_key: str) -> StoreRecord:
        try:
            return self._records[store_key]
        except KeyError as exc:
            raise KeyError(f"unknown store {store_key!r}") from exc

    @staticmethod
    def _require_generation(record: StoreRecord, expected_generation: int) -> None:
        if record.lifecycle_generation != expected_generation:
            raise StaleLifecycleGenerationError(
                f"expected generation {expected_generation}, found {record.lifecycle_generation}"
            )

    def _require_writable(self, store_key: str, expected_generation: int) -> StoreRecord:
        record = self._require(store_key)
        self._require_generation(record, expected_generation)
        if record.lifecycle_state not in {
            StoreLifecycleState.ACTIVE,
            StoreLifecycleState.SYNCING,
        }:
            raise LifecycleStateRejectedError(
                f"store state {record.lifecycle_state.value} rejects retrieval writes"
            )
        return record

    def _require_cleanup_generation(self, store_key: str, expected_generation: int) -> StoreRecord:
        record = self._require(store_key)
        self._require_generation(record, expected_generation)
        if record.lifecycle_state is not StoreLifecycleState.DEACTIVATING:
            raise LifecycleStateRejectedError("cleanup requires DEACTIVATING state")
        return record

    @staticmethod
    def _copy(record: StoreRecord) -> StoreRecord:
        return StoreRecord(
            store_key=record.store_key,
            lifecycle_state=record.lifecycle_state,
            lifecycle_generation=record.lifecycle_generation,
            last_lifecycle_transition=record.last_lifecycle_transition,
            active_users=set(record.active_users),
            retrieval_state=dict(record.retrieval_state),
            documents=dict(record.documents),
        )


class StagingStore(Protocol):
    async def get(self, staging_uri: str) -> bytes: ...


class InMemoryStagingStore:
    def __init__(self, bodies: dict[str, bytes] | None = None) -> None:
        self._bodies = dict(bodies or {})

    async def get(self, staging_uri: str) -> bytes:
        return self._bodies[staging_uri]

    def put(self, staging_uri: str, body: bytes) -> None:
        self._bodies[staging_uri] = body
