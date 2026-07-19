"""Persistence ports and the deterministic in-memory reference adapter.

Production implementations must validate lifecycle state and generation in the same transaction
as every mutation. Searchable content stays process-local and never enters Workflow Event History.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from retrieval.temporal.models.documents import DocumentRef
from retrieval.temporal.models.lifecycle import NewGeneration, StoreLifecycleState


class StaleLifecycleGenerationError(RuntimeError):
    """The mutation's expected generation is no longer authoritative."""

    def __init__(self, expected_generation: int, actual_generation: int) -> None:
        self.expected_generation = expected_generation
        self.actual_generation = actual_generation
        super().__init__(f"expected generation {expected_generation}, found {actual_generation}")


class LifecycleStateRejectedError(RuntimeError):
    """The authoritative store state does not permit the requested mutation."""


class IdempotencyConflictError(RuntimeError):
    """An idempotency key was reused for a different logical mutation."""


class CleanupIncompleteError(RuntimeError):
    """A store cannot become inactive while retrieval rows remain."""


@dataclass(frozen=True)
class StoreSnapshot:
    store_key: str
    display_name: str
    lifecycle_state: StoreLifecycleState
    lifecycle_generation: int
    last_lifecycle_transition: datetime
    active_user_count: int = 0
    document_count: int = 0
    chunk_count: int = 0


@dataclass(frozen=True)
class SearchChunk:
    ordinal: int
    text: str
    content_hash: str


@dataclass(frozen=True)
class SearchableDocument:
    reference: DocumentRef
    title: str
    source_uri: str | None
    body_hash: str
    chunks: tuple[SearchChunk, ...]


@dataclass(frozen=True)
class CommitOutcome:
    duplicate: bool = False
    chunks_written: int = 0


@dataclass(frozen=True)
class CleanupBatchOutcome:
    deleted_documents: int
    deleted_chunks: int
    remaining: bool


@dataclass(frozen=True)
class _WriteReceipt:
    operation_type: str
    document_key: str
    lifecycle_generation: int
    payload_hash: str
    outcome: CommitOutcome


@dataclass
class StoreRecord:
    """In-memory aggregate retained only inside the local reference adapter."""

    store_key: str
    display_name: str
    lifecycle_state: StoreLifecycleState = StoreLifecycleState.ACTIVE
    lifecycle_generation: int = 0
    last_lifecycle_transition: datetime = field(default_factory=lambda: datetime.now(UTC))
    active_users: set[str] = field(default_factory=set)
    retrieval_state: dict[str, str] = field(default_factory=dict)
    documents: dict[str, SearchableDocument] = field(default_factory=dict)
    write_receipts: dict[str, _WriteReceipt] = field(default_factory=dict)


class RetrievalRepository(Protocol):
    """Atomic store lifecycle and generation-fenced side-effect authority."""

    async def create_store(
        self,
        store_key: str,
        display_name: str,
        *,
        generation: int = 0,
        state: StoreLifecycleState = StoreLifecycleState.ACTIVE,
    ) -> StoreSnapshot: ...

    async def get_store(self, store_key: str) -> StoreSnapshot: ...

    async def begin_deactivation(
        self, store_key: str, expected_generation: int
    ) -> NewGeneration: ...

    async def resume_deactivation(
        self, store_key: str, lifecycle_generation: int
    ) -> NewGeneration: ...

    async def mark_inactive(self, store_key: str, expected_generation: int) -> StoreSnapshot: ...

    async def mark_deactivation_failed(
        self, store_key: str, expected_generation: int
    ) -> StoreSnapshot: ...

    async def activate_user_if_current(
        self, store_key: str, expected_generation: int, user_key: str
    ) -> None: ...

    async def mutate_retrieval_state_if_current(
        self, store_key: str, expected_generation: int, key: str, value: str
    ) -> None: ...

    async def commit_document_if_current(
        self,
        store_key: str,
        expected_generation: int,
        document: SearchableDocument,
        idempotency_key: str,
    ) -> CommitOutcome: ...

    async def delete_document_if_current(
        self,
        store_key: str,
        expected_generation: int,
        document_key: str,
        idempotency_key: str,
    ) -> CommitOutcome: ...

    async def remove_object_batch_if_current(
        self, store_key: str, expected_generation: int, batch_size: int
    ) -> CleanupBatchOutcome: ...

    async def deactivate_users_if_current(
        self, store_key: str, expected_generation: int, user_keys: tuple[str, ...]
    ) -> int: ...


def document_payload_hash(
    document: SearchableDocument,
    *,
    expected_generation: int,
) -> str:
    payload = {
        "operation_type": "upsert_document",
        "lifecycle_generation": expected_generation,
        "reference": {
            "document_key": document.reference.document_key,
            "source_version": document.reference.source_version,
            "staging_uri": document.reference.staging_uri,
            "content_hash": document.reference.content_hash,
        },
        "title": document.title,
        "source_uri": document.source_uri,
        "body_hash": document.body_hash,
        "chunks": [
            {
                "ordinal": chunk.ordinal,
                "text": chunk.text,
                "content_hash": chunk.content_hash,
            }
            for chunk in document.chunks
        ],
    }
    return _canonical_hash(payload)


def delete_payload_hash(document_key: str, *, expected_generation: int) -> str:
    return _canonical_hash(
        {
            "operation_type": "delete_document",
            "lifecycle_generation": expected_generation,
            "document_key": document_key,
        }
    )


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class InMemoryRetrievalRepository:
    """Single-process reference adapter; never use it for shared durable state."""

    def __init__(self) -> None:
        self._records: dict[str, StoreRecord] = {}
        self._lock = asyncio.Lock()

    async def create_store(
        self,
        store_key: str,
        display_name: str,
        *,
        generation: int = 0,
        state: StoreLifecycleState = StoreLifecycleState.ACTIVE,
    ) -> StoreSnapshot:
        if generation < 0:
            raise ValueError("generation must be non-negative")
        async with self._lock:
            existing = self._records.get(store_key)
            if existing is not None:
                if (
                    existing.display_name != display_name
                    or existing.lifecycle_generation != generation
                    or existing.lifecycle_state is not state
                ):
                    raise IdempotencyConflictError(
                        f"store {store_key!r} already exists with different attributes"
                    )
                return self._snapshot(existing)
            record = StoreRecord(
                store_key=store_key,
                display_name=display_name,
                lifecycle_state=state,
                lifecycle_generation=generation,
            )
            self._records[store_key] = record
            return self._snapshot(record)

    async def ensure_store(
        self,
        store_key: str,
        *,
        display_name: str | None = None,
        generation: int = 0,
        state: StoreLifecycleState = StoreLifecycleState.ACTIVE,
    ) -> StoreSnapshot:
        """Create a test store or return its current snapshot without rewinding it."""

        async with self._lock:
            record = self._records.get(store_key)
            if record is None:
                record = StoreRecord(
                    store_key=store_key,
                    display_name=display_name or store_key,
                    lifecycle_state=state,
                    lifecycle_generation=generation,
                )
                self._records[store_key] = record
            return self._snapshot(record)

    async def get_store(self, store_key: str) -> StoreSnapshot:
        async with self._lock:
            return self._snapshot(self._require(store_key))

    async def inspect_store(self, store_key: str) -> StoreRecord:
        """Return a defensive aggregate copy for local tests and in-memory search."""

        async with self._lock:
            return self._copy_record(self._require(store_key))

    async def begin_deactivation(self, store_key: str, expected_generation: int) -> NewGeneration:
        async with self._lock:
            record = self._require(store_key)
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

    async def mark_inactive(self, store_key: str, expected_generation: int) -> StoreSnapshot:
        async with self._lock:
            record = self._require(store_key)
            self._require_generation(record, expected_generation)
            if record.lifecycle_state is StoreLifecycleState.INACTIVE:
                return self._snapshot(record)
            if record.lifecycle_state is not StoreLifecycleState.DEACTIVATING:
                raise LifecycleStateRejectedError("store must be deactivating before inactive")
            chunk_count = sum(len(document.chunks) for document in record.documents.values())
            if record.active_users or record.documents or record.retrieval_state or chunk_count:
                raise CleanupIncompleteError("store cleanup is incomplete")
            record.lifecycle_state = StoreLifecycleState.INACTIVE
            record.last_lifecycle_transition = datetime.now(UTC)
            return self._snapshot(record)

    async def mark_deactivation_failed(
        self, store_key: str, expected_generation: int
    ) -> StoreSnapshot:
        async with self._lock:
            record = self._require(store_key)
            self._require_generation(record, expected_generation)
            if record.lifecycle_state is StoreLifecycleState.DEACTIVATION_FAILED:
                return self._snapshot(record)
            if record.lifecycle_state is not StoreLifecycleState.DEACTIVATING:
                raise LifecycleStateRejectedError(
                    "store must be deactivating before recording deactivation failure"
                )
            record.lifecycle_state = StoreLifecycleState.DEACTIVATION_FAILED
            record.last_lifecycle_transition = datetime.now(UTC)
            return self._snapshot(record)

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

    async def commit_document_if_current(
        self,
        store_key: str,
        expected_generation: int,
        document: SearchableDocument,
        idempotency_key: str,
    ) -> CommitOutcome:
        payload_hash = document_payload_hash(
            document,
            expected_generation=expected_generation,
        )
        async with self._lock:
            record = self._require(store_key)
            self._require_writable_record(record, expected_generation)
            duplicate = self._duplicate_receipt(
                record,
                idempotency_key,
                operation_type="upsert_document",
                document_key=document.reference.document_key,
                lifecycle_generation=expected_generation,
                payload_hash=payload_hash,
            )
            if duplicate is not None:
                return duplicate
            outcome = CommitOutcome(chunks_written=len(document.chunks))
            record.documents[document.reference.document_key] = document
            record.write_receipts[idempotency_key] = _WriteReceipt(
                operation_type="upsert_document",
                document_key=document.reference.document_key,
                lifecycle_generation=expected_generation,
                payload_hash=payload_hash,
                outcome=outcome,
            )
            return outcome

    async def delete_document_if_current(
        self,
        store_key: str,
        expected_generation: int,
        document_key: str,
        idempotency_key: str,
    ) -> CommitOutcome:
        payload_hash = delete_payload_hash(
            document_key,
            expected_generation=expected_generation,
        )
        async with self._lock:
            record = self._require(store_key)
            self._require_writable_record(record, expected_generation)
            duplicate = self._duplicate_receipt(
                record,
                idempotency_key,
                operation_type="delete_document",
                document_key=document_key,
                lifecycle_generation=expected_generation,
                payload_hash=payload_hash,
            )
            if duplicate is not None:
                return duplicate
            record.documents.pop(document_key, None)
            outcome = CommitOutcome()
            record.write_receipts[idempotency_key] = _WriteReceipt(
                operation_type="delete_document",
                document_key=document_key,
                lifecycle_generation=expected_generation,
                payload_hash=payload_hash,
                outcome=outcome,
            )
            return outcome

    async def remove_object_batch_if_current(
        self, store_key: str, expected_generation: int, batch_size: int
    ) -> CleanupBatchOutcome:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        async with self._lock:
            record = self._require_cleanup_generation(store_key, expected_generation)
            document_keys = tuple(sorted(record.documents)[:batch_size])
            deleted_chunks = sum(len(record.documents[key].chunks) for key in document_keys)
            for document_key in document_keys:
                del record.documents[document_key]
            if not record.documents:
                record.retrieval_state.clear()
            return CleanupBatchOutcome(
                deleted_documents=len(document_keys),
                deleted_chunks=deleted_chunks,
                remaining=bool(record.documents),
            )

    async def remove_all_objects_if_current(self, store_key: str, expected_generation: int) -> int:
        """Compatibility helper for direct callers; workflows use bounded batches."""

        deleted = 0
        while True:
            outcome = await self.remove_object_batch_if_current(
                store_key,
                expected_generation,
                250,
            )
            deleted += outcome.deleted_documents
            if not outcome.remaining:
                return deleted

    async def deactivate_users_if_current(
        self, store_key: str, expected_generation: int, user_keys: tuple[str, ...]
    ) -> int:
        async with self._lock:
            record = self._require_cleanup_generation(store_key, expected_generation)
            targets = record.active_users if not user_keys else set(user_keys)
            removed = len(record.active_users.intersection(targets))
            record.active_users.difference_update(targets)
            return removed

    @staticmethod
    def _duplicate_receipt(
        record: StoreRecord,
        idempotency_key: str,
        *,
        operation_type: str,
        document_key: str,
        lifecycle_generation: int,
        payload_hash: str,
    ) -> CommitOutcome | None:
        receipt = record.write_receipts.get(idempotency_key)
        if receipt is None:
            return None
        if (
            receipt.operation_type != operation_type
            or receipt.document_key != document_key
            or receipt.lifecycle_generation != lifecycle_generation
            or receipt.payload_hash != payload_hash
        ):
            raise IdempotencyConflictError("idempotency key was reused with a different payload")
        return CommitOutcome(
            duplicate=True,
            chunks_written=receipt.outcome.chunks_written,
        )

    def _require(self, store_key: str) -> StoreRecord:
        try:
            return self._records[store_key]
        except KeyError as exc:
            raise KeyError(f"unknown store {store_key!r}") from exc

    @staticmethod
    def _require_generation(record: StoreRecord, expected_generation: int) -> None:
        if record.lifecycle_generation != expected_generation:
            raise StaleLifecycleGenerationError(
                expected_generation,
                record.lifecycle_generation,
            )

    def _require_writable(self, store_key: str, expected_generation: int) -> StoreRecord:
        record = self._require(store_key)
        self._require_writable_record(record, expected_generation)
        return record

    def _require_writable_record(self, record: StoreRecord, expected_generation: int) -> None:
        self._require_generation(record, expected_generation)
        if record.lifecycle_state not in {
            StoreLifecycleState.ACTIVE,
            StoreLifecycleState.SYNCING,
        }:
            raise LifecycleStateRejectedError(
                f"store state {record.lifecycle_state.value} rejects retrieval writes"
            )

    def _require_cleanup_generation(self, store_key: str, expected_generation: int) -> StoreRecord:
        record = self._require(store_key)
        self._require_generation(record, expected_generation)
        if record.lifecycle_state is not StoreLifecycleState.DEACTIVATING:
            raise LifecycleStateRejectedError("cleanup requires DEACTIVATING state")
        return record

    @staticmethod
    def _snapshot(record: StoreRecord) -> StoreSnapshot:
        return StoreSnapshot(
            store_key=record.store_key,
            display_name=record.display_name,
            lifecycle_state=record.lifecycle_state,
            lifecycle_generation=record.lifecycle_generation,
            last_lifecycle_transition=record.last_lifecycle_transition,
            active_user_count=len(record.active_users),
            document_count=len(record.documents),
            chunk_count=sum(len(document.chunks) for document in record.documents.values()),
        )

    @staticmethod
    def _copy_record(record: StoreRecord) -> StoreRecord:
        return StoreRecord(
            store_key=record.store_key,
            display_name=record.display_name,
            lifecycle_state=record.lifecycle_state,
            lifecycle_generation=record.lifecycle_generation,
            last_lifecycle_transition=record.last_lifecycle_transition,
            active_users=set(record.active_users),
            retrieval_state=dict(record.retrieval_state),
            documents=dict(record.documents),
            write_receipts=dict(record.write_receipts),
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
