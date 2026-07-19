from __future__ import annotations

import hashlib

import pytest

from retrieval.temporal.activities.repositories import (
    CleanupIncompleteError,
    IdempotencyConflictError,
    InMemoryRetrievalRepository,
    LifecycleStateRejectedError,
    SearchableDocument,
    SearchChunk,
    StaleLifecycleGenerationError,
)
from retrieval.temporal.models.documents import DocumentRef
from retrieval.temporal.models.lifecycle import StoreLifecycleState


def _document(key: str, body: str = "Northstar renewal evidence") -> SearchableDocument:
    encoded = body.encode()
    digest = hashlib.sha256(encoded).hexdigest()
    return SearchableDocument(
        reference=DocumentRef(
            document_key=key,
            source_version="v1",
            staging_uri=f"fixture://northstar/{key}",
            content_hash=digest,
        ),
        title=key,
        source_uri=f"https://example.invalid/{key}",
        body_hash=digest,
        chunks=(SearchChunk(ordinal=0, text=body, content_hash=digest),),
    )


def _document_with_repeated_chunks(key: str) -> SearchableDocument:
    body = "Repeated section\nRepeated section"
    digest = hashlib.sha256(body.encode()).hexdigest()
    chunk_digest = hashlib.sha256(b"Repeated section").hexdigest()
    return SearchableDocument(
        reference=DocumentRef(
            document_key=key,
            source_version="v1",
            staging_uri=f"fixture://northstar/{key}",
            content_hash=digest,
        ),
        title=key,
        source_uri=f"https://example.invalid/{key}",
        body_hash=digest,
        chunks=(
            SearchChunk(ordinal=0, text="Repeated section", content_hash=chunk_digest),
            SearchChunk(ordinal=1, text="Repeated section", content_hash=chunk_digest),
        ),
    )


@pytest.fixture
def repository() -> InMemoryRetrievalRepository:
    return InMemoryRetrievalRepository()


@pytest.mark.asyncio
async def test_snapshot_and_store_creation_are_exactly_idempotent(
    repository: InMemoryRetrievalRepository,
) -> None:
    created = await repository.create_store("store", "Northstar", generation=7)
    duplicate = await repository.create_store("store", "Northstar", generation=7)

    assert created == duplicate
    assert created.lifecycle_state is StoreLifecycleState.ACTIVE
    assert created.document_count == created.chunk_count == 0
    with pytest.raises(IdempotencyConflictError):
        await repository.create_store("store", "Different", generation=7)


@pytest.mark.asyncio
async def test_document_receipts_are_duplicate_safe_and_conflicts_fail_closed(
    repository: InMemoryRetrievalRepository,
) -> None:
    await repository.create_store("store", "Northstar", generation=7)
    document = _document("renewal-plan.md")

    first = await repository.commit_document_if_current("store", 7, document, "commit-1")
    duplicate = await repository.commit_document_if_current("store", 7, document, "commit-1")

    assert first.duplicate is False
    assert duplicate.duplicate is True
    assert (await repository.get_store("store")).document_count == 1
    with pytest.raises(IdempotencyConflictError):
        await repository.commit_document_if_current(
            "store", 7, _document("renewal-plan.md", "changed"), "commit-1"
        )


@pytest.mark.asyncio
async def test_repeated_chunk_content_is_valid_at_distinct_ordinals(
    repository: InMemoryRetrievalRepository,
) -> None:
    await repository.create_store("store", "Northstar", generation=7)

    committed = await repository.commit_document_if_current(
        "store",
        7,
        _document_with_repeated_chunks("repeated.md"),
        "commit-repeated",
    )

    assert committed.chunks_written == 2
    assert (await repository.get_store("store")).chunk_count == 2


@pytest.mark.asyncio
async def test_fence_linearizes_before_late_write_even_when_a_receipt_exists(
    repository: InMemoryRetrievalRepository,
) -> None:
    await repository.create_store("store", "Northstar", generation=7)
    document = _document("renewal-plan.md")
    await repository.commit_document_if_current("store", 7, document, "commit-1")

    transition = await repository.begin_deactivation("store", 7)

    assert transition.lifecycle_generation == 8
    with pytest.raises(StaleLifecycleGenerationError):
        await repository.commit_document_if_current("store", 7, document, "commit-1")


@pytest.mark.asyncio
async def test_cleanup_is_bounded_and_inactive_requires_zero_rows(
    repository: InMemoryRetrievalRepository,
) -> None:
    await repository.create_store("store", "Northstar", generation=7)
    await repository.activate_user_if_current("store", 7, "user")
    for index in range(3):
        await repository.commit_document_if_current(
            "store", 7, _document(f"document-{index}.md"), f"commit-{index}"
        )
    transition = await repository.begin_deactivation("store", 7)

    with pytest.raises(CleanupIncompleteError):
        await repository.mark_inactive("store", 8)
    first = await repository.remove_object_batch_if_current("store", 8, 2)
    second = await repository.remove_object_batch_if_current("store", 8, 2)

    assert (first.deleted_documents, first.remaining) == (2, True)
    assert (second.deleted_documents, second.remaining) == (1, False)
    with pytest.raises(CleanupIncompleteError):
        await repository.mark_inactive("store", 8)
    assert await repository.deactivate_users_if_current("store", 8, ()) == 1
    inactive = await repository.mark_inactive("store", transition.lifecycle_generation)
    assert inactive.lifecycle_state is StoreLifecycleState.INACTIVE
    assert inactive.document_count == inactive.chunk_count == inactive.active_user_count == 0


@pytest.mark.asyncio
async def test_cleanup_and_writes_enforce_disjoint_states(
    repository: InMemoryRetrievalRepository,
) -> None:
    await repository.create_store("store", "Northstar", generation=7)
    with pytest.raises(LifecycleStateRejectedError):
        await repository.remove_object_batch_if_current("store", 7, 1)

    await repository.begin_deactivation("store", 7)
    with pytest.raises(LifecycleStateRejectedError):
        await repository.commit_document_if_current("store", 8, _document("late.md"), "late")
