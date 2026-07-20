from __future__ import annotations

import asyncio
import hashlib

import pytest

from retrieval.temporal.activities import ingestion as ingestion_module
from retrieval.temporal.activities.ingestion import IngestionActivities
from retrieval.temporal.activities.repositories import (
    InMemoryRetrievalRepository,
    InMemoryStagingStore,
    StaleLifecycleGenerationError,
)
from retrieval.temporal.models.documents import DocumentIngestionInput, DocumentRef
from retrieval.temporal.models.lifecycle import StoreLifecycleState
from retrieval.temporal.models.operations import ResultStatus


def document_input(store_key: str, generation: int, body: bytes) -> DocumentIngestionInput:
    return DocumentIngestionInput(
        store_key=store_key,
        lifecycle_generation=generation,
        document=DocumentRef(
            document_key="document",
            source_version="version-1",
            staging_uri="stage://document",
            content_hash=hashlib.sha256(body).hexdigest(),
        ),
        idempotency_key="ingest/document/version-1",
    )


@pytest.mark.asyncio
async def test_old_generation_document_write_is_rejected_after_fence() -> None:
    body = b"document body"
    repository = InMemoryRetrievalRepository()
    await repository.ensure_store("store", generation=7)
    staging = InMemoryStagingStore({"stage://document": body})
    activities = IngestionActivities(repository, staging)

    await repository.begin_deactivation("store", 7)
    result = await activities.ingest_staged_document(document_input("store", 7, body))

    assert result.status is ResultStatus.STALE_GENERATION
    assert (await repository.get_store("store")).documents == {}


@pytest.mark.asyncio
async def test_late_ingestion_cannot_recreate_objects_removed_by_deactivation() -> None:
    body = b"document body"
    repository = InMemoryRetrievalRepository()
    await repository.ensure_store("store", generation=2)
    staging = InMemoryStagingStore({"stage://document": body})
    activities = IngestionActivities(repository, staging)
    command = document_input("store", 2, body)
    assert (await activities.ingest_staged_document(command)).status is ResultStatus.SUCCEEDED

    new_generation = await repository.begin_deactivation("store", 2)
    await repository.remove_all_objects_if_current("store", new_generation.lifecycle_generation)
    late_result = await activities.ingest_staged_document(command)

    assert late_result.status is ResultStatus.STALE_GENERATION
    assert (await repository.get_store("store")).documents == {}


@pytest.mark.asyncio
async def test_begin_deactivation_is_atomic_and_idempotent_under_activity_retry() -> None:
    repository = InMemoryRetrievalRepository()
    await repository.ensure_store("store", generation=4)

    results = await asyncio.gather(
        repository.begin_deactivation("store", 4),
        repository.begin_deactivation("store", 4),
        return_exceptions=True,
    )

    assert all(not isinstance(result, BaseException) for result in results)
    assert {result.lifecycle_generation for result in results} == {5}
    assert (await repository.get_store("store")).lifecycle_generation == 5


@pytest.mark.asyncio
async def test_truly_stale_deactivation_generation_is_rejected() -> None:
    repository = InMemoryRetrievalRepository()
    await repository.ensure_store("store", generation=8)

    with pytest.raises(StaleLifecycleGenerationError):
        await repository.begin_deactivation("store", 4)


@pytest.mark.asyncio
async def test_failed_deactivation_can_resume_without_advancing_generation() -> None:
    repository = InMemoryRetrievalRepository()
    await repository.ensure_store(
        "store",
        generation=5,
        state=StoreLifecycleState.DEACTIVATION_FAILED,
    )

    first = await repository.resume_deactivation("store", 5)
    duplicate = await repository.resume_deactivation("store", 5)
    record = await repository.get_store("store")

    assert first.lifecycle_generation == duplicate.lifecycle_generation == 5
    assert first.previous_generation == duplicate.previous_generation == 4
    assert record.lifecycle_generation == 5
    assert record.lifecycle_state is StoreLifecycleState.DEACTIVATING


@pytest.mark.asyncio
async def test_slow_staging_read_heartbeats_until_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = b"document body"
    release = asyncio.Event()
    heartbeats: list[str] = []

    class SlowStagingStore:
        async def get(self, _staging_uri: str) -> bytes:
            await release.wait()
            return body

    repository = InMemoryRetrievalRepository()
    await repository.ensure_store("store", generation=1)
    activities = IngestionActivities(repository, SlowStagingStore())
    monkeypatch.setattr(ingestion_module, "_heartbeat", heartbeats.append)
    monkeypatch.setattr(ingestion_module, "_STAGING_HEARTBEAT_INTERVAL_SECONDS", 0.001)

    running = asyncio.create_task(
        activities.ingest_staged_document(document_input("store", 1, body))
    )
    await asyncio.sleep(0.005)
    release.set()

    assert (await running).status is ResultStatus.SUCCEEDED
    assert heartbeats.count("loading-staged-document") >= 2
