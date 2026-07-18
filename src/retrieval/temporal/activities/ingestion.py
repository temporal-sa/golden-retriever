"""Staged-reference document ingestion with an atomic generation-fenced commit."""

from __future__ import annotations

import asyncio
import hashlib

from temporalio import activity

from retrieval.temporal.common.metrics import (
    INGESTION_RESULTS,
    STALE_GENERATION_REJECTIONS,
    activity_metrics,
)
from retrieval.temporal.models.documents import (
    DocumentIngestionInput,
    DocumentIngestionResult,
    DocumentMutation,
)
from retrieval.temporal.models.operations import ResultStatus

from .repositories import (
    LifecycleStateRejectedError,
    RetrievalRepository,
    StagingStore,
    StaleLifecycleGenerationError,
)

_STAGING_HEARTBEAT_INTERVAL_SECONDS = 10.0


def _heartbeat(stage: str) -> None:
    """Heartbeat in a real Activity while keeping direct unit calls usable."""

    try:
        activity.heartbeat(stage)
    except RuntimeError:
        # Unit tests exercise the Activity implementation without a worker
        # context. Temporal raises here because no Activity is running.
        pass


async def _load_staged_document(staging_store: StagingStore, staging_uri: str) -> bytes:
    """Keep the Activity heartbeat alive while an adapter performs a slow read."""

    load_task = asyncio.create_task(staging_store.get(staging_uri))
    try:
        while not load_task.done():
            _heartbeat("loading-staged-document")
            await asyncio.wait(
                {load_task},
                timeout=_STAGING_HEARTBEAT_INTERVAL_SECONDS,
            )
        return await load_task
    finally:
        if not load_task.done():
            load_task.cancel()
            await asyncio.gather(load_task, return_exceptions=True)


class IngestionActivities:
    def __init__(self, repository: RetrievalRepository, staging_store: StagingStore) -> None:
        self._repository = repository
        self._staging_store = staging_store

    @activity.defn(name="ingest_staged_document")
    async def ingest_staged_document(
        self, command: DocumentIngestionInput
    ) -> DocumentIngestionResult:
        try:
            _heartbeat("validating-generation-fenced-mutation")
            if command.mutation is DocumentMutation.DELETE:
                await self._repository.delete_document_if_current(
                    command.store_key,
                    command.lifecycle_generation,
                    command.document.document_key,
                )
            else:
                body = await _load_staged_document(
                    self._staging_store,
                    command.document.staging_uri,
                )
                content_hash = hashlib.sha256(body).hexdigest()
                if content_hash != command.document.content_hash:
                    return self._result(
                        command,
                        ResultStatus.REJECTED,
                        "staged content hash does not match DocumentRef",
                    )
                _heartbeat("committing-document")
                await self._repository.upsert_document_if_current(
                    command.store_key,
                    command.lifecycle_generation,
                    command.document,
                )
        except (StaleLifecycleGenerationError, LifecycleStateRejectedError) as exc:
            status = (
                ResultStatus.STALE_GENERATION
                if isinstance(exc, StaleLifecycleGenerationError)
                else ResultStatus.REJECTED
            )
            return self._result(command, status, str(exc))
        return self._result(command, ResultStatus.SUCCEEDED)

    @staticmethod
    def _result(
        command: DocumentIngestionInput,
        status: ResultStatus,
        message: str | None = None,
    ) -> DocumentIngestionResult:
        metrics = activity_metrics(
            operation="document_ingestion",
            mutation=command.mutation,
        )
        metrics.increment(INGESTION_RESULTS, attributes={"status": status})
        if status is ResultStatus.STALE_GENERATION:
            metrics.increment(STALE_GENERATION_REJECTIONS)
        return DocumentIngestionResult(
            document_key=command.document.document_key,
            source_version=command.document.source_version,
            status=status,
            lifecycle_generation=command.lifecycle_generation,
            content_hash=command.document.content_hash,
            message=message,
        )
