"""Staged document materialization with an atomic generation-fenced commit."""

from __future__ import annotations

import asyncio
import hashlib

from temporalio import activity

from retrieval.content import InvalidDocumentPayloadError, chunk_text, parse_staged_document
from retrieval.embeddings import EmbeddingProvider
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

from .hooks import (
    BeforeDocumentCommitHook,
    IngestionEvent,
    IngestionEventSink,
    NoopBeforeDocumentCommitHook,
    NoopIngestionEventSink,
)
from .repositories import (
    IdempotencyConflictError,
    LifecycleStateRejectedError,
    RetrievalRepository,
    SearchableDocument,
    SearchChunk,
    StagingStore,
    StaleLifecycleGenerationError,
)

_STAGING_HEARTBEAT_INTERVAL_SECONDS = 10.0


def _heartbeat(stage: str) -> None:
    """Heartbeat in a real Activity while keeping direct unit calls usable."""

    try:
        activity.heartbeat(stage)
    except RuntimeError:
        pass


def _workflow_id() -> str | None:
    try:
        return activity.info().workflow_id
    except RuntimeError:
        return None


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
    def __init__(
        self,
        repository: RetrievalRepository,
        staging_store: StagingStore,
        *,
        before_commit: BeforeDocumentCommitHook | None = None,
        event_sink: IngestionEventSink | None = None,
        embedding_provider: EmbeddingProvider | None = None,
    ) -> None:
        self._repository = repository
        self._staging_store = staging_store
        self._before_commit = before_commit or NoopBeforeDocumentCommitHook()
        self._event_sink = event_sink or NoopIngestionEventSink()
        self._embedding_provider = embedding_provider

    @activity.defn(name="ingest_staged_document")
    async def ingest_staged_document(
        self, command: DocumentIngestionInput
    ) -> DocumentIngestionResult:
        idempotency_key_hash = hashlib.sha256(command.idempotency_key.encode("utf-8")).hexdigest()
        try:
            _heartbeat("validating-generation-fenced-mutation")
            if command.mutation is DocumentMutation.DELETE:
                outcome = await self._repository.delete_document_if_current(
                    command.store_key,
                    command.lifecycle_generation,
                    command.document.document_key,
                    command.idempotency_key,
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
                parsed = parse_staged_document(
                    body,
                    fallback_title=command.document.document_key,
                )
                parsed_chunks = tuple(chunk_text(parsed.text))
                embeddings: tuple[tuple[float, ...], ...] | tuple[None, ...]
                embedding_model: str | None
                if self._embedding_provider is None:
                    embeddings = (None,) * len(parsed_chunks)
                    embedding_model = None
                else:
                    _heartbeat("embedding-document-chunks")
                    embeddings = await self._embedding_provider.embed(
                        [chunk.text for chunk in parsed_chunks]
                    )
                    if len(embeddings) != len(parsed_chunks):
                        raise RuntimeError(
                            "embedding provider returned the wrong number of vectors"
                        )
                    embedding_model = self._embedding_provider.identity
                    _heartbeat("embedded-document-chunks")
                searchable = SearchableDocument(
                    reference=command.document,
                    title=parsed.title,
                    source_uri=parsed.source_uri,
                    body_hash=content_hash,
                    chunks=tuple(
                        SearchChunk(
                            chunk.ordinal,
                            chunk.text,
                            chunk.content_hash,
                            embedding=embedding,
                            embedding_model=embedding_model,
                        )
                        for chunk, embedding in zip(parsed_chunks, embeddings, strict=True)
                    ),
                )
                await self._before_commit.wait(command)
                _heartbeat("committing-document")
                outcome = await self._repository.commit_document_if_current(
                    command.store_key,
                    command.lifecycle_generation,
                    searchable,
                    command.idempotency_key,
                )
        except StaleLifecycleGenerationError as exc:
            await self._record_best_effort(
                IngestionEvent(
                    event_type="stale_generation_rejected",
                    store_key=command.store_key,
                    document_key=command.document.document_key,
                    idempotency_key_hash=idempotency_key_hash,
                    expected_generation=command.lifecycle_generation,
                    actual_generation=exc.actual_generation,
                    operation_id=command.sync_sequence or None,
                    workflow_id=_workflow_id(),
                )
            )
            return self._result(command, ResultStatus.STALE_GENERATION, str(exc))
        except (LifecycleStateRejectedError, IdempotencyConflictError) as exc:
            return self._result(command, ResultStatus.REJECTED, str(exc))
        except InvalidDocumentPayloadError as exc:
            return self._result(command, ResultStatus.REJECTED, str(exc))

        await self._record_best_effort(
            IngestionEvent(
                event_type="document_committed",
                store_key=command.store_key,
                document_key=command.document.document_key,
                idempotency_key_hash=idempotency_key_hash,
                expected_generation=command.lifecycle_generation,
                actual_generation=command.lifecycle_generation,
                operation_id=command.sync_sequence or None,
                workflow_id=_workflow_id(),
                details={
                    "duplicate": outcome.duplicate,
                    "chunks_written": outcome.chunks_written,
                    "mutation": command.mutation.value,
                },
            )
        )
        return self._result(
            command,
            ResultStatus.SUCCEEDED,
            metadata={
                "duplicate": str(outcome.duplicate).lower(),
                "chunks_written": str(outcome.chunks_written),
            },
        )

    async def _record_best_effort(self, event: IngestionEvent) -> None:
        try:
            await self._event_sink.record(event)
        except Exception:
            # Presentation events are not part of the mutation's correctness boundary.
            return

    @staticmethod
    def _result(
        command: DocumentIngestionInput,
        status: ResultStatus,
        message: str | None = None,
        *,
        metadata: dict[str, str] | None = None,
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
            metadata=metadata or {},
        )
