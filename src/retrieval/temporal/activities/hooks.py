"""Process-local ingestion extension points kept out of workflow messages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from retrieval.temporal.models.documents import DocumentIngestionInput


@dataclass(frozen=True)
class IngestionEvent:
    event_type: str
    store_key: str
    document_key: str
    idempotency_key_hash: str
    expected_generation: int
    actual_generation: int | None = None
    operation_id: str | None = None
    workflow_id: str | None = None
    details: dict[str, str | int | bool | None] = field(default_factory=dict)


class BeforeDocumentCommitHook(Protocol):
    async def wait(self, command: DocumentIngestionInput) -> None: ...


class IngestionEventSink(Protocol):
    async def record(self, event: IngestionEvent) -> None: ...


class NoopBeforeDocumentCommitHook:
    async def wait(self, command: DocumentIngestionInput) -> None:
        del command


class NoopIngestionEventSink:
    async def record(self, event: IngestionEvent) -> None:
        del event
