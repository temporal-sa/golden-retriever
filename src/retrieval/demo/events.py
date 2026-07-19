"""Presentation event adapters; events explain outcomes but never establish correctness."""

from __future__ import annotations

from retrieval.temporal.activities.hooks import IngestionEvent

from .models import DemoEvent
from .store import DemoStateStore


class DemoIngestionEventSink:
    def __init__(self, state_store: DemoStateStore) -> None:
        self._state_store = state_store

    async def record(self, event: IngestionEvent) -> None:
        run = await self._state_store.get_run_by_store(event.store_key)
        await self._state_store.append_event(
            DemoEvent(
                event_id=None,
                event_key=(
                    f"ingestion:{event.event_type}:{event.document_key}:"
                    f"{event.idempotency_key_hash}"
                ),
                run_id=run.run_id,
                store_key=event.store_key,
                event_type=event.event_type,
                operation_id=event.operation_id,
                workflow_id=event.workflow_id,
                document_key=event.document_key,
                expected_generation=event.expected_generation,
                actual_generation=event.actual_generation,
                details=dict(event.details),
            )
        )


__all__ = ["DemoEvent", "DemoIngestionEventSink"]
