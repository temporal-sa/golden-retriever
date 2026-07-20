"""Typed hold/release controls that validate the committed lifecycle fence."""

from __future__ import annotations

from retrieval.temporal.activities.repositories import RetrievalRepository
from retrieval.temporal.models.lifecycle import StoreLifecycleState

from .models import DemoConflictError, DemoControls, DemoEvent
from .store import DemoStateStore


class DemoControlsManager:
    def __init__(
        self,
        state_store: DemoStateStore,
        repository: RetrievalRepository,
    ) -> None:
        self._state_store = state_store
        self._repository = repository

    async def hold(self, run_id: str) -> DemoControls:
        return await self._state_store.set_hold(run_id, enabled=True)

    async def release(self, run_id: str, *, operation_id: str | None = None) -> DemoControls:
        run = await self._state_store.get_run(run_id)
        snapshot = await self._repository.get_store(run.store_key)
        expected_fence = run.baseline_generation + 1
        if snapshot.lifecycle_generation < expected_fence or snapshot.lifecycle_state not in {
            StoreLifecycleState.DEACTIVATING,
            StoreLifecycleState.INACTIVE,
            StoreLifecycleState.DEACTIVATION_FAILED,
        }:
            raise DemoConflictError(
                "the held commit can be released only after the generation-8 fence commits"
            )
        controls = await self._state_store.request_release(run_id)
        await self._state_store.append_event(
            DemoEvent(
                event_id=None,
                event_key=f"release:requested:{controls.control_version}",
                run_id=run_id,
                store_key=run.store_key,
                event_type="held_commit_released",
                operation_id=operation_id,
                document_key=controls.held_document_key,
                expected_generation=run.baseline_generation,
                actual_generation=snapshot.lifecycle_generation,
                details={"release_requested": True},
            )
        )
        return controls


__all__ = ["DemoControls", "DemoControlsManager"]
