"""Cancellation-resistant, bounded pre-commit gate used only by demo workers."""

from __future__ import annotations

import asyncio
import hashlib

from temporalio import activity

from retrieval.temporal.models.documents import DocumentIngestionInput

from .config import DemoConfig
from .models import DemoEvent
from .store import DemoStateStore


class DemoHoldTimeoutError(TimeoutError):
    """The presenter did not release a held document within the demo safety bound."""


class DemoBeforeDocumentCommitHook:
    """Hold the configured late writer after materialization and survive one cancellation.

    Temporal may cancel the Activity coroutine as deactivation drains old work. This demo-only
    hook deliberately clears cancellation *inside the external-wait simulation*, finishes its
    bounded wait, and returns. The Activity then attempts the repository transaction and Lakebase
    rejects generation 7 after the generation-8 fence. No global cancellation policy is changed.
    """

    def __init__(
        self,
        state_store: DemoStateStore,
        *,
        config: DemoConfig | None = None,
    ) -> None:
        self._state_store = state_store
        self._config = config or DemoConfig(enabled=True)
        if not self._config.enabled:
            self._config.require_enabled()

    async def wait(self, command: DocumentIngestionInput) -> None:
        run = await self._state_store.get_run_by_store(command.store_key)
        controls = await self._state_store.get_controls(run.run_id)
        if (
            not controls.hold_before_commit
            or controls.release_requested
            or command.document.document_key != controls.held_document_key
        ):
            return

        key_hash = hashlib.sha256(command.idempotency_key.encode("utf-8")).hexdigest()
        await self._state_store.append_event(
            DemoEvent(
                event_id=None,
                event_key=f"ingestion:held:{command.document.document_key}:{key_hash}",
                run_id=run.run_id,
                store_key=command.store_key,
                event_type="document_commit_held",
                operation_id=command.sync_sequence or None,
                document_key=command.document.document_key,
                expected_generation=command.lifecycle_generation,
                details={"idempotency_key_hash": key_hash},
            )
        )

        release_task = asyncio.create_task(
            self._state_store.wait_for_release(
                run.run_id,
                timeout_seconds=self._config.hold_timeout_seconds,
            )
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._config.hold_timeout_seconds
        released = False
        cancellation_observed = False
        try:
            while not release_task.done() and loop.time() < deadline:
                try:
                    if not cancellation_observed:
                        _heartbeat()
                    await asyncio.wait(
                        {release_task},
                        timeout=min(
                            1.0,
                            self._config.control_poll_seconds,
                            max(0.01, deadline - loop.time()),
                        ),
                    )
                except asyncio.CancelledError:
                    cancellation_observed = True
                    current = asyncio.current_task()
                    if current is not None:
                        current.uncancel()
            if release_task.done():
                released = release_task.result()
        finally:
            if not release_task.done():
                release_task.cancel()
                await asyncio.gather(release_task, return_exceptions=True)

        if not released:
            raise DemoHoldTimeoutError(
                f"held document exceeded {self._config.hold_timeout_seconds:g} seconds"
            )


def _heartbeat() -> None:
    try:
        activity.heartbeat("demo-document-commit-held")
    except RuntimeError:
        # Direct unit/headless calls have no Activity context.
        pass


__all__ = ["DemoBeforeDocumentCommitHook", "DemoHoldTimeoutError"]
