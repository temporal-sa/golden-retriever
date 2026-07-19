from __future__ import annotations

import asyncio

from retrieval.demo.config import DemoConfig
from retrieval.demo.controls import DemoControlsManager
from retrieval.demo.events import DemoIngestionEventSink
from retrieval.demo.fixtures import FixtureStagingStore, load_northstar_scenario
from retrieval.demo.ingestion_gate import DemoBeforeDocumentCommitHook
from retrieval.demo.models import DemoControls, DemoRun
from retrieval.demo.store import InMemoryDemoStateStore
from retrieval.temporal.activities.ingestion import IngestionActivities
from retrieval.temporal.activities.repositories import InMemoryRetrievalRepository
from retrieval.temporal.models.documents import DocumentIngestionInput
from retrieval.temporal.models.lifecycle import StoreLifecycleState
from retrieval.temporal.models.operations import ResultStatus


async def test_cancelled_hold_finishes_then_attempts_and_rejects_stale_commit(
    monkeypatch,
) -> None:
    scenario = load_northstar_scenario()
    config = DemoConfig(enabled=True, hold_timeout_seconds=2, control_poll_seconds=0.005)
    state = InMemoryDemoStateStore()
    repository = InMemoryRetrievalRepository()
    run = DemoRun(
        run_id="00000000-0000-0000-0000-000000000008",
        store_key="northstar-000000000008",
        display_name=scenario.display_name,
        baseline_generation=scenario.baseline_generation,
    )
    await state.start()
    await state.create_run(
        run,
        DemoControls(
            run_id=run.run_id,
            quota_once_pending=True,
            quota_retry_after_seconds=5,
            held_document_key=scenario.held_document_key,
            hold_before_commit=True,
            release_requested=False,
        ),
    )
    await repository.create_store(
        run.store_key,
        run.display_name,
        generation=7,
        state=StoreLifecycleState.ACTIVE,
    )
    late = scenario.by_key[scenario.held_document_key].reference()
    ingestion = IngestionActivities(
        repository,
        FixtureStagingStore(scenario),
        before_commit=DemoBeforeDocumentCommitHook(state, config=config),
        event_sink=DemoIngestionEventSink(state),
    )
    heartbeats: list[str] = []
    monkeypatch.setattr(
        "retrieval.demo.ingestion_gate.activity.heartbeat",
        heartbeats.append,
    )
    task = asyncio.create_task(
        ingestion.ingest_staged_document(
            DocumentIngestionInput(
                store_key=run.store_key,
                lifecycle_generation=7,
                document=late,
                idempotency_key="late-writer-cancel-test",
                sync_sequence="late-writer-test",
            )
        )
    )
    await _wait_for_event(state, run.run_id, "document_commit_held")
    for _ in range(50):
        if heartbeats.count("demo-document-commit-held") >= 2:
            break
        await asyncio.sleep(0.005)

    task.cancel()
    await asyncio.sleep(0)
    assert not task.done(), "the demo-only external wait must survive Activity cancellation"

    fence = await repository.begin_deactivation(run.store_key, 7)
    await DemoControlsManager(state, repository).release(run.run_id)
    result = await asyncio.wait_for(task, timeout=1)

    assert fence.lifecycle_generation == 8
    assert result.status is ResultStatus.STALE_GENERATION
    snapshot = await repository.get_store(run.store_key)
    assert snapshot.lifecycle_state is StoreLifecycleState.DEACTIVATING
    assert snapshot.document_count == 0
    events = await state.list_events(run.run_id)
    stale = next(event for event in events if event.event_type == "stale_generation_rejected")
    assert stale.expected_generation == 7
    assert stale.actual_generation == 8
    assert heartbeats.count("demo-document-commit-held") >= 2


async def _wait_for_event(
    state: InMemoryDemoStateStore,
    run_id: str,
    event_type: str,
) -> None:
    for _ in range(200):
        if any(event.event_type == event_type for event in await state.list_events(run_id)):
            return
        await asyncio.sleep(0.005)
    raise TimeoutError(event_type)
