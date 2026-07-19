from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from retrieval.demo.fixtures import load_northstar_scenario
from retrieval.demo.models import (
    ApiIdempotencyReceipt,
    DemoControls,
    DemoIdempotencyConflictError,
    DemoOperation,
    DemoOperationStatus,
    DemoOperationType,
    DemoRun,
    DemoRunStatus,
)
from retrieval.demo.scripted_provider import ScriptedNorthstarProvider
from retrieval.demo.store import InMemoryDemoStateStore
from retrieval.temporal.activities.provider_api import (
    ListActiveUsersRequest,
    ProviderQuotaExhausted,
)


async def _seed(store: InMemoryDemoStateStore) -> DemoRun:
    scenario = load_northstar_scenario()
    run = DemoRun(
        "00000000-0000-0000-0000-000000000007",
        "northstar-000000000007",
        "Northstar AI",
        7,
    )
    await store.start()
    await store.create_run(
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
    return run


async def test_quota_consumption_is_atomic_and_events_are_idempotent() -> None:
    store = InMemoryDemoStateStore()
    run = await _seed(store)

    results = await asyncio.gather(
        *(
            store.consume_quota_once(
                run.run_id,
                request_id=f"request-{index}",
                operation="list_active_users",
            )
            for index in range(20)
        )
    )

    assert sum(result.injected for result in results) == 1
    waiting = next(result.waiting_request_id for result in results if result.injected)
    assert await store.complete_quota_wait(run.run_id, operation="list_active_users") == waiting
    assert await store.complete_quota_wait(run.run_id, operation="list_active_users") is None
    assert [event.event_type for event in await store.list_events(run.run_id)] == [
        "quota_injected",
        "quota_wait_started",
        "quota_wait_completed",
    ]


async def test_scripted_provider_raises_once_then_returns_stable_user() -> None:
    scenario = load_northstar_scenario()
    store = InMemoryDemoStateStore()
    run = await _seed(store)
    provider = ScriptedNorthstarProvider(scenario, store)
    request = ListActiveUsersRequest(
        store_key=run.store_key,
        lifecycle_generation=7,
        cursor=None,
        page_size=100,
        request_id="users-0",
    )

    with pytest.raises(ProviderQuotaExhausted) as raised:
        await provider.list_active_users(request)
    assert raised.value.limit == 2
    assert raised.value.retry_after_seconds == 5

    page = await provider.list_active_users(replace(request, request_id="users-1"))
    assert tuple(user.user_key for user in page.users) == (scenario.user_key,)
    assert [event.event_type for event in await store.list_events(run.run_id)] == [
        "quota_injected",
        "quota_wait_started",
        "quota_wait_completed",
    ]


async def test_api_receipt_rejects_conflicting_key_reuse() -> None:
    store = InMemoryDemoStateStore()
    await _seed(store)
    original = ApiIdempotencyReceipt(
        scope="scope",
        idempotency_key_hash="a" * 64,
        request_hash="b" * 64,
        status_code=200,
        response={"ok": True},
    )
    assert await store.put_idempotency_receipt(original) == original
    assert await store.put_idempotency_receipt(original) == original

    with pytest.raises(DemoIdempotencyConflictError):
        await store.put_idempotency_receipt(replace(original, request_hash="c" * 64))


async def test_terminal_operation_and_run_state_cannot_regress() -> None:
    store = InMemoryDemoStateStore()
    run = await _seed(store)
    operation = await store.put_operation(
        DemoOperation(
            operation_id="sync-operation",
            run_id=run.run_id,
            store_key=run.store_key,
            operation_type=DemoOperationType.SYNC,
            status=DemoOperationStatus.ACCEPTED,
            command_id="sync-command",
            workflow_id="store-sync/workflow",
            lifecycle_generation=7,
        )
    )

    ignored_peer_failure = await store.update_operation(
        operation.operation_id,
        DemoOperationStatus.FAILED,
        message="peer transport failed",
        require_workflow_id_absent=True,
    )
    terminal = await store.update_operation(
        operation.operation_id,
        DemoOperationStatus.COMPLETED,
        result={"result_status": "succeeded"},
    )
    regressed = await store.update_operation(
        operation.operation_id,
        DemoOperationStatus.ACCEPTED,
        result={"duplicate": True},
    )
    await store.update_run_status(run.run_id, DemoRunStatus.DEACTIVATING)
    deactivating = await store.update_run_status(run.run_id, DemoRunStatus.SYNCING)
    await store.update_run_status(run.run_id, DemoRunStatus.COMPLETED, finished=True)
    completed = await store.update_run_status(run.run_id, DemoRunStatus.FAILED)

    assert ignored_peer_failure.status is DemoOperationStatus.ACCEPTED
    assert terminal.status is DemoOperationStatus.COMPLETED
    assert regressed == terminal
    assert deactivating.status is DemoRunStatus.DEACTIVATING
    assert completed.status is DemoRunStatus.COMPLETED
    assert completed.finished_at is not None


async def test_failed_submission_without_workflow_id_can_recover_once() -> None:
    store = InMemoryDemoStateStore()
    run = await _seed(store)
    failed = await store.put_operation(
        DemoOperation(
            operation_id="failed-submission",
            run_id=run.run_id,
            store_key=run.store_key,
            operation_type=DemoOperationType.SYNC,
            status=DemoOperationStatus.FAILED,
            command_id="sync-command",
            lifecycle_generation=7,
        )
    )

    recovered = await store.update_operation(
        failed.operation_id,
        DemoOperationStatus.ACCEPTED,
        workflow_id="store-sync/recovered",
    )

    assert recovered.status is DemoOperationStatus.ACCEPTED
    assert recovered.workflow_id == "store-sync/recovered"
