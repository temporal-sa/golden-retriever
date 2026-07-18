from __future__ import annotations

import asyncio

import pytest

from retrieval.temporal.models.lifecycle import (
    CleanupWorkflowInput,
    DeactivationInput,
    DeactivationPhase,
    DeactivationResult,
    NewGeneration,
    ResumeStoreDeactivation,
)
from retrieval.temporal.models.operations import OperationStatus, ResultStatus
from retrieval.temporal.workflows import cleanup as cleanup_module
from retrieval.temporal.workflows import deactivate_store as deactivation_module
from retrieval.temporal.workflows.cleanup import CleanupResult, CleanupUsersWorkflow
from retrieval.temporal.workflows.deactivate_store import (
    DeactivateStoreWorkflow,
    DeactivationAction,
    DeactivationOrderGuard,
    deactivation_plan,
)


def test_deactivation_plan_fences_before_any_cancellation_or_cleanup() -> None:
    plan = deactivation_plan()
    fence_index = plan.index(DeactivationAction.FENCE_COMMITTED)

    assert fence_index == 0
    assert fence_index < plan.index(DeactivationAction.OWNED_WORK_CANCEL_REQUESTED)
    assert fence_index < plan.index(DeactivationAction.OLD_QUOTA_INVALIDATED)
    assert plan.index(DeactivationAction.DRAIN_WAIT_FINISHED) < plan.index(
        DeactivationAction.USERS_CLEANED
    )
    assert plan.index(DeactivationAction.OBJECTS_REMOVED) < plan.index(
        DeactivationAction.INACTIVE_COMMITTED
    )


def test_order_guard_rejects_cancel_before_fence() -> None:
    guard = DeactivationOrderGuard()

    with pytest.raises(RuntimeError, match="fence"):
        guard.ensure_fenced()
    with pytest.raises(RuntimeError, match="fence_committed"):
        guard.advance(DeactivationAction.OWNED_WORK_CANCEL_REQUESTED)


def test_real_workflow_guard_accepts_only_the_authoritative_plan() -> None:
    guard = DeactivationOrderGuard()

    for action in deactivation_plan():
        guard.advance(action)

    assert guard.completed == deactivation_plan()
    assert guard.fenced is True
    with pytest.raises(RuntimeError, match="terminal"):
        guard.advance(DeactivationAction.CONTROLLER_TERMINAL)


@pytest.mark.asyncio
async def test_post_fence_task_reaches_terminal_result_after_outer_cancellation() -> None:
    workflow_instance = DeactivateStoreWorkflow()
    release_cleanup = asyncio.Event()
    result = DeactivationResult(
        store_key="opaque-store",
        operation_id="deactivation-operation",
        lifecycle_generation=2,
        status=ResultStatus.SUCCEEDED,
        phase=DeactivationPhase.COMPLETED,
    )

    async def protected_cleanup() -> DeactivationResult:
        await release_cleanup.wait()
        return result

    cleanup_task = asyncio.create_task(protected_cleanup())
    outer_task = asyncio.create_task(workflow_instance._await_protected(cleanup_task))
    await asyncio.sleep(0)

    outer_task.cancel()
    release_cleanup.set()

    assert await outer_task == result
    assert cleanup_task.done()
    assert not cleanup_task.cancelled()


@pytest.mark.asyncio
async def test_pre_fence_failure_emits_terminal_status_at_original_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_instance = DeactivateStoreWorkflow()
    terminal: list[tuple[int, OperationStatus, ResultStatus]] = []

    async def failing_fence(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("fence rejected")

    async def capture_terminal(
        _command: DeactivationInput,
        generation: NewGeneration | int,
        *,
        operation_status: OperationStatus,
        result_status: ResultStatus,
        message: str | None,
    ) -> None:
        assert message == "fence rejected"
        assert isinstance(generation, int)
        terminal.append((generation, operation_status, result_status))

    monkeypatch.setattr(deactivation_module, "_execute_lifecycle_activity", failing_fence)
    monkeypatch.setattr(workflow_instance, "_signal_controller_terminal", capture_terminal)
    monkeypatch.setattr(deactivation_module.workflow, "now", lambda: None)
    command = DeactivationInput(
        store_key="store",
        expected_generation=4,
        command_id="command",
        operation_id="operation",
    )

    result = await workflow_instance.run(command)

    assert result.status is ResultStatus.FAILED
    assert result.lifecycle_generation == 4
    assert terminal == [(4, OperationStatus.FAILED, ResultStatus.FAILED)]


@pytest.mark.asyncio
async def test_resume_uses_same_generation_fence_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_instance = DeactivateStoreWorkflow()
    generation = NewGeneration(
        store_key="store",
        previous_generation=4,
        lifecycle_generation=5,
    )
    completed = DeactivationResult(
        store_key="store",
        operation_id="operation",
        lifecycle_generation=5,
        status=ResultStatus.SUCCEEDED,
        phase=DeactivationPhase.COMPLETED,
    )
    fenced: list[tuple[str, object]] = []

    async def capture_fence(
        name: str,
        command: object,
        _result_type: object,
    ) -> NewGeneration:
        fenced.append((name, command))
        return generation

    async def finish_cleanup(
        _command: DeactivationInput,
        received: NewGeneration,
    ) -> DeactivationResult:
        assert received == generation
        return completed

    monkeypatch.setattr(deactivation_module, "_execute_lifecycle_activity", capture_fence)
    monkeypatch.setattr(workflow_instance, "_run_after_fence", finish_cleanup)

    result = await workflow_instance.run(
        DeactivationInput(
            store_key="store",
            expected_generation=5,
            command_id="resume-command",
            operation_id="operation",
            resume_same_generation=True,
        )
    )

    assert result == completed
    assert fenced == [
        (
            "resume_store_deactivation",
            ResumeStoreDeactivation(store_key="store", lifecycle_generation=5),
        )
    ]


@pytest.mark.asyncio
async def test_cancellation_during_fence_wait_cannot_lose_committed_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_instance = DeactivateStoreWorkflow()
    fence_started = asyncio.Event()
    allow_fence_result = asyncio.Event()
    generation = NewGeneration(
        store_key="store",
        previous_generation=4,
        lifecycle_generation=5,
    )
    completed = DeactivationResult(
        store_key="store",
        operation_id="operation",
        lifecycle_generation=5,
        status=ResultStatus.SUCCEEDED,
        phase=DeactivationPhase.COMPLETED,
    )

    async def delayed_fence(*_args: object, **_kwargs: object) -> NewGeneration:
        fence_started.set()
        await allow_fence_result.wait()
        return generation

    async def finish_cleanup(
        _command: DeactivationInput, received: NewGeneration
    ) -> DeactivationResult:
        assert received == generation
        return completed

    monkeypatch.setattr(deactivation_module, "_execute_lifecycle_activity", delayed_fence)
    monkeypatch.setattr(workflow_instance, "_run_after_fence", finish_cleanup)
    command = DeactivationInput(
        store_key="store",
        expected_generation=4,
        command_id="command",
        operation_id="operation",
    )
    running = asyncio.create_task(workflow_instance.run(command))
    await fence_started.wait()

    running.cancel()
    await asyncio.sleep(0)
    allow_fence_result.set()

    assert await running == completed
    assert workflow_instance._order.fenced is True


@pytest.mark.asyncio
async def test_cleanup_batch_drains_siblings_after_one_child_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slow_child_started = asyncio.Event()
    release_slow_child = asyncio.Event()

    async def fake_child(
        _workflow: object,
        command: CleanupWorkflowInput,
        **_options: object,
    ) -> CleanupResult:
        if command.user_keys == ("failing",):
            raise RuntimeError("child failed")
        slow_child_started.set()
        await release_slow_child.wait()
        return CleanupResult(
            store_key=command.store_key,
            expected_generation=command.lifecycle_generation,
            status=ResultStatus.SUCCEEDED,
            affected=1,
        )

    monkeypatch.setattr(
        cleanup_module.workflow,
        "execute_child_workflow",
        fake_child,
    )
    running = asyncio.create_task(
        CleanupUsersWorkflow().run(
            CleanupWorkflowInput(
                store_key="store",
                lifecycle_generation=2,
                user_keys=("failing", "slow"),
                user_concurrency=2,
            )
        )
    )
    await slow_child_started.wait()
    await asyncio.sleep(0)

    assert not running.done()
    release_slow_child.set()
    result = await running

    assert result.status is ResultStatus.FAILED
    assert result.affected == 1
