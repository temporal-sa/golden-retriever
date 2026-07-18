from __future__ import annotations

from types import SimpleNamespace

import pytest
from temporalio.exceptions import ApplicationError, TemporalError

from retrieval.temporal.common.ids import store_deactivation_workflow_id
from retrieval.temporal.models.lifecycle import (
    LifecycleMutationResult,
    OperationStatusEvent,
    RemediationRegistration,
    RemediationStatusEvent,
    StoreControllerState,
    StoreLifecycleState,
    SyncRegistration,
)
from retrieval.temporal.models.operations import (
    OperationStatus,
    ResultStatus,
    StartDeactivationCommand,
)
from retrieval.temporal.workflows import store_controller as controller_module
from retrieval.temporal.workflows.store_controller import StoreControllerWorkflow


@pytest.mark.parametrize("weight", ["0", "-1", "1001", "nan"])
def test_quota_scope_rejects_unsupported_fairness_weights(weight: str) -> None:
    with pytest.raises(ApplicationError, match="fairness_weight"):
        StoreControllerWorkflow._quota_scope(
            {
                "provider": "provider",
                "credential_key": "opaque-credential",
                "fairness_weight": weight,
            }
        )


@pytest.mark.asyncio
async def test_pre_fence_terminal_failure_restores_active_state() -> None:
    operation_id = "store-deactivation/operation"
    state = StoreControllerState(
        store_key="store",
        lifecycle_state=StoreLifecycleState.DEACTIVATING,
        lifecycle_generation=4,
        active_deactivation_id=operation_id,
        active_deactivation_fenced=False,
        authority_initialized=True,
    )
    controller = StoreControllerWorkflow(state)

    await controller._operation_status(
        OperationStatusEvent(
            operation_id=operation_id,
            workflow_id=operation_id,
            lifecycle_generation=4,
            status=OperationStatus.FAILED,
            result_status=ResultStatus.FAILED,
        )
    )

    assert state.active_deactivation_id is None
    assert state.lifecycle_generation == 4
    assert state.lifecycle_state is StoreLifecycleState.ACTIVE


@pytest.mark.asyncio
async def test_pre_fence_terminal_failure_preserves_remediation_syncing_state() -> None:
    operation_id = "store-deactivation/operation"
    remediation_id = "failed-user-remediation/active"
    state = StoreControllerState(
        store_key="store",
        lifecycle_state=StoreLifecycleState.DEACTIVATING,
        lifecycle_generation=4,
        active_remediations={
            remediation_id: RemediationRegistration(
                operation_id=remediation_id,
                workflow_id=remediation_id,
                lifecycle_generation=4,
                sync_sequence="sequence",
            )
        },
        active_deactivation_id=operation_id,
        active_deactivation_fenced=False,
        authority_initialized=True,
    )

    await StoreControllerWorkflow(state)._operation_status(
        OperationStatusEvent(
            operation_id=operation_id,
            workflow_id=operation_id,
            lifecycle_generation=4,
            status=OperationStatus.FAILED,
            result_status=ResultStatus.FAILED,
        )
    )

    assert state.lifecycle_generation == 4
    assert state.lifecycle_state is StoreLifecycleState.SYNCING


@pytest.mark.asyncio
async def test_terminal_failure_infers_committed_fence_from_advanced_generation() -> None:
    operation_id = "store-deactivation/operation"
    state = StoreControllerState(
        store_key="store",
        lifecycle_state=StoreLifecycleState.DEACTIVATING,
        lifecycle_generation=4,
        active_deactivation_id=operation_id,
        active_deactivation_fenced=False,
        authority_initialized=True,
    )
    controller = StoreControllerWorkflow(state)

    await controller._operation_status(
        OperationStatusEvent(
            operation_id=operation_id,
            workflow_id=operation_id,
            lifecycle_generation=5,
            status=OperationStatus.FAILED,
            result_status=ResultStatus.FAILED,
        )
    )

    assert state.lifecycle_generation == 5
    assert state.lifecycle_state is StoreLifecycleState.DEACTIVATION_FAILED


@pytest.mark.asyncio
async def test_duplicate_active_deactivation_uses_fenced_generation() -> None:
    operation_id = "store-deactivation/operation"
    state = StoreControllerState(
        store_key="store",
        lifecycle_state=StoreLifecycleState.DEACTIVATING,
        lifecycle_generation=5,
        active_deactivation_id=operation_id,
        active_deactivation_fenced=True,
        authority_initialized=True,
    )
    controller = StoreControllerWorkflow(state)

    accepted = await controller._start_deactivation(
        StartDeactivationCommand(
            command_id="duplicate-command",
            store_key="store",
            expected_generation=4,
        )
    )

    assert accepted.duplicate is True
    assert accepted.lifecycle_generation == 5


@pytest.mark.asyncio
async def test_failed_deactivation_resumes_same_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = StoreControllerState(
        store_key="store",
        lifecycle_state=StoreLifecycleState.DEACTIVATION_FAILED,
        lifecycle_generation=5,
        authority_initialized=True,
    )
    controller = StoreControllerWorkflow(state)
    started: list[tuple[str, object, str]] = []

    async def capture_start(
        workflow_name: str,
        command: object,
        *,
        id: str,
        **_options: object,
    ) -> object:
        started.append((workflow_name, command, id))
        return object()

    monkeypatch.setattr(controller_module.workflow, "start_child_workflow", capture_start)
    monkeypatch.setattr(
        controller_module.workflow,
        "info",
        lambda: SimpleNamespace(workflow_id="store-controller/store"),
    )

    accepted = await controller._start_deactivation(
        StartDeactivationCommand(
            command_id="resume-command",
            store_key="store",
            expected_generation=5,
        )
    )

    expected_id = store_deactivation_workflow_id("store", 5)
    assert accepted.lifecycle_generation == 5
    assert accepted.workflow_id == expected_id
    assert state.active_deactivation_id == expected_id
    assert state.active_deactivation_fenced is True
    assert len(started) == 1
    workflow_name, child_input, workflow_id = started[0]
    assert workflow_name == "DeactivateStoreWorkflow"
    assert workflow_id == expected_id
    assert child_input.expected_generation == 5  # type: ignore[attr-defined]
    assert child_input.resume_same_generation is True  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_controller_bootstraps_generation_and_state_from_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = StoreControllerState(store_key="store")
    controller = StoreControllerWorkflow(state)

    async def fake_execute_activity(
        name: str, fence: object, **_options: object
    ) -> LifecycleMutationResult:
        assert name == "validate_lifecycle_generation"
        assert set(fence.allowed_states) == set(StoreLifecycleState)  # type: ignore[attr-defined]
        return LifecycleMutationResult(
            store_key="store",
            expected_generation=0,
            authoritative_generation=7,
            status=ResultStatus.STALE_GENERATION,
            lifecycle_state=StoreLifecycleState.INACTIVE,
        )

    monkeypatch.setattr(
        controller_module.workflow,
        "execute_activity",
        fake_execute_activity,
    )

    await controller._initialize_authority()

    assert state.authority_initialized is True
    assert state.lifecycle_generation == 7
    assert state.lifecycle_state is StoreLifecycleState.INACTIVE


@pytest.mark.asyncio
async def test_controller_authority_bootstrap_preserves_active_sync_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = SyncRegistration(
        operation_id="sync",
        workflow_id="sync",
        lifecycle_generation=3,
        sync_sequence="sequence",
    )
    state = StoreControllerState(
        store_key="store",
        lifecycle_state=StoreLifecycleState.SYNCING,
        lifecycle_generation=3,
        active_syncs={"sync": registration},
    )
    controller = StoreControllerWorkflow(state)

    async def fake_execute_activity(
        _name: str, _fence: object, **_options: object
    ) -> LifecycleMutationResult:
        return LifecycleMutationResult(
            store_key="store",
            expected_generation=3,
            authoritative_generation=3,
            status=ResultStatus.SUCCEEDED,
            lifecycle_state=StoreLifecycleState.ACTIVE,
        )

    monkeypatch.setattr(controller_module.workflow, "execute_activity", fake_execute_activity)

    await controller._initialize_authority()

    assert state.lifecycle_state is StoreLifecycleState.SYNCING


class _UnavailableExternalHandle:
    async def signal(self, *_args: object, **_kwargs: object) -> None:
        raise TemporalError("execution is closed")

    async def cancel(self) -> None:
        raise TemporalError("execution is closed")


@pytest.mark.asyncio
async def test_closed_deactivation_does_not_fail_drain_forwarding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = StoreControllerState(
        store_key="store",
        active_deactivation_id="deactivation",
        authority_initialized=True,
    )
    controller = StoreControllerWorkflow(state)
    monkeypatch.setattr(
        controller_module.workflow,
        "get_external_workflow_handle",
        lambda _workflow_id: _UnavailableExternalHandle(),
    )
    monkeypatch.setattr(
        controller_module.workflow,
        "logger",
        SimpleNamespace(warning=lambda *_args: None),
    )

    await controller._forward_drained(
        OperationStatusEvent(
            operation_id="sync",
            workflow_id="sync",
            lifecycle_generation=0,
            status=OperationStatus.COMPLETED,
        )
    )


@pytest.mark.asyncio
async def test_closed_late_remediation_does_not_fail_controller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = StoreControllerState(
        store_key="store",
        lifecycle_state=StoreLifecycleState.DEACTIVATING,
        active_deactivation_id="deactivation",
        authority_initialized=True,
    )
    controller = StoreControllerWorkflow(state)
    monkeypatch.setattr(controller_module.workflow, "now", lambda: None)
    monkeypatch.setattr(
        controller_module.workflow,
        "get_external_workflow_handle",
        lambda _workflow_id: _UnavailableExternalHandle(),
    )
    monkeypatch.setattr(
        controller_module.workflow,
        "logger",
        SimpleNamespace(warning=lambda *_args: None),
    )

    await controller._remediation_started(
        RemediationStatusEvent(
            operation_id="remediation",
            workflow_id="remediation",
            lifecycle_generation=0,
            sync_sequence="sequence",
            status=OperationStatus.RUNNING,
        )
    )

    assert "remediation" in state.active_remediations


@pytest.mark.asyncio
async def test_remediation_finished_is_idempotent_and_forwards_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = StoreControllerState(
        store_key="store",
        active_remediations={
            "remediation": RemediationRegistration(
                operation_id="remediation",
                workflow_id="remediation",
                lifecycle_generation=2,
                sync_sequence="sequence",
            )
        },
        authority_initialized=True,
    )
    controller = StoreControllerWorkflow(state)
    drained: list[OperationStatusEvent] = []

    async def capture_drained(event: OperationStatusEvent) -> None:
        drained.append(event)

    monkeypatch.setattr(controller, "_forward_drained", capture_drained)
    event = RemediationStatusEvent(
        operation_id="remediation",
        workflow_id="remediation",
        lifecycle_generation=2,
        sync_sequence="sequence",
        status=OperationStatus.COMPLETED,
        result_status=ResultStatus.SUCCEEDED,
    )

    await controller._remediation_finished(event)
    await controller._remediation_finished(event)

    assert state.active_remediations == {}
    assert len(drained) == 1
    assert drained[0].result_status is ResultStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_continue_as_new_wait_wakes_to_drain_new_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = StoreControllerState(
        store_key="store",
        continue_as_new_requested=True,
        authority_initialized=True,
    )
    controller = StoreControllerWorkflow(state)
    monkeypatch.setattr(
        controller_module.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: False),
    )
    monkeypatch.setattr(
        controller_module.workflow,
        "all_handlers_finished",
        lambda: False,
    )

    async def fake_wait_condition(predicate: object) -> None:
        controller._commands.put_nowait(controller_module._CommandEnvelope("continue_as_new", None))
        assert predicate() is True  # type: ignore[operator]

    def unexpected_continue_as_new(_state: object) -> None:
        raise AssertionError("queued command must be drained before Continue-As-New")

    monkeypatch.setattr(
        controller_module.workflow,
        "wait_condition",
        fake_wait_condition,
    )
    monkeypatch.setattr(
        controller_module.workflow,
        "continue_as_new",
        unexpected_continue_as_new,
    )

    await controller._maybe_continue_as_new()

    assert not controller._commands.empty()
    assert state.authority_initialized is True
