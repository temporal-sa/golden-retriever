from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from temporalio import workflow
from temporalio.exceptions import ApplicationError
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from retrieval.temporal.common.ids import store_sync_workflow_id
from retrieval.temporal.models.lifecycle import StoreControllerState
from retrieval.temporal.models.operations import SyncCommand
from retrieval.temporal.worker import V2_WORKFLOW_TYPES
from retrieval.temporal.workflows import store_controller as controller_module
from retrieval.temporal.workflows.activate_user import ActivateUserWorkflow
from retrieval.temporal.workflows.failed_user_remediation import (
    FailedUserRemediationWorkflow,
)
from retrieval.temporal.workflows.files_page import FilesPageWorkflow
from retrieval.temporal.workflows.resource_pages import ResourcePagesWorkflow
from retrieval.temporal.workflows.root_sync import RootSyncWorkflow
from retrieval.temporal.workflows.store_controller import StoreControllerWorkflow

WORKFLOW_CONTRACTS = (
    (
        StoreControllerWorkflow,
        "StoreControllerWorkflow",
        {
            "deactivation_fenced",
            "operation_status",
            "remediation_finished",
            "remediation_started",
            "request_continue_as_new",
        },
        {"get_status"},
        {"cancel_sync", "request_sync", "start_deactivation"},
    ),
    (
        RootSyncWorkflow,
        "RootSyncWorkflow",
        {"quota_granted"},
        {"get_progress"},
        set(),
    ),
    (
        ResourcePagesWorkflow,
        "ResourcePagesWorkflow",
        {"quota_granted"},
        {"get_progress"},
        set(),
    ),
    (
        FilesPageWorkflow,
        "FilesPageWorkflow",
        set(),
        {"get_status"},
        set(),
    ),
    (
        ActivateUserWorkflow,
        "ActivateUserWorkflow",
        set(),
        {"get_phase"},
        set(),
    ),
    (
        FailedUserRemediationWorkflow,
        "FailedUserRemediationWorkflow",
        set(),
        {"get_progress"},
        set(),
    ),
)


def test_worker_registers_each_audited_workflow_once() -> None:
    audited = {contract[0] for contract in WORKFLOW_CONTRACTS}

    assert audited.issubset(set(V2_WORKFLOW_TYPES))
    assert len(V2_WORKFLOW_TYPES) == 17
    assert len(set(V2_WORKFLOW_TYPES)) == len(V2_WORKFLOW_TYPES)


def test_controller_updates_have_synchronous_validators() -> None:
    definition = workflow._Definition.must_from_class(StoreControllerWorkflow)

    assert all(
        definition.updates[name].validator is not None
        for name in ("request_sync", "cancel_sync", "start_deactivation")
    )


def test_controller_validator_rejects_malformed_sync_before_enqueue() -> None:
    controller = StoreControllerWorkflow(StoreControllerState(store_key="store"))

    with pytest.raises(ApplicationError, match="sync_sequence"):
        controller.validate_request_sync(
            SyncCommand(
                command_id="command",
                store_key="store",
                expected_generation=0,
                sync_sequence=" ",
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("workflow_type", "wire_name", "signals", "queries", "updates"),
    WORKFLOW_CONTRACTS,
)
async def test_workflow_contract_registers_and_prepares_in_sandbox(
    workflow_type: type,
    wire_name: str,
    signals: set[str],
    queries: set[str],
    updates: set[str],
) -> None:
    definition = workflow._Definition.must_from_class(workflow_type)

    assert definition.name == wire_name
    assert set(definition.signals) == signals
    assert set(definition.queries) == queries
    assert set(definition.updates) == updates

    # This imports the workflow in the same restricted runner used by a real
    # sandboxed Worker and catches non-deterministic/import-time violations.
    SandboxedWorkflowRunner().prepare_workflow(definition)


@pytest.mark.asyncio
async def test_controller_starts_sync_once_with_stable_opaque_id_and_abandon_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = StoreControllerState(
        store_key="customer@example.test",
        lifecycle_generation=4,
        enable_search_attributes=True,
    )
    controller = StoreControllerWorkflow(state)
    starts: list[tuple[str, object, dict[str, object]]] = []

    async def fake_start_child(
        workflow_name: str,
        child_input: object,
        **options: object,
    ) -> object:
        starts.append((workflow_name, child_input, options))
        return object()

    monkeypatch.setattr(
        controller_module.workflow,
        "info",
        lambda: SimpleNamespace(workflow_id="store-controller/opaque"),
    )
    monkeypatch.setattr(controller_module.workflow, "now", lambda: None)
    monkeypatch.setattr(
        controller_module.workflow,
        "start_child_workflow",
        fake_start_child,
    )
    command = SyncCommand(
        command_id="command-1",
        store_key=state.store_key,
        expected_generation=4,
        sync_sequence="sync-42",
    )

    accepted = await controller._request_sync(command)
    duplicate = await controller._request_sync(command)

    expected_id = store_sync_workflow_id(
        state.store_key,
        command.expected_generation,
        command.sync_sequence,
    )
    assert accepted.operation_id == expected_id
    assert not accepted.duplicate
    assert duplicate.operation_id == expected_id
    assert duplicate.duplicate
    assert len(starts) == 1
    workflow_name, child_input, options = starts[0]
    assert workflow_name == "RootSyncWorkflow"
    assert child_input.store_key == state.store_key
    assert options["id"] == expected_id
    assert options["parent_close_policy"] is workflow.ParentClosePolicy.ABANDON
    assert options["cancellation_type"] is workflow.ChildWorkflowCancellationType.ABANDON
    assert options["search_attributes"] is not None
    assert state.store_key not in expected_id


@pytest.mark.asyncio
async def test_controller_does_not_swallow_cancellation_during_an_update() -> None:
    state = StoreControllerState(store_key="opaque-store", authority_initialized=True)
    controller = StoreControllerWorkflow(state)
    processing_started = asyncio.Event()

    async def blocked_process(_envelope: object) -> None:
        processing_started.set()
        await asyncio.Future()

    controller._process = blocked_process  # type: ignore[method-assign]
    response = asyncio.get_running_loop().create_future()
    controller._commands.put_nowait(
        controller_module._CommandEnvelope("request_sync", None, response)
    )
    running = asyncio.create_task(controller.run(state))
    await asyncio.wait_for(processing_started.wait(), timeout=1)

    running.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(running, timeout=1)
    assert not response.done()
    response.cancel()


def test_controller_query_is_compact_and_sorts_operation_ids() -> None:
    state = StoreControllerState(store_key="opaque-store", lifecycle_generation=3)
    state.active_syncs = {
        "sync/z": SimpleNamespace(),
        "sync/a": SimpleNamespace(),
    }
    state.active_remediations = {
        "remediation/z": SimpleNamespace(),
        "remediation/a": SimpleNamespace(),
    }

    snapshot = StoreControllerWorkflow(state).get_status()

    assert snapshot.active_sync_ids == ("sync/a", "sync/z")
    assert snapshot.active_remediation_ids == (
        "remediation/a",
        "remediation/z",
    )
    assert not hasattr(snapshot, "recent_command_results")
