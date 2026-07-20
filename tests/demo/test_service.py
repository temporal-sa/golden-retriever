from __future__ import annotations

import asyncio
from dataclasses import asdict

import pytest
from temporalio.client import WorkflowUpdateFailedError
from temporalio.exceptions import ApplicationError

from retrieval.demo.config import DemoConfig
from retrieval.demo.fixtures import load_northstar_scenario
from retrieval.demo.models import (
    ApiIdempotencyReceipt,
    DemoConflictError,
    DemoRunStatus,
    DemoSearchHit,
)
from retrieval.demo.service import DemoService, InMemoryTextSearch
from retrieval.demo.store import InMemoryDemoStateStore
from retrieval.temporal.activities.repositories import InMemoryRetrievalRepository
from retrieval.temporal.common.ids import (
    store_controller_workflow_id,
    user_quota_workflow_id,
)
from retrieval.temporal.models.lifecycle import StoreControllerSnapshot, StoreLifecycleState
from retrieval.temporal.models.operations import (
    CommandResult,
    OperationAccepted,
    OperationStatus,
    OperationType,
    ResultStatus,
    SyncCommand,
)


class CapturingGateway:
    def __init__(
        self,
        *,
        terminal_status: OperationStatus = OperationStatus.COMPLETED,
        terminal_result: ResultStatus = ResultStatus.SUCCEEDED,
    ) -> None:
        self.commands: list[SyncCommand] = []
        self.started = False
        self.terminal_status = terminal_status
        self.terminal_result = terminal_result

    async def start(self) -> None:
        self.started = True

    async def ready(self) -> bool:
        return self.started

    async def aclose(self) -> None:
        self.started = False

    async def request_sync(self, command: SyncCommand) -> OperationAccepted:
        self.commands.append(command)
        return OperationAccepted(
            command_id=command.command_id,
            operation_id="temporal-sync-operation",
            workflow_id="store-sync/workflow",
            operation_type=OperationType.SYNC,
            lifecycle_generation=command.expected_generation,
        )

    async def start_deactivation(self, command):
        raise AssertionError(f"unexpected command: {command}")

    async def get_status(self, store_key: str) -> StoreControllerSnapshot:
        return StoreControllerSnapshot(
            store_key=store_key,
            lifecycle_state=StoreLifecycleState.ACTIVE,
            lifecycle_generation=7,
        )

    async def get_operation_result(
        self,
        store_key: str,
        operation_id: str,
    ) -> CommandResult | None:
        del store_key
        command = next(
            (command for command in self.commands if operation_id == "store-sync/workflow"),
            None,
        )
        if command is None:
            return None
        return CommandResult(
            command_id=command.command_id,
            operation_id=operation_id,
            workflow_id=operation_id,
            operation_type=OperationType.SYNC,
            status=self.terminal_status,
            lifecycle_generation=command.expected_generation,
            result_status=self.terminal_result,
        )

    async def start_preflight(self, request) -> str:
        self.preflight_request = request
        return f"retrieval-preflight-{request.request_id}"

    async def get_preflight(self, workflow_id: str):
        return {
            "workflow_id": workflow_id,
            "status": "completed",
            "result": {
                "request_id": self.preflight_request.request_id,
                "provider": "google-drive",
                "files": [{"name": "Roadmap", "searchable": True}],
            },
        }


async def test_preflight_is_stably_identified_and_persisted_in_demo_state() -> None:
    scenario = load_northstar_scenario()
    repository = InMemoryRetrievalRepository()
    state = InMemoryDemoStateStore()
    gateway = CapturingGateway()
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=scenario,
        state_store=state,
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=gateway,
    )
    await service.start()
    try:
        started = await service.start_preflight(idempotency_key="stable-preflight")
        completed = await service.get_preflight(str(started["workflow_id"]))

        assert started["status"] == "running"
        assert completed["status"] == "completed"
        assert completed["result"]["files"][0]["name"] == "Roadmap"
        assert await state.get_preflight(str(started["workflow_id"])) == completed
    finally:
        await service.aclose()


class ReceiptFailureStore(InMemoryDemoStateStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_deactivation_receipt_once = True

    async def put_idempotency_receipt(
        self, receipt: ApiIdempotencyReceipt
    ) -> ApiIdempotencyReceipt:
        if self.fail_deactivation_receipt_once and receipt.scope.endswith(":deactivation"):
            self.fail_deactivation_receipt_once = False
            raise RuntimeError("simulated crash before receipt commit")
        return await super().put_idempotency_receipt(receipt)


class BlockingOperationReceiptStore(InMemoryDemoStateStore):
    def __init__(self) -> None:
        super().__init__()
        self.receipt_waiting = asyncio.Event()
        self.release_receipts = asyncio.Event()

    async def put_idempotency_receipt(
        self, receipt: ApiIdempotencyReceipt
    ) -> ApiIdempotencyReceipt:
        if receipt.scope.endswith(":sync"):
            self.receipt_waiting.set()
            await self.release_receipts.wait()
        return await super().put_idempotency_receipt(receipt)


class FencingGateway(CapturingGateway):
    def __init__(self, repository: InMemoryRetrievalRepository) -> None:
        super().__init__()
        self.repository = repository
        self.deactivation_calls = 0

    async def start_deactivation(self, command):
        self.deactivation_calls += 1
        transition = await self.repository.begin_deactivation(
            command.store_key,
            command.expected_generation,
        )
        return OperationAccepted(
            command_id=command.command_id,
            operation_id="temporal-deactivation-operation",
            workflow_id="deactivate-store/workflow",
            operation_type=OperationType.DEACTIVATION,
            lifecycle_generation=transition.lifecycle_generation,
            duplicate=self.deactivation_calls > 1,
        )


class PreFenceFailureGateway(CapturingGateway):
    async def start_deactivation(self, command):
        self.commands.append(command)
        return OperationAccepted(
            command_id=command.command_id,
            operation_id="temporal-deactivation-operation",
            workflow_id="deactivate-store/pre-fence-failure",
            operation_type=OperationType.DEACTIVATION,
            lifecycle_generation=command.expected_generation,
        )

    async def get_operation_result(
        self,
        store_key: str,
        operation_id: str,
    ) -> CommandResult | None:
        del store_key
        if not self.commands or operation_id != "deactivate-store/pre-fence-failure":
            return None
        command = self.commands[-1]
        return CommandResult(
            command_id=command.command_id,
            operation_id=operation_id,
            workflow_id=operation_id,
            operation_type=OperationType.DEACTIVATION,
            status=self.terminal_status,
            lifecycle_generation=command.expected_generation,
            result_status=self.terminal_result,
        )


class InterleavedGateway(CapturingGateway):
    def __init__(self) -> None:
        super().__init__()
        self.second_arrived = asyncio.Event()
        self.release_second = asyncio.Event()

    async def request_sync(self, command: SyncCommand) -> OperationAccepted:
        self.commands.append(command)
        call_number = len(self.commands)
        if call_number == 1:
            await self.second_arrived.wait()
        else:
            self.second_arrived.set()
            await self.release_second.wait()
        return OperationAccepted(
            command_id=command.command_id,
            operation_id="temporal-sync-operation",
            workflow_id="store-sync/workflow",
            operation_type=OperationType.SYNC,
            lifecycle_generation=command.expected_generation,
            duplicate=call_number > 1,
        )


class ResumeDeactivationGateway(CapturingGateway):
    def __init__(self) -> None:
        super().__init__()
        self.deactivation_commands = []

    async def start_deactivation(self, command):
        self.deactivation_commands.append(command)
        return OperationAccepted(
            command_id=command.command_id,
            operation_id="temporal-deactivation-resume",
            workflow_id="deactivate-store/resume-generation-8",
            operation_type=OperationType.DEACTIVATION,
            lifecycle_generation=command.expected_generation,
        )


class BarrierSearch:
    backend = "barrier"

    def __init__(self) -> None:
        self.arrived = 0
        self.release = asyncio.Event()

    async def search(self, store_key: str, query: str, limit: int = 8):
        del store_key, query, limit
        self.arrived += 1
        call_number = self.arrived
        if self.arrived == 2:
            self.release.set()
        await self.release.wait()
        document_key = "renewal-plan.md" if call_number == 1 else "stakeholders.md"
        return (
            DemoSearchHit(
                document_key=document_key,
                chunk_ordinal=0,
                title=f"Result {call_number}",
                text=f"Evidence {call_number}",
                score=1.0,
            ),
        )


class RejectingGateway(CapturingGateway):
    def __init__(self, *, wrapped: bool) -> None:
        super().__init__()
        self.wrapped = wrapped

    async def request_sync(self, command: SyncCommand) -> OperationAccepted:
        self.commands.append(command)
        rejection = ApplicationError(
            "store is not syncable",
            type="StoreNotSyncable",
            non_retryable=True,
        )
        if self.wrapped:
            raise WorkflowUpdateFailedError(rejection)
        raise rejection


async def test_sync_configures_quota_scope_and_snapshot_exposes_workflow_ids() -> None:
    scenario = load_northstar_scenario()
    repository = InMemoryRetrievalRepository()
    state = InMemoryDemoStateStore()
    gateway = CapturingGateway()
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=scenario,
        state_store=state,
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=gateway,
    )
    await service.start()
    try:
        run = await service.create_run(idempotency_key="service-run")
        first = await service.start_sync(run.run_id, idempotency_key="service-sync")
        completed = await service.get_operation(first.operation_id)
        duplicate = await service.start_sync(run.run_id, idempotency_key="service-sync")
        duplicate_run = await service.create_run(idempotency_key="service-run")
        snapshot = await service.get_snapshot(run.run_id)
        proof = await service.get_proof(run.run_id)

        assert first == duplicate
        assert duplicate.status.value == "accepted"
        assert duplicate_run == run
        assert duplicate_run.status.value == "ready"
        assert completed.status.value == "completed"
        assert len(gateway.commands) == 1
        metadata = gateway.commands[0].metadata
        assert metadata["provider"] == "northstar-scripted"
        assert metadata["quota_class"] == "demo"
        assert metadata["resource_types"] == "files"
        assert metadata["credential_key"] == f"northstar-demo-run:{run.run_id}"
        assert snapshot.controller is not None
        assert snapshot.story_state == "ready"
        assert proof["proof_query"] == (
            f"SELECT * FROM retrieval_demo_ui.generation_proof('{run.store_key}');"
        )
        assert snapshot.controller["controller_workflow_id"] == store_controller_workflow_id(
            run.store_key
        )
        assert snapshot.controller["quota_workflow_ids"] == (
            user_quota_workflow_id(
                "northstar-scripted",
                f"northstar-demo-run:{run.run_id}",
                "demo",
            ),
        )
        assert asdict(first)["result"]["duplicate"] is False
    finally:
        await service.aclose()


async def test_control_receipt_replays_original_response_after_later_mutation() -> None:
    scenario = load_northstar_scenario()
    repository = InMemoryRetrievalRepository()
    state = InMemoryDemoStateStore()
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=scenario,
        state_store=state,
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=CapturingGateway(),
    )
    await service.start()
    try:
        run = await service.create_run(idempotency_key="control-replay-run")
        held = await service.hold_late_document(
            run.run_id,
            idempotency_key="control-replay-hold",
        )
        await repository.begin_deactivation(run.store_key, run.baseline_generation)
        released = await service.release_late_document(
            run.run_id,
            idempotency_key="control-replay-release",
        )
        replayed_hold = await service.hold_late_document(
            run.run_id,
            idempotency_key="control-replay-hold",
        )

        assert released.release_requested is True
        assert released.control_version > held.control_version
        assert replayed_hold == held
        assert replayed_hold.release_requested is False
    finally:
        await service.aclose()


async def test_concurrent_ask_requests_return_the_canonical_receipt_response() -> None:
    scenario = load_northstar_scenario()
    repository = InMemoryRetrievalRepository()
    state = InMemoryDemoStateStore()
    search = BarrierSearch()
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=scenario,
        state_store=state,
        repository=repository,
        search_adapter=search,
        command_gateway=CapturingGateway(),
    )
    await service.start()
    try:
        run = await service.create_run(idempotency_key="concurrent-ask-run")
        first, second = await asyncio.gather(
            service.ask(run.run_id, "renewal priorities", idempotency_key="same-ask"),
            service.ask(run.run_id, "renewal priorities", idempotency_key="same-ask"),
        )
        replay = await service.ask(
            run.run_id,
            "renewal priorities",
            idempotency_key="same-ask",
        )

        assert search.arrived == 2
        assert first == second == replay
        assert len(first.citations) == 1
    finally:
        await service.aclose()


async def test_slow_duplicate_cannot_regress_terminal_operation_or_run() -> None:
    scenario = load_northstar_scenario()
    repository = InMemoryRetrievalRepository()
    state = BlockingOperationReceiptStore()
    gateway = InterleavedGateway()
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=scenario,
        state_store=state,
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=gateway,
    )
    await service.start()
    try:
        run = await service.create_run(idempotency_key="interleaved-run")
        first_task = asyncio.create_task(
            service.start_sync(run.run_id, idempotency_key="interleaved-sync")
        )
        await asyncio.sleep(0)
        second_task = asyncio.create_task(
            service.start_sync(run.run_id, idempotency_key="interleaved-sync")
        )
        await state.receipt_waiting.wait()

        persisted = next(iter(state._operations.values()))
        terminal = await service.get_operation(persisted.operation_id)
        gateway.release_second.set()
        await asyncio.sleep(0)
        state.release_receipts.set()
        first, second = await asyncio.gather(first_task, second_task)

        current = await state.get_operation(persisted.operation_id)
        current_run = await state.get_run(run.run_id)
        assert first == second
        assert terminal.status.value == "completed"
        assert current.status.value == "completed"
        assert current_run.status.value == "ready"
    finally:
        state.release_receipts.set()
        gateway.release_second.set()
        await service.aclose()


async def test_deactivation_retry_repairs_receipt_after_temporal_acceptance() -> None:
    scenario = load_northstar_scenario()
    repository = InMemoryRetrievalRepository()
    state = ReceiptFailureStore()
    gateway = FencingGateway(repository)
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=scenario,
        state_store=state,
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=gateway,
    )
    await service.start()
    try:
        run = await service.create_run(idempotency_key="crash-window-run")

        with pytest.raises(RuntimeError, match="before receipt commit"):
            await service.start_deactivation(
                run.run_id,
                idempotency_key="crash-window-deactivation",
            )

        fenced = await repository.get_store(run.store_key)
        assert fenced.lifecycle_state is StoreLifecycleState.DEACTIVATING
        assert fenced.lifecycle_generation == 8

        recovered = await service.start_deactivation(
            run.run_id,
            idempotency_key="crash-window-deactivation",
        )
        duplicate = await service.start_deactivation(
            run.run_id,
            idempotency_key="crash-window-deactivation",
        )

        assert recovered == duplicate
        assert recovered.workflow_id == "deactivate-store/workflow"
        assert gateway.deactivation_calls == 1
    finally:
        await service.aclose()


@pytest.mark.parametrize(
    ("terminal_status", "terminal_result", "expected"),
    [
        (OperationStatus.FAILED, ResultStatus.FAILED, "failed"),
        (OperationStatus.CANCELED, ResultStatus.CANCELED, "canceled"),
    ],
)
async def test_sync_operation_uses_controller_terminal_result_instead_of_active_ids(
    terminal_status: OperationStatus,
    terminal_result: ResultStatus,
    expected: str,
) -> None:
    scenario = load_northstar_scenario()
    repository = InMemoryRetrievalRepository()
    state = InMemoryDemoStateStore()
    gateway = CapturingGateway(
        terminal_status=terminal_status,
        terminal_result=terminal_result,
    )
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=scenario,
        state_store=state,
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=gateway,
    )
    await service.start()
    try:
        run = await service.create_run(idempotency_key=f"terminal-run:{expected}")
        accepted = await service.start_sync(
            run.run_id,
            idempotency_key=f"terminal-sync:{expected}",
        )

        terminal = await service.get_operation(accepted.operation_id)

        assert terminal.status.value == expected
        assert terminal.result["result_status"] == terminal_result.value
    finally:
        await service.aclose()


async def test_deactivation_pre_fence_failure_uses_controller_terminal_result() -> None:
    scenario = load_northstar_scenario()
    repository = InMemoryRetrievalRepository()
    state = InMemoryDemoStateStore()
    gateway = PreFenceFailureGateway(
        terminal_status=OperationStatus.FAILED,
        terminal_result=ResultStatus.FAILED,
    )
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=scenario,
        state_store=state,
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=gateway,
    )
    await service.start()
    try:
        run = await service.create_run(idempotency_key="pre-fence-run")
        accepted = await service.start_deactivation(
            run.run_id,
            idempotency_key="pre-fence-deactivation",
        )

        terminal = await service.get_operation(accepted.operation_id)

        assert terminal.status.value == "failed"
        assert (await repository.get_store(run.store_key)).lifecycle_state is (
            StoreLifecycleState.ACTIVE
        )
    finally:
        await service.aclose()


async def test_failed_deactivation_can_resume_at_the_same_generation() -> None:
    scenario = load_northstar_scenario()
    repository = InMemoryRetrievalRepository()
    state = InMemoryDemoStateStore()
    gateway = ResumeDeactivationGateway()
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=scenario,
        state_store=state,
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=gateway,
    )
    await service.start()
    try:
        run = await service.create_run(idempotency_key="resume-run")
        await repository.begin_deactivation(run.store_key, run.baseline_generation)
        await repository.mark_deactivation_failed(run.store_key, 8)
        await state.update_run_status(run.run_id, DemoRunStatus.FAILED)

        accepted = await service.start_deactivation(
            run.run_id,
            idempotency_key="resume-generation-8",
        )

        assert accepted.lifecycle_generation == 8
        assert gateway.deactivation_commands[0].expected_generation == 8
        assert (await state.get_run(run.run_id)).status is DemoRunStatus.DEACTIVATING
    finally:
        await service.aclose()


@pytest.mark.parametrize("wrapped", [False, True])
async def test_controller_business_rejection_is_a_stable_conflict(wrapped: bool) -> None:
    scenario = load_northstar_scenario()
    repository = InMemoryRetrievalRepository()
    state = InMemoryDemoStateStore()
    gateway = RejectingGateway(wrapped=wrapped)
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=scenario,
        state_store=state,
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=gateway,
    )
    await service.start()
    try:
        run = await service.create_run(idempotency_key=f"rejection-run:{wrapped}")
        for _ in range(2):
            with pytest.raises(DemoConflictError, match="StoreNotSyncable"):
                await service.start_sync(
                    run.run_id,
                    idempotency_key="rejected-sync",
                )

        assert len(gateway.commands) == 1
        operation = next(iter(state._operations.values()))
        assert operation.status.value == "rejected"
    finally:
        await service.aclose()
