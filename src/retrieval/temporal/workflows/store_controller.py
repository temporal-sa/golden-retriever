"""Long-lived, command-serialized store lifecycle controller."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Literal

from temporalio import workflow
from temporalio.exceptions import ApplicationError, TemporalError

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.common.ids import (
        store_deactivation_workflow_id,
        store_sync_workflow_id,
        user_quota_workflow_id,
    )
    from retrieval.temporal.common.search_attributes import operation_search_attributes
    from retrieval.temporal.models.lifecycle import (
        DeactivationFencedEvent,
        DeactivationInput,
        LifecycleFence,
        LifecycleMutationResult,
        OperationDrained,
        OperationStatusEvent,
        RemediationRegistration,
        RemediationStatusEvent,
        StoreControllerSnapshot,
        StoreControllerState,
        StoreLifecycleState,
        SyncRegistration,
    )
    from retrieval.temporal.models.operations import (
        CancellationAccepted,
        CancelSyncCommand,
        CommandResult,
        OperationAccepted,
        OperationStatus,
        OperationType,
        ResultStatus,
        StartDeactivationCommand,
        SyncCommand,
    )
    from retrieval.temporal.models.quota import QuotaScope
    from retrieval.temporal.models.sync import StoreSyncInput, SyncMode
    from retrieval.temporal.workflows._policies import metadata_activity_options


@dataclass
class _CommandEnvelope:
    kind: Literal[
        "request_sync",
        "cancel_sync",
        "start_deactivation",
        "operation_status",
        "remediation_started",
        "remediation_finished",
        "deactivation_fenced",
        "continue_as_new",
    ]
    payload: Any
    response: asyncio.Future[Any] | None = None


@workflow.defn(name="StoreControllerWorkflow")
class StoreControllerWorkflow:
    @workflow.init
    def __init__(self, initial_state: StoreControllerState) -> None:
        self._state = initial_state
        self._commands: asyncio.Queue[_CommandEnvelope] = asyncio.Queue()
        self._processing_command = False

    def _enqueue_update(self, kind: str, payload: Any) -> asyncio.Future[Any]:
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._commands.put_nowait(_CommandEnvelope(kind, payload, future))  # type: ignore[arg-type]
        return future

    @workflow.update(name="request_sync")
    async def request_sync(self, command: SyncCommand) -> OperationAccepted:
        return await self._enqueue_update("request_sync", command)

    @workflow.update(name="cancel_sync")
    async def cancel_sync(self, command: CancelSyncCommand) -> CancellationAccepted:
        return await self._enqueue_update("cancel_sync", command)

    @workflow.update(name="start_deactivation")
    async def start_deactivation(self, command: StartDeactivationCommand) -> OperationAccepted:
        return await self._enqueue_update("start_deactivation", command)

    @request_sync.validator
    def validate_request_sync(self, command: SyncCommand) -> None:
        self._validate_command_identity(command.command_id, command.store_key)
        self._validate_generation(command.expected_generation)
        if not command.sync_sequence.strip():
            raise ApplicationError(
                "sync_sequence must not be empty",
                type="InvalidSyncCommand",
                non_retryable=True,
            )

    @cancel_sync.validator
    def validate_cancel_sync(self, command: CancelSyncCommand) -> None:
        self._validate_command_identity(command.command_id, command.store_key)
        if not command.operation_id.strip():
            raise ApplicationError(
                "operation_id must not be empty",
                type="InvalidCancelCommand",
                non_retryable=True,
            )

    @start_deactivation.validator
    def validate_start_deactivation(self, command: StartDeactivationCommand) -> None:
        self._validate_command_identity(command.command_id, command.store_key)
        self._validate_generation(command.expected_generation)

    @workflow.signal(name="operation_status")
    def operation_status(self, event: OperationStatusEvent) -> None:
        self._commands.put_nowait(_CommandEnvelope("operation_status", event))

    @workflow.signal(name="remediation_started")
    def remediation_started(self, event: RemediationStatusEvent) -> None:
        self._commands.put_nowait(_CommandEnvelope("remediation_started", event))

    @workflow.signal(name="remediation_finished")
    def remediation_finished(self, event: RemediationStatusEvent) -> None:
        self._commands.put_nowait(_CommandEnvelope("remediation_finished", event))

    @workflow.signal(name="deactivation_fenced")
    def deactivation_fenced(self, event: DeactivationFencedEvent) -> None:
        self._commands.put_nowait(_CommandEnvelope("deactivation_fenced", event))

    @workflow.signal(name="request_continue_as_new")
    def request_continue_as_new(self) -> None:
        self._commands.put_nowait(_CommandEnvelope("continue_as_new", None))

    @workflow.query(name="get_status")
    def get_status(self) -> StoreControllerSnapshot:
        return StoreControllerSnapshot(
            store_key=self._state.store_key,
            lifecycle_state=self._state.lifecycle_state,
            lifecycle_generation=self._state.lifecycle_generation,
            active_sync_ids=tuple(sorted(self._state.active_syncs)),
            active_remediation_ids=tuple(sorted(self._state.active_remediations)),
            active_deactivation_id=self._state.active_deactivation_id,
            recent_command_count=len(self._state.recent_command_results),
        )

    def _validate_store(self, store_key: str) -> None:
        if store_key != self._state.store_key:
            raise ApplicationError(
                "command store does not match controller",
                type="ControllerStoreMismatch",
                non_retryable=True,
            )

    def _validate_command_identity(self, command_id: str, store_key: str) -> None:
        self._validate_store(store_key)
        if not command_id.strip():
            raise ApplicationError(
                "command_id must not be empty",
                type="InvalidCommand",
                non_retryable=True,
            )

    @staticmethod
    def _validate_generation(generation: int) -> None:
        if generation < 0:
            raise ApplicationError(
                "expected_generation must not be negative",
                type="InvalidCommand",
                non_retryable=True,
            )

    def _remember(self, result: CommandResult) -> None:
        recent = self._state.recent_command_results
        recent[result.command_id] = result
        overflow = len(recent) - max(1, self._state.command_dedup_window_size)
        for command_id in list(recent)[: max(0, overflow)]:
            del recent[command_id]

    def _duplicate_operation(self, result: CommandResult) -> OperationAccepted:
        return OperationAccepted(
            command_id=result.command_id,
            operation_id=result.operation_id,
            workflow_id=result.workflow_id or result.operation_id,
            operation_type=result.operation_type,
            lifecycle_generation=result.lifecycle_generation,
            duplicate=True,
        )

    @staticmethod
    def _quota_scope(metadata: dict[str, str]) -> QuotaScope | None:
        provider = metadata.get("provider")
        credential_key = metadata.get("credential_key")
        if not provider or not credential_key:
            return None
        try:
            weight = float(metadata.get("fairness_weight", "1"))
        except ValueError as exc:
            raise ApplicationError(
                "fairness_weight must be numeric",
                type="InvalidSyncCommand",
                non_retryable=True,
            ) from exc
        if not 0.001 <= weight <= 1000:
            raise ApplicationError(
                "fairness_weight must be between 0.001 and 1000",
                type="InvalidSyncCommand",
                non_retryable=True,
            )
        return QuotaScope(
            provider=provider,
            credential_key=credential_key,
            quota_class=metadata.get("quota_class", "default"),
            fairness_weight=weight,
        )

    @staticmethod
    def _positive_int(metadata: dict[str, str], key: str, default: int) -> int:
        try:
            value = int(metadata.get(key, str(default)))
        except ValueError as exc:
            raise ApplicationError(
                f"{key} must be an integer",
                type="InvalidSyncCommand",
                non_retryable=True,
            ) from exc
        if value <= 0:
            raise ApplicationError(
                f"{key} must be positive",
                type="InvalidSyncCommand",
                non_retryable=True,
            )
        return value

    def _sync_input(self, command: SyncCommand) -> StoreSyncInput:
        metadata = command.metadata
        scope = self._quota_scope(metadata)
        mode_value = metadata.get("mode", SyncMode.ORDINARY.value)
        try:
            mode = SyncMode(mode_value)
        except ValueError as exc:
            raise ApplicationError(
                f"unsupported sync mode {mode_value!r}",
                type="InvalidSyncCommand",
                non_retryable=True,
            ) from exc
        resources = tuple(
            item.strip()
            for item in metadata.get("resource_types", "files").split(",")
            if item.strip()
        )
        return StoreSyncInput(
            store_key=command.store_key,
            lifecycle_generation=command.expected_generation,
            sync_sequence=command.sync_sequence,
            quota_scope=scope,
            work_class=command.work_class,
            mode=mode,
            resource_types=resources or ("files",),
            max_active_users=self._positive_int(metadata, "max_active_users", 20),
            user_page_size=self._positive_int(metadata, "user_page_size", 100),
            round_user_window_size=self._positive_int(metadata, "round_user_window_size", 20),
            round_page_slice_size=self._positive_int(metadata, "round_page_slice_size", 5),
            resource_concurrency=self._positive_int(metadata, "resource_concurrency", 8),
            files_page_window_size=self._positive_int(metadata, "files_page_window_size", 5),
            files_per_page_concurrency=self._positive_int(
                metadata, "files_per_page_concurrency", 10
            ),
            document_ingestion_concurrency=self._positive_int(
                metadata, "document_ingestion_concurrency", 20
            ),
            provider_page_size=self._positive_int(metadata, "provider_page_size", 100),
            provider_task_queue=metadata.get("provider_task_queue", "retrieval-provider-v2"),
            priority_fairness_enabled=metadata.get("priority_fairness_enabled", "false").lower()
            in {"1", "true", "yes", "on"},
            controller_workflow_id=workflow.info().workflow_id,
            activation_recent_page_cap=self._positive_int(
                metadata, "activation_recent_page_cap", 5
            ),
            enable_search_attributes=self._state.enable_search_attributes,
        )

    async def _request_sync(self, command: SyncCommand) -> OperationAccepted:
        self._validate_store(command.store_key)
        duplicate = self._state.recent_command_results.get(command.command_id)
        if duplicate is not None:
            return self._duplicate_operation(duplicate)
        if command.expected_generation != self._state.lifecycle_generation:
            raise ApplicationError(
                "sync lifecycle generation is stale",
                type="StaleLifecycleGeneration",
                non_retryable=True,
            )
        if self._state.active_deactivation_id is not None or self._state.lifecycle_state in {
            StoreLifecycleState.DEACTIVATING,
            StoreLifecycleState.INACTIVE,
            StoreLifecycleState.DEACTIVATION_FAILED,
        }:
            raise ApplicationError(
                f"store state {self._state.lifecycle_state.value} rejects sync",
                type="StoreNotSyncable",
                non_retryable=True,
            )
        if self._state.active_syncs:
            raise ApplicationError(
                "a sync is already active for this store",
                type="SyncAlreadyRunning",
                non_retryable=True,
            )

        sync_input = self._sync_input(command)
        workflow_id = store_sync_workflow_id(
            command.store_key,
            command.expected_generation,
            command.sync_sequence,
        )
        search_attributes = None
        if self._state.enable_search_attributes:
            search_attributes = operation_search_attributes(
                store_key=command.store_key,
                lifecycle_generation=command.expected_generation,
                operation_type=OperationType.SYNC,
                sync_sequence=command.sync_sequence,
                quota_scope=sync_input.quota_scope,
                work_class=command.work_class,
                current_phase="accepted",
            )
        await workflow.start_child_workflow(
            "RootSyncWorkflow",
            sync_input,
            id=workflow_id,
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            cancellation_type=workflow.ChildWorkflowCancellationType.ABANDON,
            search_attributes=search_attributes,
        )
        self._state.active_syncs[workflow_id] = SyncRegistration(
            operation_id=workflow_id,
            workflow_id=workflow_id,
            lifecycle_generation=command.expected_generation,
            sync_sequence=command.sync_sequence,
            status=OperationStatus.RUNNING,
            started_at=workflow.now(),
        )
        if sync_input.quota_scope is not None:
            quota_workflow_id = user_quota_workflow_id(
                sync_input.quota_scope.provider,
                sync_input.quota_scope.credential_key,
                sync_input.quota_scope.quota_class,
            )
            if quota_workflow_id not in self._state.quota_workflow_ids:
                self._state.quota_workflow_ids.append(quota_workflow_id)
        self._state.lifecycle_state = StoreLifecycleState.SYNCING
        result = CommandResult(
            command_id=command.command_id,
            operation_id=workflow_id,
            workflow_id=workflow_id,
            operation_type=OperationType.SYNC,
            status=OperationStatus.ACCEPTED,
            lifecycle_generation=command.expected_generation,
        )
        self._remember(result)
        return OperationAccepted(
            command_id=command.command_id,
            operation_id=workflow_id,
            workflow_id=workflow_id,
            operation_type=OperationType.SYNC,
            lifecycle_generation=command.expected_generation,
            duplicate=False,
        )

    async def _cancel_sync(self, command: CancelSyncCommand) -> CancellationAccepted:
        self._validate_store(command.store_key)
        duplicate = self._state.recent_command_results.get(command.command_id)
        if duplicate is not None:
            return CancellationAccepted(
                command_id=command.command_id,
                operation_id=duplicate.operation_id,
                accepted=bool(duplicate.details.get("accepted", True)),
                duplicate=True,
                reason=duplicate.message,
            )
        registration = self._state.active_syncs.get(command.operation_id)
        accepted = registration is not None
        if registration is not None:
            await workflow.get_external_workflow_handle(registration.workflow_id).cancel()
        result = CommandResult(
            command_id=command.command_id,
            operation_id=command.operation_id,
            workflow_id=registration.workflow_id if registration else None,
            operation_type=OperationType.SYNC,
            status=OperationStatus.ACCEPTED if accepted else OperationStatus.REJECTED,
            lifecycle_generation=self._state.lifecycle_generation,
            message=None if accepted else "sync is not active",
            details={"accepted": accepted, "kind": "cancellation"},
        )
        self._remember(result)
        return CancellationAccepted(
            command_id=command.command_id,
            operation_id=command.operation_id,
            accepted=accepted,
            reason=result.message,
        )

    async def _start_deactivation(self, command: StartDeactivationCommand) -> OperationAccepted:
        self._validate_store(command.store_key)
        duplicate = self._state.recent_command_results.get(command.command_id)
        if duplicate is not None:
            return self._duplicate_operation(duplicate)
        if self._state.active_deactivation_id is not None:
            workflow_id = self._state.active_deactivation_id
            operation_generation = (
                self._state.lifecycle_generation
                if self._state.active_deactivation_fenced
                else self._state.lifecycle_generation + 1
            )
            result = CommandResult(
                command_id=command.command_id,
                operation_id=workflow_id,
                workflow_id=workflow_id,
                operation_type=OperationType.DEACTIVATION,
                status=OperationStatus.ACCEPTED,
                lifecycle_generation=operation_generation,
            )
            self._remember(result)
            return self._duplicate_operation(result)
        if self._state.lifecycle_state is StoreLifecycleState.INACTIVE and (
            command.expected_generation == self._state.lifecycle_generation
            or command.expected_generation + 1 == self._state.lifecycle_generation
        ):
            workflow_id = store_deactivation_workflow_id(
                command.store_key, self._state.lifecycle_generation
            )
            result = CommandResult(
                command_id=command.command_id,
                operation_id=workflow_id,
                workflow_id=workflow_id,
                operation_type=OperationType.DEACTIVATION,
                status=OperationStatus.COMPLETED,
                lifecycle_generation=self._state.lifecycle_generation,
            )
            self._remember(result)
            return self._duplicate_operation(result)
        if (
            self._state.lifecycle_state is StoreLifecycleState.DEACTIVATION_FAILED
            and command.expected_generation + 1 == self._state.lifecycle_generation
        ):
            workflow_id = store_deactivation_workflow_id(
                command.store_key, self._state.lifecycle_generation
            )
            result = CommandResult(
                command_id=command.command_id,
                operation_id=workflow_id,
                workflow_id=workflow_id,
                operation_type=OperationType.DEACTIVATION,
                status=OperationStatus.FAILED,
                lifecycle_generation=self._state.lifecycle_generation,
            )
            self._remember(result)
            return self._duplicate_operation(result)
        if command.expected_generation != self._state.lifecycle_generation:
            raise ApplicationError(
                "deactivation lifecycle generation is stale",
                type="StaleLifecycleGeneration",
                non_retryable=True,
            )

        resume_same_generation = self._state.lifecycle_state in {
            StoreLifecycleState.DEACTIVATING,
            StoreLifecycleState.DEACTIVATION_FAILED,
        }
        new_generation = (
            self._state.lifecycle_generation
            if resume_same_generation
            else command.expected_generation + 1
        )
        workflow_id = store_deactivation_workflow_id(command.store_key, new_generation)
        await workflow.start_child_workflow(
            "DeactivateStoreWorkflow",
            DeactivationInput(
                store_key=command.store_key,
                expected_generation=command.expected_generation,
                command_id=command.command_id,
                operation_id=workflow_id,
                drain_timeout_seconds=(self._state.deactivation_drain_timeout_seconds),
                controller_workflow_id=workflow.info().workflow_id,
                sync_workflow_ids=tuple(
                    registration.workflow_id for registration in self._state.active_syncs.values()
                ),
                remediation_workflow_ids=tuple(
                    registration.workflow_id
                    for registration in self._state.active_remediations.values()
                ),
                quota_workflow_ids=tuple(sorted(self._state.quota_workflow_ids)),
                enable_search_attributes=self._state.enable_search_attributes,
                resume_same_generation=resume_same_generation,
            ),
            id=workflow_id,
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            cancellation_type=workflow.ChildWorkflowCancellationType.ABANDON,
            search_attributes=(
                operation_search_attributes(
                    store_key=command.store_key,
                    lifecycle_generation=new_generation,
                    operation_type=OperationType.DEACTIVATION,
                    current_phase="accepted",
                )
                if self._state.enable_search_attributes
                else None
            ),
        )
        self._state.active_deactivation_id = workflow_id
        self._state.active_deactivation_fenced = resume_same_generation
        self._state.lifecycle_state = StoreLifecycleState.DEACTIVATING
        result = CommandResult(
            command_id=command.command_id,
            operation_id=workflow_id,
            workflow_id=workflow_id,
            operation_type=OperationType.DEACTIVATION,
            status=OperationStatus.ACCEPTED,
            lifecycle_generation=new_generation,
        )
        self._remember(result)
        return OperationAccepted(
            command_id=command.command_id,
            operation_id=workflow_id,
            workflow_id=workflow_id,
            operation_type=OperationType.DEACTIVATION,
            lifecycle_generation=new_generation,
        )

    async def _forward_drained(self, event: OperationStatusEvent) -> None:
        if self._state.active_deactivation_id is None:
            return
        try:
            await workflow.get_external_workflow_handle(self._state.active_deactivation_id).signal(
                "operation_drained",
                OperationDrained(
                    operation_id=event.operation_id,
                    workflow_id=event.workflow_id,
                ),
            )
        except TemporalError as exc:
            workflow.logger.warning("Unable to forward drain event: %s", exc)

    async def _operation_status(self, event: OperationStatusEvent) -> None:
        terminal = event.status in {
            OperationStatus.COMPLETED,
            OperationStatus.FAILED,
            OperationStatus.CANCELED,
            OperationStatus.REJECTED,
        }
        if event.operation_id in self._state.active_syncs and terminal:
            del self._state.active_syncs[event.operation_id]
            await self._forward_drained(event)
        if event.operation_id in self._state.active_remediations and terminal:
            del self._state.active_remediations[event.operation_id]
            await self._forward_drained(event)
        if event.operation_id == self._state.active_deactivation_id and terminal:
            fenced = (
                self._state.active_deactivation_fenced
                or event.lifecycle_generation > self._state.lifecycle_generation
            )
            self._state.active_deactivation_id = None
            self._state.active_deactivation_fenced = False
            if fenced:
                self._state.lifecycle_generation = max(
                    self._state.lifecycle_generation,
                    event.lifecycle_generation,
                )
                self._state.lifecycle_state = (
                    StoreLifecycleState.INACTIVE
                    if event.result_status
                    in {
                        ResultStatus.SUCCEEDED,
                        ResultStatus.PARTIAL,
                    }
                    else StoreLifecycleState.DEACTIVATION_FAILED
                )
            else:
                # The authoritative generation never advanced.  A failed
                # start must not strand the logical controller in DEACTIVATING.
                self._state.lifecycle_state = (
                    StoreLifecycleState.SYNCING
                    if self._state.active_syncs
                    else StoreLifecycleState.ACTIVE
                )
        elif (
            not self._state.active_syncs
            and self._state.lifecycle_state is StoreLifecycleState.SYNCING
        ):
            self._state.lifecycle_state = StoreLifecycleState.ACTIVE
        if (
            not self._state.active_syncs
            and not self._state.active_remediations
            and self._state.active_deactivation_id is None
        ):
            self._state.quota_workflow_ids.clear()

    async def _remediation_started(self, event: RemediationStatusEvent) -> None:
        existing = self._state.active_remediations.get(event.operation_id)
        if existing is not None:
            return
        self._state.active_remediations[event.operation_id] = RemediationRegistration(
            operation_id=event.operation_id,
            workflow_id=event.workflow_id,
            lifecycle_generation=event.lifecycle_generation,
            sync_sequence=event.sync_sequence,
            status=event.status,
            started_at=workflow.now(),
        )
        if self._state.lifecycle_state in {
            StoreLifecycleState.DEACTIVATING,
            StoreLifecycleState.INACTIVE,
            StoreLifecycleState.DEACTIVATION_FAILED,
        }:
            try:
                await workflow.get_external_workflow_handle(event.workflow_id).cancel()
            except TemporalError as exc:
                workflow.logger.warning("Unable to cancel late remediation: %s", exc)

    async def _remediation_finished(self, event: RemediationStatusEvent) -> None:
        if event.status not in {
            OperationStatus.COMPLETED,
            OperationStatus.FAILED,
            OperationStatus.CANCELED,
            OperationStatus.REJECTED,
        }:
            return
        await self._operation_status(
            OperationStatusEvent(
                operation_id=event.operation_id,
                workflow_id=event.workflow_id,
                lifecycle_generation=event.lifecycle_generation,
                status=event.status,
                result_status=event.result_status,
                message=event.message,
            )
        )

    def _deactivation_fenced(self, event: DeactivationFencedEvent) -> None:
        if event.operation_id != self._state.active_deactivation_id:
            return
        if event.lifecycle_generation < self._state.lifecycle_generation:
            return
        self._state.lifecycle_generation = event.lifecycle_generation
        self._state.lifecycle_state = StoreLifecycleState.DEACTIVATING
        self._state.active_deactivation_fenced = True

    async def _initialize_authority(self) -> None:
        if self._state.authority_initialized:
            return
        result = await workflow.execute_activity(
            "validate_lifecycle_generation",
            LifecycleFence(
                store_key=self._state.store_key,
                expected_generation=self._state.lifecycle_generation,
                allowed_states=tuple(StoreLifecycleState),
            ),
            result_type=LifecycleMutationResult,
            **metadata_activity_options(),
        )
        self._state.lifecycle_generation = result.authoritative_generation
        self._state.lifecycle_state = (
            StoreLifecycleState.SYNCING
            if result.lifecycle_state is StoreLifecycleState.ACTIVE and self._state.active_syncs
            else result.lifecycle_state
        )
        self._state.authority_initialized = True

    async def _process(self, envelope: _CommandEnvelope) -> Any:
        if envelope.kind == "request_sync":
            return await self._request_sync(envelope.payload)
        if envelope.kind == "cancel_sync":
            return await self._cancel_sync(envelope.payload)
        if envelope.kind == "start_deactivation":
            return await self._start_deactivation(envelope.payload)
        if envelope.kind == "operation_status":
            await self._operation_status(envelope.payload)
        elif envelope.kind == "remediation_started":
            await self._remediation_started(envelope.payload)
        elif envelope.kind == "remediation_finished":
            await self._remediation_finished(envelope.payload)
        elif envelope.kind == "deactivation_fenced":
            self._deactivation_fenced(envelope.payload)
        elif envelope.kind == "continue_as_new":
            self._state.continue_as_new_requested = True
        return None

    async def _maybe_continue_as_new(self) -> None:
        if not (
            self._state.continue_as_new_requested or workflow.info().is_continue_as_new_suggested()
        ):
            return
        if self._processing_command or not self._commands.empty():
            return
        await workflow.wait_condition(
            lambda: workflow.all_handlers_finished() or not self._commands.empty()
        )
        if not self._commands.empty():
            return
        self._state.continue_as_new_requested = False
        self._state.authority_initialized = False
        workflow.continue_as_new(self._state)

    @workflow.run
    async def run(self, _initial_state: StoreControllerState) -> None:
        await self._initialize_authority()
        while True:
            envelope = await self._commands.get()
            self._processing_command = True
            try:
                result = await self._process(envelope)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if envelope.response is not None and not envelope.response.done():
                    envelope.response.set_exception(exc)
                else:
                    raise
            else:
                if envelope.response is not None and not envelope.response.done():
                    envelope.response.set_result(result)
            finally:
                self._processing_command = False
            await self._maybe_continue_as_new()
