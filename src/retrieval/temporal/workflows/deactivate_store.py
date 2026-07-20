"""Generation-fenced store deactivation orchestration."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from enum import StrEnum
from typing import TypeVar

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import TemporalError
from temporalio.workflow import (
    ActivityCancellationType,
    ChildWorkflowCancellationType,
    ParentClosePolicy,
)

from retrieval.temporal.common.ids import opaque_key
from retrieval.temporal.common.metrics import (
    DEACTIVATION_DRAIN_DURATION,
    workflow_metrics,
)
from retrieval.temporal.models.lifecycle import (
    BeginStoreDeactivation,
    CleanupWorkflowInput,
    DeactivationFencedEvent,
    DeactivationInput,
    DeactivationPhase,
    DeactivationResult,
    LifecycleMutationResult,
    NewGeneration,
    OperationDrained,
    OperationStatusEvent,
    ResumeStoreDeactivation,
)
from retrieval.temporal.models.operations import OperationStatus, ResultStatus
from retrieval.temporal.models.quota import CancelGenerationPermits

from .cleanup import CleanupResult, CleanupUsersWorkflow, RemoveObjectsWorkflow

_T = TypeVar("_T")

_DEFAULT_DRAIN_TIMEOUT = timedelta(minutes=5)
_ACTIVITY_START_TO_CLOSE = timedelta(minutes=2)
_ACTIVITY_SCHEDULE_TO_CLOSE = timedelta(minutes=10)
_CHILD_EXECUTION_TIMEOUT = timedelta(hours=2)
_CHILD_RUN_TIMEOUT = timedelta(hours=1)
_CHILD_TASK_TIMEOUT = timedelta(seconds=10)

_ACTIVITY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
    non_retryable_error_types=[
        "StaleLifecycleGenerationError",
        "LifecycleStateRejectedError",
        "CleanupIncompleteError",
    ],
)
_CHILD_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=2,
)


class DeactivationAction(StrEnum):
    FENCE_COMMITTED = "fence_committed"
    CONTROLLER_FENCED = "controller_fenced"
    OWNED_WORK_CANCEL_REQUESTED = "owned_work_cancel_requested"
    OLD_QUOTA_INVALIDATED = "old_quota_invalidated"
    DRAIN_WAIT_FINISHED = "drain_wait_finished"
    USERS_CLEANED = "users_cleaned"
    OBJECTS_REMOVED = "objects_removed"
    INACTIVE_COMMITTED = "inactive_committed"
    CONTROLLER_TERMINAL = "controller_terminal"


_DEACTIVATION_PLAN = (
    DeactivationAction.FENCE_COMMITTED,
    DeactivationAction.CONTROLLER_FENCED,
    DeactivationAction.OWNED_WORK_CANCEL_REQUESTED,
    DeactivationAction.OLD_QUOTA_INVALIDATED,
    DeactivationAction.DRAIN_WAIT_FINISHED,
    DeactivationAction.USERS_CLEANED,
    DeactivationAction.OBJECTS_REMOVED,
    DeactivationAction.INACTIVE_COMMITTED,
    DeactivationAction.CONTROLLER_TERMINAL,
)


def deactivation_plan() -> tuple[DeactivationAction, ...]:
    """Return the authoritative side-effect ordering for tests and diagnostics."""

    return _DEACTIVATION_PLAN


class DeactivationOrderGuard:
    """Pure invariant guard used by the real workflow orchestration."""

    def __init__(self) -> None:
        self._completed: list[DeactivationAction] = []

    @property
    def completed(self) -> tuple[DeactivationAction, ...]:
        return tuple(self._completed)

    @property
    def fenced(self) -> bool:
        return bool(self._completed) and self._completed[0] is DeactivationAction.FENCE_COMMITTED

    def advance(self, action: DeactivationAction) -> None:
        expected_index = len(self._completed)
        if expected_index >= len(_DEACTIVATION_PLAN):
            raise RuntimeError("deactivation sequence is already terminal")
        expected = _DEACTIVATION_PLAN[expected_index]
        if action is not expected:
            raise RuntimeError(
                f"deactivation action {action.value!r} cannot run before {expected.value!r}"
            )
        self._completed.append(action)

    def ensure_fenced(self) -> None:
        if not self.fenced:
            raise RuntimeError("old work cannot be canceled before the fence commits")


class _DeactivationStageError(RuntimeError):
    pass


def _child_options(workflow_id: str) -> dict[str, object]:
    # These children are owned and joined.  REQUEST_CANCEL avoids orphaning a
    # cleanup subtree if the stable top-level execution is forcibly closed.
    return {
        "id": workflow_id,
        "parent_close_policy": ParentClosePolicy.REQUEST_CANCEL,
        "cancellation_type": ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
        "execution_timeout": _CHILD_EXECUTION_TIMEOUT,
        "run_timeout": _CHILD_RUN_TIMEOUT,
        "task_timeout": _CHILD_TASK_TIMEOUT,
        "retry_policy": _CHILD_RETRY_POLICY,
    }


async def _execute_lifecycle_activity(
    activity_name: str,
    argument: object,
    result_type: type[NewGeneration] | type[LifecycleMutationResult],
) -> NewGeneration | LifecycleMutationResult:
    return await workflow.execute_activity(
        activity_name,
        argument,
        result_type=result_type,
        start_to_close_timeout=_ACTIVITY_START_TO_CLOSE,
        schedule_to_close_timeout=_ACTIVITY_SCHEDULE_TO_CLOSE,
        retry_policy=_ACTIVITY_RETRY_POLICY,
        cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
    )


@workflow.defn(name="DeactivateStoreWorkflow")
class DeactivateStoreWorkflow:
    """Fence, cancel, drain, clean, and persist one store deactivation."""

    def __init__(self) -> None:
        self._phase = DeactivationPhase.PENDING
        self._drained_workflow_ids: set[str] = set()
        self._outer_cancellation_requested = False
        self._order = DeactivationOrderGuard()

    @workflow.signal(name="operation_drained")
    def operation_drained(self, event: OperationDrained) -> None:
        if event.workflow_id:
            self._drained_workflow_ids.add(event.workflow_id)

    @workflow.query(name="get_deactivation_phase")
    def get_deactivation_phase(self) -> DeactivationPhase:
        return self._phase

    @workflow.run
    async def run(self, command: DeactivationInput) -> DeactivationResult:
        self._phase = DeactivationPhase.FENCING
        if command.resume_same_generation:
            fence_activity = "resume_store_deactivation"
            fence_command: object = ResumeStoreDeactivation(
                store_key=command.store_key,
                lifecycle_generation=command.expected_generation,
            )
        else:
            fence_activity = "begin_store_deactivation"
            fence_command = BeginStoreDeactivation(
                store_key=command.store_key,
                expected_generation=command.expected_generation,
            )
        fence_task = asyncio.create_task(
            _execute_lifecycle_activity(
                fence_activity,
                fence_command,
                NewGeneration,
            )
        )
        try:
            generation_result = await self._await_protected(fence_task)
            if not isinstance(generation_result, NewGeneration):
                raise _DeactivationStageError(f"{fence_activity} returned an invalid result")
        except asyncio.CancelledError as exc:
            return await self._finish_pre_fence_failed(command, str(exc))
        except Exception as exc:
            return await self._finish_pre_fence_failed(command, str(exc))
        generation = generation_result
        self._order.advance(DeactivationAction.FENCE_COMMITTED)

        # Create a distinct task immediately after the fence, before the next
        # await can deliver outer cancellation.  Shielding this owned task keeps
        # the store fenced and drives it to a safe terminal state.
        protected = asyncio.create_task(self._run_after_fence(command, generation))
        return await self._await_protected(protected)

    async def _await_protected(self, task: asyncio.Task[_T]) -> _T:
        while True:
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                if task.done():
                    return await task
                self._outer_cancellation_requested = True
                # Python 3.11+ cancellation is counted.  Clearing the caught
                # request lets this orchestrator keep awaiting its shielded
                # cleanup task, including on SDK versions before 1.28.
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()

    async def _run_after_fence(
        self, command: DeactivationInput, generation: NewGeneration
    ) -> DeactivationResult:
        warnings: list[str] = []
        try:
            controller_warning = await self._signal_controller_fenced(command, generation)
            if controller_warning:
                warnings.append(controller_warning)
            self._order.advance(DeactivationAction.CONTROLLER_FENCED)

            self._phase = DeactivationPhase.CANCELING
            self._order.ensure_fenced()
            cancellation_failures = await self._cancel_owned_work(command)
            if cancellation_failures:
                warnings.append(f"{cancellation_failures} owned cancellation request(s) failed")
            self._order.advance(DeactivationAction.OWNED_WORK_CANCEL_REQUESTED)

            quota_failures = await self._invalidate_old_generation_quota(
                command, generation.previous_generation
            )
            if quota_failures:
                warnings.append(f"{quota_failures} quota invalidation signal(s) failed")
            self._order.advance(DeactivationAction.OLD_QUOTA_INVALIDATED)

            self._phase = DeactivationPhase.DRAINING
            outstanding = await self._wait_for_operation_drain(command)
            if outstanding:
                warnings.append(
                    f"drain timeout elapsed with {outstanding} operation(s) outstanding"
                )
            self._order.advance(DeactivationAction.DRAIN_WAIT_FINISHED)

            self._phase = DeactivationPhase.CLEANING_USERS
            cleanup_result = await self._cleanup_users(command, generation)
            if cleanup_result.status is not ResultStatus.SUCCEEDED:
                raise _DeactivationStageError(
                    cleanup_result.message or "user cleanup did not succeed"
                )
            self._order.advance(DeactivationAction.USERS_CLEANED)

            self._phase = DeactivationPhase.REMOVING_OBJECTS
            remove_result = await self._remove_objects(command, generation)
            if remove_result.status is not ResultStatus.SUCCEEDED:
                raise _DeactivationStageError(
                    remove_result.message or "object removal did not succeed"
                )
            self._order.advance(DeactivationAction.OBJECTS_REMOVED)

            self._phase = DeactivationPhase.MARKING_INACTIVE
            inactive_result = await _execute_lifecycle_activity(
                "mark_store_inactive", generation, LifecycleMutationResult
            )
            if (
                not isinstance(inactive_result, LifecycleMutationResult)
                or inactive_result.status is not ResultStatus.SUCCEEDED
            ):
                raise _DeactivationStageError("marking the store inactive failed")
            self._order.advance(DeactivationAction.INACTIVE_COMMITTED)

            self._phase = DeactivationPhase.COMPLETED
            status = ResultStatus.PARTIAL if warnings else ResultStatus.SUCCEEDED
            terminal_warning = await self._signal_controller_terminal(
                command,
                generation,
                operation_status=OperationStatus.COMPLETED,
                result_status=status,
                message=self._warning_message(warnings),
            )
            if terminal_warning:
                warnings.append(terminal_warning)
                status = ResultStatus.PARTIAL
            self._order.advance(DeactivationAction.CONTROLLER_TERMINAL)
            return DeactivationResult(
                store_key=command.store_key,
                operation_id=command.operation_id,
                lifecycle_generation=generation.lifecycle_generation,
                status=status,
                phase=DeactivationPhase.COMPLETED,
                message=self._warning_message(warnings),
                completed_at=workflow.now(),
            )
        except Exception as exc:
            return await self._finish_failed(command, generation, str(exc))

    async def _finish_pre_fence_failed(
        self,
        command: DeactivationInput,
        message: str,
    ) -> DeactivationResult:
        self._phase = DeactivationPhase.FAILED
        failure_message = message[:500] or "deactivation fence did not commit"
        terminal_warning = await self._signal_controller_terminal(
            command,
            command.expected_generation,
            operation_status=OperationStatus.FAILED,
            result_status=ResultStatus.FAILED,
            message=failure_message,
        )
        if terminal_warning:
            failure_message = f"{failure_message}; {terminal_warning}"[:1_000]
        return DeactivationResult(
            store_key=command.store_key,
            operation_id=command.operation_id,
            lifecycle_generation=command.expected_generation,
            status=ResultStatus.FAILED,
            phase=DeactivationPhase.FAILED,
            message=failure_message,
            completed_at=workflow.now(),
        )

    async def _signal_controller_fenced(
        self, command: DeactivationInput, generation: NewGeneration
    ) -> str | None:
        if command.controller_workflow_id is None:
            return None
        event = DeactivationFencedEvent(
            operation_id=command.operation_id,
            workflow_id=workflow.info().workflow_id,
            lifecycle_generation=generation.lifecycle_generation,
        )
        try:
            await workflow.get_external_workflow_handle(command.controller_workflow_id).signal(
                "deactivation_fenced", event
            )
        except TemporalError:
            # The persisted generation and stable operation ID remain the
            # recovery authority if the controller is temporarily unavailable.
            return "controller did not acknowledge the generation fence"
        return None

    async def _cancel_owned_work(self, command: DeactivationInput) -> int:
        self._order.ensure_fenced()
        workflow_ids = tuple(
            dict.fromkeys((*command.sync_workflow_ids, *command.remediation_workflow_ids))
        )
        if not workflow_ids:
            return 0
        results = await asyncio.gather(
            *(
                workflow.get_external_workflow_handle(workflow_id).cancel()
                for workflow_id in workflow_ids
            ),
            return_exceptions=True,
        )
        failures = 0
        for result in results:
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                failures += 1
        return failures

    async def _invalidate_old_generation_quota(
        self,
        command: DeactivationInput,
        old_generation: int,
    ) -> int:
        quota_ids = tuple(dict.fromkeys(command.quota_workflow_ids))
        if not quota_ids:
            return 0
        cancellation = CancelGenerationPermits(
            store_key=command.store_key,
            lifecycle_generation=old_generation,
        )
        results = await asyncio.gather(
            *(
                workflow.get_external_workflow_handle(quota_id).signal(
                    "cancel_generation", cancellation
                )
                for quota_id in quota_ids
            ),
            return_exceptions=True,
        )
        failures = 0
        for result in results:
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                failures += 1
        return failures

    async def _wait_for_operation_drain(self, command: DeactivationInput) -> int:
        expected = set(command.sync_workflow_ids) | set(command.remediation_workflow_ids)
        if not expected:
            return 0
        started_at = workflow.now()
        timed_out = False
        timeout = (
            _DEFAULT_DRAIN_TIMEOUT.total_seconds()
            if command.drain_timeout_seconds is None
            else max(0.0, command.drain_timeout_seconds)
        )
        try:
            await workflow.wait_condition(
                lambda: expected.issubset(self._drained_workflow_ids),
                timeout=timeout,
                timeout_summary="store deactivation operation drain",
            )
        except TimeoutError:
            timed_out = True
        outstanding = len(expected - self._drained_workflow_ids)
        duration_ms = max(
            0,
            int((workflow.now() - started_at).total_seconds() * 1_000),
        )
        workflow_metrics(operation="store_deactivation").histogram(
            DEACTIVATION_DRAIN_DURATION,
            duration_ms,
            attributes={"status": "timed_out" if timed_out and outstanding else "drained"},
            unit="ms",
        )
        return outstanding

    async def _cleanup_users(
        self, command: DeactivationInput, generation: NewGeneration
    ) -> CleanupResult:
        cleanup_input = CleanupWorkflowInput(
            store_key=command.store_key,
            lifecycle_generation=generation.lifecycle_generation,
            object_batch_size=command.object_cleanup_batch_size,
        )
        workflow_id = "cleanup-users/" + opaque_key(
            command.store_key,
            generation.lifecycle_generation,
            namespace="cleanup-users-workflow",
        )
        return await workflow.execute_child_workflow(
            CleanupUsersWorkflow.run,
            cleanup_input,
            **_child_options(workflow_id),
        )

    async def _remove_objects(
        self, command: DeactivationInput, generation: NewGeneration
    ) -> CleanupResult:
        cleanup_input = CleanupWorkflowInput(
            store_key=command.store_key,
            lifecycle_generation=generation.lifecycle_generation,
            object_batch_size=command.object_cleanup_batch_size,
        )
        workflow_id = "remove-objects/" + opaque_key(
            command.store_key,
            generation.lifecycle_generation,
            namespace="remove-objects-workflow",
        )
        return await workflow.execute_child_workflow(
            RemoveObjectsWorkflow.run,
            cleanup_input,
            **_child_options(workflow_id),
        )

    async def _finish_failed(
        self,
        command: DeactivationInput,
        generation: NewGeneration,
        message: str,
    ) -> DeactivationResult:
        self._phase = DeactivationPhase.FAILED
        failure_message = message[:500] or "deactivation did not complete"
        try:
            await _execute_lifecycle_activity(
                "mark_store_deactivation_failed",
                generation,
                LifecycleMutationResult,
            )
        except TemporalError:
            failure_message += "; failed state could not be persisted"
        await self._signal_controller_terminal(
            command,
            generation,
            operation_status=OperationStatus.FAILED,
            result_status=ResultStatus.FAILED,
            message=failure_message,
        )
        return DeactivationResult(
            store_key=command.store_key,
            operation_id=command.operation_id,
            lifecycle_generation=generation.lifecycle_generation,
            status=ResultStatus.FAILED,
            phase=DeactivationPhase.FAILED,
            message=failure_message,
            completed_at=workflow.now(),
        )

    async def _signal_controller_terminal(
        self,
        command: DeactivationInput,
        generation: NewGeneration | int,
        *,
        operation_status: OperationStatus,
        result_status: ResultStatus,
        message: str | None,
    ) -> str | None:
        if command.controller_workflow_id is None:
            return None
        lifecycle_generation = (
            generation.lifecycle_generation if isinstance(generation, NewGeneration) else generation
        )
        event = OperationStatusEvent(
            operation_id=command.operation_id,
            workflow_id=workflow.info().workflow_id,
            lifecycle_generation=lifecycle_generation,
            status=operation_status,
            result_status=result_status,
            message=message,
        )
        try:
            await workflow.get_external_workflow_handle(command.controller_workflow_id).signal(
                "operation_status", event
            )
        except TemporalError:
            return "controller did not acknowledge terminal operation status"
        return None

    @staticmethod
    def _warning_message(warnings: list[str]) -> str | None:
        if not warnings:
            return None
        # Keep the terminal result compact even if many external operations
        # were supplied in the input.
        return "; ".join(warnings[:10])[:1_000]


__all__ = [
    "DeactivateStoreWorkflow",
    "DeactivationAction",
    "DeactivationOrderGuard",
    "deactivation_plan",
]
