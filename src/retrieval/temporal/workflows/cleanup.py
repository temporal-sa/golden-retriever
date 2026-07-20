"""Bounded cleanup and user-deactivation workflow tree."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import (
    ActivityCancellationType,
    ChildWorkflowCancellationType,
    ParentClosePolicy,
)

from retrieval.temporal.common.ids import opaque_key
from retrieval.temporal.models.lifecycle import CleanupWorkflowInput
from retrieval.temporal.models.operations import ResultStatus

_ACTIVITY_START_TO_CLOSE = timedelta(minutes=2)
_ACTIVITY_SCHEDULE_TO_CLOSE = timedelta(minutes=10)
_CHILD_EXECUTION_TIMEOUT = timedelta(minutes=30)
_CHILD_RUN_TIMEOUT = timedelta(minutes=20)
_CHILD_TASK_TIMEOUT = timedelta(seconds=10)

_ACTIVITY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
    non_retryable_error_types=[
        "StaleLifecycleGenerationError",
        "LifecycleStateRejectedError",
    ],
)
_CHILD_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=2,
)


# These are structural wire contracts for the Activities with the same-named
# dataclasses.  Defining them here avoids importing Activity implementations
# (and their repository adapters) into Temporal's workflow sandbox.
@dataclass(frozen=True)
class CleanupUsersRequest:
    store_key: str
    expected_generation: int
    user_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class CleanupResult:
    store_key: str
    expected_generation: int
    status: ResultStatus
    affected: int = 0
    message: str | None = None


def _cleanup_workflow_id(kind: str, command: CleanupWorkflowInput, *parts: str) -> str:
    digest = opaque_key(
        command.store_key,
        command.lifecycle_generation,
        *parts,
        namespace=f"cleanup-{kind}",
    )
    return f"{kind}/{digest}"


def _activity_request(command: CleanupWorkflowInput) -> CleanupUsersRequest:
    return CleanupUsersRequest(
        store_key=command.store_key,
        expected_generation=command.lifecycle_generation,
        user_keys=command.user_keys,
    )


async def _execute_cleanup_activity(
    activity_name: str, command: CleanupWorkflowInput
) -> CleanupResult:
    return await workflow.execute_activity(
        activity_name,
        _activity_request(command),
        result_type=CleanupResult,
        start_to_close_timeout=_ACTIVITY_START_TO_CLOSE,
        schedule_to_close_timeout=_ACTIVITY_SCHEDULE_TO_CLOSE,
        retry_policy=_ACTIVITY_RETRY_POLICY,
        cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
    )


def _child_options(workflow_id: str) -> dict[str, object]:
    """Options for cleanup children owned and joined by their parent."""

    return {
        "id": workflow_id,
        "parent_close_policy": ParentClosePolicy.REQUEST_CANCEL,
        "cancellation_type": ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
        "execution_timeout": _CHILD_EXECUTION_TIMEOUT,
        "run_timeout": _CHILD_RUN_TIMEOUT,
        "task_timeout": _CHILD_TASK_TIMEOUT,
        "retry_policy": _CHILD_RETRY_POLICY,
    }


@workflow.defn(name="DeactivateOneUserWorkflow")
class DeactivateOneUserWorkflow:
    @workflow.run
    async def run(self, command: CleanupWorkflowInput) -> CleanupResult:
        if len(command.user_keys) != 1:
            raise ValueError("DeactivateOneUserWorkflow requires exactly one user key")
        return await _execute_cleanup_activity("deactivate_users_generation_fenced", command)


@workflow.defn(name="DeactivateAllUsersWorkflow")
class DeactivateAllUsersWorkflow:
    @workflow.run
    async def run(self, command: CleanupWorkflowInput) -> CleanupResult:
        # An empty user tuple is the repository contract for all users.  Do not
        # accidentally turn an explicit subset into an all-user operation.
        all_users = CleanupWorkflowInput(
            store_key=command.store_key,
            lifecycle_generation=command.lifecycle_generation,
            user_keys=(),
            user_concurrency=command.user_concurrency,
        )
        return await _execute_cleanup_activity("deactivate_users_generation_fenced", all_users)


@workflow.defn(name="DeactivateUserWorkflow")
class DeactivateUserWorkflow:
    """Compatibility boundary that owns one joined per-user child."""

    @workflow.run
    async def run(self, command: CleanupWorkflowInput) -> CleanupResult:
        if len(command.user_keys) != 1:
            raise ValueError("DeactivateUserWorkflow requires exactly one user key")
        user_key = command.user_keys[0]
        return await workflow.execute_child_workflow(
            DeactivateOneUserWorkflow.run,
            command,
            **_child_options(_cleanup_workflow_id("deactivate-one-user", command, user_key)),
        )


@workflow.defn(name="CleanupUsersWorkflow")
class CleanupUsersWorkflow:
    """Deactivate users with a finite number of owned children per batch."""

    @workflow.run
    async def run(self, command: CleanupWorkflowInput) -> CleanupResult:
        if command.user_concurrency <= 0:
            raise ValueError("user_concurrency must be positive")

        if not command.user_keys:
            return await workflow.execute_child_workflow(
                DeactivateAllUsersWorkflow.run,
                command,
                **_child_options(_cleanup_workflow_id("deactivate-all-users", command)),
            )

        total_affected = 0
        failure: CleanupResult | None = None
        user_keys = tuple(dict.fromkeys(command.user_keys))
        # Batch joins are explicit completion barriers.  A later batch cannot
        # start while any child from the current bounded batch remains pending.
        for offset in range(0, len(user_keys), command.user_concurrency):
            user_batch = user_keys[offset : offset + command.user_concurrency]
            results = await asyncio.gather(
                *(
                    workflow.execute_child_workflow(
                        DeactivateUserWorkflow.run,
                        CleanupWorkflowInput(
                            store_key=command.store_key,
                            lifecycle_generation=command.lifecycle_generation,
                            user_keys=(user_key,),
                            user_concurrency=1,
                        ),
                        **_child_options(
                            _cleanup_workflow_id("deactivate-user", command, user_key)
                        ),
                    )
                    for user_key in user_batch
                ),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, asyncio.CancelledError):
                    raise result
                if isinstance(result, BaseException):
                    if failure is None:
                        failure = CleanupResult(
                            store_key=command.store_key,
                            expected_generation=command.lifecycle_generation,
                            status=ResultStatus.FAILED,
                            affected=0,
                            message=type(result).__name__,
                        )
                    continue
                total_affected += result.affected
                if result.status is not ResultStatus.SUCCEEDED and failure is None:
                    failure = result
            if failure is not None:
                break

        if failure is not None:
            return CleanupResult(
                store_key=command.store_key,
                expected_generation=command.lifecycle_generation,
                status=failure.status,
                affected=total_affected,
                message=failure.message,
            )
        return CleanupResult(
            store_key=command.store_key,
            expected_generation=command.lifecycle_generation,
            status=ResultStatus.SUCCEEDED,
            affected=total_affected,
        )


@workflow.defn(name="RemoveObjectsWorkflow")
class RemoveObjectsWorkflow:
    @workflow.run
    async def run(self, command: CleanupWorkflowInput) -> CleanupResult:
        return await _execute_cleanup_activity("remove_objects_generation_fenced", command)


__all__ = [
    "CleanupUsersWorkflow",
    "DeactivateAllUsersWorkflow",
    "DeactivateOneUserWorkflow",
    "DeactivateUserWorkflow",
    "RemoveObjectsWorkflow",
]
