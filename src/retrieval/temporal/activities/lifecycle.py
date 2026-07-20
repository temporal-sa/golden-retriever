"""Atomic lifecycle transition and generation validation Activities."""

from __future__ import annotations

from temporalio import activity
from temporalio.exceptions import ApplicationError

from retrieval.temporal.common.metrics import (
    LIFECYCLE_TRANSITIONS,
    STALE_GENERATION_REJECTIONS,
    activity_metrics,
)
from retrieval.temporal.models.lifecycle import (
    BeginStoreDeactivation,
    LifecycleFence,
    LifecycleMutationResult,
    NewGeneration,
    ResumeStoreDeactivation,
)
from retrieval.temporal.models.operations import ResultStatus
from retrieval.temporal.models.sync import ActivateUserInput

from .repositories import (
    CleanupIncompleteError,
    LifecycleStateRejectedError,
    RetrievalRepository,
    StaleLifecycleGenerationError,
)


class LifecycleActivities:
    def __init__(self, repository: RetrievalRepository) -> None:
        self._repository = repository

    @activity.defn(name="begin_store_deactivation")
    async def begin_store_deactivation(self, command: BeginStoreDeactivation) -> NewGeneration:
        metrics = activity_metrics(operation="begin_store_deactivation")
        try:
            generation = await self._repository.begin_deactivation(
                command.store_key, command.expected_generation
            )
        except (StaleLifecycleGenerationError, LifecycleStateRejectedError) as exc:
            status = (
                "stale_generation"
                if isinstance(exc, StaleLifecycleGenerationError)
                else "state_rejected"
            )
            metrics.increment(
                LIFECYCLE_TRANSITIONS,
                attributes={"transition": "deactivating", "status": status},
            )
            if isinstance(exc, StaleLifecycleGenerationError):
                metrics.increment(STALE_GENERATION_REJECTIONS)
            raise ApplicationError(str(exc), type=type(exc).__name__, non_retryable=True) from exc
        metrics.increment(
            LIFECYCLE_TRANSITIONS,
            attributes={"transition": "deactivating", "status": "succeeded"},
        )
        return generation

    @activity.defn(name="resume_store_deactivation")
    async def resume_store_deactivation(self, command: ResumeStoreDeactivation) -> NewGeneration:
        metrics = activity_metrics(operation="resume_store_deactivation")
        try:
            generation = await self._repository.resume_deactivation(
                command.store_key,
                command.lifecycle_generation,
            )
        except (StaleLifecycleGenerationError, LifecycleStateRejectedError) as exc:
            status = (
                "stale_generation"
                if isinstance(exc, StaleLifecycleGenerationError)
                else "state_rejected"
            )
            metrics.increment(
                LIFECYCLE_TRANSITIONS,
                attributes={"transition": "resume_deactivation", "status": status},
            )
            if isinstance(exc, StaleLifecycleGenerationError):
                metrics.increment(STALE_GENERATION_REJECTIONS)
            raise ApplicationError(str(exc), type=type(exc).__name__, non_retryable=True) from exc
        metrics.increment(
            LIFECYCLE_TRANSITIONS,
            attributes={"transition": "resume_deactivation", "status": "succeeded"},
        )
        return generation

    @activity.defn(name="validate_lifecycle_generation")
    async def validate_lifecycle_generation(self, fence: LifecycleFence) -> LifecycleMutationResult:
        record = await self._repository.get_store(fence.store_key)
        if record.lifecycle_generation != fence.expected_generation:
            activity_metrics(operation="validate_lifecycle_generation").increment(
                STALE_GENERATION_REJECTIONS
            )
            return LifecycleMutationResult(
                store_key=fence.store_key,
                expected_generation=fence.expected_generation,
                authoritative_generation=record.lifecycle_generation,
                status=ResultStatus.STALE_GENERATION,
                lifecycle_state=record.lifecycle_state,
                message="lifecycle generation is stale",
            )
        if record.lifecycle_state not in fence.allowed_states:
            return LifecycleMutationResult(
                store_key=fence.store_key,
                expected_generation=fence.expected_generation,
                authoritative_generation=record.lifecycle_generation,
                status=ResultStatus.REJECTED,
                lifecycle_state=record.lifecycle_state,
                message="lifecycle state rejects this operation",
            )
        return LifecycleMutationResult(
            store_key=fence.store_key,
            expected_generation=fence.expected_generation,
            authoritative_generation=record.lifecycle_generation,
            status=ResultStatus.SUCCEEDED,
            lifecycle_state=record.lifecycle_state,
        )

    @activity.defn(name="activate_user_generation_fenced")
    async def activate_user_generation_fenced(
        self, command: ActivateUserInput
    ) -> LifecycleMutationResult:
        metrics = activity_metrics(operation="activate_user")
        try:
            await self._repository.activate_user_if_current(
                command.store_key,
                command.lifecycle_generation,
                command.user_key,
            )
        except (StaleLifecycleGenerationError, LifecycleStateRejectedError) as exc:
            if isinstance(exc, StaleLifecycleGenerationError):
                metrics.increment(STALE_GENERATION_REJECTIONS)
            current = await self._repository.get_store(command.store_key)
            return LifecycleMutationResult(
                store_key=command.store_key,
                expected_generation=command.lifecycle_generation,
                authoritative_generation=current.lifecycle_generation,
                status=(
                    ResultStatus.STALE_GENERATION
                    if isinstance(exc, StaleLifecycleGenerationError)
                    else ResultStatus.REJECTED
                ),
                lifecycle_state=current.lifecycle_state,
                message=str(exc),
            )
        current = await self._repository.get_store(command.store_key)
        return LifecycleMutationResult(
            store_key=command.store_key,
            expected_generation=command.lifecycle_generation,
            authoritative_generation=current.lifecycle_generation,
            status=ResultStatus.SUCCEEDED,
            lifecycle_state=current.lifecycle_state,
        )

    @activity.defn(name="mark_store_inactive")
    async def mark_store_inactive(self, generation: NewGeneration) -> LifecycleMutationResult:
        metrics = activity_metrics(operation="mark_store_inactive")
        try:
            record = await self._repository.mark_inactive(
                generation.store_key, generation.lifecycle_generation
            )
        except (
            CleanupIncompleteError,
            LifecycleStateRejectedError,
            StaleLifecycleGenerationError,
        ) as exc:
            is_stale = isinstance(exc, StaleLifecycleGenerationError)
            if is_stale:
                metrics.increment(STALE_GENERATION_REJECTIONS)
            metrics.increment(
                LIFECYCLE_TRANSITIONS,
                attributes={
                    "transition": "inactive",
                    "status": "stale_generation" if is_stale else "state_rejected",
                },
            )
            current = await self._repository.get_store(generation.store_key)
            return LifecycleMutationResult(
                store_key=generation.store_key,
                expected_generation=generation.lifecycle_generation,
                authoritative_generation=current.lifecycle_generation,
                status=(ResultStatus.STALE_GENERATION if is_stale else ResultStatus.REJECTED),
                lifecycle_state=current.lifecycle_state,
                message=str(exc),
            )
        metrics.increment(
            LIFECYCLE_TRANSITIONS,
            attributes={"transition": "inactive", "status": "succeeded"},
        )
        return LifecycleMutationResult(
            store_key=generation.store_key,
            expected_generation=generation.lifecycle_generation,
            authoritative_generation=record.lifecycle_generation,
            status=ResultStatus.SUCCEEDED,
            lifecycle_state=record.lifecycle_state,
        )

    @activity.defn(name="mark_store_deactivation_failed")
    async def mark_store_deactivation_failed(
        self, generation: NewGeneration
    ) -> LifecycleMutationResult:
        record = await self._repository.mark_deactivation_failed(
            generation.store_key, generation.lifecycle_generation
        )
        activity_metrics(operation="mark_store_deactivation_failed").increment(
            LIFECYCLE_TRANSITIONS,
            attributes={"transition": "deactivation_failed", "status": "succeeded"},
        )
        return LifecycleMutationResult(
            store_key=generation.store_key,
            expected_generation=generation.lifecycle_generation,
            authoritative_generation=record.lifecycle_generation,
            status=ResultStatus.FAILED,
            lifecycle_state=record.lifecycle_state,
        )
