"""Generation-fenced cleanup Activity boundaries."""

from __future__ import annotations

from dataclasses import dataclass

from temporalio import activity

from retrieval.temporal.models.operations import ResultStatus

from .repositories import (
    LifecycleStateRejectedError,
    RetrievalRepository,
    StaleLifecycleGenerationError,
)


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


class CleanupActivities:
    def __init__(self, repository: RetrievalRepository) -> None:
        self._repository = repository

    @activity.defn(name="deactivate_users_generation_fenced")
    async def deactivate_users(self, command: CleanupUsersRequest) -> CleanupResult:
        try:
            affected = await self._repository.deactivate_users_if_current(
                command.store_key, command.expected_generation, command.user_keys
            )
        except (StaleLifecycleGenerationError, LifecycleStateRejectedError) as exc:
            return self._failure(command, exc)
        return CleanupResult(
            store_key=command.store_key,
            expected_generation=command.expected_generation,
            status=ResultStatus.SUCCEEDED,
            affected=affected,
        )

    @activity.defn(name="remove_objects_generation_fenced")
    async def remove_objects(self, command: CleanupUsersRequest) -> CleanupResult:
        try:
            affected = await self._repository.remove_all_objects_if_current(
                command.store_key, command.expected_generation
            )
        except (StaleLifecycleGenerationError, LifecycleStateRejectedError) as exc:
            return self._failure(command, exc)
        return CleanupResult(
            store_key=command.store_key,
            expected_generation=command.expected_generation,
            status=ResultStatus.SUCCEEDED,
            affected=affected,
        )

    @staticmethod
    def _failure(command: CleanupUsersRequest, exc: Exception) -> CleanupResult:
        return CleanupResult(
            store_key=command.store_key,
            expected_generation=command.expected_generation,
            status=(
                ResultStatus.STALE_GENERATION
                if isinstance(exc, StaleLifecycleGenerationError)
                else ResultStatus.REJECTED
            ),
            message=str(exc),
        )
