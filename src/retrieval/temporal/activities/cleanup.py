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
    batch_size: int = 250
    batch_id: str = ""


@dataclass(frozen=True)
class CleanupResult:
    store_key: str
    expected_generation: int
    status: ResultStatus
    affected: int = 0
    message: str | None = None
    deleted_chunks: int = 0
    remaining: bool = False


def _heartbeat(value: object) -> None:
    try:
        activity.heartbeat(value)
    except RuntimeError:
        pass


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
        """Compatibility one-shot boundary for histories created before batching."""

        deleted_documents = 0
        deleted_chunks = 0
        try:
            while True:
                outcome = await self._repository.remove_object_batch_if_current(
                    command.store_key,
                    command.expected_generation,
                    command.batch_size,
                )
                deleted_documents += outcome.deleted_documents
                deleted_chunks += outcome.deleted_chunks
                _heartbeat(
                    {
                        "documents_deleted": deleted_documents,
                        "chunks_deleted": deleted_chunks,
                    }
                )
                if not outcome.remaining:
                    break
        except (StaleLifecycleGenerationError, LifecycleStateRejectedError) as exc:
            return self._failure(command, exc)
        return CleanupResult(
            store_key=command.store_key,
            expected_generation=command.expected_generation,
            status=ResultStatus.SUCCEEDED,
            affected=deleted_documents,
            deleted_chunks=deleted_chunks,
        )

    @activity.defn(name="remove_object_batch_generation_fenced")
    async def remove_object_batch(self, command: CleanupUsersRequest) -> CleanupResult:
        try:
            outcome = await self._repository.remove_object_batch_if_current(
                command.store_key,
                command.expected_generation,
                command.batch_size,
            )
        except (StaleLifecycleGenerationError, LifecycleStateRejectedError) as exc:
            return self._failure(command, exc)
        _heartbeat(
            {
                "batch_id": command.batch_id,
                "documents_deleted": outcome.deleted_documents,
                "chunks_deleted": outcome.deleted_chunks,
                "remaining": outcome.remaining,
            }
        )
        return CleanupResult(
            store_key=command.store_key,
            expected_generation=command.expected_generation,
            status=ResultStatus.SUCCEEDED,
            affected=outcome.deleted_documents,
            deleted_chunks=outcome.deleted_chunks,
            remaining=outcome.remaining,
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
