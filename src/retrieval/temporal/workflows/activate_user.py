"""Two-wave user activation: capped recent sync followed by full backfill."""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.models.lifecycle import LifecycleFence, LifecycleMutationResult
    from retrieval.temporal.models.operations import ResultStatus, WorkClass
    from retrieval.temporal.models.sync import (
        ActivateUserInput,
        SyncProgress,
        SyncResult,
        UserSyncInput,
    )
    from retrieval.temporal.workflows._policies import metadata_activity_options


@workflow.defn(name="ActivateUserWorkflow")
class ActivateUserWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"

    @workflow.query(name="get_phase")
    def get_phase(self) -> str:
        return self._phase

    @staticmethod
    def _user_input(
        command: ActivateUserInput,
        *,
        sync_sequence: str,
        work_class: WorkClass,
        cursor: str | None,
        page_limit: int | None,
        resource_cursors: dict[str, str | None] | None = None,
        completed_resource_types: tuple[str, ...] = (),
    ) -> UserSyncInput:
        return UserSyncInput(
            store_key=command.store_key,
            lifecycle_generation=command.lifecycle_generation,
            sync_sequence=sync_sequence,
            user_key=command.user_key,
            quota_scope=command.quota_scope,
            work_class=work_class,
            resource_types=command.resource_types,
            cursor=cursor,
            resource_cursors=dict(resource_cursors or {}),
            completed_resource_types=completed_resource_types,
            page_limit=page_limit,
            resource_concurrency=command.resource_concurrency,
            files_page_window_size=command.files_page_window_size,
            files_per_page_concurrency=command.files_per_page_concurrency,
            document_ingestion_concurrency=command.document_ingestion_concurrency,
            provider_page_size=command.provider_page_size,
            provider_task_queue=command.provider_task_queue,
            priority_fairness_enabled=command.priority_fairness_enabled,
        )

    @workflow.run
    async def run(self, command: ActivateUserInput) -> SyncResult:
        self._phase = "recent"
        recent = await workflow.execute_child_workflow(
            "UserSyncWorkflow",
            self._user_input(
                command,
                sync_sequence=f"{command.sync_sequence}:recent",
                work_class=WorkClass.RECENT_ACTIVATION,
                cursor=None,
                page_limit=max(1, command.recent_page_cap),
            ),
            cancellation_type=workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        if recent.status not in {ResultStatus.SUCCEEDED, ResultStatus.PARTIAL}:
            return recent

        # Cancellation is observed at this await boundary, then generation is explicitly
        # revalidated before the second wave emits any provider work.
        self._phase = "generation_check"
        fence = await workflow.execute_activity(
            "validate_lifecycle_generation",
            LifecycleFence(
                store_key=command.store_key,
                expected_generation=command.lifecycle_generation,
            ),
            result_type=LifecycleMutationResult,
            **metadata_activity_options(),
        )
        if fence.status is not ResultStatus.SUCCEEDED:
            return SyncResult(
                store_key=command.store_key,
                lifecycle_generation=command.lifecycle_generation,
                sync_sequence=command.sync_sequence,
                status=fence.status,
                progress=SyncProgress(phase="generation_rejected"),
                errors=(fence.message or fence.status.value,),
            )

        self._phase = "backfill"
        recent_cursor = recent.details.get("next_cursor")
        recent_resource_cursors = recent.details.get("resource_cursors", {})
        recent_completed_resources = recent.details.get("completed_resource_types", ())
        backfill = await workflow.execute_child_workflow(
            "UserSyncWorkflow",
            self._user_input(
                command,
                sync_sequence=f"{command.sync_sequence}:backfill",
                work_class=WorkClass.BACKFILL,
                cursor=recent_cursor if isinstance(recent_cursor, str) else None,
                page_limit=None,
                resource_cursors=(
                    dict(recent_resource_cursors)
                    if isinstance(recent_resource_cursors, dict)
                    else {}
                ),
                completed_resource_types=tuple(recent_completed_resources),
            ),
            cancellation_type=workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        if backfill.status not in {ResultStatus.SUCCEEDED, ResultStatus.PARTIAL}:
            return backfill

        self._phase = "activating"
        activation = await workflow.execute_activity(
            "activate_user_generation_fenced",
            command,
            result_type=LifecycleMutationResult,
            **metadata_activity_options(),
        )
        self._phase = "completed"
        return SyncResult(
            store_key=command.store_key,
            lifecycle_generation=command.lifecycle_generation,
            sync_sequence=command.sync_sequence,
            status=(
                activation.status
                if activation.status is not ResultStatus.SUCCEEDED
                else (
                    ResultStatus.PARTIAL
                    if recent.errors or backfill.errors
                    else ResultStatus.SUCCEEDED
                )
            ),
            progress=backfill.progress,
            errors=recent.errors + backfill.errors,
            details={"recent": recent.details, "backfill": backfill.details},
        )
