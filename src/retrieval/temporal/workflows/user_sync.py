"""Bounded, joined resource fan-out for one user."""

from __future__ import annotations

import asyncio

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.common.ids import resource_sync_workflow_id
    from retrieval.temporal.models.operations import ResultStatus
    from retrieval.temporal.models.sync import (
        ResourceSyncInput,
        SyncProgress,
        SyncResult,
        UserSyncInput,
    )


@workflow.defn(name="UserSyncWorkflow")
class UserSyncWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._pending = 0

    @workflow.query(name="get_progress")
    def get_progress(self) -> SyncProgress:
        return SyncProgress(phase=self._phase, pending_children=self._pending)

    async def _run_resource(
        self,
        command: UserSyncInput,
        resource_key: str,
        semaphore: asyncio.Semaphore,
    ) -> SyncResult:
        async with semaphore:
            self._pending += 1
            try:
                return await workflow.execute_child_workflow(
                    "ResourceSyncWorkflow",
                    ResourceSyncInput(
                        store_key=command.store_key,
                        lifecycle_generation=command.lifecycle_generation,
                        sync_sequence=command.sync_sequence,
                        user_key=command.user_key,
                        resource_key=resource_key,
                        quota_scope=command.quota_scope,
                        work_class=command.work_class,
                        cursor=command.resource_cursors.get(resource_key, command.cursor),
                        idempotency_context=workflow.info().workflow_id,
                        page_limit=command.page_limit,
                        files_page_window_size=command.files_page_window_size,
                        files_per_page_concurrency=command.files_per_page_concurrency,
                        document_ingestion_concurrency=(command.document_ingestion_concurrency),
                        provider_page_size=command.provider_page_size,
                        provider_task_queue=command.provider_task_queue,
                        priority_fairness_enabled=command.priority_fairness_enabled,
                    ),
                    id=resource_sync_workflow_id(
                        command.store_key,
                        command.lifecycle_generation,
                        command.sync_sequence,
                        command.user_key,
                        resource_key,
                    ),
                    cancellation_type=workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
                )
            finally:
                self._pending -= 1

    @workflow.run
    async def run(self, command: UserSyncInput) -> SyncResult:
        self._phase = "resources"
        requested_resource_types = command.resource_types or ("files",)
        already_completed = set(command.completed_resource_types)
        resource_types = tuple(
            resource for resource in requested_resource_types if resource not in already_completed
        )
        semaphore = asyncio.Semaphore(max(1, command.resource_concurrency))
        tasks = [
            asyncio.create_task(self._run_resource(command, resource, semaphore))
            for resource in resource_types
        ]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        errors: list[str] = []
        resource_cursors: dict[str, str | None] = dict(command.resource_cursors)
        completed_resource_types = list(command.completed_resource_types)
        all_finished = True
        completed = 0
        for resource, result in zip(resource_types, results, strict=True):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                errors.append(f"{resource}:{type(result).__name__}")
                all_finished = False
                continue
            completed += 1
            errors.extend(result.errors)
            cursor = result.details.get("next_cursor")
            resource_cursors[resource] = cursor if isinstance(cursor, str) else None
            resource_finished = bool(result.details.get("finished", True))
            all_finished = all_finished and resource_finished
            if resource_finished and resource not in completed_resource_types:
                completed_resource_types.append(resource)

        next_cursor = next(
            (cursor for cursor in resource_cursors.values() if cursor is not None),
            None,
        )
        self._phase = "completed"
        return SyncResult(
            store_key=command.store_key,
            lifecycle_generation=command.lifecycle_generation,
            sync_sequence=command.sync_sequence,
            status=ResultStatus.PARTIAL if errors else ResultStatus.SUCCEEDED,
            progress=SyncProgress(
                phase="completed",
                resources_completed=completed,
                cursor=next_cursor,
            ),
            errors=tuple(errors),
            details={
                "resource_cursors": resource_cursors,
                "completed_resource_types": tuple(completed_resource_types),
                "next_cursor": next_cursor,
                "finished": all_finished
                and len(completed_resource_types) == len(requested_resource_types),
            },
        )
