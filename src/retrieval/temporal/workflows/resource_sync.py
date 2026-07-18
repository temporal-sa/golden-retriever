"""Resource-level cursor/configuration owner."""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.models.sync import (
        ResourcePagesInput,
        ResourceSyncInput,
        SyncResult,
    )


@workflow.defn(name="ResourceSyncWorkflow")
class ResourceSyncWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"

    @workflow.query(name="get_status")
    def get_status(self) -> str:
        return self._phase

    @workflow.run
    async def run(self, command: ResourceSyncInput) -> SyncResult:
        self._phase = "pages"
        result = await workflow.execute_child_workflow(
            "ResourcePagesWorkflow",
            ResourcePagesInput(
                store_key=command.store_key,
                lifecycle_generation=command.lifecycle_generation,
                sync_sequence=command.sync_sequence,
                user_key=command.user_key,
                resource_key=command.resource_key,
                quota_scope=command.quota_scope,
                work_class=command.work_class,
                next_page_cursor=command.cursor,
                max_pages=command.page_limit,
                page_size=command.provider_page_size,
                files_page_window_size=command.files_page_window_size,
                files_per_page_concurrency=command.files_per_page_concurrency,
                document_ingestion_concurrency=command.document_ingestion_concurrency,
                provider_task_queue=command.provider_task_queue,
                priority_fairness_enabled=command.priority_fairness_enabled,
            ),
            result_type=SyncResult,
            cancellation_type=workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        self._phase = "completed"
        return result
