"""Preserved comments-resync history/retry boundary."""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.models.sync import ResourceSyncInput, SyncResult


@workflow.defn(name="CommentsResyncWorkflow")
class CommentsResyncWorkflow:
    @workflow.run
    async def run(self, command: ResourceSyncInput) -> SyncResult:
        return await workflow.execute_child_workflow(
            "ResourceSyncWorkflow",
            command,
            cancellation_type=workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
