"""Detached, durably tracked failed-user remediation."""

from __future__ import annotations

import asyncio
from dataclasses import replace

from temporalio import workflow
from temporalio.exceptions import TemporalError

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.common.ids import user_sync_workflow_id
    from retrieval.temporal.models.lifecycle import RemediationStatusEvent
    from retrieval.temporal.models.operations import (
        OperationStatus,
        ResultStatus,
    )
    from retrieval.temporal.models.sync import (
        ActivateUserInput,
        FailedUserRemediationInput,
        SyncProgress,
        SyncResult,
    )


@workflow.defn(name="FailedUserRemediationWorkflow")
class FailedUserRemediationWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._completed = 0
        self._pending = 0

    @workflow.query(name="get_progress")
    def get_progress(self) -> SyncProgress:
        return SyncProgress(
            phase=self._phase,
            users_completed=self._completed,
            pending_children=self._pending,
        )

    async def _activate(
        self,
        command: FailedUserRemediationInput,
        user_key: str,
        semaphore: asyncio.Semaphore,
    ) -> SyncResult:
        async with semaphore:
            self._pending += 1
            try:
                child_sequence = f"{command.sync_sequence}:remediation"
                return await workflow.execute_child_workflow(
                    "ActivateUserWorkflow",
                    ActivateUserInput(
                        store_key=command.store_key,
                        lifecycle_generation=command.lifecycle_generation,
                        sync_sequence=child_sequence,
                        user_key=user_key,
                        quota_scope=command.quota_scope,
                        work_class=command.work_class,
                        recent_page_cap=max(1, command.recent_page_cap),
                        resource_types=command.resource_types,
                        resource_concurrency=command.resource_concurrency,
                        files_page_window_size=command.files_page_window_size,
                        files_per_page_concurrency=command.files_per_page_concurrency,
                        document_ingestion_concurrency=(command.document_ingestion_concurrency),
                        provider_page_size=command.provider_page_size,
                        provider_task_queue=command.provider_task_queue,
                        priority_fairness_enabled=command.priority_fairness_enabled,
                    ),
                    id=user_sync_workflow_id(
                        command.store_key,
                        command.lifecycle_generation,
                        child_sequence,
                        user_key,
                    ),
                    cancellation_type=workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
                )
            finally:
                self._pending -= 1

    async def _report_terminal(
        self,
        command: FailedUserRemediationInput,
        status: OperationStatus,
        result_status: ResultStatus,
        message: str | None = None,
    ) -> None:
        if command.controller_workflow_id is None:
            return
        event = RemediationStatusEvent(
            operation_id=command.operation_id,
            workflow_id=workflow.info().workflow_id,
            lifecycle_generation=command.lifecycle_generation,
            sync_sequence=command.sync_sequence,
            status=status,
            result_status=result_status,
            message=message,
        )
        try:
            await workflow.get_external_workflow_handle(command.controller_workflow_id).signal(
                "remediation_finished", event
            )
        except TemporalError as exc:
            workflow.logger.warning(
                "Unable to report remediation terminal state: %s",
                type(exc).__name__,
            )

    @workflow.run
    async def run(self, command: FailedUserRemediationInput) -> SyncResult:
        self._phase = "activating_users"
        batch_size = max(1, min(8, command.resource_concurrency))
        self._completed = command.prior_completed_count
        error_count = command.prior_error_count
        error_sample: list[str] = []

        for offset in range(0, len(command.failed_user_keys), batch_size):
            batch = command.failed_user_keys[offset : offset + batch_size]
            semaphore = asyncio.Semaphore(batch_size)
            tasks = [
                asyncio.create_task(self._activate(command, user_key, semaphore))
                for user_key in batch
            ]
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
            except asyncio.CancelledError:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self._report_terminal(
                    command,
                    OperationStatus.CANCELED,
                    ResultStatus.CANCELED,
                    "remediation canceled",
                )
                raise

            for result in results:
                if isinstance(result, asyncio.CancelledError):
                    await self._report_terminal(
                        command,
                        OperationStatus.CANCELED,
                        ResultStatus.CANCELED,
                    )
                    raise result
                if isinstance(result, BaseException):
                    errors = (type(result).__name__,)
                elif result.status in {
                    ResultStatus.FAILED,
                    ResultStatus.REJECTED,
                    ResultStatus.CANCELED,
                }:
                    errors = result.errors or (result.status.value,)
                else:
                    self._completed += 1
                    errors = result.errors
                error_count += len(errors)
                remaining_sample = max(0, 100 - len(error_sample))
                error_sample.extend(errors[:remaining_sample])

            next_offset = offset + len(batch)
            if (
                next_offset < len(command.failed_user_keys)
                and workflow.info().is_continue_as_new_suggested()
            ):
                workflow.continue_as_new(
                    replace(
                        command,
                        failed_user_keys=command.failed_user_keys[next_offset:],
                        prior_completed_count=self._completed,
                        prior_error_count=error_count,
                    )
                )

        status = ResultStatus.PARTIAL if error_count else ResultStatus.SUCCEEDED
        self._phase = "completed"
        await self._report_terminal(
            command,
            OperationStatus.COMPLETED if not error_count else OperationStatus.FAILED,
            status,
        )
        return SyncResult(
            store_key=command.store_key,
            lifecycle_generation=command.lifecycle_generation,
            sync_sequence=command.sync_sequence,
            status=status,
            progress=SyncProgress(
                phase="completed",
                users_completed=self._completed,
                users_failed=error_count,
            ),
            errors=tuple(error_sample),
            details={"error_count": error_count},
        )
