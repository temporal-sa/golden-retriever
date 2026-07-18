"""Sliding window of FilesPageWorkflow children with quota-aware page fetches."""

from __future__ import annotations

import asyncio
from dataclasses import replace

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.activities.provider_api import (
        FetchResourcePageRequest,
        ResourcePageManifest,
    )
    from retrieval.temporal.common.ids import (
        files_page_workflow_id,
        permit_request_id,
    )
    from retrieval.temporal.common.quota_waiter import QuotaWaiterMixin
    from retrieval.temporal.models.operations import ResultStatus
    from retrieval.temporal.models.quota import PermitRequest
    from retrieval.temporal.models.sync import (
        FilesPageInput,
        PageResult,
        ResourcePagesInput,
        SyncProgress,
        SyncResult,
    )
    from retrieval.temporal.workflows._policies import provider_activity_options


class _QuotaRetryRollover(Exception):
    """Internal control flow used to drain page children before Continue-As-New."""

    def __init__(self, next_attempt: int) -> None:
        super().__init__(f"quota retry rollover at attempt {next_attempt}")
        self.next_attempt = next_attempt


@workflow.defn(name="ResourcePagesWorkflow")
class ResourcePagesWorkflow(QuotaWaiterMixin):
    def __init__(self) -> None:
        self._cursor: str | None = None
        self._pending_children = 0
        self._pages_completed = 0
        self._phase = "pending"

    @workflow.query(name="get_progress")
    def get_progress(self) -> SyncProgress:
        return SyncProgress(
            phase=self._phase,
            pages_completed=self._pages_completed,
            pending_children=self._pending_children,
            cursor=self._cursor,
        )

    async def _fetch_manifest(
        self,
        command: ResourcePagesInput,
        cursor: str | None,
        *,
        attempt: int | None = None,
    ) -> ResourcePageManifest:
        attempt = command.page_fetch_attempt if attempt is None else attempt
        while True:
            request_id = permit_request_id(
                store_key=command.store_key,
                lifecycle_generation=command.lifecycle_generation,
                sync_sequence=command.sync_sequence,
                user_key=command.user_key,
                resource_key=command.resource_key,
                cursor=(cursor, attempt),
                operation="fetch-resource-page",
                quota_class=(
                    command.quota_scope.quota_class
                    if command.quota_scope is not None
                    else "unmetered"
                ),
            )
            grant = None
            if command.quota_scope is not None:
                grant = await self.request_quota_permit(
                    PermitRequest(
                        request_id=request_id,
                        requester_workflow_id=workflow.info().workflow_id,
                        store_key=command.store_key,
                        lifecycle_generation=command.lifecycle_generation,
                        quota_scope=command.quota_scope,
                        work_class=command.work_class,
                        requested_at=workflow.now(),
                    )
                )

            response: ResourcePageManifest | None = None
            try:
                response = await workflow.execute_activity(
                    "provider_fetch_resource_page",
                    FetchResourcePageRequest(
                        store_key=command.store_key,
                        lifecycle_generation=command.lifecycle_generation,
                        sync_sequence=command.sync_sequence,
                        user_key=command.user_key,
                        resource_key=command.resource_key,
                        cursor=cursor,
                        page_size=command.page_size,
                        request_id=request_id,
                        quota_scope=command.quota_scope,
                    ),
                    result_type=ResourcePageManifest,
                    **provider_activity_options(
                        task_queue=command.provider_task_queue,
                        work_class=command.work_class,
                        quota_scope=command.quota_scope,
                        priority_fairness_enabled=command.priority_fairness_enabled,
                    ),
                )
                if response.observation is not None:
                    await self.report_quota_observation(response.observation)
            finally:
                if grant is not None:
                    await self.complete_quota_permit(grant)

            assert response is not None
            if not response.quota_exhausted:
                return response
            # The cursor is deliberately unchanged. A deterministic attempt suffix creates
            # a new permit request after the shared reset timer releases it.
            attempt += 1
            if workflow.info().is_continue_as_new_suggested():
                raise _QuotaRetryRollover(attempt)

    async def _start_page_child(
        self, command: ResourcePagesInput, manifest: ResourcePageManifest
    ) -> workflow.ChildWorkflowHandle:
        child_input = FilesPageInput(
            store_key=command.store_key,
            lifecycle_generation=command.lifecycle_generation,
            sync_sequence=command.sync_sequence,
            user_key=command.user_key,
            resource_key=command.resource_key,
            page_key=manifest.page_key,
            documents=manifest.documents,
            deleted_document_keys=manifest.deleted_document_keys,
            quota_scope=command.quota_scope,
            work_class=command.work_class,
            document_ingestion_concurrency=max(
                1,
                min(
                    command.files_per_page_concurrency,
                    command.document_ingestion_concurrency,
                ),
            ),
        )
        return await workflow.start_child_workflow(
            "FilesPageWorkflow",
            child_input,
            id=files_page_workflow_id(workflow.info().workflow_id, manifest.page_key),
            result_type=PageResult,
            cancellation_type=workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
        )

    @workflow.run
    async def run(self, command: ResourcePagesInput) -> SyncResult:
        self._phase = "fetching_pages"
        self._cursor = command.next_page_cursor
        cursor = command.next_page_cursor
        may_fetch = True
        pending: dict[workflow.ChildWorkflowHandle, tuple[int, str | None]] = {}
        errors: list[str] = []
        failures: list[tuple[int, str | None]] = []
        pages_started = 0
        page_fetch_attempt = command.page_fetch_attempt
        stop_for_continue_as_new = False
        window_size = max(1, command.files_page_window_size)

        try:
            while may_fetch or pending:
                while may_fetch and len(pending) < window_size:
                    if (
                        command.max_pages is not None
                        and command.completed_page_count + pages_started >= command.max_pages
                    ):
                        may_fetch = False
                        break
                    try:
                        manifest = await self._fetch_manifest(
                            command,
                            cursor,
                            attempt=page_fetch_attempt,
                        )
                    except _QuotaRetryRollover as rollover:
                        # No cursor was consumed. Stop admitting work, drain every
                        # page child already started, then roll over at the barrier.
                        page_fetch_attempt = rollover.next_attempt
                        may_fetch = False
                        stop_for_continue_as_new = True
                        break
                    page_fetch_attempt = 0
                    page_cursor = cursor
                    handle = await self._start_page_child(command, manifest)
                    pending[handle] = (pages_started, page_cursor)
                    self._pending_children = len(pending)
                    pages_started += 1
                    cursor = manifest.next_cursor
                    self._cursor = cursor
                    if cursor is None:
                        may_fetch = False
                    if workflow.info().is_continue_as_new_suggested():
                        may_fetch = False
                        stop_for_continue_as_new = cursor is not None
                        break

                if not pending:
                    break
                done, _still_pending = await workflow.wait(
                    sorted(pending, key=lambda handle: handle.id),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for handle in sorted(done, key=lambda item: item.id):
                    sequence, page_cursor = pending.pop(handle)
                    try:
                        result: PageResult = await handle
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        errors.append(type(exc).__name__)
                        failures.append((sequence, page_cursor))
                        may_fetch = False
                        stop_for_continue_as_new = False
                    else:
                        result_status = getattr(result, "status", ResultStatus.SUCCEEDED)
                        if result_status is ResultStatus.SUCCEEDED and not result.errors:
                            self._pages_completed += 1
                        else:
                            result_errors = result.errors or (result_status.value,)
                            errors.extend(result_errors)
                            failures.append((sequence, page_cursor))
                            may_fetch = False
                            stop_for_continue_as_new = False
                self._pending_children = len(pending)

            # A Continue-As-New boundary is legal only after every page child drained.
            if stop_for_continue_as_new and not failures:
                workflow.continue_as_new(
                    replace(
                        command,
                        next_page_cursor=cursor,
                        page_fetch_attempt=page_fetch_attempt,
                        completed_page_count=(command.completed_page_count + self._pages_completed),
                        prior_error_count=(command.prior_error_count + len(errors)),
                    )
                )
        except asyncio.CancelledError:
            ordered_pending = sorted(pending, key=lambda handle: handle.id)
            for handle in ordered_pending:
                handle.cancel()
            if ordered_pending:
                await asyncio.gather(*ordered_pending, return_exceptions=True)
            raise

        self._phase = "completed"
        checkpoint_cursor = min(failures, key=lambda failure: failure[0])[1] if failures else cursor
        finished = not failures and cursor is None
        total_error_count = command.prior_error_count + len(errors)
        return SyncResult(
            store_key=command.store_key,
            lifecycle_generation=command.lifecycle_generation,
            sync_sequence=command.sync_sequence,
            status=(ResultStatus.PARTIAL if total_error_count else ResultStatus.SUCCEEDED),
            progress=SyncProgress(
                phase="completed",
                pages_completed=command.completed_page_count + self._pages_completed,
                pending_children=0,
                cursor=checkpoint_cursor,
            ),
            errors=tuple(errors),
            details={
                "next_cursor": checkpoint_cursor,
                "finished": finished,
                "error_count": total_error_count,
            },
        )
