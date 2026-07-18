"""Root store sync preserving ordinary page barriers and connector round scheduling."""

from __future__ import annotations

import asyncio
from dataclasses import replace

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.activities.provider_api import (
        ActiveUsersPage,
        ListActiveUsersRequest,
        UserDescriptor,
    )
    from retrieval.temporal.common.ids import (
        failed_user_remediation_workflow_id,
        permit_request_id,
        user_sync_workflow_id,
    )
    from retrieval.temporal.common.quota_waiter import QuotaWaiterMixin
    from retrieval.temporal.common.search_attributes import operation_search_attributes
    from retrieval.temporal.models.lifecycle import (
        OperationStatusEvent,
        RemediationStatusEvent,
    )
    from retrieval.temporal.models.operations import (
        OperationStatus,
        OperationType,
        ResultStatus,
    )
    from retrieval.temporal.models.quota import PermitRequest
    from retrieval.temporal.models.sync import (
        FailedUserRemediationInput,
        RoundState,
        StoreSyncInput,
        SyncMode,
        SyncProgress,
        SyncResult,
        UserCursor,
        UserSyncInput,
    )
    from retrieval.temporal.workflows._policies import provider_activity_options


@workflow.defn(name="RootSyncWorkflow")
class RootSyncWorkflow(QuotaWaiterMixin):
    def __init__(self) -> None:
        self._phase = "pending"
        self._users_completed = 0
        self._users_failed = 0
        self._active_children: dict[str, workflow.ChildWorkflowHandle] = {}
        self._cursor: str | None = None

    @workflow.query(name="get_progress")
    def get_progress(self) -> SyncProgress:
        return SyncProgress(
            phase=self._phase,
            users_completed=self._users_completed,
            users_failed=self._users_failed,
            pending_children=len(self._active_children),
            cursor=self._cursor,
        )

    async def _list_users(
        self,
        command: StoreSyncInput,
        cursor: str | None,
        *,
        attempt: int | None = None,
        rollover_command: StoreSyncInput | None = None,
    ) -> ActiveUsersPage:
        attempt = command.user_page_attempt if attempt is None else attempt
        while True:
            request_id = permit_request_id(
                store_key=command.store_key,
                lifecycle_generation=command.lifecycle_generation,
                sync_sequence=command.sync_sequence,
                user_key="active-user-index",
                resource_key="users",
                cursor=(cursor, attempt),
                operation="list-active-users",
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
            page: ActiveUsersPage | None = None
            try:
                page = await workflow.execute_activity(
                    "provider_list_active_users",
                    ListActiveUsersRequest(
                        store_key=command.store_key,
                        lifecycle_generation=command.lifecycle_generation,
                        cursor=cursor,
                        page_size=command.user_page_size,
                        request_id=request_id,
                        quota_scope=command.quota_scope,
                    ),
                    result_type=ActiveUsersPage,
                    **provider_activity_options(
                        task_queue=command.provider_task_queue,
                        work_class=command.work_class,
                        quota_scope=command.quota_scope,
                        priority_fairness_enabled=command.priority_fairness_enabled,
                    ),
                )
                if page.observation is not None:
                    await self.report_quota_observation(page.observation)
            finally:
                if grant is not None:
                    await self.complete_quota_permit(grant)
            assert page is not None
            if not page.quota_exhausted:
                return page
            attempt += 1
            if workflow.info().is_continue_as_new_suggested():
                # The cursor has not advanced. Carry the retry suffix so the next
                # run asks for a fresh permit instead of waiting on a terminal ID.
                workflow.continue_as_new(
                    replace(
                        rollover_command or replace(command, user_cursor=cursor),
                        user_page_attempt=attempt,
                    )
                )

    def _user_input(
        self,
        command: StoreSyncInput,
        user: UserCursor,
        *,
        sync_sequence: str,
        page_limit: int | None,
    ) -> UserSyncInput:
        return UserSyncInput(
            store_key=command.store_key,
            lifecycle_generation=command.lifecycle_generation,
            sync_sequence=sync_sequence,
            user_key=user.user_key,
            quota_scope=command.quota_scope,
            work_class=command.work_class,
            resource_types=command.resource_types,
            cursor=user.cursor,
            resource_cursors=user.resource_cursors,
            completed_resource_types=user.completed_resource_types,
            page_limit=page_limit,
            resource_concurrency=command.resource_concurrency,
            files_page_window_size=command.files_page_window_size,
            files_per_page_concurrency=command.files_per_page_concurrency,
            document_ingestion_concurrency=command.document_ingestion_concurrency,
            provider_page_size=command.provider_page_size,
            provider_task_queue=command.provider_task_queue,
            priority_fairness_enabled=command.priority_fairness_enabled,
        )

    async def _execute_user(
        self,
        command: StoreSyncInput,
        user: UserCursor,
        *,
        sync_sequence: str,
        page_limit: int | None,
        semaphore: asyncio.Semaphore,
    ) -> SyncResult:
        async with semaphore:
            workflow_id = user_sync_workflow_id(
                command.store_key,
                command.lifecycle_generation,
                sync_sequence,
                user.user_key,
            )
            handle = await workflow.start_child_workflow(
                "UserSyncWorkflow",
                self._user_input(
                    command,
                    user,
                    sync_sequence=sync_sequence,
                    page_limit=page_limit,
                ),
                id=workflow_id,
                cancellation_type=workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
            )
            self._active_children[workflow_id] = handle
            try:
                return await handle
            finally:
                self._active_children.pop(workflow_id, None)

    async def _join_user_batch(
        self,
        command: StoreSyncInput,
        users: list[UserCursor],
        *,
        sync_sequence: str,
        page_limit: int | None,
        concurrency: int,
    ) -> list[SyncResult | BaseException]:
        semaphore = asyncio.Semaphore(max(1, concurrency))
        tasks = [
            asyncio.create_task(
                self._execute_user(
                    command,
                    user,
                    sync_sequence=sync_sequence,
                    page_limit=page_limit,
                    semaphore=semaphore,
                )
            )
            for user in users
        ]
        try:
            # The page/round barrier and explicit exception classification are intentional.
            return list(await asyncio.gather(*tasks, return_exceptions=True))
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            for workflow_id in sorted(self._active_children):
                self._active_children[workflow_id].cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    @staticmethod
    def _valid_users(users: tuple[UserDescriptor, ...]) -> list[UserCursor]:
        return [UserCursor(user_key=user.user_key) for user in users if user.valid]

    async def _ordinary(self, command: StoreSyncInput) -> tuple[SyncResult, tuple[str, ...]]:
        cursor = command.user_cursor
        user_page_attempt = command.user_page_attempt
        failed = list(command.failed_user_keys)
        errors: list[str] = []
        while True:
            self._cursor = cursor
            page = await self._list_users(
                command,
                cursor,
                attempt=user_page_attempt,
                rollover_command=replace(
                    command,
                    user_cursor=cursor,
                    user_page_attempt=user_page_attempt,
                    failed_user_keys=tuple(dict.fromkeys(failed)),
                    prior_error_count=(command.prior_error_count + len(errors)),
                ),
            )
            user_page_attempt = 0
            users = self._valid_users(page.users)
            results = await self._join_user_batch(
                command,
                users,
                sync_sequence=command.sync_sequence,
                page_limit=None,
                concurrency=command.max_active_users,
            )
            for user, result in zip(users, results, strict=True):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                if isinstance(result, BaseException):
                    failed.append(user.user_key)
                    errors.append(type(result).__name__)
                    self._users_failed += 1
                elif result.status in {ResultStatus.FAILED, ResultStatus.REJECTED}:
                    failed.append(user.user_key)
                    errors.extend(result.errors or (result.status.value,))
                    self._users_failed += 1
                else:
                    self._users_completed += 1
                    errors.extend(result.errors)

            cursor = page.next_cursor
            self._cursor = cursor
            if cursor is None:
                break
            if workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(
                    replace(
                        command,
                        user_cursor=cursor,
                        user_page_attempt=0,
                        failed_user_keys=tuple(dict.fromkeys(failed)),
                        prior_error_count=(command.prior_error_count + len(errors)),
                    )
                )

        total_error_count = command.prior_error_count + len(errors)
        status = ResultStatus.PARTIAL if total_error_count else ResultStatus.SUCCEEDED
        return (
            SyncResult(
                store_key=command.store_key,
                lifecycle_generation=command.lifecycle_generation,
                sync_sequence=command.sync_sequence,
                status=status,
                progress=SyncProgress(
                    phase="completed",
                    users_completed=self._users_completed,
                    users_failed=self._users_failed,
                ),
                failed_user_keys=tuple(dict.fromkeys(failed)),
                errors=tuple(errors),
                details={"error_count": total_error_count},
            ),
            tuple(dict.fromkeys(failed)),
        )

    async def _refill_round_users(
        self,
        command: StoreSyncInput,
        active: list[UserCursor],
        buffered: list[UserCursor],
        cursor: str | None,
        exhausted: bool,
        *,
        user_page_attempt: int,
        round_number: int,
        failed_user_keys: tuple[str, ...],
        prior_error_count: int,
    ) -> tuple[str | None, bool, int]:
        window = max(1, command.round_user_window_size)
        while len(active) < window:
            while buffered and len(active) < window:
                active.append(buffered.pop(0))
            if len(active) >= window or exhausted:
                break
            page = await self._list_users(
                command,
                cursor,
                attempt=user_page_attempt,
                rollover_command=replace(
                    command,
                    user_page_attempt=user_page_attempt,
                    prior_error_count=prior_error_count,
                    round_state=RoundState(
                        active_users=tuple(active),
                        buffered_users=tuple(buffered),
                        next_user_cursor=cursor,
                        round_number=round_number,
                        users_exhausted=exhausted,
                        failed_user_keys=failed_user_keys,
                    ),
                ),
            )
            user_page_attempt = 0
            buffered.extend(self._valid_users(page.users))
            cursor = page.next_cursor
            exhausted = cursor is None
            if not buffered and exhausted:
                break
        return cursor, exhausted, user_page_attempt

    async def _round_mode(self, command: StoreSyncInput) -> tuple[SyncResult, tuple[str, ...]]:
        state = command.round_state or RoundState(next_user_cursor=command.user_cursor)
        active = list(state.active_users)
        buffered = list(state.buffered_users)
        cursor = state.next_user_cursor
        exhausted = state.users_exhausted
        failed = list(state.failed_user_keys)
        errors: list[str] = []
        round_number = state.round_number
        user_page_attempt = command.user_page_attempt

        cursor, exhausted, user_page_attempt = await self._refill_round_users(
            command,
            active,
            buffered,
            cursor,
            exhausted,
            user_page_attempt=user_page_attempt,
            round_number=round_number,
            failed_user_keys=tuple(dict.fromkeys(failed)),
            prior_error_count=(command.prior_error_count + len(errors)),
        )
        while active:
            self._phase = f"round:{round_number}"
            round_sequence = f"{command.sync_sequence}:round:{round_number}"
            results = await self._join_user_batch(
                command,
                active,
                sync_sequence=round_sequence,
                page_limit=max(1, command.round_page_slice_size),
                concurrency=command.round_user_window_size,
            )
            next_active: list[UserCursor] = []
            for user, result in zip(active, results, strict=True):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                if isinstance(result, BaseException):
                    failed.append(user.user_key)
                    errors.append(type(result).__name__)
                    self._users_failed += 1
                    continue
                if result.status in {ResultStatus.FAILED, ResultStatus.REJECTED}:
                    failed.append(user.user_key)
                    errors.extend(result.errors or (result.status.value,))
                    self._users_failed += 1
                    continue
                errors.extend(result.errors)
                if bool(result.details.get("finished", True)):
                    self._users_completed += 1
                else:
                    next_cursor = result.details.get("next_cursor")
                    next_active.append(
                        UserCursor(
                            user_key=user.user_key,
                            cursor=next_cursor if isinstance(next_cursor, str) else None,
                            resource_cursors={
                                key: value if isinstance(value, str) else None
                                for key, value in dict(
                                    result.details.get("resource_cursors", {})
                                ).items()
                            },
                            completed_resource_types=tuple(
                                result.details.get("completed_resource_types", ())
                            ),
                            pages_completed=(user.pages_completed + command.round_page_slice_size),
                            finished=False,
                        )
                    )

            # The whole round is drained before carrying unfinished users and refilling.
            active = next_active
            round_number += 1
            cursor, exhausted, user_page_attempt = await self._refill_round_users(
                command,
                active,
                buffered,
                cursor,
                exhausted,
                user_page_attempt=user_page_attempt,
                round_number=round_number,
                failed_user_keys=tuple(dict.fromkeys(failed)),
                prior_error_count=(command.prior_error_count + len(errors)),
            )
            if workflow.info().is_continue_as_new_suggested() and (
                active or buffered or not exhausted
            ):
                workflow.continue_as_new(
                    replace(
                        command,
                        user_page_attempt=0,
                        prior_error_count=(command.prior_error_count + len(errors)),
                        round_state=RoundState(
                            active_users=tuple(active),
                            buffered_users=tuple(buffered),
                            next_user_cursor=cursor,
                            round_number=round_number,
                            users_exhausted=exhausted,
                            failed_user_keys=tuple(dict.fromkeys(failed)),
                        ),
                    )
                )

        total_error_count = command.prior_error_count + len(errors)
        status = ResultStatus.PARTIAL if total_error_count else ResultStatus.SUCCEEDED
        return (
            SyncResult(
                store_key=command.store_key,
                lifecycle_generation=command.lifecycle_generation,
                sync_sequence=command.sync_sequence,
                status=status,
                progress=SyncProgress(
                    phase="completed",
                    users_completed=self._users_completed,
                    users_failed=self._users_failed,
                ),
                failed_user_keys=tuple(dict.fromkeys(failed)),
                errors=tuple(errors),
                details={
                    "rounds_completed": round_number,
                    "error_count": total_error_count,
                },
            ),
            tuple(dict.fromkeys(failed)),
        )

    async def _start_remediation(
        self, command: StoreSyncInput, failed_user_keys: tuple[str, ...]
    ) -> str | None:
        if not failed_user_keys:
            return None
        workflow_id = failed_user_remediation_workflow_id(
            command.store_key,
            command.lifecycle_generation,
            command.sync_sequence,
        )
        remediation_input = FailedUserRemediationInput(
            store_key=command.store_key,
            lifecycle_generation=command.lifecycle_generation,
            sync_sequence=command.sync_sequence,
            operation_id=workflow_id,
            failed_user_keys=failed_user_keys,
            quota_scope=command.quota_scope,
            resource_types=command.resource_types,
            resource_concurrency=command.resource_concurrency,
            files_page_window_size=command.files_page_window_size,
            files_per_page_concurrency=command.files_per_page_concurrency,
            document_ingestion_concurrency=command.document_ingestion_concurrency,
            provider_page_size=command.provider_page_size,
            provider_task_queue=command.provider_task_queue,
            priority_fairness_enabled=command.priority_fairness_enabled,
            controller_workflow_id=command.controller_workflow_id,
            recent_page_cap=command.activation_recent_page_cap,
            enable_search_attributes=command.enable_search_attributes,
        )
        handle = await workflow.start_child_workflow(
            "FailedUserRemediationWorkflow",
            remediation_input,
            id=workflow_id,
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            cancellation_type=workflow.ChildWorkflowCancellationType.ABANDON,
            search_attributes=(
                operation_search_attributes(
                    store_key=command.store_key,
                    lifecycle_generation=command.lifecycle_generation,
                    operation_type=OperationType.REMEDIATION,
                    sync_sequence=command.sync_sequence,
                    quota_scope=command.quota_scope,
                    work_class=command.work_class,
                    current_phase="activating_users",
                )
                if command.enable_search_attributes
                else None
            ),
        )
        # start_child_workflow was awaited, so the registration follows a durable
        # ChildWorkflowExecutionStarted acknowledgement.
        if command.controller_workflow_id is not None:
            registration = asyncio.create_task(
                workflow.get_external_workflow_handle(command.controller_workflow_id).signal(
                    "remediation_started",
                    RemediationStatusEvent(
                        operation_id=workflow_id,
                        workflow_id=workflow_id,
                        lifecycle_generation=command.lifecycle_generation,
                        sync_sequence=command.sync_sequence,
                        status=OperationStatus.RUNNING,
                    ),
                )
            )
            try:
                await asyncio.shield(registration)
            except asyncio.CancelledError:
                # The detached child cannot be left running without durable ownership.
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()
                try:
                    await registration
                finally:
                    raise
            except Exception:
                handle.cancel()
                await asyncio.gather(handle, return_exceptions=True)
                raise
        return workflow_id

    async def _report_sync_terminal(
        self,
        command: StoreSyncInput,
        status: OperationStatus,
        result_status: ResultStatus,
        message: str | None = None,
    ) -> None:
        if command.controller_workflow_id is None:
            return
        await workflow.get_external_workflow_handle(command.controller_workflow_id).signal(
            "operation_status",
            OperationStatusEvent(
                operation_id=workflow.info().workflow_id,
                workflow_id=workflow.info().workflow_id,
                lifecycle_generation=command.lifecycle_generation,
                status=status,
                result_status=result_status,
                message=message,
            ),
        )

    @workflow.run
    async def run(self, command: StoreSyncInput) -> SyncResult:
        self._phase = command.mode.value
        try:
            if command.mode is SyncMode.ROUND:
                result, failed = await self._round_mode(command)
            else:
                result, failed = await self._ordinary(command)
            remediation_id = await self._start_remediation(command, failed)
            result.details["remediation_workflow_id"] = remediation_id
            await self._report_sync_terminal(
                command,
                OperationStatus.COMPLETED,
                result.status,
            )
            self._phase = "completed"
            return result
        except asyncio.CancelledError:
            active_handles = [
                self._active_children[workflow_id] for workflow_id in sorted(self._active_children)
            ]
            for handle in active_handles:
                handle.cancel()
            if self._active_children:
                await asyncio.gather(*active_handles, return_exceptions=True)
            await self._report_sync_terminal(
                command,
                OperationStatus.CANCELED,
                ResultStatus.CANCELED,
                "sync canceled",
            )
            raise
        except Exception as exc:
            await self._report_sync_terminal(
                command,
                OperationStatus.FAILED,
                ResultStatus.FAILED,
                type(exc).__name__,
            )
            raise
