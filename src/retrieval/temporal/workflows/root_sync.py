"""Root store sync with ordinary page barriers and provider round scheduling."""

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


_FAILED_USER_SAMPLE_LIMIT = 100
_ERROR_SAMPLE_LIMIT = 100
_REMEDIATION_ID_SAMPLE_LIMIT = 20
_MAX_ACTIVE_REMEDIATIONS = 4


@workflow.defn(name="RootSyncWorkflow")
class RootSyncWorkflow(QuotaWaiterMixin):
    def __init__(self) -> None:
        self._phase = "pending"
        self._users_completed = 0
        self._users_failed = 0
        self._active_children: dict[str, workflow.ChildWorkflowHandle] = {}
        self._active_remediations: dict[str, workflow.ChildWorkflowHandle] = {}
        self._cursor: str | None = None

    @staticmethod
    def _extend_sample(target: list[str], values: tuple[str, ...] | list[str], limit: int) -> None:
        remaining = max(0, limit - len(target))
        if remaining:
            target.extend(value for value in values[:remaining] if value not in target)

    def _initialize_progress(self, command: StoreSyncInput) -> None:
        self._users_completed = max(0, command.prior_users_completed)
        self._users_failed = max(0, command.prior_users_failed)

    async def _drain_remediations(self, *, drain_all: bool) -> None:
        target = 0 if drain_all else _MAX_ACTIVE_REMEDIATIONS - 1
        while len(self._active_remediations) > target:
            handles = sorted(self._active_remediations.values(), key=lambda handle: handle.id)
            done, _pending = await workflow.wait(
                handles,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for handle in sorted(done, key=lambda item: item.id):
                self._active_remediations.pop(handle.id, None)
                try:
                    await handle
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    workflow.logger.warning(
                        "Detached remediation %s closed with %s",
                        handle.id,
                        type(exc).__name__,
                    )

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
                result_type=SyncResult,
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
        self._initialize_progress(command)
        cursor = command.user_cursor
        user_page_attempt = command.user_page_attempt
        failed_sample = list(command.failed_user_keys[:_FAILED_USER_SAMPLE_LIMIT])
        error_sample = list(command.error_sample[:_ERROR_SAMPLE_LIMIT])
        error_count = command.prior_error_count
        pages_completed = command.user_pages_completed
        remediation_count = command.remediation_workflow_count
        remediation_ids = list(
            command.remediation_workflow_id_sample[:_REMEDIATION_ID_SAMPLE_LIMIT]
        )
        failed_sample_remediated = (
            command.failed_user_keys_remediated or not command.failed_user_keys
        )

        if (
            command.failed_user_keys
            and not command.failed_user_keys_remediated
            and command.controller_workflow_id is not None
        ):
            remediation_id = await self._start_remediation(
                command,
                tuple(command.failed_user_keys),
                partition=("legacy-carried", pages_completed),
            )
            if remediation_id is not None:
                failed_sample_remediated = True
                remediation_count += 1
                self._extend_sample(
                    remediation_ids,
                    [remediation_id],
                    _REMEDIATION_ID_SAMPLE_LIMIT,
                )
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
                    failed_user_keys=tuple(failed_sample),
                    prior_error_count=error_count,
                    prior_users_completed=self._users_completed,
                    prior_users_failed=self._users_failed,
                    user_pages_completed=pages_completed,
                    error_sample=tuple(error_sample),
                    remediation_workflow_count=remediation_count,
                    remediation_workflow_id_sample=tuple(remediation_ids),
                    failed_user_keys_remediated=failed_sample_remediated,
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
            page_failed: list[str] = []
            for user, result in zip(users, results, strict=True):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                if isinstance(result, BaseException):
                    result_errors = (type(result).__name__,)
                    incomplete = True
                else:
                    result_errors = result.errors
                    incomplete = (
                        result.status is not ResultStatus.SUCCEEDED
                        or bool(result.errors)
                        or not bool(result.details.get("finished", True))
                    )
                if incomplete:
                    page_failed.append(user.user_key)
                    self._extend_sample(
                        failed_sample,
                        [user.user_key],
                        _FAILED_USER_SAMPLE_LIMIT,
                    )
                    if not result_errors:
                        result_errors = (
                            result.status.value
                            if not isinstance(result, BaseException)
                            else "user sync failed",
                        )
                    error_count += len(result_errors)
                    self._extend_sample(error_sample, list(result_errors), _ERROR_SAMPLE_LIMIT)
                    self._users_failed += 1
                else:
                    self._users_completed += 1

            if page_failed:
                remediation_id = await self._start_remediation(
                    command,
                    tuple(page_failed),
                    partition=("ordinary", pages_completed),
                )
                if remediation_id is not None:
                    failed_sample_remediated = True
                    remediation_count += 1
                    self._extend_sample(
                        remediation_ids,
                        [remediation_id],
                        _REMEDIATION_ID_SAMPLE_LIMIT,
                    )

            pages_completed += 1
            cursor = page.next_cursor
            self._cursor = cursor
            if cursor is None:
                break
            if workflow.info().is_continue_as_new_suggested():
                await self._drain_remediations(drain_all=True)
                workflow.continue_as_new(
                    replace(
                        command,
                        user_cursor=cursor,
                        user_page_attempt=0,
                        failed_user_keys=tuple(failed_sample),
                        prior_error_count=error_count,
                        prior_users_completed=self._users_completed,
                        prior_users_failed=self._users_failed,
                        user_pages_completed=pages_completed,
                        error_sample=tuple(error_sample),
                        remediation_workflow_count=remediation_count,
                        remediation_workflow_id_sample=tuple(remediation_ids),
                        failed_user_keys_remediated=failed_sample_remediated,
                    )
                )

        status = ResultStatus.PARTIAL if error_count else ResultStatus.SUCCEEDED
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
                failed_user_keys=tuple(failed_sample),
                errors=tuple(error_sample),
                details={
                    "error_count": error_count,
                    "failed_user_count": self._users_failed,
                    "user_pages_completed": pages_completed,
                    "remediation_workflow_count": remediation_count,
                    "remediation_workflow_ids": tuple(remediation_ids),
                    "remediation_workflow_id": (
                        remediation_ids[0] if remediation_count == 1 and remediation_ids else None
                    ),
                },
            ),
            tuple(failed_sample),
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
        prior_users_completed: int,
        prior_users_failed: int,
        error_sample: tuple[str, ...],
        remediation_workflow_count: int,
        remediation_workflow_id_sample: tuple[str, ...],
        failed_user_keys_remediated: bool,
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
                    prior_users_completed=prior_users_completed,
                    prior_users_failed=prior_users_failed,
                    error_sample=error_sample,
                    remediation_workflow_count=remediation_workflow_count,
                    remediation_workflow_id_sample=remediation_workflow_id_sample,
                    failed_user_keys=failed_user_keys,
                    failed_user_keys_remediated=failed_user_keys_remediated,
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
        self._initialize_progress(command)
        state = command.round_state or RoundState(next_user_cursor=command.user_cursor)
        active = list(state.active_users)
        buffered = list(state.buffered_users)
        cursor = state.next_user_cursor
        exhausted = state.users_exhausted
        failed_sample: list[str] = []
        self._extend_sample(
            failed_sample,
            list(command.failed_user_keys) + list(state.failed_user_keys),
            _FAILED_USER_SAMPLE_LIMIT,
        )
        error_sample = list(command.error_sample[:_ERROR_SAMPLE_LIMIT])
        error_count = command.prior_error_count
        remediation_count = command.remediation_workflow_count
        remediation_ids = list(
            command.remediation_workflow_id_sample[:_REMEDIATION_ID_SAMPLE_LIMIT]
        )
        failed_sample_remediated = command.failed_user_keys_remediated or not failed_sample
        round_number = state.round_number
        user_page_attempt = command.user_page_attempt

        if (
            failed_sample
            and not command.failed_user_keys_remediated
            and command.controller_workflow_id is not None
        ):
            remediation_id = await self._start_remediation(
                command,
                tuple(failed_sample),
                partition=("legacy-round", round_number),
            )
            if remediation_id is not None:
                failed_sample_remediated = True
                remediation_count += 1
                self._extend_sample(
                    remediation_ids,
                    [remediation_id],
                    _REMEDIATION_ID_SAMPLE_LIMIT,
                )

        cursor, exhausted, user_page_attempt = await self._refill_round_users(
            command,
            active,
            buffered,
            cursor,
            exhausted,
            user_page_attempt=user_page_attempt,
            round_number=round_number,
            failed_user_keys=tuple(failed_sample),
            prior_error_count=error_count,
            prior_users_completed=self._users_completed,
            prior_users_failed=self._users_failed,
            error_sample=tuple(error_sample),
            remediation_workflow_count=remediation_count,
            remediation_workflow_id_sample=tuple(remediation_ids),
            failed_user_keys_remediated=failed_sample_remediated,
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
            round_failed: list[str] = []
            for user, result in zip(active, results, strict=True):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                if isinstance(result, BaseException):
                    result_errors = (type(result).__name__,)
                    incomplete = True
                else:
                    result_errors = result.errors
                    incomplete = result.status is not ResultStatus.SUCCEEDED or bool(result.errors)
                if incomplete:
                    round_failed.append(user.user_key)
                    self._extend_sample(
                        failed_sample,
                        [user.user_key],
                        _FAILED_USER_SAMPLE_LIMIT,
                    )
                    if not result_errors:
                        result_errors = (
                            result.status.value
                            if not isinstance(result, BaseException)
                            else "user sync failed",
                        )
                    error_count += len(result_errors)
                    self._extend_sample(error_sample, list(result_errors), _ERROR_SAMPLE_LIMIT)
                    self._users_failed += 1
                    continue
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

            if round_failed:
                remediation_id = await self._start_remediation(
                    command,
                    tuple(round_failed),
                    partition=("round", round_number),
                )
                if remediation_id is not None:
                    failed_sample_remediated = True
                    remediation_count += 1
                    self._extend_sample(
                        remediation_ids,
                        [remediation_id],
                        _REMEDIATION_ID_SAMPLE_LIMIT,
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
                failed_user_keys=tuple(failed_sample),
                prior_error_count=error_count,
                prior_users_completed=self._users_completed,
                prior_users_failed=self._users_failed,
                error_sample=tuple(error_sample),
                remediation_workflow_count=remediation_count,
                remediation_workflow_id_sample=tuple(remediation_ids),
                failed_user_keys_remediated=failed_sample_remediated,
            )
            if workflow.info().is_continue_as_new_suggested() and (
                active or buffered or not exhausted
            ):
                await self._drain_remediations(drain_all=True)
                workflow.continue_as_new(
                    replace(
                        command,
                        user_page_attempt=0,
                        failed_user_keys=tuple(failed_sample),
                        prior_error_count=error_count,
                        prior_users_completed=self._users_completed,
                        prior_users_failed=self._users_failed,
                        error_sample=tuple(error_sample),
                        remediation_workflow_count=remediation_count,
                        remediation_workflow_id_sample=tuple(remediation_ids),
                        failed_user_keys_remediated=failed_sample_remediated,
                        round_state=RoundState(
                            active_users=tuple(active),
                            buffered_users=tuple(buffered),
                            next_user_cursor=cursor,
                            round_number=round_number,
                            users_exhausted=exhausted,
                            failed_user_keys=tuple(failed_sample),
                        ),
                    )
                )

        status = ResultStatus.PARTIAL if error_count else ResultStatus.SUCCEEDED
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
                failed_user_keys=tuple(failed_sample),
                errors=tuple(error_sample),
                details={
                    "rounds_completed": round_number,
                    "error_count": error_count,
                    "failed_user_count": self._users_failed,
                    "remediation_workflow_count": remediation_count,
                    "remediation_workflow_ids": tuple(remediation_ids),
                    "remediation_workflow_id": (
                        remediation_ids[0] if remediation_count == 1 and remediation_ids else None
                    ),
                },
            ),
            tuple(failed_sample),
        )

    async def _start_remediation(
        self,
        command: StoreSyncInput,
        failed_user_keys: tuple[str, ...],
        *,
        partition: object | None = None,
    ) -> str | None:
        if not failed_user_keys:
            return None
        await self._drain_remediations(drain_all=False)
        workflow_id = failed_user_remediation_workflow_id(
            command.store_key,
            command.lifecycle_generation,
            command.sync_sequence,
            partition,
        )
        remediation_sequence = (
            command.sync_sequence
            if partition is None
            else f"{command.sync_sequence}:partition:{partition}"
        )
        remediation_input = FailedUserRemediationInput(
            store_key=command.store_key,
            lifecycle_generation=command.lifecycle_generation,
            sync_sequence=remediation_sequence,
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
            result_type=SyncResult,
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            cancellation_type=workflow.ChildWorkflowCancellationType.ABANDON,
            search_attributes=(
                operation_search_attributes(
                    store_key=command.store_key,
                    lifecycle_generation=command.lifecycle_generation,
                    operation_type=OperationType.REMEDIATION,
                    sync_sequence=remediation_sequence,
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
                        sync_sequence=remediation_sequence,
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
        self._active_remediations[workflow_id] = handle
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
                result, _failed_sample = await self._round_mode(command)
            else:
                result, _failed_sample = await self._ordinary(command)
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
