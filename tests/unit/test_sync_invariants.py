from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from temporalio import workflow

from retrieval.temporal.activities.provider_api import (
    ActiveUsersPage,
    ResourcePageManifest,
    UserDescriptor,
)
from retrieval.temporal.common.ids import (
    document_ingest_workflow_id,
    failed_user_remediation_workflow_id,
    user_sync_workflow_id,
)
from retrieval.temporal.models.documents import DocumentMutation, DocumentRef
from retrieval.temporal.models.lifecycle import (
    LifecycleMutationResult,
    StoreLifecycleState,
)
from retrieval.temporal.models.operations import OperationStatus, ResultStatus, WorkClass
from retrieval.temporal.models.sync import (
    ActivateUserInput,
    FailedUserRemediationInput,
    FilesPageInput,
    ResourcePagesInput,
    RoundState,
    StoreSyncInput,
    SyncMode,
    SyncProgress,
    SyncResult,
    UserCursor,
)
from retrieval.temporal.workflows import activate_user as activate_module
from retrieval.temporal.workflows import files_page as files_page_module
from retrieval.temporal.workflows import resource_pages as resource_pages_module
from retrieval.temporal.workflows import root_sync as root_sync_module
from retrieval.temporal.workflows.activate_user import ActivateUserWorkflow
from retrieval.temporal.workflows.failed_user_remediation import (
    FailedUserRemediationWorkflow,
)
from retrieval.temporal.workflows.files_page import FilesPageWorkflow
from retrieval.temporal.workflows.resource_pages import ResourcePagesWorkflow
from retrieval.temporal.workflows.root_sync import RootSyncWorkflow


def sync_result(
    sync_sequence: str,
    *,
    status: ResultStatus = ResultStatus.SUCCEEDED,
    details: dict[str, object] | None = None,
) -> SyncResult:
    return SyncResult(
        store_key="opaque-store",
        lifecycle_generation=3,
        sync_sequence=sync_sequence,
        status=status,
        progress=SyncProgress(phase="completed"),
        details=details or {},
    )


@pytest.mark.asyncio
async def test_ordinary_user_batch_joins_all_tasks_and_returns_user_failures() -> None:
    root = RootSyncWorkflow()
    command = StoreSyncInput("opaque-store", 3, "sync")
    users = [UserCursor("good"), UserCursor("bad")]
    completed: list[str] = []

    async def fake_execute_user(
        _command: StoreSyncInput,
        user: UserCursor,
        **_options: object,
    ) -> SyncResult:
        await asyncio.sleep(0)
        completed.append(user.user_key)
        if user.user_key == "bad":
            raise RuntimeError("per-user failure")
        return sync_result("sync")

    root._execute_user = fake_execute_user  # type: ignore[method-assign]

    results = await root._join_user_batch(
        command,
        users,
        sync_sequence="sync",
        page_limit=None,
        concurrency=2,
    )

    assert completed == ["good", "bad"]
    assert isinstance(results[0], SyncResult)
    assert isinstance(results[1], RuntimeError)


@pytest.mark.asyncio
async def test_outer_batch_cancellation_cancels_and_drains_every_user_task() -> None:
    root = RootSyncWorkflow()
    command = StoreSyncInput("opaque-store", 3, "sync")
    users = [UserCursor("one"), UserCursor("two")]
    all_started = asyncio.Event()
    release = asyncio.Event()
    started: set[str] = set()
    canceled: set[str] = set()

    async def blocking_execute_user(
        _command: StoreSyncInput,
        user: UserCursor,
        **_options: object,
    ) -> SyncResult:
        started.add(user.user_key)
        if len(started) == len(users):
            all_started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            canceled.add(user.user_key)
            raise
        return sync_result("sync")

    root._execute_user = blocking_execute_user  # type: ignore[method-assign]
    batch = asyncio.create_task(
        root._join_user_batch(
            command,
            users,
            sync_sequence="sync",
            page_limit=None,
            concurrency=2,
        )
    )
    await asyncio.wait_for(all_started.wait(), timeout=1)

    batch.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(batch, timeout=1)
    assert canceled == {"one", "two"}


@pytest.mark.asyncio
async def test_nested_cancellation_is_not_classified_as_an_ordinary_user_failure() -> None:
    root = RootSyncWorkflow()
    command = StoreSyncInput("opaque-store", 3, "sync")

    async def one_page(
        _command: StoreSyncInput,
        _cursor: str | None,
        **_options: object,
    ) -> ActiveUsersPage:
        return ActiveUsersPage(
            request_id="users",
            users=(UserDescriptor("user-1"),),
        )

    async def canceled_batch(*_args: object, **_kwargs: object) -> list[BaseException]:
        return [asyncio.CancelledError()]

    root._list_users = one_page  # type: ignore[method-assign]
    root._join_user_batch = canceled_batch  # type: ignore[method-assign]

    with pytest.raises(asyncio.CancelledError):
        await root._ordinary(command)
    assert root._users_failed == 0


@pytest.mark.asyncio
async def test_continue_as_new_is_not_reported_as_terminal_sync_failure() -> None:
    root = RootSyncWorkflow()
    command = StoreSyncInput("opaque-store", 3, "sync")
    terminal_reports: list[tuple[object, ...]] = []

    class SimulatedContinueAsNew(workflow.ContinueAsNewError):
        pass

    async def continue_ordinary(
        _command: StoreSyncInput,
    ) -> tuple[SyncResult, tuple[str, ...]]:
        raise SimulatedContinueAsNew()

    async def record_terminal(*args: object, **_kwargs: object) -> None:
        terminal_reports.append(args)

    root._ordinary = continue_ordinary  # type: ignore[method-assign]
    root._report_sync_terminal = record_terminal  # type: ignore[method-assign]

    with pytest.raises(workflow.ContinueAsNewError):
        await root.run(command)
    assert terminal_reports == []


class ContinueAsNewCalled(BaseException):
    pass


@pytest.mark.asyncio
async def test_root_quota_retry_carries_attempt_and_uses_a_fresh_request_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    carried: list[StoreSyncInput] = []
    requests: list[object] = []
    responses = [
        ActiveUsersPage(request_id="quota", quota_exhausted=True),
        ActiveUsersPage(request_id="success"),
    ]
    suggested = True

    async def execute_activity(
        _activity_name: str,
        activity_input: object,
        **_options: object,
    ) -> ActiveUsersPage:
        requests.append(activity_input)
        return responses.pop(0)

    def info() -> SimpleNamespace:
        return SimpleNamespace(is_continue_as_new_suggested=lambda: suggested)

    def continue_as_new(command: StoreSyncInput) -> None:
        carried.append(command)
        raise ContinueAsNewCalled

    monkeypatch.setattr(root_sync_module.workflow, "execute_activity", execute_activity)
    monkeypatch.setattr(root_sync_module.workflow, "info", info)
    monkeypatch.setattr(root_sync_module.workflow, "continue_as_new", continue_as_new)
    command = StoreSyncInput(
        "opaque-store",
        3,
        "sync",
        user_cursor="cursor-7",
        failed_user_keys=("failed-before-rollover",),
        prior_error_count=2,
        user_page_attempt=4,
    )

    with pytest.raises(ContinueAsNewCalled):
        await RootSyncWorkflow()._ordinary(command)

    assert carried == [
        StoreSyncInput(
            "opaque-store",
            3,
            "sync",
            user_cursor="cursor-7",
            failed_user_keys=("failed-before-rollover",),
            prior_error_count=2,
            user_page_attempt=5,
        )
    ]

    suggested = False
    result, failed = await RootSyncWorkflow()._ordinary(carried[0])

    assert requests[0].cursor == requests[1].cursor == "cursor-7"
    assert requests[0].request_id != requests[1].request_id
    assert failed == ("failed-before-rollover",)
    assert result.status is ResultStatus.PARTIAL


@pytest.mark.asyncio
async def test_round_mode_drains_the_round_before_continue_as_new(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = RootSyncWorkflow()
    events: list[str] = []
    carried: list[StoreSyncInput] = []

    async def no_refill(
        _command: StoreSyncInput,
        _active: list[UserCursor],
        _buffered: list[UserCursor],
        cursor: str | None,
        exhausted: bool,
        **options: object,
    ) -> tuple[str | None, bool, int]:
        events.append("refill")
        return cursor, exhausted, int(options["user_page_attempt"])

    async def drained_round(
        _command: StoreSyncInput,
        _users: list[UserCursor],
        **_options: object,
    ) -> list[SyncResult]:
        events.append("round-started")
        await asyncio.sleep(0)
        events.append("round-drained")
        return [
            sync_result(
                "sync:round:0",
                details={
                    "finished": False,
                    "next_cursor": "page-2",
                    "resource_cursors": {
                        "files": "files-page-2",
                        "comments": "comments-page-7",
                    },
                },
            )
        ]

    def continue_as_new(command: StoreSyncInput) -> None:
        events.append("continue-as-new")
        carried.append(command)
        raise ContinueAsNewCalled

    root._refill_round_users = no_refill  # type: ignore[method-assign]
    root._join_user_batch = drained_round  # type: ignore[method-assign]
    monkeypatch.setattr(
        root_sync_module.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: True),
    )
    monkeypatch.setattr(
        root_sync_module.workflow,
        "continue_as_new",
        continue_as_new,
    )
    command = StoreSyncInput(
        "opaque-store",
        3,
        "sync",
        mode=SyncMode.ROUND,
        round_page_slice_size=2,
        round_state=RoundState(
            active_users=(UserCursor("user-1", cursor="page-1"),),
            users_exhausted=True,
        ),
    )

    with pytest.raises(ContinueAsNewCalled):
        await root._round_mode(command)

    assert events.index("round-drained") < events.index("continue-as-new")
    assert carried[0].round_state is not None
    assert carried[0].round_state.active_users == (
        UserCursor(
            "user-1",
            cursor="page-2",
            resource_cursors={
                "files": "files-page-2",
                "comments": "comments-page-7",
            },
            pages_completed=2,
        ),
    )
    assert carried[0].round_state.round_number == 1


@pytest.mark.asyncio
async def test_resource_pages_drains_started_children_before_continue_as_new(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = ResourcePagesWorkflow()
    events: list[str] = []
    carried: list[ResourcePagesInput] = []

    class PageHandle:
        id = "files-page/test"

        def __await__(self):  # type: ignore[no-untyped-def]
            async def finish() -> object:
                events.append("page-drained")
                return SimpleNamespace(errors=())

            return finish().__await__()

        def cancel(self) -> None:
            events.append("page-canceled")

    async def fetch(
        _command: ResourcePagesInput,
        _cursor: str | None,
        **_options: object,
    ) -> ResourcePageManifest:
        events.append("page-fetched")
        return ResourcePageManifest(
            request_id="request",
            page_key="page-1",
            next_cursor="cursor-2",
        )

    async def start(_command: ResourcePagesInput, _manifest: ResourcePageManifest) -> PageHandle:
        events.append("page-started")
        return PageHandle()

    async def wait_for_page(
        handles: set[PageHandle], **_kwargs: object
    ) -> tuple[set[PageHandle], set[PageHandle]]:
        events.append("wait-first-completed")
        return handles, set()

    def continue_as_new(command: ResourcePagesInput) -> None:
        events.append("continue-as-new")
        carried.append(command)
        raise ContinueAsNewCalled

    pages._fetch_manifest = fetch  # type: ignore[method-assign]
    pages._start_page_child = start  # type: ignore[method-assign]
    monkeypatch.setattr(resource_pages_module.workflow, "wait", wait_for_page)
    monkeypatch.setattr(
        resource_pages_module.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: True),
    )
    monkeypatch.setattr(
        resource_pages_module.workflow,
        "continue_as_new",
        continue_as_new,
    )
    command = ResourcePagesInput(
        store_key="opaque-store",
        lifecycle_generation=3,
        sync_sequence="sync",
        user_key="user",
        resource_key="files",
    )

    with pytest.raises(ContinueAsNewCalled):
        await pages.run(command)

    assert events.index("page-drained") < events.index("continue-as-new")
    assert pages._pending_children == 0
    assert carried[0].next_page_cursor == "cursor-2"
    assert carried[0].completed_page_count == 1


@pytest.mark.asyncio
async def test_resource_page_limit_is_preserved_across_continue_as_new() -> None:
    pages = ResourcePagesWorkflow()
    fetches = 0

    async def unexpected_fetch(
        _command: ResourcePagesInput,
        _cursor: str | None,
        **_options: object,
    ) -> ResourcePageManifest:
        nonlocal fetches
        fetches += 1
        raise AssertionError("page budget was already exhausted")

    pages._fetch_manifest = unexpected_fetch  # type: ignore[method-assign]
    result = await pages.run(
        ResourcePagesInput(
            store_key="opaque-store",
            lifecycle_generation=3,
            sync_sequence="sync",
            user_key="user",
            resource_key="files",
            next_page_cursor="cursor-3",
            max_pages=2,
            completed_page_count=2,
            prior_error_count=2,
        )
    )

    assert fetches == 0
    assert result.status is ResultStatus.PARTIAL
    assert result.details == {
        "next_cursor": "cursor-3",
        "finished": False,
        "error_count": 2,
    }


@pytest.mark.asyncio
async def test_resource_quota_retry_drains_children_and_carries_fresh_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = ResourcePagesWorkflow()
    events: list[str] = []
    carried: list[ResourcePagesInput] = []
    requests: list[object] = []
    responses = [
        ResourcePageManifest(
            request_id="page-1-request",
            page_key="page-1",
            next_cursor="cursor-2",
        ),
        ResourcePageManifest(
            request_id="quota-request",
            page_key="quota",
            quota_exhausted=True,
        ),
        ResourcePageManifest(
            request_id="page-2-request",
            page_key="page-2",
            next_cursor=None,
        ),
    ]
    suggestions = iter((False, True, False))

    class PageHandle:
        def __init__(self, page_key: str) -> None:
            self.id = f"files-page/{page_key}"

        def __await__(self):  # type: ignore[no-untyped-def]
            async def finish() -> object:
                events.append(f"drained:{self.id}")
                return SimpleNamespace(errors=())

            return finish().__await__()

        def cancel(self) -> None:
            events.append(f"canceled:{self.id}")

    async def execute_activity(
        _activity_name: str,
        activity_input: object,
        **_options: object,
    ) -> ResourcePageManifest:
        requests.append(activity_input)
        return responses.pop(0)

    async def start_page(
        _command: ResourcePagesInput,
        manifest: ResourcePageManifest,
    ) -> PageHandle:
        events.append(f"started:{manifest.page_key}")
        return PageHandle(manifest.page_key)

    async def wait_for_page(
        handles: list[PageHandle],
        **_options: object,
    ) -> tuple[set[PageHandle], set[PageHandle]]:
        return set(handles), set()

    def info() -> SimpleNamespace:
        return SimpleNamespace(
            is_continue_as_new_suggested=lambda: next(suggestions),
        )

    def continue_as_new(command: ResourcePagesInput) -> None:
        events.append("continue-as-new")
        carried.append(command)
        raise ContinueAsNewCalled

    monkeypatch.setattr(resource_pages_module.workflow, "execute_activity", execute_activity)
    monkeypatch.setattr(resource_pages_module.workflow, "wait", wait_for_page)
    monkeypatch.setattr(resource_pages_module.workflow, "info", info)
    monkeypatch.setattr(
        resource_pages_module.workflow,
        "continue_as_new",
        continue_as_new,
    )
    pages._start_page_child = start_page  # type: ignore[method-assign]
    command = ResourcePagesInput(
        store_key="opaque-store",
        lifecycle_generation=3,
        sync_sequence="sync",
        user_key="user",
        resource_key="files",
    )

    with pytest.raises(ContinueAsNewCalled):
        await pages.run(command)

    assert events.index("drained:files-page/page-1") < events.index("continue-as-new")
    assert carried[0].next_page_cursor == "cursor-2"
    assert carried[0].completed_page_count == 1
    assert carried[0].page_fetch_attempt == 1

    resumed = ResourcePagesWorkflow()
    resumed._start_page_child = start_page  # type: ignore[method-assign]
    result = await resumed.run(carried[0])

    assert requests[1].cursor == requests[2].cursor == "cursor-2"
    assert requests[1].request_id != requests[2].request_id
    assert result.details["finished"] is True


@pytest.mark.asyncio
async def test_failed_remediation_is_started_detached_with_deterministic_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = RootSyncWorkflow()
    starts: list[tuple[str, object, dict[str, object]]] = []

    async def fake_start(workflow_name: str, child_input: object, **options: object) -> object:
        starts.append((workflow_name, child_input, options))
        return object()

    monkeypatch.setattr(root_sync_module.workflow, "start_child_workflow", fake_start)
    command = StoreSyncInput("opaque-store", 3, "sync-9", enable_search_attributes=True)

    workflow_id = await root._start_remediation(command, ("failed-user",))

    expected_id = failed_user_remediation_workflow_id(
        command.store_key,
        command.lifecycle_generation,
        command.sync_sequence,
    )
    assert workflow_id == expected_id
    assert len(starts) == 1
    workflow_name, child_input, options = starts[0]
    assert workflow_name == "FailedUserRemediationWorkflow"
    assert child_input.failed_user_keys == ("failed-user",)
    assert child_input.recent_page_cap == command.activation_recent_page_cap
    assert options["id"] == expected_id
    assert options["parent_close_policy"] is workflow.ParentClosePolicy.ABANDON
    assert options["cancellation_type"] is workflow.ChildWorkflowCancellationType.ABANDON
    assert options["search_attributes"] is not None


@pytest.mark.asyncio
async def test_detached_remediation_finishes_controller_registration_on_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = RootSyncWorkflow()
    registration_started = asyncio.Event()
    release_registration = asyncio.Event()
    events: list[str] = []

    class ControllerHandle:
        async def signal(self, _name: str, _event: object) -> None:
            events.append("registration-started")
            registration_started.set()
            await release_registration.wait()
            events.append("registration-finished")

    async def fake_start(*_args: object, **_kwargs: object) -> object:
        events.append("child-started")
        return object()

    monkeypatch.setattr(root_sync_module.workflow, "start_child_workflow", fake_start)
    monkeypatch.setattr(
        root_sync_module.workflow,
        "get_external_workflow_handle",
        lambda _workflow_id: ControllerHandle(),
    )
    command = StoreSyncInput(
        "opaque-store",
        3,
        "sync-9",
        controller_workflow_id="store-controller/opaque",
    )
    starting = asyncio.create_task(root._start_remediation(command, ("failed-user",)))
    await asyncio.wait_for(registration_started.wait(), timeout=1)

    starting.cancel()
    release_registration.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(starting, timeout=1)
    assert events == [
        "child-started",
        "registration-started",
        "registration-finished",
    ]


def test_activation_recent_wave_has_a_positive_default_cap() -> None:
    command = ActivateUserInput("opaque-store", 3, "sync", "user")

    assert command.recent_page_cap is not None
    assert command.recent_page_cap > 0


@pytest.mark.asyncio
async def test_activation_completes_recent_then_generation_check_then_backfill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    activation = ActivateUserWorkflow()
    events: list[tuple[str, object]] = []

    async def execute_child(
        _workflow_name: str, child_input: object, **_options: object
    ) -> SyncResult:
        events.append(("child", child_input))
        if child_input.work_class is WorkClass.RECENT_ACTIVATION:
            return sync_result(
                child_input.sync_sequence,
                details={"next_cursor": "recent-cursor"},
            )
        return sync_result(child_input.sync_sequence)

    async def execute_activity(
        activity_name: str, activity_input: object, **_options: object
    ) -> LifecycleMutationResult:
        events.append((activity_name, activity_input))
        return LifecycleMutationResult(
            store_key="opaque-store",
            expected_generation=3,
            authoritative_generation=3,
            status=ResultStatus.SUCCEEDED,
            lifecycle_state=StoreLifecycleState.ACTIVE,
        )

    monkeypatch.setattr(
        activate_module.workflow,
        "execute_child_workflow",
        execute_child,
    )
    monkeypatch.setattr(
        activate_module.workflow,
        "execute_activity",
        execute_activity,
    )
    command = ActivateUserInput(
        "opaque-store",
        3,
        "sync",
        "user",
        recent_page_cap=4,
    )

    result = await activation.run(command)

    assert [event[0] for event in events] == [
        "child",
        "validate_lifecycle_generation",
        "child",
        "activate_user_generation_fenced",
    ]
    recent_input = events[0][1]
    backfill_input = events[2][1]
    assert recent_input.work_class is WorkClass.RECENT_ACTIVATION
    assert recent_input.page_limit == 4
    assert backfill_input.work_class is WorkClass.BACKFILL
    assert backfill_input.cursor == "recent-cursor"
    assert backfill_input.page_limit is None
    assert result.status is ResultStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_files_page_joins_children_with_generation_fenced_stable_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    child_calls: list[tuple[object, dict[str, object]]] = []

    async def execute_child(_workflow_name: str, child_input: object, **options: object) -> object:
        child_calls.append((child_input, options))
        return SimpleNamespace(status=ResultStatus.SUCCEEDED)

    monkeypatch.setattr(
        files_page_module.workflow,
        "execute_child_workflow",
        execute_child,
    )
    document = DocumentRef(
        document_key="document-1",
        source_version="v7",
        staging_uri="s3://staging/reference",
        content_hash="sha256:abc",
    )
    command = FilesPageInput(
        store_key="opaque-store",
        lifecycle_generation=3,
        sync_sequence="sync",
        user_key="user",
        resource_key="files",
        page_key="page-7",
        documents=(document,),
        deleted_document_keys=("deleted-document",),
        document_ingestion_concurrency=2,
    )

    result = await FilesPageWorkflow().run(command)

    by_document = {
        child_input.document.document_key: (child_input, options)
        for child_input, options in child_calls
    }
    upsert_input, upsert_options = by_document["document-1"]
    deletion_input, deletion_options = by_document["deleted-document"]
    expected_upsert_id = document_ingest_workflow_id(
        command.store_key,
        command.lifecycle_generation,
        document.document_key,
        document.source_version,
    )
    expected_delete_id = document_ingest_workflow_id(
        command.store_key,
        command.lifecycle_generation,
        "deleted-document",
        "deleted:page-7",
    )
    assert upsert_input.mutation is DocumentMutation.UPSERT
    assert upsert_input.idempotency_key == expected_upsert_id
    assert upsert_options["id"] == expected_upsert_id
    assert deletion_input.mutation is DocumentMutation.DELETE
    assert deletion_input.idempotency_key == expected_delete_id
    assert deletion_options["id"] == expected_delete_id
    assert result.status is ResultStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_remediation_activation_child_id_is_stable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remediation = FailedUserRemediationWorkflow()
    calls: list[tuple[object, dict[str, object]]] = []

    async def execute_child(
        _workflow_name: str, child_input: object, **options: object
    ) -> SyncResult:
        calls.append((child_input, options))
        return sync_result(child_input.sync_sequence)

    from retrieval.temporal.workflows import (
        failed_user_remediation as remediation_module,
    )

    monkeypatch.setattr(
        remediation_module.workflow,
        "execute_child_workflow",
        execute_child,
    )
    command = FailedUserRemediationInput(
        store_key="opaque-store",
        lifecycle_generation=3,
        sync_sequence="sync",
        operation_id="remediation-operation",
        failed_user_keys=("user-1",),
    )

    await remediation._activate(command, "user-1", asyncio.Semaphore(1))

    child_input, options = calls[0]
    expected_id = user_sync_workflow_id(
        command.store_key,
        command.lifecycle_generation,
        "sync:remediation",
        "user-1",
    )
    assert child_input.sync_sequence == "sync:remediation"
    assert options["id"] == expected_id


@pytest.mark.asyncio
async def test_remediation_batches_before_continue_as_new(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remediation = FailedUserRemediationWorkflow()
    processed: list[str] = []
    carried: list[FailedUserRemediationInput] = []

    async def activate(
        command: FailedUserRemediationInput,
        user_key: str,
        _semaphore: asyncio.Semaphore,
    ) -> SyncResult:
        processed.append(user_key)
        return sync_result(command.sync_sequence)

    def continue_as_new(command: FailedUserRemediationInput) -> None:
        carried.append(command)
        raise ContinueAsNewCalled

    from retrieval.temporal.workflows import (
        failed_user_remediation as remediation_module,
    )

    monkeypatch.setattr(remediation, "_activate", activate)
    monkeypatch.setattr(
        remediation_module.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: True),
    )
    monkeypatch.setattr(remediation_module.workflow, "continue_as_new", continue_as_new)
    command = FailedUserRemediationInput(
        store_key="opaque-store",
        lifecycle_generation=3,
        sync_sequence="sync",
        operation_id="remediation-operation",
        failed_user_keys=tuple(f"user-{index}" for index in range(10)),
        resource_concurrency=3,
    )

    with pytest.raises(ContinueAsNewCalled):
        await remediation.run(command)

    assert processed == ["user-0", "user-1", "user-2"]
    assert len(carried) == 1
    assert carried[0].failed_user_keys == tuple(f"user-{index}" for index in range(3, 10))
    assert carried[0].prior_completed_count == 3
    assert carried[0].prior_error_count == 0


@pytest.mark.asyncio
async def test_remediation_reports_named_finished_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remediation = FailedUserRemediationWorkflow()
    signals: list[tuple[str, object]] = []

    class ControllerHandle:
        async def signal(self, name: str, event: object) -> None:
            signals.append((name, event))

    from retrieval.temporal.workflows import (
        failed_user_remediation as remediation_module,
    )

    monkeypatch.setattr(
        remediation_module.workflow,
        "get_external_workflow_handle",
        lambda _workflow_id: ControllerHandle(),
    )
    monkeypatch.setattr(
        remediation_module.workflow,
        "info",
        lambda: SimpleNamespace(workflow_id="failed-user-remediation/stable"),
    )
    command = FailedUserRemediationInput(
        store_key="opaque-store",
        lifecycle_generation=3,
        sync_sequence="sync",
        operation_id="remediation-operation",
        controller_workflow_id="store-controller/opaque",
    )

    await remediation._report_terminal(
        command,
        OperationStatus.COMPLETED,
        ResultStatus.SUCCEEDED,
    )

    assert len(signals) == 1
    signal_name, event = signals[0]
    assert signal_name == "remediation_finished"
    assert event.operation_id == command.operation_id  # type: ignore[attr-defined]
    assert event.result_status is ResultStatus.SUCCEEDED  # type: ignore[attr-defined]
