"""Compact inputs and results for the preserved sync workflow boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from .documents import DocumentRef
from .operations import ResultStatus, WorkClass
from .quota import QuotaScope


class SyncMode(StrEnum):
    ORDINARY = "ordinary"
    ROUND = "round"


@dataclass(frozen=True)
class UserCursor:
    user_key: str
    cursor: str | None = None
    resource_cursors: dict[str, str | None] = field(default_factory=dict)
    completed_resource_types: tuple[str, ...] = ()
    pages_completed: int = 0
    finished: bool = False


@dataclass(frozen=True)
class RoundState:
    active_users: tuple[UserCursor, ...] = ()
    buffered_users: tuple[UserCursor, ...] = ()
    next_user_cursor: str | None = None
    round_number: int = 0
    users_exhausted: bool = False
    failed_user_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class StoreSyncInput:
    store_key: str
    lifecycle_generation: int
    sync_sequence: str
    quota_scope: QuotaScope | None = None
    work_class: WorkClass = WorkClass.INCREMENTAL
    mode: SyncMode = SyncMode.ORDINARY
    user_cursor: str | None = None
    round_state: RoundState | None = None
    resource_types: tuple[str, ...] = ("files",)
    max_active_users: int = 20
    user_page_size: int = 100
    round_user_window_size: int = 20
    round_page_slice_size: int = 5
    resource_concurrency: int = 8
    files_page_window_size: int = 5
    files_per_page_concurrency: int = 10
    document_ingestion_concurrency: int = 20
    provider_page_size: int = 100
    provider_task_queue: str = "retrieval-provider-v2"
    priority_fairness_enabled: bool = False
    controller_workflow_id: str | None = None
    failed_user_keys: tuple[str, ...] = ()
    activation_recent_page_cap: int = 5
    enable_search_attributes: bool = False
    prior_error_count: int = 0
    user_page_attempt: int = 0


# The specification uses both names for the root role.  They intentionally
# share one wire shape.
RootSyncInput = StoreSyncInput


@dataclass(frozen=True)
class FailedUserRemediationInput:
    store_key: str
    lifecycle_generation: int
    sync_sequence: str
    operation_id: str
    failed_user_keys: tuple[str, ...] = ()
    quota_scope: QuotaScope | None = None
    work_class: WorkClass = WorkClass.RECENT_ACTIVATION
    resource_types: tuple[str, ...] = ("files",)
    resource_concurrency: int = 8
    files_page_window_size: int = 5
    files_per_page_concurrency: int = 10
    document_ingestion_concurrency: int = 20
    provider_page_size: int = 100
    provider_task_queue: str = "retrieval-provider-v2"
    priority_fairness_enabled: bool = False
    controller_workflow_id: str | None = None
    recent_page_cap: int = 5
    enable_search_attributes: bool = False
    prior_completed_count: int = 0
    prior_error_count: int = 0


@dataclass(frozen=True)
class ActivateUserInput:
    store_key: str
    lifecycle_generation: int
    sync_sequence: str
    user_key: str
    quota_scope: QuotaScope | None = None
    work_class: WorkClass = WorkClass.RECENT_ACTIVATION
    recent_page_cap: int = 5
    resource_types: tuple[str, ...] = ("files",)
    resource_concurrency: int = 8
    files_page_window_size: int = 5
    files_per_page_concurrency: int = 10
    document_ingestion_concurrency: int = 20
    provider_page_size: int = 100
    provider_task_queue: str = "retrieval-provider-v2"
    priority_fairness_enabled: bool = False


@dataclass(frozen=True)
class UserSyncInput:
    store_key: str
    lifecycle_generation: int
    sync_sequence: str
    user_key: str
    quota_scope: QuotaScope | None = None
    work_class: WorkClass = WorkClass.INCREMENTAL
    resource_types: tuple[str, ...] = ()
    cursor: str | None = None
    resource_cursors: dict[str, str | None] = field(default_factory=dict)
    completed_resource_types: tuple[str, ...] = ()
    page_limit: int | None = None
    resource_concurrency: int = 8
    files_page_window_size: int = 5
    files_per_page_concurrency: int = 10
    document_ingestion_concurrency: int = 20
    provider_page_size: int = 100
    provider_task_queue: str = "retrieval-provider-v2"
    priority_fairness_enabled: bool = False


@dataclass(frozen=True)
class ResourceSyncInput:
    store_key: str
    lifecycle_generation: int
    sync_sequence: str
    user_key: str
    resource_key: str
    quota_scope: QuotaScope | None = None
    work_class: WorkClass = WorkClass.INCREMENTAL
    cursor: str | None = None
    idempotency_context: str = ""
    page_limit: int | None = None
    files_page_window_size: int = 5
    files_per_page_concurrency: int = 10
    document_ingestion_concurrency: int = 20
    provider_page_size: int = 100
    provider_task_queue: str = "retrieval-provider-v2"
    priority_fairness_enabled: bool = False


@dataclass(frozen=True)
class ResourcePagesInput:
    store_key: str
    lifecycle_generation: int
    sync_sequence: str
    user_key: str
    resource_key: str
    quota_scope: QuotaScope | None = None
    work_class: WorkClass = WorkClass.INCREMENTAL
    next_page_cursor: str | None = None
    completed_page_count: int = 0
    max_pages: int | None = None
    page_size: int = 100
    files_page_window_size: int = 5
    files_per_page_concurrency: int = 10
    document_ingestion_concurrency: int = 20
    provider_task_queue: str = "retrieval-provider-v2"
    priority_fairness_enabled: bool = False
    prior_error_count: int = 0
    page_fetch_attempt: int = 0


@dataclass(frozen=True)
class FilesPageInput:
    store_key: str
    lifecycle_generation: int
    sync_sequence: str
    user_key: str
    resource_key: str
    page_key: str
    documents: tuple[DocumentRef, ...] = ()
    deleted_document_keys: tuple[str, ...] = ()
    quota_scope: QuotaScope | None = None
    work_class: WorkClass = WorkClass.INCREMENTAL
    document_ingestion_concurrency: int = 20


@dataclass(frozen=True)
class SyncProgress:
    phase: str
    users_completed: int = 0
    users_failed: int = 0
    resources_completed: int = 0
    pages_completed: int = 0
    documents_completed: int = 0
    pending_children: int = 0
    cursor: str | None = None


@dataclass(frozen=True)
class SyncResult:
    store_key: str
    lifecycle_generation: int
    sync_sequence: str
    status: ResultStatus
    progress: SyncProgress = field(default_factory=lambda: SyncProgress(phase="done"))
    failed_user_keys: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PageResult:
    page_key: str
    status: ResultStatus
    next_cursor: str | None = None
    changed_documents: int = 0
    deleted_documents: int = 0
    errors: tuple[str, ...] = ()
