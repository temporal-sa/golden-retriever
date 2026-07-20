"""Workflow registrations used by production-history replay tests.

Keep this list explicit.  A replay suite that silently omits a modified Workflow Type is
more dangerous than a little duplication with the worker registration.
"""

from retrieval.temporal.workflows.activate_user import ActivateUserWorkflow
from retrieval.temporal.workflows.cleanup import (
    CleanupUsersWorkflow,
    DeactivateAllUsersWorkflow,
    DeactivateOneUserWorkflow,
    DeactivateUserWorkflow,
    RemoveObjectsWorkflow,
)
from retrieval.temporal.workflows.comments_resync import CommentsResyncWorkflow
from retrieval.temporal.workflows.deactivate_store import DeactivateStoreWorkflow
from retrieval.temporal.workflows.document_ingestion import DocumentIngestionWorkflow
from retrieval.temporal.workflows.failed_user_remediation import (
    FailedUserRemediationWorkflow,
)
from retrieval.temporal.workflows.files_page import FilesPageWorkflow
from retrieval.temporal.workflows.provider_preflight import ProviderPreflightWorkflow
from retrieval.temporal.workflows.resource_pages import ResourcePagesWorkflow
from retrieval.temporal.workflows.resource_sync import ResourceSyncWorkflow
from retrieval.temporal.workflows.root_sync import RootSyncWorkflow
from retrieval.temporal.workflows.store_controller import StoreControllerWorkflow
from retrieval.temporal.workflows.user_quota import UserQuotaWorkflow
from retrieval.temporal.workflows.user_sync import UserSyncWorkflow

REPLAY_WORKFLOWS: tuple[type, ...] = (
    ActivateUserWorkflow,
    CleanupUsersWorkflow,
    CommentsResyncWorkflow,
    DeactivateAllUsersWorkflow,
    DeactivateOneUserWorkflow,
    DeactivateStoreWorkflow,
    DeactivateUserWorkflow,
    DocumentIngestionWorkflow,
    FailedUserRemediationWorkflow,
    FilesPageWorkflow,
    RemoveObjectsWorkflow,
    ProviderPreflightWorkflow,
    ResourcePagesWorkflow,
    ResourceSyncWorkflow,
    RootSyncWorkflow,
    StoreControllerWorkflow,
    UserQuotaWorkflow,
    UserSyncWorkflow,
)


__all__ = ["REPLAY_WORKFLOWS"]
