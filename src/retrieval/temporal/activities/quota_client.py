"""Short atomic Signal-with-Start bridge for the shared quota workflow."""

from __future__ import annotations

from dataclasses import dataclass

from temporalio import activity
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from retrieval.temporal.common.ids import user_quota_workflow_id
from retrieval.temporal.models.quota import (
    MAX_QUOTA_PENDING_REQUESTS,
    PermitRequest,
    UserQuotaState,
)


@dataclass(frozen=True)
class QuotaSignalAccepted:
    request_id: str
    quota_workflow_id: str


class QuotaClientActivities:
    """Uses a normal Temporal Client outside workflow sandbox code."""

    def __init__(
        self,
        client: Client,
        *,
        task_queue: str,
        max_in_flight: int,
        max_pending_requests: int = MAX_QUOTA_PENDING_REQUESTS,
        configured_limit: int | None = None,
        dedup_window_size: int = 2_000,
        continue_as_new_message_count: int = 10_000,
    ) -> None:
        self._client = client
        self._task_queue = task_queue
        self._max_in_flight = max_in_flight
        self._max_pending_requests = max_pending_requests
        self._configured_limit = configured_limit
        self._dedup_window_size = dedup_window_size
        self._continue_as_new_message_count = continue_as_new_message_count

    @activity.defn(name="signal_with_start_user_quota")
    async def signal_with_start_user_quota(self, request: PermitRequest) -> QuotaSignalAccepted:
        workflow_id = user_quota_workflow_id(
            request.quota_scope.provider,
            request.quota_scope.credential_key,
            request.quota_scope.quota_class,
        )
        initial_state = UserQuotaState(
            quota_scope=request.quota_scope,
            configured_limit=self._configured_limit,
            remaining=self._configured_limit,
            max_in_flight=self._max_in_flight,
            max_pending_requests=self._max_pending_requests,
            dedup_window_size=self._dedup_window_size,
            continue_as_new_message_count=self._continue_as_new_message_count,
        )
        await self._client.start_workflow(
            "UserQuotaWorkflow",
            initial_state,
            id=workflow_id,
            task_queue=self._task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            start_signal="request_permit",
            start_signal_args=[request],
        )
        return QuotaSignalAccepted(
            request_id=request.request_id,
            quota_workflow_id=workflow_id,
        )
