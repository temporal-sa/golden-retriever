"""Controller-first client APIs using short Update-with-Start operations."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from temporalio.client import Client, WithStartWorkflowOperation
from temporalio.common import WorkflowIDConflictPolicy

from retrieval.config import RetrievalTemporalConfig
from retrieval.temporal.common.ids import store_controller_workflow_id
from retrieval.temporal.common.priorities import priority_capability
from retrieval.temporal.common.search_attributes import operation_search_attributes
from retrieval.temporal.models.lifecycle import (
    DeactivationResult,
    StoreControllerSnapshot,
    StoreControllerState,
)
from retrieval.temporal.models.operations import (
    CancellationAccepted,
    CancelSyncCommand,
    OperationAccepted,
    OperationType,
    StartDeactivationCommand,
    SyncCommand,
)
from retrieval.temporal.runtime_config import TemporalRuntimeConfig


class RetrievalClient:
    def __init__(
        self,
        client: Client,
        *,
        task_queue: str,
        provider_task_queue: str,
        config: RetrievalTemporalConfig | None = None,
        priority_fairness_active: bool = False,
        enable_search_attributes: bool = False,
    ) -> None:
        self._client = client
        self._task_queue = task_queue
        self._provider_task_queue = provider_task_queue
        self._config = config or RetrievalTemporalConfig()
        self._priority_fairness_active = priority_fairness_active
        self._enable_search_attributes = enable_search_attributes

    @classmethod
    def from_runtime(
        cls,
        client: Client,
        *,
        runtime: TemporalRuntimeConfig,
        config: RetrievalTemporalConfig | None = None,
    ) -> RetrievalClient:
        """Build a client with the same capability gates used by workers."""

        resolved_config = config or RetrievalTemporalConfig()
        sdk_capability = priority_capability(resolved_config.temporal_enable_priority_fairness)
        return cls(
            client,
            task_queue=runtime.retrieval_task_queue,
            provider_task_queue=runtime.provider_task_queue,
            config=resolved_config,
            priority_fairness_active=(
                sdk_capability.active and runtime.server_priority_fairness_supported
            ),
            enable_search_attributes=runtime.enable_search_attributes,
        )

    def _controller_start(
        self, store_key: str, generation: int
    ) -> WithStartWorkflowOperation[Any, Any]:
        search_attributes = None
        if self._enable_search_attributes:
            search_attributes = operation_search_attributes(
                store_key=store_key,
                lifecycle_generation=generation,
                operation_type=OperationType.SYNC,
                current_phase="controller",
            )
        return WithStartWorkflowOperation(
            "StoreControllerWorkflow",
            StoreControllerState(
                store_key=store_key,
                lifecycle_generation=generation,
                command_dedup_window_size=self._config.user_quota_dedup_window_size,
                deactivation_drain_timeout_seconds=(
                    self._config.deactivation_drain_timeout_seconds
                ),
                enable_search_attributes=self._enable_search_attributes,
            ),
            id=store_controller_workflow_id(store_key),
            task_queue=self._task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            search_attributes=search_attributes,
        )

    def _apply_sync_policy(self, command: SyncCommand) -> SyncCommand:
        metadata = dict(command.metadata)
        defaults = {
            "max_active_users": self._config.store_sync_max_active_users,
            "user_page_size": self._config.store_sync_user_page_size,
            "round_user_window_size": self._config.round_user_window_size,
            "round_page_slice_size": self._config.round_page_slice_size,
            "resource_concurrency": self._config.resource_concurrency,
            "files_page_window_size": self._config.files_page_window_size,
            "files_per_page_concurrency": self._config.files_per_page_concurrency,
            "document_ingestion_concurrency": (self._config.document_ingestion_concurrency),
            "provider_task_queue": self._provider_task_queue,
            "priority_fairness_enabled": str(self._priority_fairness_active).lower(),
        }
        for key, value in defaults.items():
            metadata.setdefault(key, str(value))
        return replace(command, metadata=metadata)

    async def request_sync(self, command: SyncCommand) -> OperationAccepted:
        command = self._apply_sync_policy(command)
        start = self._controller_start(command.store_key, command.expected_generation)
        return await self._client.execute_update_with_start_workflow(
            "request_sync",
            command,
            start_workflow_operation=start,
            id=command.command_id,
            result_type=OperationAccepted,
        )

    async def cancel_sync(self, command: CancelSyncCommand) -> CancellationAccepted:
        start = self._controller_start(command.store_key, 0)
        return await self._client.execute_update_with_start_workflow(
            "cancel_sync",
            command,
            start_workflow_operation=start,
            id=command.command_id,
            result_type=CancellationAccepted,
        )

    async def start_deactivation(self, command: StartDeactivationCommand) -> OperationAccepted:
        start = self._controller_start(command.store_key, command.expected_generation)
        return await self._client.execute_update_with_start_workflow(
            "start_deactivation",
            command,
            start_workflow_operation=start,
            id=command.command_id,
            result_type=OperationAccepted,
        )

    async def deactivate_and_wait(self, command: StartDeactivationCommand) -> DeactivationResult:
        accepted = await self.start_deactivation(command)
        handle = self._client.get_workflow_handle(
            accepted.workflow_id,
            result_type=DeactivationResult,
        )
        return await handle.result()

    async def get_status(self, store_key: str) -> StoreControllerSnapshot:
        handle = self._client.get_workflow_handle(store_controller_workflow_id(store_key))
        return await handle.query("get_status", result_type=StoreControllerSnapshot)
