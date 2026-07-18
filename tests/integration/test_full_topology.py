from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from uuid import uuid4

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment

from retrieval.config import RetrievalTemporalConfig
from retrieval.temporal.activities.provider_api import EmptyProviderGateway
from retrieval.temporal.activities.repositories import (
    InMemoryRetrievalRepository,
    InMemoryStagingStore,
)
from retrieval.temporal.client import RetrievalClient
from retrieval.temporal.common.ids import store_controller_workflow_id
from retrieval.temporal.models.lifecycle import (
    DeactivationResult,
    StoreLifecycleState,
)
from retrieval.temporal.models.operations import (
    StartDeactivationCommand,
    SyncCommand,
)
from retrieval.temporal.models.sync import SyncResult
from retrieval.temporal.runtime_config import TemporalRuntimeConfig
from retrieval.temporal.worker import build_workers

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_TEMPORAL_INTEGRATION") != "1",
        reason="set RUN_TEMPORAL_INTEGRATION=1 to start a Temporal test server",
    ),
]


async def test_controller_sync_and_duplicate_deactivation_end_to_end() -> None:
    suffix = uuid4().hex
    store_key = f"integration-store-{suffix}"
    repository = InMemoryRetrievalRepository()
    await repository.ensure_store(store_key)
    runtime = TemporalRuntimeConfig(
        retrieval_task_queue=f"retrieval-full-{suffix}",
        provider_task_queue=f"retrieval-provider-full-{suffix}",
        allow_unsafe_in_memory_adapters=True,
    )
    config = RetrievalTemporalConfig()

    async with await WorkflowEnvironment.start_local() as environment:
        retrieval_worker, provider_worker = build_workers(
            environment.client,
            runtime=runtime,
            config=config,
            repository=repository,
            staging_store=InMemoryStagingStore(),
            provider_gateway=EmptyProviderGateway(),
        )
        async with retrieval_worker, provider_worker:
            retrieval = RetrievalClient.from_runtime(
                environment.client,
                runtime=runtime,
                config=config,
            )
            sync_accepted = await retrieval.request_sync(
                SyncCommand(
                    command_id=f"sync-command-{suffix}",
                    store_key=store_key,
                    expected_generation=0,
                    sync_sequence="initial",
                )
            )
            sync_result = await environment.client.get_workflow_handle(
                sync_accepted.workflow_id,
                result_type=SyncResult,
            ).result()
            assert sync_result.failed_user_keys == ()

            for _ in range(50):
                snapshot = await retrieval.get_status(store_key)
                if not snapshot.active_sync_ids:
                    break
                await asyncio.sleep(0.02)
            assert snapshot.lifecycle_state is StoreLifecycleState.ACTIVE

            first, duplicate = await asyncio.gather(
                retrieval.start_deactivation(
                    StartDeactivationCommand(
                        command_id=f"deactivate-a-{suffix}",
                        store_key=store_key,
                        expected_generation=0,
                    )
                ),
                retrieval.start_deactivation(
                    StartDeactivationCommand(
                        command_id=f"deactivate-b-{suffix}",
                        store_key=store_key,
                        expected_generation=0,
                    )
                ),
            )
            assert first.operation_id == duplicate.operation_id
            assert first.lifecycle_generation == duplicate.lifecycle_generation == 1

            deactivation = await environment.client.get_workflow_handle(
                first.workflow_id,
                result_type=DeactivationResult,
            ).result()
            assert deactivation.lifecycle_generation == 1
            assert (await repository.get_store(store_key)).lifecycle_state is (
                StoreLifecycleState.INACTIVE
            )

            for _ in range(50):
                snapshot = await retrieval.get_status(store_key)
                if snapshot.lifecycle_state is StoreLifecycleState.INACTIVE:
                    break
                await asyncio.sleep(0.02)
            assert snapshot.active_deactivation_id is None
            assert snapshot.lifecycle_state is StoreLifecycleState.INACTIVE

            controller = environment.client.get_workflow_handle(
                store_controller_workflow_id(store_key)
            )
            await controller.cancel()
            with suppress(WorkflowFailureError):
                await controller.result()
