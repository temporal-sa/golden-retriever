from __future__ import annotations

import asyncio
import hashlib
import os
from contextlib import suppress
from uuid import uuid4

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment

from retrieval.config import RetrievalTemporalConfig
from retrieval.temporal.activities.provider_api import EmptyProviderGateway, UserDescriptor
from retrieval.temporal.activities.repositories import (
    InMemoryRetrievalRepository,
    InMemoryStagingStore,
)
from retrieval.temporal.client import RetrievalClient
from retrieval.temporal.common.ids import permit_request_id, store_controller_workflow_id
from retrieval.temporal.models.documents import DocumentRef
from retrieval.temporal.models.lifecycle import (
    DeactivationResult,
    StoreLifecycleState,
)
from retrieval.temporal.models.operations import (
    ResultStatus,
    StartDeactivationCommand,
    SyncCommand,
)
from retrieval.temporal.models.sync import SyncResult
from retrieval.temporal.runtime_config import TemporalRuntimeConfig
from retrieval.temporal.test_starter import execute_workflow_smoke_test
from retrieval.temporal.worker import build_workers

from .fake_provider import FakeProviderGateway, FakeProviderOutcome

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_TEMPORAL_INTEGRATION") != "1",
        reason="set RUN_TEMPORAL_INTEGRATION=1 to start a Temporal test server",
    ),
]


async def test_local_starter_executes_sync_and_deactivation() -> None:
    suffix = uuid4().hex
    async with await WorkflowEnvironment.start_local() as environment:
        result = await execute_workflow_smoke_test(
            environment.client,
            store_key=f"starter-integration-store-{suffix}",
        )

    assert result.sync_status == "succeeded"
    assert result.deactivation_status == "succeeded"
    assert result.lifecycle_generation == 1
    assert result.final_lifecycle_state == "inactive"


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


async def test_full_topology_ingests_a_provider_document_end_to_end() -> None:
    suffix = uuid4().hex
    store_key = f"document-integration-store-{suffix}"
    sync_sequence = f"document-sync-{suffix}"
    user_key = f"user-{suffix}"
    body = b"retrieval integration payload"
    staging_uri = f"stage://{suffix}/document"
    document = DocumentRef(
        document_key=f"document-{suffix}",
        source_version="v1",
        staging_uri=staging_uri,
        content_hash=hashlib.sha256(body).hexdigest(),
    )
    repository = InMemoryRetrievalRepository()
    staging_store = InMemoryStagingStore({staging_uri: body})
    gateway = FakeProviderGateway()
    await repository.ensure_store(store_key)

    gateway.queue_outcomes(
        permit_request_id(
            store_key=store_key,
            lifecycle_generation=0,
            sync_sequence=sync_sequence,
            user_key="active-user-index",
            resource_key="users",
            cursor=(None, 0),
            operation="list-active-users",
            quota_class="unmetered",
        ),
        FakeProviderOutcome(users=(UserDescriptor(user_key),)),
    )
    gateway.queue_outcomes(
        permit_request_id(
            store_key=store_key,
            lifecycle_generation=0,
            sync_sequence=sync_sequence,
            user_key=user_key,
            resource_key="files",
            cursor=(None, 0),
            operation="fetch-resource-page",
            quota_class="unmetered",
        ),
        FakeProviderOutcome(documents=(document,)),
    )
    runtime = TemporalRuntimeConfig(
        retrieval_task_queue=f"retrieval-document-{suffix}",
        provider_task_queue=f"retrieval-provider-document-{suffix}",
        allow_unsafe_in_memory_adapters=True,
    )

    async with await WorkflowEnvironment.start_local() as environment:
        retrieval_worker, provider_worker = build_workers(
            environment.client,
            runtime=runtime,
            config=RetrievalTemporalConfig(),
            repository=repository,
            staging_store=staging_store,
            provider_gateway=gateway,
        )
        async with retrieval_worker, provider_worker:
            retrieval = RetrievalClient.from_runtime(
                environment.client,
                runtime=runtime,
                config=RetrievalTemporalConfig(),
            )
            accepted = await retrieval.request_sync(
                SyncCommand(
                    command_id=f"document-command-{suffix}",
                    store_key=store_key,
                    expected_generation=0,
                    sync_sequence=sync_sequence,
                )
            )
            result = await environment.client.get_workflow_handle(
                accepted.workflow_id,
                result_type=SyncResult,
            ).result()
            stored = await repository.get_store(store_key)

            assert result.status is ResultStatus.SUCCEEDED
            assert result.progress.users_completed == 1
            assert stored.documents == {document.document_key: document}
            assert [call.operation for call in gateway.calls] == [
                "list_active_users",
                "fetch_resource_page",
            ]

            controller = environment.client.get_workflow_handle(
                store_controller_workflow_id(store_key)
            )
            await controller.cancel()
            with suppress(WorkflowFailureError):
                await controller.result()
