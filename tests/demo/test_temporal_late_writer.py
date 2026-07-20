from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from datetime import timedelta
from uuid import uuid4

import pytest
from temporalio.client import Client, WorkflowFailureError
from temporalio.exceptions import TemporalError
from temporalio.testing import WorkflowEnvironment

from retrieval.config import RetrievalTemporalConfig
from retrieval.demo.config import DemoConfig
from retrieval.demo.events import DemoIngestionEventSink
from retrieval.demo.fixtures import FixtureStagingStore, load_northstar_scenario
from retrieval.demo.ingestion_gate import DemoBeforeDocumentCommitHook
from retrieval.demo.scripted_provider import ScriptedNorthstarProvider
from retrieval.demo.service import DemoService, InMemoryTextSearch
from retrieval.demo.store import InMemoryDemoStateStore
from retrieval.temporal.activities.repositories import InMemoryRetrievalRepository
from retrieval.temporal.client import RetrievalClient
from retrieval.temporal.common.ids import (
    store_controller_workflow_id,
    user_quota_workflow_id,
)
from retrieval.temporal.models.lifecycle import (
    DeactivationResult,
    StoreControllerSnapshot,
    StoreLifecycleState,
)
from retrieval.temporal.models.operations import (
    CommandResult,
    OperationAccepted,
    ResultStatus,
    StartDeactivationCommand,
    SyncCommand,
)
from retrieval.temporal.models.quota import QuotaSnapshot
from retrieval.temporal.runtime_config import TemporalRuntimeConfig
from retrieval.temporal.worker import build_workers

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_TEMPORAL_INTEGRATION") != "1",
        reason="set RUN_TEMPORAL_INTEGRATION=1 to start the Temporal development server",
    ),
]


class DirectTemporalGateway:
    """Demo gateway that reuses the already-connected ephemeral test client."""

    def __init__(self, client: RetrievalClient) -> None:
        self._client = client

    async def start(self) -> None:
        return None

    async def ready(self) -> bool:
        return True

    async def aclose(self) -> None:
        return None

    async def request_sync(self, command: SyncCommand) -> OperationAccepted:
        return await self._client.request_sync(command)

    async def start_deactivation(self, command: StartDeactivationCommand) -> OperationAccepted:
        return await self._client.start_deactivation(command)

    async def get_status(self, store_key: str) -> StoreControllerSnapshot:
        return await self._client.get_status(store_key)

    async def get_operation_result(
        self,
        store_key: str,
        operation_id: str,
    ) -> CommandResult | None:
        return await self._client.get_operation_result(store_key, operation_id)


async def test_full_northstar_topology_releases_held_write_after_terminal_cleanup() -> None:
    scenario = load_northstar_scenario()
    suffix = uuid4().hex
    runtime = TemporalRuntimeConfig(
        retrieval_task_queue=f"northstar-retrieval-{suffix}",
        provider_task_queue=f"northstar-provider-{suffix}",
        allow_unsafe_in_memory_adapters=True,
    )
    retrieval_config = RetrievalTemporalConfig(
        object_cleanup_batch_size=2,
        deactivation_drain_timeout=timedelta(milliseconds=50),
    )
    demo_config = DemoConfig(
        enabled=True,
        hold_timeout_seconds=15,
        control_poll_seconds=0.01,
    )
    state = InMemoryDemoStateStore()
    repository = InMemoryRetrievalRepository()
    provider = ScriptedNorthstarProvider(scenario, state)

    async with await WorkflowEnvironment.start_time_skipping() as environment:
        retrieval_client = RetrievalClient.from_runtime(
            environment.client,
            runtime=runtime,
            config=retrieval_config,
        )
        service = DemoService(
            config=demo_config,
            scenario=scenario,
            state_store=state,
            repository=repository,
            search_adapter=InMemoryTextSearch(repository),
            command_gateway=DirectTemporalGateway(retrieval_client),
        )
        retrieval_worker, provider_worker = build_workers(
            environment.client,
            runtime=runtime,
            config=retrieval_config,
            repository=repository,
            staging_store=FixtureStagingStore(scenario),
            provider_gateway=provider,
            before_document_commit=DemoBeforeDocumentCommitHook(state, config=demo_config),
            ingestion_event_sink=DemoIngestionEventSink(state),
        )
        async with retrieval_worker, provider_worker:
            await service.start()
            try:
                run = await service.create_run(idempotency_key=f"run:{suffix}")
                sync = await service.start_sync(run.run_id, idempotency_key=f"sync:{suffix}")

                await _wait_for_event(state, run.run_id, "quota_wait_started")
                await _wait_for_quota_block(
                    environment.client,
                    user_quota_workflow_id(
                        "northstar-scripted",
                        f"northstar-demo-run:{run.run_id}",
                        "demo",
                    ),
                )
                await _advance_until_event(
                    environment,
                    state,
                    run.run_id,
                    "document_commit_held",
                    max_virtual_seconds=int(scenario.quota_retry_after_seconds) + 10,
                )
                await _wait_for_document_count(repository, run.store_key, 4)

                answer = await service.ask(
                    run.run_id,
                    "What should the account team prioritize before Northstar's renewal?",
                    idempotency_key=f"ask:{suffix}",
                )
                assert {citation.document_key for citation in answer.citations} == {
                    "renewal-plan.md",
                    "support-escalation.md",
                    "northstar-qbr.md",
                    "stakeholders.md",
                }

                deactivation = await service.start_deactivation(
                    run.run_id,
                    idempotency_key=f"deactivate:{suffix}",
                )
                result = await environment.client.get_workflow_handle(
                    deactivation.workflow_id,
                    result_type=DeactivationResult,
                ).result()
                assert result.status in {ResultStatus.SUCCEEDED, ResultStatus.PARTIAL}

                terminal = await repository.get_store(run.store_key)
                assert terminal.lifecycle_state is StoreLifecycleState.INACTIVE
                assert terminal.lifecycle_generation == 8
                assert terminal.document_count == terminal.chunk_count == 0
                sync_terminal = await service.get_operation(sync.operation_id)
                assert sync_terminal.status.value == "canceled"

                # This deliberately releases after cleanup is already terminal,
                # proving the UI cannot miss a brief deactivating state.
                await service.release_late_document(
                    run.run_id,
                    idempotency_key=f"release:{suffix}",
                )
                await _wait_for_event(
                    state,
                    run.run_id,
                    "stale_generation_rejected",
                )
                await service.get_snapshot(run.run_id)
                event_types = {event.event_type for event in await state.list_events(run.run_id)}
                assert {
                    "deactivation_fenced",
                    "held_commit_released",
                    "stale_generation_rejected",
                    "cleanup_batch_completed",
                    "store_inactive",
                }.issubset(event_types)

                controller = environment.client.get_workflow_handle(
                    store_controller_workflow_id(run.store_key)
                )
                await controller.cancel()
                with suppress(WorkflowFailureError):
                    await controller.result()
            finally:
                await service.aclose()


async def _wait_for_event(
    state: InMemoryDemoStateStore,
    run_id: str,
    event_type: str,
    *,
    timeout_seconds: float = 5,
) -> None:
    async with asyncio.timeout(timeout_seconds):
        while True:
            if any(event.event_type == event_type for event in await state.list_events(run_id)):
                return
            await asyncio.sleep(0.01)


async def _advance_until_event(
    environment: WorkflowEnvironment,
    state: InMemoryDemoStateStore,
    run_id: str,
    event_type: str,
    *,
    max_virtual_seconds: int,
) -> None:
    for _ in range(max_virtual_seconds):
        if any(event.event_type == event_type for event in await state.list_events(run_id)):
            return
        # Advance in one-second steps so the five-second quota timer fires,
        # while checking before the bounded 15-second held commit can expire.
        await environment.sleep(1)
    raise TimeoutError(f"event {event_type!r} did not appear while advancing virtual time")


async def _wait_for_document_count(
    repository: InMemoryRetrievalRepository,
    store_key: str,
    expected: int,
) -> None:
    async with asyncio.timeout(5):
        while True:
            if (await repository.get_store(store_key)).document_count == expected:
                return
            await asyncio.sleep(0.01)


async def _wait_for_quota_block(client: Client, workflow_id: str) -> None:
    handle = client.get_workflow_handle(workflow_id)
    async with asyncio.timeout(5):
        while True:
            try:
                snapshot = await handle.query(
                    "get_quota_state",
                    result_type=QuotaSnapshot,
                )
            except TemporalError:
                await asyncio.sleep(0.01)
                continue
            if snapshot.blocked_until is not None:
                return
            await asyncio.sleep(0.01)
