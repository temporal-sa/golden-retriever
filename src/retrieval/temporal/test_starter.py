"""Executable smoke test for the retrieval workflows on a local Temporal server."""

from __future__ import annotations

import argparse
import asyncio
import json
from contextlib import suppress
from dataclasses import asdict, dataclass
from uuid import uuid4

from temporalio.client import Client, WorkflowFailureError
from temporalio.service import RPCError

from retrieval.config import RetrievalTemporalConfig
from retrieval.temporal.activities.provider_api import EmptyProviderGateway
from retrieval.temporal.activities.repositories import (
    InMemoryRetrievalRepository,
    InMemoryStagingStore,
)
from retrieval.temporal.client import RetrievalClient
from retrieval.temporal.common.ids import store_controller_workflow_id
from retrieval.temporal.models.lifecycle import (
    DeactivationPhase,
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
from retrieval.temporal.worker import build_workers


@dataclass(frozen=True)
class WorkflowSmokeTestResult:
    """Machine-readable summary emitted after a successful smoke test."""

    store_key: str
    sync_workflow_id: str
    sync_status: str
    deactivation_workflow_id: str
    deactivation_status: str
    lifecycle_generation: int
    final_lifecycle_state: str


async def _wait_for_controller_state(
    retrieval: RetrievalClient,
    store_key: str,
    *,
    state: StoreLifecycleState,
    timeout_seconds: float = 10,
) -> None:
    async with asyncio.timeout(timeout_seconds):
        while True:
            snapshot = await retrieval.get_status(store_key)
            no_active_operations = (
                not snapshot.active_sync_ids
                and not snapshot.active_remediation_ids
                and snapshot.active_deactivation_id is None
            )
            if snapshot.lifecycle_state is state and no_active_operations:
                return
            await asyncio.sleep(0.05)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


async def execute_workflow_smoke_test(
    client: Client,
    *,
    store_key: str | None = None,
    config: RetrievalTemporalConfig | None = None,
) -> WorkflowSmokeTestResult:
    """Run sync and deactivation through real workers on ``client``'s server.

    The starter owns isolated task queues and in-memory adapters, so it is safe to run next to a
    normal development worker. Every invocation uses a fresh store unless ``store_key`` is given.
    """

    suffix = uuid4().hex
    resolved_store_key = store_key or f"local-smoke-{suffix}"
    runtime = TemporalRuntimeConfig(
        retrieval_task_queue=f"retrieval-smoke-{suffix}",
        provider_task_queue=f"retrieval-provider-smoke-{suffix}",
        allow_unsafe_in_memory_adapters=True,
    )
    resolved_config = config or RetrievalTemporalConfig()
    repository = InMemoryRetrievalRepository()
    await repository.ensure_store(resolved_store_key)

    retrieval_worker, provider_worker = build_workers(
        client,
        runtime=runtime,
        config=resolved_config,
        repository=repository,
        staging_store=InMemoryStagingStore(),
        provider_gateway=EmptyProviderGateway(),
    )
    retrieval = RetrievalClient.from_runtime(
        client,
        runtime=runtime,
        config=resolved_config,
    )
    controller = client.get_workflow_handle(store_controller_workflow_id(resolved_store_key))
    controller_started = False

    async with retrieval_worker, provider_worker:
        try:
            controller_started = True
            sync_accepted = await retrieval.request_sync(
                SyncCommand(
                    command_id=f"smoke-sync-command-{suffix}",
                    store_key=resolved_store_key,
                    expected_generation=0,
                    sync_sequence=f"smoke-sync-{suffix}",
                )
            )
            sync_result = await client.get_workflow_handle(
                sync_accepted.workflow_id,
                result_type=SyncResult,
            ).result()
            _require(
                sync_result.status is ResultStatus.SUCCEEDED,
                f"sync finished with status {sync_result.status.value}",
            )
            _require(
                not sync_result.failed_user_keys,
                f"sync reported failed users: {sync_result.failed_user_keys}",
            )
            await _wait_for_controller_state(
                retrieval,
                resolved_store_key,
                state=StoreLifecycleState.ACTIVE,
            )

            deactivation_accepted = await retrieval.start_deactivation(
                StartDeactivationCommand(
                    command_id=f"smoke-deactivate-command-{suffix}",
                    store_key=resolved_store_key,
                    expected_generation=0,
                )
            )
            deactivation_result = await client.get_workflow_handle(
                deactivation_accepted.workflow_id,
                result_type=DeactivationResult,
            ).result()
            _require(
                deactivation_result.status is ResultStatus.SUCCEEDED,
                f"deactivation finished with status {deactivation_result.status.value}",
            )
            _require(
                deactivation_result.phase is DeactivationPhase.COMPLETED,
                f"deactivation stopped in phase {deactivation_result.phase.value}",
            )
            await _wait_for_controller_state(
                retrieval,
                resolved_store_key,
                state=StoreLifecycleState.INACTIVE,
            )

            record = await repository.get_store(resolved_store_key)
            _require(
                record.lifecycle_state is StoreLifecycleState.INACTIVE,
                f"repository finished in state {record.lifecycle_state.value}",
            )
            _require(
                record.lifecycle_generation == deactivation_result.lifecycle_generation == 1,
                "deactivation did not advance the lifecycle generation exactly once",
            )

            return WorkflowSmokeTestResult(
                store_key=resolved_store_key,
                sync_workflow_id=sync_accepted.workflow_id,
                sync_status=sync_result.status.value,
                deactivation_workflow_id=deactivation_accepted.workflow_id,
                deactivation_status=deactivation_result.status.value,
                lifecycle_generation=record.lifecycle_generation,
                final_lifecycle_state=record.lifecycle_state.value,
            )
        finally:
            if controller_started:
                with suppress(WorkflowFailureError, RPCError, TimeoutError):
                    async with asyncio.timeout(5):
                        await controller.cancel()
                        await controller.result()


async def run_local_workflow_smoke_test(
    *,
    address: str = "localhost:7233",
    namespace: str = "default",
    store_key: str | None = None,
) -> WorkflowSmokeTestResult:
    """Connect to a running local Temporal dev server and execute the smoke test."""

    client = await Client.connect(address, namespace=namespace)
    return await execute_workflow_smoke_test(client, store_key=store_key)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the retrieval sync and deactivation smoke test against a running "
            "Temporal development server."
        )
    )
    parser.add_argument(
        "--address",
        default="localhost:7233",
        help="Temporal frontend address (default: localhost:7233)",
    )
    parser.add_argument(
        "--namespace",
        default="default",
        help="Temporal namespace (default: default)",
    )
    parser.add_argument(
        "--store-key",
        help="optional store key; a unique local smoke-test key is generated by default",
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    try:
        result = asyncio.run(
            run_local_workflow_smoke_test(
                address=args.address,
                namespace=args.namespace,
                store_key=args.store_key,
            )
        )
    except Exception as exc:
        raise SystemExit(f"retrieval workflow smoke test failed: {exc}") from exc
    print(json.dumps(asdict(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
