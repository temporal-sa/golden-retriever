"""Worker registration and deployment-versioned process entry point."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import signal
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

from temporalio.client import Client
from temporalio.common import (
    VersioningBehavior,
    WorkerDeploymentVersion,
)
from temporalio.worker import Worker, WorkerDeploymentConfig

from retrieval.config import RetrievalTemporalConfig
from retrieval.environment import inject_environment
from retrieval.temporal.activities.cleanup import CleanupActivities
from retrieval.temporal.activities.hooks import (
    BeforeDocumentCommitHook,
    IngestionEventSink,
    NoopBeforeDocumentCommitHook,
    NoopIngestionEventSink,
)
from retrieval.temporal.activities.ingestion import IngestionActivities
from retrieval.temporal.activities.lifecycle import LifecycleActivities
from retrieval.temporal.activities.provider_api import (
    EmptyProviderGateway,
    ProviderActivities,
    ProviderGateway,
)
from retrieval.temporal.activities.quota_client import QuotaClientActivities
from retrieval.temporal.activities.repositories import (
    InMemoryRetrievalRepository,
    InMemoryStagingStore,
    RetrievalRepository,
    StagingStore,
)
from retrieval.temporal.common.priorities import priority_capability
from retrieval.temporal.runtime_config import TemporalRuntimeConfig
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
from retrieval.temporal.workflows.legacy import LEGACY_DRAIN_WORKFLOWS
from retrieval.temporal.workflows.resource_pages import ResourcePagesWorkflow
from retrieval.temporal.workflows.resource_sync import ResourceSyncWorkflow
from retrieval.temporal.workflows.root_sync import RootSyncWorkflow
from retrieval.temporal.workflows.store_controller import StoreControllerWorkflow
from retrieval.temporal.workflows.user_quota import UserQuotaWorkflow
from retrieval.temporal.workflows.user_sync import UserSyncWorkflow

logger = logging.getLogger(__name__)

WORKER_GRACEFUL_SHUTDOWN_TIMEOUT = timedelta(seconds=45)


class _WorkerHandle(Protocol):
    async def run(self) -> None: ...

    async def shutdown(self) -> None: ...


@dataclass
class AdapterBundle:
    """Process-owned adapters and demo hooks with one explicit close boundary."""

    repository: RetrievalRepository
    staging_store: StagingStore
    provider_gateway: ProviderGateway
    before_document_commit: BeforeDocumentCommitHook | None = None
    ingestion_event_sink: IngestionEventSink | None = None

    def __iter__(self):
        """Retain tuple-unpacking compatibility for existing adapter factories/tests."""

        yield self.repository
        yield self.staging_store
        yield self.provider_gateway

    async def aclose(self) -> None:
        """Close each unique process-owned resource, including async pools."""

        await _close_unique_resources(
            (
                self.ingestion_event_sink,
                self.before_document_commit,
                self.provider_gateway,
                self.staging_store,
                self.repository,
            )
        )


async def _close_unique_resources(resources: tuple[Any | None, ...]) -> None:
    """Attempt every unique closer before propagating any close failures."""

    seen: set[int] = set()
    errors: list[Exception] = []
    for resource in resources:
        if resource is None or id(resource) in seen:
            continue
        seen.add(id(resource))
        close = getattr(resource, "aclose", None) or getattr(resource, "close", None)
        if close is None:
            continue
        try:
            result = close()
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            errors.append(exc)
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise ExceptionGroup("multiple adapter close failures", errors)


V2_WORKFLOW_TYPES = (
    StoreControllerWorkflow,
    RootSyncWorkflow,
    FailedUserRemediationWorkflow,
    ActivateUserWorkflow,
    UserSyncWorkflow,
    ResourceSyncWorkflow,
    ResourcePagesWorkflow,
    FilesPageWorkflow,
    CommentsResyncWorkflow,
    DocumentIngestionWorkflow,
    UserQuotaWorkflow,
    DeactivateStoreWorkflow,
    CleanupUsersWorkflow,
    DeactivateUserWorkflow,
    DeactivateOneUserWorkflow,
    DeactivateAllUsersWorkflow,
    RemoveObjectsWorkflow,
)


def _deployment_config(
    runtime: TemporalRuntimeConfig,
) -> WorkerDeploymentConfig | None:
    if not runtime.use_worker_versioning:
        return None
    return WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(
            deployment_name=runtime.deployment_name,
            build_id=runtime.build_id,
        ),
        use_worker_versioning=True,
        default_versioning_behavior=VersioningBehavior.PINNED,
    )


def build_workers(
    client: Client,
    *,
    runtime: TemporalRuntimeConfig,
    config: RetrievalTemporalConfig,
    repository: RetrievalRepository,
    staging_store: StagingStore,
    provider_gateway: ProviderGateway,
    before_document_commit: BeforeDocumentCommitHook | None = None,
    ingestion_event_sink: IngestionEventSink | None = None,
) -> tuple[Worker, Worker]:
    lifecycle = LifecycleActivities(repository)
    ingestion = IngestionActivities(
        repository,
        staging_store,
        before_commit=before_document_commit or NoopBeforeDocumentCommitHook(),
        event_sink=ingestion_event_sink or NoopIngestionEventSink(),
    )
    cleanup = CleanupActivities(repository)
    quota_client = QuotaClientActivities(
        client,
        task_queue=runtime.retrieval_task_queue,
        max_in_flight=config.user_quota_max_in_flight,
        max_pending_requests=config.user_quota_max_pending_requests,
        dedup_window_size=config.user_quota_dedup_window_size,
        continue_as_new_message_count=(config.user_quota_continue_as_new_message_count),
    )
    provider = ProviderActivities(provider_gateway)
    workflows = list(V2_WORKFLOW_TYPES)
    if runtime.register_legacy_drain_types:
        workflows.extend(LEGACY_DRAIN_WORKFLOWS)

    deployment = _deployment_config(runtime)
    retrieval_worker = Worker(
        client,
        task_queue=runtime.retrieval_task_queue,
        workflows=workflows,
        activities=[
            lifecycle.begin_store_deactivation,
            lifecycle.resume_store_deactivation,
            lifecycle.validate_lifecycle_generation,
            lifecycle.activate_user_generation_fenced,
            lifecycle.mark_store_inactive,
            lifecycle.mark_store_deactivation_failed,
            ingestion.ingest_staged_document,
            cleanup.deactivate_users,
            cleanup.remove_objects,
            cleanup.remove_object_batch,
            quota_client.signal_with_start_user_quota,
        ],
        graceful_shutdown_timeout=WORKER_GRACEFUL_SHUTDOWN_TIMEOUT,
        deployment_config=deployment,
    )
    provider_worker = Worker(
        client,
        task_queue=runtime.provider_task_queue,
        activities=[provider.list_active_users, provider.fetch_resource_page],
        max_task_queue_activities_per_second=config.temporal_provider_queue_rps,
        graceful_shutdown_timeout=WORKER_GRACEFUL_SHUTDOWN_TIMEOUT,
        deployment_config=deployment,
    )
    return retrieval_worker, provider_worker


async def _load_factory(path: str) -> Any:
    module_name, separator, attribute_name = path.partition(":")
    if not separator or not module_name or not attribute_name:
        raise ValueError("adapter factory must use module:function syntax")
    factory: Callable[[], Any] = getattr(importlib.import_module(module_name), attribute_name)
    value = factory()
    return await value if inspect.isawaitable(value) else value


async def _load_adapters(
    runtime: TemporalRuntimeConfig,
) -> AdapterBundle:
    if runtime.adapter_bundle_factory:
        if (
            runtime.repository_factory
            or runtime.staging_store_factory
            or runtime.provider_gateway_factory
            or runtime.allow_unsafe_in_memory_adapters
        ):
            raise RuntimeError(
                "RETRIEVAL_ADAPTER_BUNDLE_FACTORY cannot be combined with individual "
                "adapter factories or unsafe in-memory adapters"
            )
        bundle = await _load_factory(runtime.adapter_bundle_factory)
        if not isinstance(bundle, AdapterBundle):
            raise TypeError("adapter bundle factory must return AdapterBundle")
        return bundle

    configured = (
        runtime.repository_factory,
        runtime.staging_store_factory,
        runtime.provider_gateway_factory,
    )
    if any(configured) and not all(configured):
        raise RuntimeError(
            "Production adapter configuration is all-or-nothing: configure "
            "RETRIEVAL_REPOSITORY_FACTORY, RETRIEVAL_STAGING_STORE_FACTORY, and "
            "RETRIEVAL_PROVIDER_GATEWAY_FACTORY together."
        )
    if all(configured):
        loaded = await asyncio.gather(
            *(_load_factory(path) for path in configured if path is not None),
            return_exceptions=True,
        )
        failures = [result for result in loaded if isinstance(result, BaseException)]
        if failures:
            successful = tuple(
                result for result in reversed(loaded) if not isinstance(result, BaseException)
            )
            try:
                await _close_unique_resources(successful)
            except Exception:
                logger.exception("Failed to close one or more partially loaded adapters")
            raise failures[0]
        repository, staging_store, provider_gateway = loaded
        return AdapterBundle(repository, staging_store, provider_gateway)
    if not runtime.allow_unsafe_in_memory_adapters:
        raise RuntimeError(
            "Production adapters are required. Configure RETRIEVAL_REPOSITORY_FACTORY, "
            "RETRIEVAL_STAGING_STORE_FACTORY, and RETRIEVAL_PROVIDER_GATEWAY_FACTORY, "
            "or explicitly allow unsafe in-memory adapters for local development."
        )
    logger.warning("Using non-durable local retrieval adapters")
    return AdapterBundle(
        repository=InMemoryRetrievalRepository(),
        staging_store=InMemoryStagingStore(),
        provider_gateway=EmptyProviderGateway(),
    )


def _install_signal_handlers(
    shutdown_requested: asyncio.Event,
    *,
    loop: asyncio.AbstractEventLoop | None = None,
) -> Callable[[], None]:
    """Route process termination signals through the worker shutdown path."""

    event_loop = loop or asyncio.get_running_loop()
    installed: list[signal.Signals] = []

    def request_shutdown(received: signal.Signals) -> None:
        logger.info("Received %s; requesting Temporal worker shutdown", received.name)
        shutdown_requested.set()

    for received in (signal.SIGINT, signal.SIGTERM):
        try:
            event_loop.add_signal_handler(received, request_shutdown, received)
        except (NotImplementedError, RuntimeError):
            logger.warning("Event-loop signal handlers are unavailable for %s", received.name)
        else:
            installed.append(received)

    def remove() -> None:
        for received in installed:
            event_loop.remove_signal_handler(received)

    return remove


async def _run_workers_until_stopped(
    workers: Sequence[_WorkerHandle],
    shutdown_requested: asyncio.Event,
) -> None:
    """Supervise pollers and fully stop every worker before returning.

    A signal, a normal poller exit, or a fatal poller exception initiates the
    same coordinated shutdown. All run-task results are consumed before a
    fatal exception is re-raised, so no sibling can keep polling while owned
    adapters are being closed.
    """

    run_tasks = tuple(
        asyncio.create_task(worker.run(), name=f"temporal-worker-{index}")
        for index, worker in enumerate(workers)
    )
    signal_task = asyncio.create_task(
        shutdown_requested.wait(), name="temporal-worker-shutdown-signal"
    )
    shutdown_results: list[BaseException | None] = []
    run_results: list[BaseException | None] = []
    try:
        await asyncio.wait((*run_tasks, signal_task), return_when=asyncio.FIRST_COMPLETED)
    finally:
        raw_shutdown_results = await asyncio.gather(
            *(worker.shutdown() for worker in workers), return_exceptions=True
        )
        shutdown_results = [
            result if isinstance(result, BaseException) else None for result in raw_shutdown_results
        ]
        raw_run_results = await asyncio.gather(*run_tasks, return_exceptions=True)
        run_results = [
            result if isinstance(result, BaseException) else None for result in raw_run_results
        ]
        signal_task.cancel()
        await asyncio.gather(signal_task, return_exceptions=True)

    errors = [error for error in (*run_results, *shutdown_results) if error is not None]
    if errors:
        for secondary in errors[1:]:
            logger.error(
                "Additional error while draining Temporal workers",
                exc_info=(type(secondary), secondary, secondary.__traceback__),
            )
        raise errors[0]


async def run_worker() -> None:
    logging.basicConfig(level=logging.INFO)
    shutdown_requested = asyncio.Event()
    remove_signal_handlers = _install_signal_handlers(shutdown_requested)
    try:
        runtime = TemporalRuntimeConfig.from_env()
        config = RetrievalTemporalConfig.from_env()
        client = await Client.connect(
            runtime.address,
            namespace=runtime.namespace,
            api_key=runtime.api_key,
            tls=runtime.tls,
        )
        adapters = await _load_adapters(runtime)
        sdk_capability = priority_capability(config.temporal_enable_priority_fairness)
        fairness_active = sdk_capability.active and runtime.server_priority_fairness_supported
        logger.info(
            "Temporal priority/fairness mode sdk=%s server_confirmed=%s active=%s",
            sdk_capability.mode,
            runtime.server_priority_fairness_supported,
            fairness_active,
        )
        if config.temporal_fairness_key_rps_default is not None:
            logger.info(
                "Per-fairness-key RPS default=%s must be configured on Temporal "
                "Server/Cloud; the SDK has no per-key queue limiter",
                config.temporal_fairness_key_rps_default,
            )
        try:
            workers = build_workers(
                client,
                runtime=runtime,
                config=config,
                repository=adapters.repository,
                staging_store=adapters.staging_store,
                provider_gateway=adapters.provider_gateway,
                before_document_commit=adapters.before_document_commit,
                ingestion_event_sink=adapters.ingestion_event_sink,
            )
            await _run_workers_until_stopped(workers, shutdown_requested)
        finally:
            await adapters.aclose()
    finally:
        remove_signal_handlers()


def main() -> None:
    inject_environment()
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
