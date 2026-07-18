"""Worker registration and deployment-versioned process entry point."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
from collections.abc import Callable
from typing import Any

from temporalio.client import Client
from temporalio.common import (
    VersioningBehavior,
    WorkerDeploymentVersion,
)
from temporalio.worker import Worker, WorkerDeploymentConfig

from retrieval.config import RetrievalTemporalConfig
from retrieval.temporal.activities.cleanup import CleanupActivities
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
) -> tuple[Worker, Worker]:
    lifecycle = LifecycleActivities(repository)
    ingestion = IngestionActivities(repository, staging_store)
    cleanup = CleanupActivities(repository)
    quota_client = QuotaClientActivities(
        client,
        task_queue=runtime.retrieval_task_queue,
        max_in_flight=config.user_quota_max_in_flight,
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
            quota_client.signal_with_start_user_quota,
        ],
        deployment_config=deployment,
    )
    provider_worker = Worker(
        client,
        task_queue=runtime.provider_task_queue,
        activities=[provider.list_active_users, provider.fetch_resource_page],
        max_task_queue_activities_per_second=config.temporal_provider_queue_rps,
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
) -> tuple[RetrievalRepository, StagingStore, ProviderGateway]:
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
        repository, staging_store, provider_gateway = await asyncio.gather(
            *(_load_factory(path) for path in configured if path is not None)
        )
        return repository, staging_store, provider_gateway
    if not runtime.allow_unsafe_in_memory_adapters:
        raise RuntimeError(
            "Production adapters are required. Configure RETRIEVAL_REPOSITORY_FACTORY, "
            "RETRIEVAL_STAGING_STORE_FACTORY, and RETRIEVAL_PROVIDER_GATEWAY_FACTORY, "
            "or explicitly allow unsafe in-memory adapters for local development."
        )
    logger.warning("Using non-durable local retrieval adapters")
    return (
        InMemoryRetrievalRepository(),
        InMemoryStagingStore(),
        EmptyProviderGateway(),
    )


async def run_worker() -> None:
    logging.basicConfig(level=logging.INFO)
    runtime = TemporalRuntimeConfig.from_env()
    config = RetrievalTemporalConfig.from_env()
    client = await Client.connect(
        runtime.address,
        namespace=runtime.namespace,
        api_key=runtime.api_key,
        tls=runtime.tls,
    )
    repository, staging_store, provider_gateway = await _load_adapters(runtime)
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
    workers = build_workers(
        client,
        runtime=runtime,
        config=config,
        repository=repository,
        staging_store=staging_store,
        provider_gateway=provider_gateway,
    )
    await asyncio.gather(*(worker.run() for worker in workers))


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
