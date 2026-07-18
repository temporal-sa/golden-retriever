"""Opt-in integration scenarios against a real Temporal development namespace."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from uuid import uuid4

import pytest
from temporalio import workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from retrieval.temporal.activities.provider_api import (
    ActiveUsersPage,
    FetchResourcePageRequest,
    ListActiveUsersRequest,
    ProviderActivities,
    ResourcePageManifest,
    UserDescriptor,
)
from retrieval.temporal.activities.quota_client import QuotaClientActivities
from retrieval.temporal.common.ids import user_quota_workflow_id
from retrieval.temporal.common.quota_waiter import QuotaWaiterMixin
from retrieval.temporal.models.quota import PermitGrant, PermitRequest, QuotaScope
from retrieval.temporal.workflows.user_quota import UserQuotaWorkflow

from .fake_provider import FakeProviderGateway, FakeProviderOutcome

RUN_INTEGRATION = os.getenv("RUN_TEMPORAL_INTEGRATION") == "1"
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not RUN_INTEGRATION,
        reason="set RUN_TEMPORAL_INTEGRATION=1 to start/use a Temporal test server",
    ),
]


@workflow.defn(name="IntegrationProviderListProbeWorkflow")
class IntegrationProviderListProbeWorkflow:
    @workflow.run
    async def run(self, request: ListActiveUsersRequest) -> ActiveUsersPage:
        return await workflow.execute_activity(
            "provider_list_active_users",
            request,
            result_type=ActiveUsersPage,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


@workflow.defn(name="IntegrationProviderResourceProbeWorkflow")
class IntegrationProviderResourceProbeWorkflow:
    @workflow.run
    async def run(self, request: FetchResourcePageRequest) -> ResourcePageManifest:
        return await workflow.execute_activity(
            "provider_fetch_resource_page",
            request,
            result_type=ResourcePageManifest,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


@workflow.defn(name="IntegrationQuotaRequesterWorkflow")
class IntegrationQuotaRequesterWorkflow(QuotaWaiterMixin):
    @workflow.run
    async def run(self, request: PermitRequest) -> PermitGrant:
        grant = await self.request_quota_permit(request)
        await self.complete_quota_permit(grant)
        return grant


@asynccontextmanager
async def _temporal_environment() -> AsyncIterator[WorkflowEnvironment]:
    address = os.getenv("TEMPORAL_INTEGRATION_ADDRESS")
    if address:
        api_key = os.getenv("TEMPORAL_INTEGRATION_API_KEY")
        client = await Client.connect(
            address,
            namespace=os.getenv("TEMPORAL_INTEGRATION_NAMESPACE", "default"),
            api_key=api_key,
            tls=bool(api_key),
        )
        yield WorkflowEnvironment.from_client(client)
        return

    # This may download the matching Temporal CLI binary, which is why the entire
    # module is opt-in instead of silently doing network/process work in a normal test run.
    async with await WorkflowEnvironment.start_local() as environment:
        yield environment


def _list_request(request_id: str, scope: QuotaScope) -> ListActiveUsersRequest:
    return ListActiveUsersRequest(
        store_key="integration-store",
        lifecycle_generation=1,
        cursor=None,
        page_size=25,
        request_id=request_id,
        quota_scope=scope,
    )


def _resource_request(request_id: str, scope: QuotaScope) -> FetchResourcePageRequest:
    return FetchResourcePageRequest(
        store_key="integration-store",
        lifecycle_generation=1,
        sync_sequence="integration-sync",
        user_key="integration-user",
        resource_key="files",
        cursor=None,
        page_size=25,
        request_id=request_id,
        quota_scope=scope,
    )


async def test_fake_provider_scenarios_through_temporal() -> None:
    gateway = FakeProviderGateway()
    provider_activities = ProviderActivities(gateway)
    suffix = uuid4().hex
    task_queue = f"retrieval-provider-integration-{suffix}"
    scope = QuotaScope("fake", f"shared-credential-{suffix}")

    gateway.queue_outcomes(
        "shared-a",
        FakeProviderOutcome(
            delay_seconds=0.02,
            users=(UserDescriptor("user-a"),),
        ),
    )
    gateway.queue_outcomes(
        "shared-b",
        FakeProviderOutcome(users=(UserDescriptor("user-b"),)),
    )
    gateway.queue_outcomes(
        "quota-429",
        FakeProviderOutcome.quota_exhausted(
            limit=10,
            remaining=0,
            retry_after_seconds=1,
        ),
    )
    gateway.queue_outcomes("auth-failure", FakeProviderOutcome.auth_failure())
    gateway.queue_outcomes(
        "cancelled-call",
        FakeProviderOutcome(wait_for_release=True),
    )

    async with _temporal_environment() as environment:
        async with Worker(
            environment.client,
            task_queue=task_queue,
            workflows=[
                IntegrationProviderListProbeWorkflow,
                IntegrationProviderResourceProbeWorkflow,
            ],
            activities=[
                provider_activities.list_active_users,
                provider_activities.fetch_resource_page,
            ],
        ):
            shared_results = await asyncio.gather(
                environment.client.execute_workflow(
                    IntegrationProviderListProbeWorkflow.run,
                    _list_request("shared-a", scope),
                    id=f"provider-shared-a-{suffix}",
                    task_queue=task_queue,
                ),
                environment.client.execute_workflow(
                    IntegrationProviderListProbeWorkflow.run,
                    _list_request("shared-b", scope),
                    id=f"provider-shared-b-{suffix}",
                    task_queue=task_queue,
                ),
            )
            assert {result.users[0].user_key for result in shared_results} == {
                "user-a",
                "user-b",
            }
            assert len(gateway.calls_for_credential(scope.credential_key)) == 2
            delayed_call = next(call for call in gateway.calls if call.request_id == "shared-a")
            assert delayed_call.finished_at is not None
            assert delayed_call.finished_at - delayed_call.started_at >= 0.01

            quota_result = await environment.client.execute_workflow(
                IntegrationProviderResourceProbeWorkflow.run,
                _resource_request("quota-429", scope),
                id=f"provider-quota-{suffix}",
                task_queue=task_queue,
            )
            assert quota_result.quota_exhausted is True
            assert quota_result.observation is not None
            assert quota_result.observation.quota_scope == scope

            with pytest.raises(WorkflowFailureError):
                await environment.client.execute_workflow(
                    IntegrationProviderListProbeWorkflow.run,
                    _list_request("auth-failure", scope),
                    id=f"provider-auth-{suffix}",
                    task_queue=task_queue,
                )

            cancelled_handle = await environment.client.start_workflow(
                IntegrationProviderListProbeWorkflow.run,
                _list_request("cancelled-call", scope),
                id=f"provider-cancel-{suffix}",
                task_queue=task_queue,
            )
            await gateway.wait_until_started("cancelled-call")
            await cancelled_handle.cancel()
            # Short provider calls may finish after workflow cancellation because they do
            # not heartbeat.  Release the fake and verify the workflow still closes canceled.
            gateway.release("cancelled-call")
            with pytest.raises(WorkflowFailureError):
                await cancelled_handle.result()


async def test_shared_credential_reuses_one_quota_workflow() -> None:
    suffix = uuid4().hex
    task_queue = f"retrieval-quota-integration-{suffix}"
    scope = QuotaScope(
        provider="fake",
        credential_key=f"shared-credential-{suffix}",
        quota_class="standard",
    )
    quota_workflow_id = user_quota_workflow_id(
        scope.provider,
        scope.credential_key,
        scope.quota_class,
    )

    async with _temporal_environment() as environment:
        quota_activities = QuotaClientActivities(
            environment.client,
            task_queue=task_queue,
            max_in_flight=1,
            configured_limit=None,
        )
        async with Worker(
            environment.client,
            task_queue=task_queue,
            workflows=[UserQuotaWorkflow, IntegrationQuotaRequesterWorkflow],
            activities=[quota_activities.signal_with_start_user_quota],
        ):
            requester_ids = [
                f"quota-requester-a-{suffix}",
                f"quota-requester-b-{suffix}",
            ]
            handles = []
            for index, requester_id in enumerate(requester_ids):
                request = PermitRequest(
                    request_id=f"permit-request-{index}-{suffix}",
                    requester_workflow_id=requester_id,
                    store_key="integration-store",
                    lifecycle_generation=1,
                    quota_scope=scope,
                )
                handles.append(
                    await environment.client.start_workflow(
                        IntegrationQuotaRequesterWorkflow.run,
                        request,
                        id=requester_id,
                        task_queue=task_queue,
                    )
                )

            grants = await asyncio.gather(*(handle.result() for handle in handles))
            assert len({grant.permit_id for grant in grants}) == 2
            assert {grant.quota_scope for grant in grants} == {scope}

            quota_handle = environment.client.get_workflow_handle_for(
                UserQuotaWorkflow.run,
                quota_workflow_id,
            )
            snapshot = await quota_handle.query(UserQuotaWorkflow.get_quota_state)
            assert snapshot.pending_count == 0
            assert snapshot.in_flight == 0
            assert snapshot.reservation_count == 0

            # Do not leave the entity workflow running in a user-supplied namespace.
            await quota_handle.cancel()
            with suppress(WorkflowFailureError):
                await quota_handle.result()
