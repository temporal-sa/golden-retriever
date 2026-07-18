"""Fast provider-contract tests; no Temporal service is required."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from temporalio.exceptions import ApplicationError

from retrieval.temporal.activities.provider_api import (
    FetchResourcePageRequest,
    ListActiveUsersRequest,
    ProviderActivities,
    UserDescriptor,
)
from retrieval.temporal.models.quota import QuotaScope

from .fake_provider import FakeProviderGateway, FakeProviderOutcome


def _list_request(request_id: str, scope: QuotaScope) -> ListActiveUsersRequest:
    return ListActiveUsersRequest(
        store_key="store-a",
        lifecycle_generation=7,
        cursor=None,
        page_size=50,
        request_id=request_id,
        quota_scope=scope,
    )


def _resource_request(request_id: str, scope: QuotaScope) -> FetchResourcePageRequest:
    return FetchResourcePageRequest(
        store_key="store-a",
        lifecycle_generation=7,
        sync_sequence="sync-a",
        user_key="user-a",
        resource_key="files",
        cursor=None,
        page_size=50,
        request_id=request_id,
        quota_scope=scope,
    )


async def test_shared_credential_is_visible_as_one_provider_scope() -> None:
    gateway = FakeProviderGateway()
    shared_scope = QuotaScope(
        provider="fake",
        credential_key="opaque-shared-credential",
        quota_class="standard",
    )
    gateway.queue_outcomes(
        "request-a",
        FakeProviderOutcome(users=(UserDescriptor("user-a"),)),
    )
    gateway.queue_outcomes(
        "request-b",
        FakeProviderOutcome(users=(UserDescriptor("user-b"),)),
    )

    await asyncio.gather(
        gateway.list_active_users(_list_request("request-a", shared_scope)),
        gateway.list_active_users(_list_request("request-b", shared_scope)),
    )

    calls = gateway.calls_for_credential(shared_scope.credential_key)
    assert {call.request_id for call in calls} == {"request-a", "request-b"}


async def test_429_maps_to_a_scope_wide_quota_observation() -> None:
    gateway = FakeProviderGateway()
    activities = ProviderActivities(gateway)
    scope = QuotaScope("fake", "opaque-shared-credential")
    reset_at = datetime.now(tz=UTC) + timedelta(minutes=1)
    gateway.queue_outcomes(
        "quota-request",
        FakeProviderOutcome.quota_exhausted(
            limit=100,
            remaining=0,
            reset_at=reset_at,
            retry_after_seconds=60,
        ),
    )

    result = await activities.fetch_resource_page(_resource_request("quota-request", scope))

    assert result.quota_exhausted is True
    assert result.observation is not None
    assert result.observation.quota_scope == scope
    assert result.observation.remaining == 0
    assert result.observation.reset_at == reset_at


async def test_delay_and_cancellation_are_controllable_without_sleeping_long() -> None:
    gateway = FakeProviderGateway()
    scope = QuotaScope("fake", "opaque-credential")
    gateway.queue_outcomes(
        "blocked-request",
        FakeProviderOutcome(wait_for_release=True),
    )
    task = asyncio.create_task(gateway.list_active_users(_list_request("blocked-request", scope)))
    await gateway.wait_until_started("blocked-request")

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    call = gateway.calls[-1]
    assert call.cancelled is True
    assert call.finished_at is not None


async def test_auth_failure_is_non_retryable_application_error() -> None:
    gateway = FakeProviderGateway()
    activities = ProviderActivities(gateway)
    scope = QuotaScope("fake", "invalid-credential")
    gateway.queue_outcomes("auth-request", FakeProviderOutcome.auth_failure())

    with pytest.raises(ApplicationError) as raised:
        await activities.list_active_users(_list_request("auth-request", scope))

    assert raised.value.type == "InvalidCredentials"
    assert raised.value.non_retryable is True
