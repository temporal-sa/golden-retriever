from __future__ import annotations

import pytest

from retrieval.config import RetrievalTemporalConfig
from retrieval.temporal.activities.provider_api import EmptyProviderGateway
from retrieval.temporal.activities.quota_client import QuotaClientActivities
from retrieval.temporal.activities.repositories import (
    InMemoryRetrievalRepository,
    InMemoryStagingStore,
)
from retrieval.temporal.models.quota import (
    CancelPermit,
    PermitRequest,
    QuotaScope,
    UserQuotaState,
)
from retrieval.temporal.runtime_config import TemporalRuntimeConfig
from retrieval.temporal.workflows import user_quota as quota_module
from retrieval.temporal.workflows.user_quota import (
    UserQuotaWorkflow,
    apply_cancel_permit,
)

SCOPE = QuotaScope("provider", "opaque-credential", "reads")


class _FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def start_workflow(self, *args: object, **kwargs: object) -> None:
        self.calls.append((args, kwargs))


@pytest.mark.asyncio
async def test_quota_client_carries_config_into_initial_workflow_state() -> None:
    client = _FakeClient()
    activities = QuotaClientActivities(
        client,  # type: ignore[arg-type]
        task_queue="retrieval",
        max_in_flight=3,
        max_pending_requests=19,
        dedup_window_size=17,
        continue_as_new_message_count=23,
    )
    request = PermitRequest(
        request_id="request",
        requester_workflow_id="requester",
        store_key="store",
        lifecycle_generation=1,
        quota_scope=SCOPE,
    )

    await activities.signal_with_start_user_quota(request)

    initial_state = client.calls[0][0][1]
    assert isinstance(initial_state, UserQuotaState)
    assert initial_state.max_in_flight == 3
    assert initial_state.max_pending_requests == 19
    assert initial_state.dedup_window_size == 17
    assert initial_state.continue_as_new_message_count == 23


def test_state_dedup_window_is_used_without_call_site_override() -> None:
    state = UserQuotaState(quota_scope=SCOPE, dedup_window_size=2)

    for request_id in ("first", "second", "third"):
        apply_cancel_permit(state, CancelPermit(request_id))

    assert state.recent_terminal_request_ids == ["second", "third"]
    assert state.recent_terminal_request_order == ["second", "third"]


def test_state_continue_as_new_threshold_controls_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Info:
        @staticmethod
        def is_continue_as_new_suggested() -> bool:
            return False

    class _Workflow:
        @staticmethod
        def info() -> _Info:
            return _Info()

    monkeypatch.setattr(quota_module, "workflow", _Workflow())
    workflow_instance = UserQuotaWorkflow()
    state = UserQuotaState(
        quota_scope=SCOPE,
        processed_message_count=2,
        continue_as_new_message_count=3,
    )
    workflow_instance._initialize(state)

    assert workflow_instance._should_continue_as_new() is False
    state.processed_message_count = 3
    assert workflow_instance._should_continue_as_new() is True


def test_build_workers_passes_both_quota_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from retrieval.temporal import worker as worker_module

    captured: dict[str, object] = {}

    class _QuotaActivities:
        def __init__(self, _client: object, **kwargs: object) -> None:
            captured.update(kwargs)

        async def signal_with_start_user_quota(self) -> None:
            pass

    class _Worker:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

    monkeypatch.setattr(worker_module, "QuotaClientActivities", _QuotaActivities)
    monkeypatch.setattr(worker_module, "Worker", _Worker)
    config = RetrievalTemporalConfig(
        user_quota_max_in_flight=4,
        user_quota_max_pending_requests=29,
        user_quota_dedup_window_size=31,
        user_quota_continue_as_new_message_count=37,
    )

    worker_module.build_workers(
        object(),  # type: ignore[arg-type]
        runtime=TemporalRuntimeConfig(),
        config=config,
        repository=InMemoryRetrievalRepository(),
        staging_store=InMemoryStagingStore(),
        provider_gateway=EmptyProviderGateway(),
    )

    assert captured["max_in_flight"] == 4
    assert captured["max_pending_requests"] == 29
    assert captured["dedup_window_size"] == 31
    assert captured["continue_as_new_message_count"] == 37
