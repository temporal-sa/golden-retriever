from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from retrieval.temporal.common import quota_waiter as quota_waiter_module
from retrieval.temporal.common.ids import permit_id
from retrieval.temporal.common.quota_waiter import (
    QuotaPermitDeniedError,
    QuotaWaiterMixin,
)
from retrieval.temporal.models.quota import (
    CancelGenerationPermits,
    CancelPermit,
    PermitCompleted,
    PermitDenied,
    PermitGrant,
    PermitRequest,
    QuotaObservation,
    QuotaScope,
    UserQuotaState,
)
from retrieval.temporal.workflows.user_quota import (
    UserQuotaWorkflow,
    apply_cancel_generation_permits,
    apply_cancel_permit,
    apply_permit_completed,
    apply_permit_request,
    apply_quota_observation,
    compact_quota_state,
    grant_available_permits,
    next_quota_timer_delay,
    permit_request_denial,
    quota_snapshot,
)

NOW = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)
SCOPE = QuotaScope("example", "opaque-credential", "api")


def request(
    request_id: str,
    *,
    cost: int = 1,
    requester: str | None = None,
    scope: QuotaScope = SCOPE,
    store_key: str = "opaque-store",
    generation: int = 7,
) -> PermitRequest:
    return PermitRequest(
        request_id=request_id,
        requester_workflow_id=requester or f"requester/{request_id}",
        store_key=store_key,
        lifecycle_generation=generation,
        quota_scope=scope,
        cost=cost,
    )


def test_fifo_grants_respect_max_in_flight_and_release_on_completion() -> None:
    state = UserQuotaState(
        quota_scope=SCOPE,
        configured_limit=3,
        remaining=3,
        max_in_flight=2,
    )
    for request_id in ("r1", "r2", "r3"):
        assert apply_permit_request(state, request(request_id))

    first = grant_available_permits(state, now=NOW)

    assert [grant.request_id for grant in first] == ["r1", "r2"]
    assert state.pending_order == ["r3"]
    assert state.remaining == 1
    assert state.in_flight == 2
    assert first[0].permit_id == permit_id("r1", "initial")

    assert apply_permit_completed(
        state,
        PermitCompleted("r1", first[0].permit_id),
    )
    second = grant_available_permits(state, now=NOW)

    assert [grant.request_id for grant in second] == ["r3"]
    assert state.remaining == 0
    assert state.in_flight == 2
    assert state.pending_order == []


def test_known_remaining_is_never_oversubscribed() -> None:
    state = UserQuotaState(
        quota_scope=SCOPE,
        configured_limit=1,
        remaining=1,
        max_in_flight=5,
    )
    apply_permit_request(state, request("r1"))
    apply_permit_request(state, request("r2"))

    [grant] = grant_available_permits(state, now=NOW)
    assert apply_permit_completed(
        state,
        PermitCompleted(grant.request_id, grant.permit_id),
    )

    assert grant_available_permits(state, now=NOW) == []
    assert state.pending_order == ["r2"]
    assert state.remaining == 0


def test_fifo_does_not_bypass_a_high_cost_head_request() -> None:
    state = UserQuotaState(
        quota_scope=SCOPE,
        remaining=1,
        max_in_flight=3,
    )
    apply_permit_request(state, request("expensive", cost=2))
    apply_permit_request(state, request("cheap", cost=1))

    assert grant_available_permits(state, now=NOW) == []
    assert state.pending_order == ["expensive", "cheap"]


def test_duplicate_and_terminal_requests_are_idempotent() -> None:
    state = UserQuotaState(quota_scope=SCOPE)
    permit_request = request("same")

    assert apply_permit_request(state, permit_request)
    assert not apply_permit_request(state, permit_request)
    assert apply_cancel_permit(state, CancelPermit("same"))
    assert not apply_cancel_permit(state, CancelPermit("same"))
    assert not apply_permit_request(state, permit_request)
    assert state.pending == {}
    assert state.pending_order == []


def test_pending_queue_capacity_is_bounded_and_returns_explicit_denial() -> None:
    state = UserQuotaState(
        quota_scope=SCOPE,
        max_in_flight=1,
        max_pending_requests=2,
    )
    assert apply_permit_request(state, request("r1"))
    assert apply_permit_request(state, request("r2"))

    overflow = request("r3")
    denial = permit_request_denial(state, overflow)

    assert denial == PermitDenied(
        request_id="r3",
        quota_scope=SCOPE,
        reason="quota pending queue is at capacity",
        retryable=True,
    )
    assert not apply_permit_request(state, overflow)
    assert state.pending_order == ["r1", "r2"]


def test_quota_workflow_queues_denial_for_capacity_overflow() -> None:
    quota = UserQuotaWorkflow()
    state = UserQuotaState(quota_scope=SCOPE, max_pending_requests=1)
    quota._initialize(state)
    quota._apply_message("request", request("accepted"))
    quota._apply_message("request", request("overflow"))

    assert state.pending_order == ["accepted"]
    assert quota._pending_denials == [
        (
            "requester/overflow",
            PermitDenied(
                "overflow",
                SCOPE,
                "quota pending queue is at capacity",
                retryable=True,
            ),
        )
    ]


def test_cancel_before_request_tombstones_the_race() -> None:
    state = UserQuotaState(quota_scope=SCOPE)

    assert apply_cancel_permit(state, CancelPermit("late-request"))
    assert not apply_permit_request(state, request("late-request"))


def test_cancel_generation_removes_pending_and_granted_old_work() -> None:
    state = UserQuotaState(
        quota_scope=SCOPE,
        remaining=10,
        max_in_flight=1,
    )
    apply_permit_request(
        state,
        request("old-granted", store_key="store-a", generation=6),
    )
    [old_grant] = grant_available_permits(state, now=NOW)
    apply_permit_request(
        state,
        request("old-pending", store_key="store-a", generation=6),
    )
    apply_permit_request(
        state,
        request("current", store_key="store-a", generation=7),
    )
    apply_permit_request(
        state,
        request("other-store", store_key="store-b", generation=6),
    )

    canceled = apply_cancel_generation_permits(
        state,
        CancelGenerationPermits("store-a", 6),
    )

    assert canceled == 2
    assert old_grant.request_id not in state.reservations
    assert state.in_flight == 0
    assert state.remaining == 9
    assert state.pending_order == ["current", "other-store"]
    assert set(state.recent_terminal_request_ids).issuperset({"old-granted", "old-pending"})


def test_terminal_dedup_window_keeps_most_recent_ids() -> None:
    state = UserQuotaState(quota_scope=SCOPE)

    for request_id in ("r1", "r2", "r3"):
        apply_cancel_permit(state, CancelPermit(request_id), dedup_window_size=2)

    assert state.recent_terminal_request_ids == ["r2", "r3"]
    assert state.recent_terminal_request_order == ["r2", "r3"]


def test_fairness_weight_does_not_change_quota_scope_identity() -> None:
    state = UserQuotaState(quota_scope=QuotaScope("example", "opaque-credential", "api", 1.0))
    weighted_scope = QuotaScope("example", "opaque-credential", "api", 7.0)

    assert weighted_scope == state.quota_scope
    assert apply_permit_request(state, request("weighted", scope=weighted_scope))


def test_exhaustion_blocks_every_request_until_the_later_reset_boundary() -> None:
    state = UserQuotaState(
        quota_scope=SCOPE,
        configured_limit=5,
        remaining=5,
        max_in_flight=2,
    )
    apply_permit_request(state, request("resource-a"))
    apply_permit_request(state, request("resource-b"))
    apply_permit_request(state, request("resource-c"))
    apply_quota_observation(
        state,
        QuotaObservation(
            quota_scope=SCOPE,
            request_id="another-resource",
            limit=5,
            remaining=0,
            reset_at=NOW + timedelta(seconds=30),
            retry_after_seconds=60,
            exhausted=True,
        ),
        now=NOW,
    )

    assert grant_available_permits(state, now=NOW) == []
    assert grant_available_permits(state, now=NOW + timedelta(seconds=59)) == []
    grants = grant_available_permits(state, now=NOW + timedelta(seconds=60))

    assert [grant.request_id for grant in grants] == ["resource-a", "resource-b"]
    assert state.pending_order == ["resource-c"]
    assert state.in_flight == 2
    assert state.remaining == 3
    assert state.blocked_until is None
    assert state.quota_window_id != "initial"


def test_headerless_exhaustion_uses_bounded_probe_timer() -> None:
    state = UserQuotaState(
        quota_scope=SCOPE,
        remaining=5,
        max_in_flight=1,
    )
    apply_permit_request(state, request("probe"))
    apply_quota_observation(
        state,
        QuotaObservation(
            quota_scope=SCOPE,
            request_id="headerless-429",
            exhausted=True,
        ),
        now=NOW,
    )

    assert next_quota_timer_delay(state, now=NOW) == timedelta(seconds=60)
    assert grant_available_permits(state, now=NOW + timedelta(seconds=59)) == []
    [grant] = grant_available_permits(state, now=NOW + timedelta(seconds=60))
    assert grant.request_id == "probe"


def test_reset_with_many_waiters_only_releases_configured_concurrency() -> None:
    reset_at = NOW + timedelta(minutes=1)
    state = UserQuotaState(
        quota_scope=SCOPE,
        configured_limit=100,
        remaining=0,
        reset_at=reset_at,
        blocked_until=reset_at,
        max_in_flight=3,
    )
    for index in range(10):
        apply_permit_request(state, request(f"r{index}"))

    grants = grant_available_permits(state, now=reset_at)

    assert [grant.request_id for grant in grants] == ["r0", "r1", "r2"]
    assert state.in_flight == 3
    assert len(state.pending) == 7


def test_canceling_a_grant_releases_concurrency_but_never_refunds_balance() -> None:
    state = UserQuotaState(
        quota_scope=SCOPE,
        remaining=2,
        max_in_flight=1,
    )
    apply_permit_request(state, request("r1"))
    apply_permit_request(state, request("r2"))
    [grant] = grant_available_permits(state, now=NOW)

    assert apply_cancel_permit(state, CancelPermit(grant.request_id))

    assert state.in_flight == 0
    assert state.remaining == 1
    assert "r1" not in state.reservations
    [next_grant] = grant_available_permits(state, now=NOW)
    assert next_grant.request_id == "r2"
    assert state.remaining == 0


def test_completion_requires_the_exact_permit_and_is_idempotent() -> None:
    state = UserQuotaState(quota_scope=SCOPE, max_in_flight=1)
    apply_permit_request(state, request("r1"))
    [grant] = grant_available_permits(state, now=NOW)

    assert not apply_permit_completed(
        state,
        PermitCompleted("r1", "wrong-permit"),
    )
    assert state.in_flight == 1
    assert apply_permit_completed(
        state,
        PermitCompleted("r1", grant.permit_id),
    )
    assert not apply_permit_completed(
        state,
        PermitCompleted("r1", grant.permit_id),
    )
    assert state.in_flight == 0


def test_out_of_order_observation_cannot_inflate_remaining() -> None:
    state = UserQuotaState(quota_scope=SCOPE, remaining=2)

    assert apply_quota_observation(
        state,
        QuotaObservation(SCOPE, "newer", limit=10, remaining=1),
        now=NOW,
    )
    assert apply_quota_observation(
        state,
        QuotaObservation(SCOPE, "older", limit=10, remaining=8),
        now=NOW,
    )

    assert state.remaining == 1


def test_timer_delay_and_snapshot_are_compact() -> None:
    reset_at = NOW + timedelta(seconds=45)
    state = UserQuotaState(
        quota_scope=SCOPE,
        configured_limit=9,
        remaining=0,
        reset_at=reset_at,
        max_in_flight=2,
    )
    apply_permit_request(state, request("r1"))

    assert next_quota_timer_delay(state, now=NOW) == timedelta(seconds=45)
    snapshot = quota_snapshot(state)
    assert snapshot.pending_count == 1
    assert snapshot.reservation_count == 0
    assert not hasattr(snapshot, "pending")


def test_compaction_preserves_pending_and_active_reservations() -> None:
    state = UserQuotaState(quota_scope=SCOPE, max_in_flight=1)
    apply_permit_request(state, request("r1"))
    apply_permit_request(state, request("r2"))
    [grant] = grant_available_permits(state, now=NOW)
    # Simulate a duplicate/stale order entry carried by an older run.
    state.pending_order.extend(["r2", "missing"])

    compact_quota_state(state)

    assert state.pending_order == ["r2"]
    assert state.pending == {"r2": request("r2")}
    assert state.reservations["r1"].permit_id == grant.permit_id
    assert state.in_flight == 1


def test_quota_waiter_records_early_exact_grant_and_ignores_wrong_or_duplicate() -> None:
    waiter = QuotaWaiterMixin()
    expected = request("wanted")
    waiter._ensure_quota_waiter_state()
    waiter._quota_expected_requests[expected.request_id] = expected

    waiter.quota_granted(PermitGrant("wrong-id", "permit/wrong", SCOPE, "window", 1))
    waiter.quota_granted(
        PermitGrant(
            "wanted",
            "permit/wrong-scope",
            QuotaScope("other", "credential"),
            "window",
            1,
        )
    )
    accepted = PermitGrant("wanted", "permit/first", SCOPE, "window", 1)
    waiter.quota_granted(accepted)
    waiter.quota_granted(PermitGrant("wanted", "permit/duplicate", SCOPE, "window", 1))

    assert waiter._quota_grants == {"wanted": accepted}


def test_quota_waiter_ignores_late_duplicate_after_terminal_consumption() -> None:
    waiter = QuotaWaiterMixin()
    expected = request("done")
    waiter._ensure_quota_waiter_state()
    waiter._quota_expected_requests[expected.request_id] = expected
    waiter._mark_quota_request_terminal(expected.request_id)

    waiter.quota_granted(PermitGrant("done", "permit/late", SCOPE, "window", 1))

    assert waiter._quota_grants == {}


def test_quota_waiter_keeps_grant_delivered_before_wait_condition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waiter = QuotaWaiterMixin()
    permit_request = request("early")
    accepted = PermitGrant("early", "permit/early", SCOPE, "window", 1)

    class FakeWorkflow:
        def __init__(self) -> None:
            self.wait_condition_was_ready = False

        async def execute_activity(self, *_args: object, **_kwargs: object) -> None:
            waiter.quota_granted(accepted)

        async def wait_condition(self, predicate: Callable[[], bool]) -> None:
            self.wait_condition_was_ready = predicate()

    fake_workflow = FakeWorkflow()
    monkeypatch.setattr(quota_waiter_module, "workflow", fake_workflow)

    result = asyncio.run(waiter.request_quota_permit(permit_request))

    assert result == accepted
    assert fake_workflow.wait_condition_was_ready


def test_quota_waiter_fails_immediately_on_exact_denial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waiter = QuotaWaiterMixin()
    permit_request = request("denied")
    denial = PermitDenied(
        "denied",
        SCOPE,
        "quota pending queue is at capacity",
        retryable=True,
    )

    class FakeWorkflow:
        async def execute_activity(self, *_args: object, **_kwargs: object) -> None:
            waiter.quota_denied(denial)

        async def wait_condition(self, predicate: Callable[[], bool]) -> None:
            assert predicate()

    monkeypatch.setattr(quota_waiter_module, "workflow", FakeWorkflow())

    with pytest.raises(QuotaPermitDeniedError) as raised:
        asyncio.run(waiter.request_quota_permit(permit_request))

    assert raised.value.denial == denial
    assert waiter._quota_expected_requests == {}


def test_quota_waiter_cancels_shared_request_when_local_wait_is_canceled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signals: list[tuple[str, object]] = []

    class FakeLogger:
        def warning(self, *_args: object) -> None:
            pass

    class FakeHandle:
        async def signal(self, name: str, message: object) -> None:
            signals.append((name, message))

    class FakeWorkflow:
        logger = FakeLogger()

        async def execute_activity(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def wait_condition(self, _predicate: object) -> None:
            raise asyncio.CancelledError

        def get_external_workflow_handle(self, _workflow_id: str) -> FakeHandle:
            return FakeHandle()

    waiter = QuotaWaiterMixin()
    monkeypatch.setattr(quota_waiter_module, "workflow", FakeWorkflow())

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(waiter.request_quota_permit(request("canceled")))

    assert len(signals) == 1
    signal_name, message = signals[0]
    assert signal_name == "cancel_permit"
    assert message == CancelPermit(
        "canceled",
        "requester canceled while waiting for quota",
    )


def test_quota_waiter_finishes_cleanup_after_repeated_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waiting_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    signals: list[tuple[str, object]] = []

    class FakeLogger:
        def warning(self, *_args: object) -> None:
            pass

    class SlowHandle:
        async def signal(self, name: str, message: object) -> None:
            cleanup_started.set()
            await allow_cleanup.wait()
            signals.append((name, message))

    class FakeWorkflow:
        logger = FakeLogger()

        async def execute_activity(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def wait_condition(self, _predicate: object) -> None:
            waiting_started.set()
            await asyncio.Future()

        def get_external_workflow_handle(self, _workflow_id: str) -> SlowHandle:
            return SlowHandle()

    waiter = QuotaWaiterMixin()
    monkeypatch.setattr(quota_waiter_module, "workflow", FakeWorkflow())

    async def exercise() -> None:
        caller = asyncio.create_task(waiter.request_quota_permit(request("repeated-cancel")))
        await asyncio.wait_for(waiting_started.wait(), timeout=1)
        caller.cancel()
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        caller.cancel()
        await asyncio.sleep(0)
        allow_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(caller, timeout=1)

    asyncio.run(exercise())

    assert len(signals) == 1
    assert signals[0][0] == "cancel_permit"
