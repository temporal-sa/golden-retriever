"""Durable coordinator for one external provider quota scope.

The functions in the first half of this module are an intentionally small,
pure state machine.  They mutate only the supplied ``UserQuotaState`` and use
only caller-supplied workflow time.  Keeping the grant algorithm independent
from Temporal makes its invariants easy to unit test and avoids hiding quota
correctness in message-handler scheduling.

``UserQuotaWorkflow`` is the Temporal adapter around that state machine.  It
parks on a workflow condition while no grant or reset is possible, so an
unavailable quota never consumes an Activity slot.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

from retrieval.temporal.common.ids import permit_id
from retrieval.temporal.models.quota import (
    CancelGenerationPermits,
    CancelPermit,
    DisableQuotaScope,
    PermitCompleted,
    PermitGrant,
    PermitRequest,
    PermitReservation,
    PermitStatus,
    QuotaObservation,
    QuotaSnapshot,
    UserQuotaState,
)

try:  # Keep the pure state machine importable in lightweight test installs.
    from temporalio import workflow
    from temporalio.exceptions import TemporalError

    from retrieval.temporal.common.metrics import (
        QUOTA_BLOCKED,
        QUOTA_GRANT_SIGNAL_FAILURES,
        QUOTA_GRANTS,
        QUOTA_IN_FLIGHT,
        QUOTA_PENDING,
        QUOTA_REQUESTS,
        QUOTA_WAIT_DURATION,
        workflow_metrics,
    )
except ImportError:  # pragma: no cover - exercised only without the SDK
    workflow = None  # type: ignore[assignment]

    class TemporalError(Exception):
        """Fallback used only when importing pure logic without Temporal."""


DEFAULT_DEDUP_WINDOW_SIZE = 2_000
DEFAULT_CONTINUE_AS_NEW_MESSAGE_COUNT = 10_000
DEFAULT_UNKNOWN_RESET_PROBE_DELAY = timedelta(seconds=60)
QUOTA_GRANTED_SIGNAL = "quota_granted"

_F = TypeVar("_F", bound=Callable[..., Any])


def _identity_decorator(*_args: Any, **_kwargs: Any) -> Callable[[_F], _F]:
    return lambda decorated: decorated


def _identity_run(decorated: _F) -> _F:
    return decorated


if workflow is None:  # pragma: no cover - allows pure-logic-only installs
    _workflow_defn = _identity_decorator
    _workflow_run = _identity_run
    _workflow_signal = _identity_decorator
    _workflow_query = _identity_decorator
else:
    _workflow_defn = workflow.defn
    _workflow_run = workflow.run
    _workflow_signal = workflow.signal
    _workflow_query = workflow.query


def _as_utc(value: datetime) -> datetime:
    """Return a comparable UTC timestamp without consulting wall-clock time."""

    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _not_after(left: datetime, right: datetime) -> bool:
    return _as_utc(left) <= _as_utc(right)


def _later(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return left if _as_utc(left) >= _as_utc(right) else right


def _mark_terminal(
    state: UserQuotaState,
    request_id: str,
    *,
    dedup_window_size: int | None = None,
) -> None:
    """Record a terminal request in a deterministic, bounded recent window."""

    dedup_window_size = state.dedup_window_size if dedup_window_size is None else dedup_window_size
    if dedup_window_size <= 0:
        state.recent_terminal_request_ids.clear()
        state.recent_terminal_request_order.clear()
        return
    if request_id in state.recent_terminal_request_ids:
        return
    state.recent_terminal_request_ids.append(request_id)
    state.recent_terminal_request_order.append(request_id)
    overflow = len(state.recent_terminal_request_order) - dedup_window_size
    if overflow > 0:
        for evicted in state.recent_terminal_request_order[:overflow]:
            if evicted in state.recent_terminal_request_ids:
                state.recent_terminal_request_ids.remove(evicted)
        del state.recent_terminal_request_order[:overflow]


def compact_quota_state(
    state: UserQuotaState,
    *,
    dedup_window_size: int | None = None,
) -> UserQuotaState:
    """Normalize state before a query or Continue-As-New boundary."""

    dedup_window_size = state.dedup_window_size if dedup_window_size is None else dedup_window_size
    active_reservations: dict[str, PermitReservation] = {}
    for request_id, reservation in state.reservations.items():
        if reservation.status == PermitStatus.GRANTED:
            active_reservations[request_id] = reservation
        else:
            _mark_terminal(
                state,
                request_id,
                dedup_window_size=dedup_window_size,
            )
    state.reservations = active_reservations
    state.in_flight = len(active_reservations)

    compact_order: list[str] = []
    seen: set[str] = set()
    for request_id in state.pending_order:
        if request_id in state.pending and request_id not in seen:
            compact_order.append(request_id)
            seen.add(request_id)
    # A legacy or manually constructed state may contain pending entries that
    # predate ``pending_order``.  Sorting only this repair tail is deterministic.
    compact_order.extend(sorted(set(state.pending) - seen))
    state.pending_order = compact_order

    terminal_order: list[str] = []
    seen_terminal: set[str] = set()
    for request_id in state.recent_terminal_request_order:
        if request_id in state.recent_terminal_request_ids and request_id not in seen_terminal:
            terminal_order.append(request_id)
            seen_terminal.add(request_id)
    # Repair pre-order state deterministically once, then preserve true arrival
    # order for every subsequent terminal request.
    terminal_order.extend(sorted(set(state.recent_terminal_request_ids) - seen_terminal))
    state.recent_terminal_request_order = terminal_order
    overflow = len(terminal_order) - max(0, dedup_window_size)
    if overflow > 0:
        for evicted in terminal_order[:overflow]:
            if evicted in state.recent_terminal_request_ids:
                state.recent_terminal_request_ids.remove(evicted)
        del state.recent_terminal_request_order[:overflow]
    return state


def apply_permit_request(state: UserQuotaState, request: PermitRequest) -> bool:
    """Idempotently append a valid request to the FIFO queue.

    Invalid scope/cost messages are ignored instead of raising from a Signal
    handler and poisoning every subsequent Workflow Task.
    """

    state.processed_message_count += 1
    request_id = request.request_id
    if (
        not request_id
        or request.cost <= 0
        or request.quota_scope != state.quota_scope
        or request_id in state.pending
        or request_id in state.reservations
        or request_id in state.recent_terminal_request_ids
    ):
        return False
    state.pending[request_id] = request
    state.pending_order.append(request_id)
    return True


def apply_cancel_permit(
    state: UserQuotaState,
    cancellation: CancelPermit,
    *,
    dedup_window_size: int | None = None,
) -> bool:
    """Cancel a pending or granted request without refunding quota."""

    state.processed_message_count += 1
    request_id = cancellation.request_id
    if request_id in state.recent_terminal_request_ids:
        return False

    pending = state.pending.pop(request_id, None)
    if pending is not None:
        state.pending_order = [
            queued_id for queued_id in state.pending_order if queued_id != request_id
        ]
        _mark_terminal(
            state,
            request_id,
            dedup_window_size=dedup_window_size,
        )
        return True

    reservation = state.reservations.pop(request_id, None)
    if reservation is None:
        # Remember cancel-before-request races.  A later delivery of the same
        # request must not resurrect work that its caller already abandoned.
        if request_id:
            _mark_terminal(
                state,
                request_id,
                dedup_window_size=dedup_window_size,
            )
            return True
        return False

    reservation.status = PermitStatus.CANCELED
    state.in_flight = max(0, state.in_flight - 1)
    _mark_terminal(
        state,
        request_id,
        dedup_window_size=dedup_window_size,
    )
    # Conservative V1 semantics deliberately leave ``remaining`` unchanged:
    # the grant may already have caused an ambiguous provider call.
    return True


def apply_cancel_generation_permits(
    state: UserQuotaState,
    command: CancelGenerationPermits,
    *,
    dedup_window_size: int | None = None,
) -> int:
    """Invalidate every request owned by one fenced store generation.

    The batch is sorted by request ID because all cancellations share one
    Signal timestamp and therefore have no meaningful relative arrival order.
    Granted costs remain consumed under conservative V1 semantics.
    """

    state.processed_message_count += 1
    pending_ids = {
        request_id
        for request_id, request in state.pending.items()
        if request.store_key == command.store_key
        and request.lifecycle_generation == command.lifecycle_generation
    }
    reservation_ids = {
        request_id
        for request_id, reservation in state.reservations.items()
        if reservation.store_key == command.store_key
        and reservation.lifecycle_generation == command.lifecycle_generation
    }
    canceled_ids = pending_ids | reservation_ids
    if not canceled_ids:
        return 0

    for request_id in sorted(canceled_ids):
        state.pending.pop(request_id, None)
        reservation = state.reservations.pop(request_id, None)
        if reservation is not None:
            reservation.status = PermitStatus.CANCELED
            state.in_flight = max(0, state.in_flight - 1)
        _mark_terminal(
            state,
            request_id,
            dedup_window_size=dedup_window_size,
        )
    state.pending_order = [
        request_id for request_id in state.pending_order if request_id not in canceled_ids
    ]
    return len(canceled_ids)


def apply_permit_completed(
    state: UserQuotaState,
    completion: PermitCompleted,
    *,
    dedup_window_size: int | None = None,
) -> bool:
    """Release one in-flight slot after an exact permit completion."""

    state.processed_message_count += 1
    if completion.request_id in state.recent_terminal_request_ids:
        return False
    reservation = state.reservations.get(completion.request_id)
    if reservation is None or reservation.permit_id != completion.permit_id:
        return False
    reservation.status = PermitStatus.COMPLETED
    del state.reservations[completion.request_id]
    state.in_flight = max(0, state.in_flight - 1)
    _mark_terminal(
        state,
        completion.request_id,
        dedup_window_size=dedup_window_size,
    )
    return True


def apply_quota_observation(
    state: UserQuotaState,
    observation: QuotaObservation,
    *,
    now: datetime,
) -> bool:
    """Apply provider quota headers or an exhaustion result conservatively."""

    state.processed_message_count += 1
    if observation.quota_scope != state.quota_scope:
        return False

    if observation.limit is not None:
        state.configured_limit = max(0, observation.limit)

    # Header responses from concurrently running calls may arrive out of
    # order.  Taking the lower balance never manufactures permits.
    if observation.remaining is not None:
        observed_remaining = max(0, observation.remaining)
        if state.remaining is None:
            state.remaining = observed_remaining
        else:
            state.remaining = min(state.remaining, observed_remaining)

    if observation.reset_at is not None:
        state.reset_at = _later(state.reset_at, observation.reset_at)

    if observation.exhausted:
        state.remaining = 0
        retry_at: datetime | None = None
        if observation.retry_after_seconds is not None:
            retry_at = now + timedelta(seconds=max(0.0, observation.retry_after_seconds))
        elif observation.reset_at is None:
            # A headerless 429 must not park the shared scope forever.  After a
            # conservative delay, unknown remaining enters probe mode bounded
            # by max_in_flight.
            retry_at = now + DEFAULT_UNKNOWN_RESET_PROBE_DELAY
        # Waiting for the later signal is conservative when providers return
        # both Retry-After and a reset timestamp with slightly different values.
        state.blocked_until = _later(
            state.blocked_until,
            _later(observation.reset_at, retry_at),
        )
    return True


def apply_disable_scope(state: UserQuotaState, _command: DisableQuotaScope) -> bool:
    """Permanently stop new grants while leaving requests cancellable."""

    state.processed_message_count += 1
    if state.disabled:
        return False
    state.disabled = True
    return True


def advance_due_quota_window(state: UserQuotaState, *, now: datetime) -> bool:
    """Advance one reset boundary when its durable timer has fired."""

    block_due = state.blocked_until is not None and _not_after(state.blocked_until, now)
    reset_due = state.reset_at is not None and _not_after(state.reset_at, now)
    if block_due and state.reset_at is not None and not reset_due:
        # Defensive normalization for carried/manual state: exhaustion cannot
        # unblock before its known reset boundary.
        state.blocked_until = state.reset_at
        block_due = False
    if not block_due and not reset_due:
        return False

    boundary = _later(
        state.blocked_until if block_due else None,
        state.reset_at if reset_due else None,
    )
    state.quota_window_id = permit_id(
        state.quota_window_id,
        _as_utc(boundary or now).isoformat(),
    ).removeprefix("permit/")
    if block_due:
        state.blocked_until = None
    if reset_due:
        state.reset_at = None
    state.remaining = state.configured_limit
    return True


def quota_is_blocked(state: UserQuotaState, *, now: datetime) -> bool:
    """Return whether no grant may be made at the supplied workflow time."""

    if state.disabled:
        return True
    return bool(state.blocked_until is not None and not _not_after(state.blocked_until, now))


def grant_available_permits(
    state: UserQuotaState,
    *,
    now: datetime,
) -> list[PermitGrant]:
    """Reserve and return the FIFO prefix allowed by balance and concurrency."""

    advance_due_quota_window(state, now=now)
    if quota_is_blocked(state, now=now) or state.max_in_flight <= 0:
        return []

    grants: list[PermitGrant] = []
    while state.pending_order and state.in_flight < state.max_in_flight:
        request_id = state.pending_order[0]
        request = state.pending.get(request_id)
        if request is None:
            state.pending_order.pop(0)
            continue
        if state.remaining is not None and state.remaining < request.cost:
            # Strict FIFO: a high-cost head request is not bypassed.
            break

        state.pending_order.pop(0)
        del state.pending[request_id]
        deterministic_permit_id = permit_id(
            request_id,
            state.quota_window_id,
        )
        reservation = PermitReservation(
            request_id=request_id,
            permit_id=deterministic_permit_id,
            requester_workflow_id=request.requester_workflow_id,
            cost=request.cost,
            quota_window_id=state.quota_window_id,
            granted_at=now,
            status=PermitStatus.GRANTED,
            store_key=request.store_key,
            lifecycle_generation=request.lifecycle_generation,
        )
        state.reservations[request_id] = reservation
        state.in_flight += 1
        if state.remaining is not None:
            state.remaining -= request.cost
        grants.append(
            PermitGrant(
                request_id=request_id,
                permit_id=deterministic_permit_id,
                quota_scope=state.quota_scope,
                quota_window_id=state.quota_window_id,
                cost=request.cost,
            )
        )
    return grants


def next_quota_timer_delay(
    state: UserQuotaState,
    *,
    now: datetime,
) -> timedelta | None:
    """Return the next reset/block timer delay, if one is scheduled."""

    candidates = [
        candidate for candidate in (state.blocked_until, state.reset_at) if candidate is not None
    ]
    if not candidates:
        return None
    deadline = min(candidates, key=_as_utc)
    delay = _as_utc(deadline) - _as_utc(now)
    return max(delay, timedelta(0))


def quota_snapshot(state: UserQuotaState) -> QuotaSnapshot:
    """Build the compact, bounded query result required by the protocol."""

    return QuotaSnapshot(
        quota_scope=state.quota_scope,
        configured_limit=state.configured_limit,
        remaining=state.remaining,
        reset_at=state.reset_at,
        blocked_until=state.blocked_until,
        max_in_flight=state.max_in_flight,
        in_flight=state.in_flight,
        disabled=state.disabled,
        pending_count=len(state.pending),
        reservation_count=len(state.reservations),
        quota_window_id=state.quota_window_id,
    )


@_workflow_defn(name="UserQuotaWorkflow")
class UserQuotaWorkflow:
    """One long-lived quota coordinator per external credential scope."""

    def __init__(self) -> None:
        self._state: UserQuotaState | None = None
        self._dirty = True
        self._continue_after_window = False
        self._deferred_messages: list[tuple[str, Any, datetime | None]] = []

    def _apply_or_defer(
        self,
        message_type: str,
        message: Any,
        *,
        now: datetime | None = None,
    ) -> None:
        if self._state is None:
            self._deferred_messages.append((message_type, message, now))
            self._dirty = True
            return
        self._apply_message(message_type, message, now=now)

    def _apply_message(
        self,
        message_type: str,
        message: Any,
        *,
        now: datetime | None = None,
    ) -> None:
        if self._state is None:  # Defensive guard for direct unit invocation.
            raise RuntimeError("quota workflow state is not initialized")
        if message_type == "request":
            accepted = apply_permit_request(self._state, message)
            if workflow is not None:
                workflow_metrics(
                    provider=self._state.quota_scope.provider,
                    quota_class=self._state.quota_scope.quota_class,
                    work_class=message.work_class,
                ).increment(
                    QUOTA_REQUESTS,
                    attributes={"status": "accepted" if accepted else "ignored"},
                )
        elif message_type == "cancel":
            apply_cancel_permit(self._state, message)
        elif message_type == "cancel_generation":
            apply_cancel_generation_permits(self._state, message)
        elif message_type == "complete":
            apply_permit_completed(self._state, message)
        elif message_type == "observe":
            if now is None:
                raise RuntimeError("quota observation requires workflow time")
            apply_quota_observation(self._state, message, now=now)
        elif message_type == "disable":
            apply_disable_scope(self._state, message)
        else:  # pragma: no cover - all call sites use constants above
            raise ValueError(f"unknown quota message type: {message_type}")
        self._dirty = True

    @_workflow_signal(name="request_permit")
    def request_permit(self, request: PermitRequest) -> None:
        self._apply_or_defer("request", request)

    @_workflow_signal(name="cancel_permit")
    def cancel_permit(self, cancellation: CancelPermit) -> None:
        self._apply_or_defer("cancel", cancellation)

    @_workflow_signal(name="cancel_generation")
    def cancel_generation(self, command: CancelGenerationPermits) -> None:
        self._apply_or_defer("cancel_generation", command)

    @_workflow_signal(name="permit_completed")
    def permit_completed(self, completion: PermitCompleted) -> None:
        self._apply_or_defer("complete", completion)

    @_workflow_signal(name="observe_quota")
    def observe_quota(self, observation: QuotaObservation) -> None:
        if workflow is None:  # pragma: no cover - direct tests use pure logic
            raise RuntimeError("Temporal SDK is required to handle signals")
        self._apply_or_defer("observe", observation, now=workflow.now())

    @_workflow_signal(name="disable_scope")
    def disable_scope(self, command: DisableQuotaScope) -> None:
        self._apply_or_defer("disable", command)

    @_workflow_query(name="get_quota_state")
    def get_quota_state(self) -> QuotaSnapshot:
        if self._state is None:
            raise RuntimeError("quota workflow state is not initialized")
        return quota_snapshot(self._state)

    def _initialize(self, initial_state: UserQuotaState) -> None:
        self._state = compact_quota_state(initial_state)
        deferred, self._deferred_messages = self._deferred_messages, []
        for message_type, message, message_time in deferred:
            self._apply_message(message_type, message, now=message_time)
        self._dirty = True

    def _should_continue_as_new(self) -> bool:
        if workflow is None or self._state is None:  # pragma: no cover
            return False
        return (
            self._continue_after_window
            or self._state.processed_message_count >= self._state.continue_as_new_message_count
            or workflow.info().is_continue_as_new_suggested()
        )

    async def _deliver_grant(self, grant: PermitGrant) -> None:
        if workflow is None or self._state is None:  # pragma: no cover
            raise RuntimeError("Temporal SDK is required to deliver a grant")
        reservation = self._state.reservations.get(grant.request_id)
        if reservation is None:
            return
        handle = workflow.get_external_workflow_handle(reservation.requester_workflow_id)
        try:
            await handle.signal(QUOTA_GRANTED_SIGNAL, grant)
        except TemporalError as exc:
            # A known delivery failure cannot consume an in-flight slot forever.
            # Cancellation still does not refund the already reserved balance.
            apply_cancel_permit(
                self._state,
                CancelPermit(
                    request_id=grant.request_id,
                    reason="grant signal delivery failed",
                ),
            )
            self._dirty = True
            workflow_metrics(
                provider=self._state.quota_scope.provider,
                quota_class=self._state.quota_scope.quota_class,
            ).increment(QUOTA_GRANT_SIGNAL_FAILURES)
            workflow.logger.warning(
                "Failed to deliver quota grant request_id=%s requester=%s: %s",
                grant.request_id,
                reservation.requester_workflow_id,
                type(exc).__name__,
            )

    @_workflow_run
    async def run(self, initial_state: UserQuotaState) -> None:
        if workflow is None:  # pragma: no cover - pure logic remains usable
            raise RuntimeError("Temporal SDK is required to run the workflow")
        self._initialize(initial_state)

        while True:
            assert self._state is not None
            now = workflow.now()
            pending_at_grant = dict(self._state.pending)
            previous_window_id = self._state.quota_window_id
            grants = grant_available_permits(self._state, now=now)
            if self._state.quota_window_id != previous_window_id:
                self._continue_after_window = True

            metrics = workflow_metrics(
                provider=self._state.quota_scope.provider,
                quota_class=self._state.quota_scope.quota_class,
            )
            if grants:
                metrics.increment(QUOTA_GRANTS, len(grants))
            for grant in grants:
                request = pending_at_grant.get(grant.request_id)
                if request is not None and request.requested_at is not None:
                    wait_ms = max(
                        0,
                        int((_as_utc(now) - _as_utc(request.requested_at)).total_seconds() * 1_000),
                    )
                    metrics.histogram(
                        QUOTA_WAIT_DURATION,
                        wait_ms,
                        attributes={"work_class": request.work_class},
                        unit="ms",
                    )
            metrics.gauge(QUOTA_PENDING, len(self._state.pending))
            metrics.gauge(QUOTA_IN_FLIGHT, self._state.in_flight)
            metrics.gauge(
                QUOTA_BLOCKED,
                int(quota_is_blocked(self._state, now=now)),
            )

            # Clear before awaiting delivery so a concurrently handled Signal
            # cannot be overwritten and lost.
            self._dirty = False
            for grant in grants:
                await self._deliver_grant(grant)

            if self._should_continue_as_new():
                compact_quota_state(self._state)
                self._state.processed_message_count = 0
                workflow.continue_as_new(self._state)

            timeout = next_quota_timer_delay(self._state, now=workflow.now())
            try:
                if timeout is None:
                    await workflow.wait_condition(
                        lambda: self._dirty or self._should_continue_as_new()
                    )
                else:
                    await workflow.wait_condition(
                        lambda: self._dirty or self._should_continue_as_new(),
                        timeout=timeout,
                        timeout_summary="user quota reset",
                    )
            except TimeoutError:
                # Durable timer fired.  The next loop advances the quota window
                # and grants only up to ``max_in_flight``.
                self._dirty = True


__all__ = [
    "DEFAULT_CONTINUE_AS_NEW_MESSAGE_COUNT",
    "DEFAULT_DEDUP_WINDOW_SIZE",
    "DEFAULT_UNKNOWN_RESET_PROBE_DELAY",
    "QUOTA_GRANTED_SIGNAL",
    "UserQuotaWorkflow",
    "advance_due_quota_window",
    "apply_cancel_generation_permits",
    "apply_cancel_permit",
    "apply_disable_scope",
    "apply_permit_completed",
    "apply_permit_request",
    "apply_quota_observation",
    "compact_quota_state",
    "grant_available_permits",
    "next_quota_timer_delay",
    "quota_is_blocked",
    "quota_snapshot",
]
