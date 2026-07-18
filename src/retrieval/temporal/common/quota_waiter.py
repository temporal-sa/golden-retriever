"""Reusable workflow-side quota permit waiting.

The Signal-with-Start Activity invoked here is deliberately short: it only
asks the Temporal service to start/reuse the shared quota coordinator and
submit a request.  Availability is then awaited with ``workflow.wait_condition``
inside the caller's own durable state.

Workflow classes can inherit ``QuotaWaiterMixin`` to get the common
``quota_granted`` Signal contract and call ``request_quota_permit`` before a
provider-facing Activity.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import Any, TypeVar

from retrieval.temporal.common.ids import user_quota_workflow_id
from retrieval.temporal.models.quota import (
    CancelPermit,
    PermitCompleted,
    PermitDenied,
    PermitGrant,
    PermitRequest,
    QuotaObservation,
    QuotaScope,
)

try:  # Keep grant-inbox logic usable without the Temporal dependency.
    from temporalio import workflow
    from temporalio.common import RetryPolicy
    from temporalio.workflow import ActivityCancellationType
except ImportError:  # pragma: no cover - only for pure unit-test installs
    ActivityCancellationType = None  # type: ignore[assignment,misc]
    workflow = None  # type: ignore[assignment]
    RetryPolicy = None  # type: ignore[assignment,misc]


SIGNAL_WITH_START_USER_QUOTA_ACTIVITY = "signal_with_start_user_quota"
QUOTA_GRANTED_SIGNAL = "quota_granted"
QUOTA_DENIED_SIGNAL = "quota_denied"
REQUEST_PERMIT_SIGNAL = "request_permit"
CANCEL_PERMIT_SIGNAL = "cancel_permit"
PERMIT_COMPLETED_SIGNAL = "permit_completed"
OBSERVE_QUOTA_SIGNAL = "observe_quota"
DEFAULT_GRANT_DEDUP_SIZE = 2_048

_F = TypeVar("_F", bound=Callable[..., Any])


def _identity_decorator(*_args: Any, **_kwargs: Any) -> Callable[[_F], _F]:
    return lambda decorated: decorated


_workflow_signal = workflow.signal if workflow is not None else _identity_decorator


class QuotaPermitDeniedError(RuntimeError):
    """The shared coordinator explicitly refused to queue a permit request."""

    def __init__(self, denial: PermitDenied) -> None:
        super().__init__(denial.reason)
        self.denial = denial


class QuotaWaiterMixin:
    """Mixin implementing the caller half of the durable permit protocol.

    State is initialized lazily so this mixin remains safe in workflow classes
    that already have an ``__init__`` and do not call ``super()``.
    """

    def _ensure_quota_waiter_state(self) -> None:
        if not hasattr(self, "_quota_expected_requests"):
            self._quota_expected_requests: dict[str, PermitRequest] = {}
        if not hasattr(self, "_quota_grants"):
            self._quota_grants: dict[str, PermitGrant] = {}
        if not hasattr(self, "_quota_denials"):
            self._quota_denials: dict[str, PermitDenied] = {}
        if not hasattr(self, "_quota_terminal_request_ids"):
            self._quota_terminal_request_ids: set[str] = set()
        if not hasattr(self, "_quota_terminal_order"):
            self._quota_terminal_order: list[str] = []

    def _mark_quota_request_terminal(self, request_id: str) -> None:
        self._ensure_quota_waiter_state()
        if request_id in self._quota_terminal_request_ids:
            return
        self._quota_terminal_request_ids.add(request_id)
        self._quota_terminal_order.append(request_id)
        overflow = len(self._quota_terminal_order) - DEFAULT_GRANT_DEDUP_SIZE
        if overflow > 0:
            for evicted in self._quota_terminal_order[:overflow]:
                self._quota_terminal_request_ids.discard(evicted)
            del self._quota_terminal_order[:overflow]

    @_workflow_signal(name=QUOTA_GRANTED_SIGNAL)
    def quota_granted(self, grant: PermitGrant) -> None:
        """Record the first exact grant and ignore wrong/duplicate Signals."""

        self._ensure_quota_waiter_state()
        expected = self._quota_expected_requests.get(grant.request_id)
        if (
            expected is None
            or grant.request_id in self._quota_terminal_request_ids
            or grant.request_id in self._quota_grants
            or grant.quota_scope != expected.quota_scope
            or grant.cost != expected.cost
        ):
            return
        self._quota_grants[grant.request_id] = grant

    @_workflow_signal(name=QUOTA_DENIED_SIGNAL)
    def quota_denied(self, denial: PermitDenied) -> None:
        """Record an exact denial so a requester never waits indefinitely."""

        self._ensure_quota_waiter_state()
        expected = self._quota_expected_requests.get(denial.request_id)
        if (
            expected is None
            or denial.request_id in self._quota_terminal_request_ids
            or denial.request_id in self._quota_grants
            or denial.request_id in self._quota_denials
            or denial.quota_scope != expected.quota_scope
        ):
            return
        self._quota_denials[denial.request_id] = denial

    @staticmethod
    def quota_workflow_id_for_scope(scope: QuotaScope) -> str:
        """Resolve the same opaque coordinator ID used by the client Activity."""

        return user_quota_workflow_id(
            scope.provider,
            scope.credential_key,
            scope.quota_class,
        )

    async def _signal_quota_workflow(
        self,
        quota_workflow_id: str,
        signal_name: str,
        message: Any,
    ) -> None:
        if workflow is None:  # pragma: no cover - direct signal tests need no SDK
            raise RuntimeError("Temporal SDK is required to signal quota workflow")
        handle = workflow.get_external_workflow_handle(quota_workflow_id)
        await handle.signal(signal_name, message)

    async def _cancel_abandoned_request(
        self,
        *,
        request_id: str,
        quota_workflow_id: str,
        reason: str,
    ) -> None:
        """Best-effort cancellation that does not replace the caller error."""

        if workflow is None:  # pragma: no cover
            return
        cleanup_task = asyncio.create_task(
            self._signal_quota_workflow(
                quota_workflow_id,
                CANCEL_PERMIT_SIGNAL,
                CancelPermit(request_id=request_id, reason=reason),
            )
        )
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                # SDKs before 1.28 do not consistently clear Python 3.11+'s
                # task cancellation counter.  Consume repeated cancellation
                # requests here; the outer handler still re-raises its original
                # CancelledError after this cleanup completes.
                current_task = asyncio.current_task()
                if current_task is not None:
                    current_task.uncancel()
        try:
            cleanup_task.result()
        except (Exception, asyncio.CancelledError) as exc:
            # The request Activity may have failed before creating the shared
            # workflow.  Preserve the original cancellation/failure and leave
            # this diagnostic replay-safe.
            workflow.logger.warning(
                "Unable to cancel quota request request_id=%s: %s",
                request_id,
                type(exc).__name__,
            )

    async def request_quota_permit(
        self,
        request: PermitRequest,
        *,
        quota_workflow_id: str | None = None,
        signal_with_start_activity: str = SIGNAL_WITH_START_USER_QUOTA_ACTIVITY,
        start_to_close_timeout: timedelta = timedelta(seconds=10),
        schedule_to_close_timeout: timedelta = timedelta(seconds=30),
    ) -> PermitGrant:
        """Submit ``request`` and durably wait for its exact grant Signal.

        The expected-request entry is installed *before* scheduling the short
        Activity.  Therefore a grant delivered immediately after the server
        accepts Signal-with-Start is retained even if it arrives before this
        method reaches ``wait_condition``.
        """

        if workflow is None:  # pragma: no cover - inbox tests call the Signal
            raise RuntimeError("Temporal SDK is required to request a permit")
        if not request.request_id:
            raise ValueError("quota request_id must not be empty")
        if request.cost <= 0:
            raise ValueError("quota request cost must be positive")

        self._ensure_quota_waiter_state()
        request_id = request.request_id
        resolved_workflow_id = quota_workflow_id or self.quota_workflow_id_for_scope(
            request.quota_scope
        )

        if request_id in self._quota_expected_requests:
            raise RuntimeError(f"quota request is already waiting: {request_id}")
        if request_id in self._quota_terminal_request_ids:
            raise RuntimeError(f"quota request was already consumed: {request_id}")

        self._quota_expected_requests[request_id] = request
        try:
            retry_policy = RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=5),
                maximum_attempts=3,
            )
            await workflow.execute_activity(
                signal_with_start_activity,
                request,
                start_to_close_timeout=start_to_close_timeout,
                schedule_to_close_timeout=schedule_to_close_timeout,
                retry_policy=retry_policy,
                cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
            )
            await workflow.wait_condition(
                lambda: request_id in self._quota_grants or request_id in self._quota_denials
            )
            denial = self._quota_denials.pop(request_id, None)
            if denial is not None:
                self._mark_quota_request_terminal(request_id)
                raise QuotaPermitDeniedError(denial)
            grant = self._quota_grants.pop(request_id)
            self._mark_quota_request_terminal(request_id)
            return grant
        except QuotaPermitDeniedError:
            self._quota_grants.pop(request_id, None)
            raise
        except asyncio.CancelledError:
            current_task = asyncio.current_task()
            if current_task is not None:
                # Balance the cancellation being handled so SDK 1.27's
                # external-Signal future can make progress on Python 3.11+.
                # The original exception is explicitly re-raised below.
                current_task.uncancel()
            await self._cancel_abandoned_request(
                request_id=request_id,
                quota_workflow_id=resolved_workflow_id,
                reason="requester canceled while waiting for quota",
            )
            self._quota_grants.pop(request_id, None)
            self._quota_denials.pop(request_id, None)
            self._mark_quota_request_terminal(request_id)
            raise
        except Exception:
            # Activity completion can be ambiguous after transport failures.
            # Tombstoning the request prevents a leaked pending reservation.
            await self._cancel_abandoned_request(
                request_id=request_id,
                quota_workflow_id=resolved_workflow_id,
                reason="requester stopped waiting after request failure",
            )
            self._quota_grants.pop(request_id, None)
            self._quota_denials.pop(request_id, None)
            self._mark_quota_request_terminal(request_id)
            raise
        finally:
            self._quota_expected_requests.pop(request_id, None)

    # Readable alias for callers that prefer the spec's wait terminology.
    wait_for_quota_permit = request_quota_permit

    async def report_quota_observation(
        self,
        observation: QuotaObservation,
        *,
        quota_workflow_id: str | None = None,
    ) -> None:
        """Report response headers or exhaustion to the shared scope."""

        resolved_workflow_id = quota_workflow_id or self.quota_workflow_id_for_scope(
            observation.quota_scope
        )
        await self._signal_quota_workflow(
            resolved_workflow_id,
            OBSERVE_QUOTA_SIGNAL,
            observation,
        )

    async def complete_quota_permit(
        self,
        grant: PermitGrant,
        *,
        quota_workflow_id: str | None = None,
    ) -> None:
        """Release the concurrency reservation without refunding its cost."""

        resolved_workflow_id = quota_workflow_id or self.quota_workflow_id_for_scope(
            grant.quota_scope
        )
        await self._signal_quota_workflow(
            resolved_workflow_id,
            PERMIT_COMPLETED_SIGNAL,
            PermitCompleted(
                request_id=grant.request_id,
                permit_id=grant.permit_id,
            ),
        )


# Both names are exported because the helper is a mixin and a waiter; keeping a
# single implementation avoids subtly different Signal contracts.
QuotaPermitWaiterMixin = QuotaWaiterMixin


__all__ = [
    "CANCEL_PERMIT_SIGNAL",
    "DEFAULT_GRANT_DEDUP_SIZE",
    "OBSERVE_QUOTA_SIGNAL",
    "PERMIT_COMPLETED_SIGNAL",
    "QUOTA_DENIED_SIGNAL",
    "QUOTA_GRANTED_SIGNAL",
    "REQUEST_PERMIT_SIGNAL",
    "SIGNAL_WITH_START_USER_QUOTA_ACTIVITY",
    "QuotaPermitDeniedError",
    "QuotaPermitWaiterMixin",
    "QuotaWaiterMixin",
]
