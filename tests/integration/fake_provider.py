"""Deterministic in-memory provider used by integration and load scenarios."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from time import monotonic

from retrieval.temporal.activities.provider_api import (
    ActiveUsersPage,
    FetchResourcePageRequest,
    InvalidCredentialsError,
    ListActiveUsersRequest,
    ProviderQuotaExhausted,
    ResourcePageManifest,
    UserDescriptor,
)
from retrieval.temporal.models.documents import DocumentRef


class FakeOutcomeKind(StrEnum):
    SUCCESS = "success"
    QUOTA_EXHAUSTED = "quota_exhausted"
    AUTH_FAILURE = "auth_failure"


@dataclass(frozen=True)
class FakeProviderOutcome:
    """One queued provider response.

    ``wait_for_release`` lets a test hold a provider call in flight without relying on a
    long real-time sleep.  Call :meth:`FakeProviderGateway.release` to resume it.
    """

    kind: FakeOutcomeKind = FakeOutcomeKind.SUCCESS
    delay_seconds: float = 0.0
    wait_for_release: bool = False
    users: tuple[UserDescriptor, ...] = ()
    documents: tuple[DocumentRef, ...] = ()
    deleted_document_keys: tuple[str, ...] = ()
    next_cursor: str | None = None
    limit: int | None = None
    remaining: int | None = None
    reset_at: datetime | None = None
    retry_after_seconds: float | None = None

    @classmethod
    def quota_exhausted(
        cls,
        *,
        limit: int | None = None,
        remaining: int | None = 0,
        reset_at: datetime | None = None,
        retry_after_seconds: float | None = None,
        delay_seconds: float = 0.0,
    ) -> FakeProviderOutcome:
        return cls(
            kind=FakeOutcomeKind.QUOTA_EXHAUSTED,
            delay_seconds=delay_seconds,
            limit=limit,
            remaining=remaining,
            reset_at=reset_at,
            retry_after_seconds=retry_after_seconds,
        )

    @classmethod
    def auth_failure(cls, *, delay_seconds: float = 0.0) -> FakeProviderOutcome:
        return cls(
            kind=FakeOutcomeKind.AUTH_FAILURE,
            delay_seconds=delay_seconds,
        )


@dataclass
class FakeProviderCall:
    operation: str
    request_id: str
    credential_key: str | None
    started_at: float
    finished_at: float | None = None
    cancelled: bool = False


class FakeProviderGateway:
    """Scriptable implementation of the production ``ProviderGateway`` protocol."""

    def __init__(self) -> None:
        self._outcomes: dict[str, deque[FakeProviderOutcome]] = defaultdict(deque)
        self._started: dict[str, asyncio.Event] = {}
        self._releases: dict[str, asyncio.Event] = {}
        self._calls: list[FakeProviderCall] = []

    @property
    def calls(self) -> tuple[FakeProviderCall, ...]:
        return tuple(self._calls)

    def queue_outcomes(self, request_id: str, *outcomes: FakeProviderOutcome) -> None:
        if not outcomes:
            raise ValueError("at least one fake provider outcome is required")
        self._outcomes[request_id].extend(outcomes)
        self._started.setdefault(request_id, asyncio.Event())
        self._releases.setdefault(request_id, asyncio.Event())

    async def wait_until_started(self, request_id: str, *, wait_seconds: float = 2.0) -> None:
        event = self._started.setdefault(request_id, asyncio.Event())
        await asyncio.wait_for(event.wait(), timeout=wait_seconds)

    def release(self, request_id: str) -> None:
        self._releases.setdefault(request_id, asyncio.Event()).set()

    def calls_for_credential(self, credential_key: str) -> tuple[FakeProviderCall, ...]:
        return tuple(call for call in self._calls if call.credential_key == credential_key)

    def _next_outcome(self, request_id: str) -> FakeProviderOutcome:
        queued = self._outcomes.get(request_id)
        return queued.popleft() if queued else FakeProviderOutcome()

    async def _run_outcome(
        self,
        operation: str,
        request: ListActiveUsersRequest | FetchResourcePageRequest,
    ) -> FakeProviderOutcome:
        scope = request.quota_scope
        call = FakeProviderCall(
            operation=operation,
            request_id=request.request_id,
            credential_key=scope.credential_key if scope is not None else None,
            started_at=monotonic(),
        )
        self._calls.append(call)
        self._started.setdefault(request.request_id, asyncio.Event()).set()
        outcome = self._next_outcome(request.request_id)
        try:
            if outcome.wait_for_release:
                await self._releases.setdefault(request.request_id, asyncio.Event()).wait()
            if outcome.delay_seconds:
                await asyncio.sleep(outcome.delay_seconds)
            if outcome.kind == FakeOutcomeKind.QUOTA_EXHAUSTED:
                raise ProviderQuotaExhausted(
                    limit=outcome.limit,
                    remaining=outcome.remaining,
                    reset_at=outcome.reset_at,
                    retry_after_seconds=outcome.retry_after_seconds,
                )
            if outcome.kind == FakeOutcomeKind.AUTH_FAILURE:
                raise InvalidCredentialsError("fake provider rejected credential")
            return outcome
        except asyncio.CancelledError:
            call.cancelled = True
            raise
        finally:
            call.finished_at = monotonic()

    async def list_active_users(self, request: ListActiveUsersRequest) -> ActiveUsersPage:
        outcome = await self._run_outcome("list_active_users", request)
        return ActiveUsersPage(
            request_id=request.request_id,
            users=outcome.users,
            next_cursor=outcome.next_cursor,
        )

    async def fetch_resource_page(self, request: FetchResourcePageRequest) -> ResourcePageManifest:
        outcome = await self._run_outcome("fetch_resource_page", request)
        return ResourcePageManifest(
            request_id=request.request_id,
            page_key=request.cursor or "initial",
            documents=outcome.documents,
            deleted_document_keys=outcome.deleted_document_keys,
            next_cursor=outcome.next_cursor,
        )


__all__ = [
    "FakeOutcomeKind",
    "FakeProviderCall",
    "FakeProviderGateway",
    "FakeProviderOutcome",
]
