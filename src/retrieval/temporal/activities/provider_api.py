"""Provider-facing Activity contracts.

Connector implementations live behind :class:`ProviderGateway`. Provider payload bodies
are staged by the gateway; only compact :class:`DocumentRef` values enter workflow history.
Quota exhaustion is returned as structured data so workflow code can block the shared
scope instead of retrying the Activity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from temporalio import activity
from temporalio.exceptions import ApplicationError

from retrieval.temporal.common.metrics import (
    PROVIDER_QUOTA_EXHAUSTED,
    PROVIDER_REQUESTS,
    activity_metrics,
)
from retrieval.temporal.models.documents import DocumentRef
from retrieval.temporal.models.quota import QuotaObservation, QuotaScope


@dataclass(frozen=True)
class UserDescriptor:
    user_key: str
    valid: bool = True


@dataclass(frozen=True)
class ListActiveUsersRequest:
    store_key: str
    lifecycle_generation: int
    cursor: str | None
    page_size: int
    request_id: str
    quota_scope: QuotaScope | None = None


@dataclass(frozen=True)
class ActiveUsersPage:
    request_id: str
    users: tuple[UserDescriptor, ...] = ()
    next_cursor: str | None = None
    observation: QuotaObservation | None = None
    quota_exhausted: bool = False


@dataclass(frozen=True)
class FetchResourcePageRequest:
    store_key: str
    lifecycle_generation: int
    sync_sequence: str
    user_key: str
    resource_key: str
    cursor: str | None
    page_size: int
    request_id: str
    quota_scope: QuotaScope | None = None


@dataclass(frozen=True)
class ResourcePageManifest:
    request_id: str
    page_key: str
    documents: tuple[DocumentRef, ...] = ()
    deleted_document_keys: tuple[str, ...] = ()
    next_cursor: str | None = None
    observation: QuotaObservation | None = None
    quota_exhausted: bool = False


@dataclass(frozen=True)
class ProviderPreflightRequest:
    request_id: str
    max_files: int = 100
    max_folders: int = 100
    page_size: int = 100
    provider_task_queue: str = "retrieval-provider-v2"


@dataclass(frozen=True)
class ProviderPreflightFile:
    document_key: str
    name: str
    mime_type: str
    modified_time: str
    source_uri: str | None
    searchable: bool
    held_for_demo: bool = False


@dataclass(frozen=True)
class ProviderPreflightResult:
    request_id: str
    provider: str
    root_folder_id: str | None
    files: tuple[ProviderPreflightFile, ...]
    folders_scanned: int
    truncated: bool = False


class ProviderQuotaExhausted(RuntimeError):
    def __init__(
        self,
        *,
        limit: int | None = None,
        remaining: int | None = 0,
        reset_at: datetime | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__("provider quota exhausted")
        self.limit = limit
        self.remaining = remaining
        self.reset_at = reset_at
        self.retry_after_seconds = retry_after_seconds


class InvalidCredentialsError(RuntimeError):
    pass


class ProviderRequestError(RuntimeError):
    """A provider rejected a request that retrying cannot repair."""

    def __init__(self, message: str, *, error_type: str = "ProviderRequestRejected") -> None:
        super().__init__(message)
        self.error_type = error_type


class ProviderGateway(Protocol):
    async def list_active_users(self, request: ListActiveUsersRequest) -> ActiveUsersPage: ...

    async def fetch_resource_page(
        self, request: FetchResourcePageRequest
    ) -> ResourcePageManifest: ...

    async def preflight(self, request: ProviderPreflightRequest) -> ProviderPreflightResult: ...


class EmptyProviderGateway:
    """Safe local adapter that produces no work; production must inject a connector."""

    async def list_active_users(self, request: ListActiveUsersRequest) -> ActiveUsersPage:
        return ActiveUsersPage(request_id=request.request_id)

    async def fetch_resource_page(self, request: FetchResourcePageRequest) -> ResourcePageManifest:
        return ResourcePageManifest(
            request_id=request.request_id,
            page_key=request.cursor or "initial",
        )

    async def preflight(self, request: ProviderPreflightRequest) -> ProviderPreflightResult:
        return ProviderPreflightResult(
            request_id=request.request_id,
            provider="empty",
            root_folder_id=None,
            files=(),
            folders_scanned=0,
        )


class ProviderActivities:
    def __init__(self, gateway: ProviderGateway) -> None:
        self._gateway = gateway

    @activity.defn(name="provider_list_active_users")
    async def list_active_users(self, request: ListActiveUsersRequest) -> ActiveUsersPage:
        metrics = self._metrics(request.quota_scope, "list_active_users")
        try:
            result = await self._gateway.list_active_users(request)
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "succeeded"})
            return result
        except ProviderQuotaExhausted as exc:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "quota_exhausted"})
            metrics.increment(PROVIDER_QUOTA_EXHAUSTED)
            if request.quota_scope is None:
                raise ApplicationError(
                    "provider quota exhaustion requires a configured quota scope",
                    type="ProviderQuotaExhausted",
                    non_retryable=True,
                ) from exc
            return ActiveUsersPage(
                request_id=request.request_id,
                observation=self._observation(request.quota_scope, request.request_id, exc),
                quota_exhausted=True,
            )
        except InvalidCredentialsError as exc:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "invalid_credentials"})
            raise ApplicationError(str(exc), type="InvalidCredentials", non_retryable=True) from exc
        except ProviderRequestError as exc:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "rejected"})
            raise ApplicationError(str(exc), type=exc.error_type, non_retryable=True) from exc
        except Exception:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "failed"})
            raise

    @activity.defn(name="provider_fetch_resource_page")
    async def fetch_resource_page(self, request: FetchResourcePageRequest) -> ResourcePageManifest:
        metrics = self._metrics(request.quota_scope, "fetch_resource_page")
        try:
            result = await self._gateway.fetch_resource_page(request)
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "succeeded"})
            return result
        except ProviderQuotaExhausted as exc:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "quota_exhausted"})
            metrics.increment(PROVIDER_QUOTA_EXHAUSTED)
            if request.quota_scope is None:
                raise ApplicationError(
                    "provider quota exhaustion requires a configured quota scope",
                    type="ProviderQuotaExhausted",
                    non_retryable=True,
                ) from exc
            return ResourcePageManifest(
                request_id=request.request_id,
                page_key=request.cursor or "initial",
                observation=self._observation(request.quota_scope, request.request_id, exc),
                quota_exhausted=True,
            )
        except InvalidCredentialsError as exc:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "invalid_credentials"})
            raise ApplicationError(str(exc), type="InvalidCredentials", non_retryable=True) from exc
        except ProviderRequestError as exc:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "rejected"})
            raise ApplicationError(str(exc), type=exc.error_type, non_retryable=True) from exc
        except Exception:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "failed"})
            raise

    @activity.defn(name="provider_preflight")
    async def preflight(self, request: ProviderPreflightRequest) -> ProviderPreflightResult:
        metrics = self._metrics(None, "preflight")
        try:
            result = await self._gateway.preflight(request)
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "succeeded"})
            return result
        except ProviderQuotaExhausted as exc:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "quota_exhausted"})
            metrics.increment(PROVIDER_QUOTA_EXHAUSTED)
            raise ApplicationError(
                "provider quota exhausted during preflight",
                type="ProviderQuotaExhausted",
            ) from exc
        except InvalidCredentialsError as exc:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "invalid_credentials"})
            raise ApplicationError(str(exc), type="InvalidCredentials", non_retryable=True) from exc
        except ProviderRequestError as exc:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "rejected"})
            raise ApplicationError(str(exc), type=exc.error_type, non_retryable=True) from exc
        except Exception:
            metrics.increment(PROVIDER_REQUESTS, attributes={"status": "failed"})
            raise

    @staticmethod
    def _metrics(scope: QuotaScope | None, operation: str):
        return activity_metrics(
            provider=scope.provider if scope is not None else "unmetered",
            quota_class=scope.quota_class if scope is not None else "unmetered",
            operation=operation,
        )

    @staticmethod
    def _observation(
        scope: QuotaScope | None,
        request_id: str,
        exc: ProviderQuotaExhausted,
    ) -> QuotaObservation | None:
        if scope is None:
            return None
        return QuotaObservation(
            quota_scope=scope,
            request_id=request_id,
            limit=exc.limit,
            remaining=exc.remaining,
            reset_at=exc.reset_at,
            retry_after_seconds=exc.retry_after_seconds,
            exhausted=True,
        )
