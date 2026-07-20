"""Demo controls layered over the real read-only Google Drive provider."""

from __future__ import annotations

from retrieval.embeddings import EmbeddingProvider
from retrieval.temporal.activities.provider_api import (
    ActiveUsersPage,
    FetchResourcePageRequest,
    ListActiveUsersRequest,
    ProviderPreflightRequest,
    ProviderPreflightResult,
    ProviderQuotaExhausted,
    ResourcePageManifest,
)

from .store import DemoStateStore


class DemoGoogleDriveProvider:
    """Inject exactly one durable throttle, then delegate to the real connector."""

    def __init__(
        self,
        delegate,
        state_store: DemoStateStore,
        *,
        embedding_provider: EmbeddingProvider | None = None,
    ) -> None:
        self._delegate = delegate
        self._state_store = state_store
        self._embedding_provider = embedding_provider

    async def preflight(self, request: ProviderPreflightRequest) -> ProviderPreflightResult:
        if self._embedding_provider is not None:
            vectors = await self._embedding_provider.embed(
                ("retrieval demo worker readiness",),
                query=False,
            )
            if len(vectors) != 1:
                raise RuntimeError("embedding readiness probe returned the wrong vector count")
        return await self._delegate.preflight(request)

    async def list_active_users(self, request: ListActiveUsersRequest) -> ActiveUsersPage:
        await self._before_operation(
            request.store_key,
            request_id=request.request_id,
            operation="list_active_users",
        )
        return await self._delegate.list_active_users(request)

    async def fetch_resource_page(
        self,
        request: FetchResourcePageRequest,
    ) -> ResourcePageManifest:
        await self._before_operation(
            request.store_key,
            request_id=request.request_id,
            operation="fetch_resource_page",
        )
        return await self._delegate.fetch_resource_page(request)

    async def _before_operation(self, store_key: str, *, request_id: str, operation: str) -> None:
        run = await self._state_store.get_run_by_store(store_key)
        if operation == "list_active_users":
            decision = await self._state_store.consume_quota_once(
                run.run_id,
                request_id=request_id,
                operation=operation,
            )
            if decision.injected:
                raise ProviderQuotaExhausted(
                    limit=2,
                    remaining=0,
                    retry_after_seconds=decision.retry_after_seconds,
                )
            await self._state_store.complete_quota_wait(run.run_id, operation=operation)

    async def aclose(self) -> None:
        await self._delegate.aclose()


__all__ = ["DemoGoogleDriveProvider"]
