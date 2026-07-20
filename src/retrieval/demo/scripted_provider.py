"""Cross-process deterministic provider for the versioned Northstar scenario."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable

from retrieval.temporal.activities.provider_api import (
    ActiveUsersPage,
    FetchResourcePageRequest,
    ListActiveUsersRequest,
    ProviderPreflightFile,
    ProviderPreflightRequest,
    ProviderPreflightResult,
    ProviderQuotaExhausted,
    ResourcePageManifest,
    UserDescriptor,
)

from .config import DemoConfig
from .fixtures import NorthstarScenario, load_northstar_scenario
from .store import DemoStateStore, PostgresDemoStateStore


class ScriptedNorthstarProvider:
    """Stable provider pages with one durable, atomic quota injection."""

    def __init__(
        self,
        scenario: NorthstarScenario,
        state_store: DemoStateStore,
        *,
        close_callback: Callable[[], Awaitable[None] | None] | None = None,
    ) -> None:
        self._scenario = scenario
        self._state_store = state_store
        self._close_callback = close_callback

    async def list_active_users(self, request: ListActiveUsersRequest) -> ActiveUsersPage:
        await self._before_operation(
            request.store_key,
            request_id=request.request_id,
            operation="list_active_users",
        )
        users = () if request.cursor is not None else (UserDescriptor(self._scenario.user_key),)
        return ActiveUsersPage(request_id=request.request_id, users=users)

    async def fetch_resource_page(self, request: FetchResourcePageRequest) -> ResourcePageManifest:
        await self._before_operation(
            request.store_key,
            request_id=request.request_id,
            operation="fetch_resource_page",
        )
        valid = (
            request.user_key == self._scenario.user_key
            and request.resource_key == self._scenario.resource_key
            and request.cursor is None
        )
        return ResourcePageManifest(
            request_id=request.request_id,
            page_key="northstar-files-v1",
            documents=self._scenario.references if valid else (),
        )

    async def preflight(self, request: ProviderPreflightRequest) -> ProviderPreflightResult:
        return ProviderPreflightResult(
            request_id=request.request_id,
            provider="scripted",
            root_folder_id=None,
            files=tuple(
                ProviderPreflightFile(
                    document_key=document.document_key,
                    name=document.document_key,
                    mime_type="text/markdown",
                    modified_time="2026-01-01T00:00:00Z",
                    source_uri=None,
                    searchable=True,
                    held_for_demo=document.document_key == self._scenario.held_document_key,
                )
                for document in self._scenario.documents[: request.max_files]
            ),
            folders_scanned=0,
            truncated=len(self._scenario.documents) > request.max_files,
        )

    async def _before_operation(self, store_key: str, *, request_id: str, operation: str) -> None:
        run = await self._state_store.get_run_by_store(store_key)
        if operation == self._scenario.quota_operation:
            decision = await self._state_store.consume_quota_once(
                run.run_id,
                request_id=request_id,
                operation=operation,
            )
            if decision.injected:
                raise ProviderQuotaExhausted(
                    # The reset window must admit both the retried user-page
                    # request and the following file-page request. A limit of
                    # one would consume the only post-reset permit on the
                    # retry and leave the deterministic demo parked forever
                    # because its synthetic success response has no next-reset
                    # headers.
                    limit=2,
                    remaining=0,
                    retry_after_seconds=decision.retry_after_seconds,
                )
            await self._state_store.complete_quota_wait(run.run_id, operation=operation)

    async def aclose(self) -> None:
        if self._close_callback is None:
            return
        result = self._close_callback()
        if inspect.isawaitable(result):
            await result


async def create_provider_gateway() -> ScriptedNorthstarProvider:
    """Build a standalone provider adapter for the legacy three-factory worker surface."""

    config = DemoConfig.from_env()
    config.require_enabled()
    from retrieval.lakebase.config import LakebaseConfig
    from retrieval.lakebase.connection import LakebaseConnectionProvider

    provider = LakebaseConnectionProvider(
        LakebaseConfig.from_env(default_pool_max_size=20),
    )
    state_store = PostgresDemoStateStore(provider, owns_provider=True)
    try:
        await state_store.start()
    except BaseException:
        await state_store.aclose()
        raise
    return ScriptedNorthstarProvider(
        load_northstar_scenario(),
        state_store,
        close_callback=state_store.aclose,
    )


async def create_adapter_bundle():
    """Build one shared Lakebase-backed worker bundle with demo hooks enabled."""

    config = DemoConfig.from_env()
    config.require_enabled()
    from retrieval.embeddings import create_embedding_provider
    from retrieval.lakebase.config import LakebaseConfig
    from retrieval.lakebase.connection import LakebaseConnectionProvider
    from retrieval.lakebase.indexing import LakebaseHybridIndexRefresher
    from retrieval.lakebase.repository import LakebaseRetrievalRepository
    from retrieval.temporal.worker import AdapterBundle

    from .events import DemoIngestionEventSink
    from .fixtures import FixtureStagingStore
    from .ingestion_gate import DemoBeforeDocumentCommitHook

    scenario = load_northstar_scenario()
    provider = LakebaseConnectionProvider(
        LakebaseConfig.from_env(default_pool_max_size=20),
    )
    try:
        await provider.open()
        await provider.wait()
        state_store = PostgresDemoStateStore(provider)
        return AdapterBundle(
            repository=LakebaseRetrievalRepository(provider),
            staging_store=FixtureStagingStore(scenario),
            provider_gateway=ScriptedNorthstarProvider(
                scenario,
                state_store,
                close_callback=provider.aclose,
            ),
            before_document_commit=DemoBeforeDocumentCommitHook(state_store, config=config),
            ingestion_event_sink=DemoIngestionEventSink(state_store),
            embedding_provider=create_embedding_provider(),
            search_index_refresher=LakebaseHybridIndexRefresher(provider),
        )
    except BaseException:
        await provider.aclose()
        raise


__all__ = [
    "ScriptedNorthstarProvider",
    "create_adapter_bundle",
    "create_provider_gateway",
]
