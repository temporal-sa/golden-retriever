"""Async Lakebase connection pooling with per-connection OAuth credentials."""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Protocol

from .config import LakebaseConfig


class LakebaseDependencyError(RuntimeError):
    """The optional Lakebase runtime dependencies are not installed."""


class LakebaseHealthCheckError(RuntimeError):
    """The pool could not prove database connectivity inside its deadline."""


class DatabaseCredentialProvider(Protocol):
    """Return a password or short-lived database OAuth token."""

    async def get_password(self) -> str: ...


@dataclass(frozen=True, slots=True)
class StaticPasswordCredentialProvider:
    """Local-development credential source; the password is never logged."""

    password: str = field(repr=False)

    async def get_password(self) -> str:
        return self.password


class DatabricksOAuthCredentialProvider:
    """Generate one fresh Lakebase OAuth token per connection attempt."""

    def __init__(
        self,
        endpoint: str,
        *,
        workspace_client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._endpoint = endpoint
        self._workspace_client_factory = (
            workspace_client_factory or _default_workspace_client_factory
        )
        self._workspace_client: Any | None = None
        self._client_lock = asyncio.Lock()

    async def get_password(self) -> str:
        client = await self._client()
        credential = await asyncio.to_thread(
            client.postgres.generate_database_credential,
            endpoint=self._endpoint,
        )
        token = getattr(credential, "token", None)
        if not isinstance(token, str) or not token:
            raise RuntimeError("Databricks returned an empty Lakebase database credential")
        return token

    async def _client(self) -> Any:
        if self._workspace_client is not None:
            return self._workspace_client
        async with self._client_lock:
            if self._workspace_client is None:
                # WorkspaceClient construction may inspect local credential files.
                self._workspace_client = await asyncio.to_thread(self._workspace_client_factory)
            return self._workspace_client


@dataclass(frozen=True, slots=True)
class PoolHealth:
    checked_at: datetime
    latency_ms: float
    stats: Mapping[str, int]


class LakebaseConnectionProvider:
    """Own an explicitly-opened Psycopg async pool.

    ``psycopg_pool>=3.3`` accepts an async callable for ``kwargs``.  The pool
    invokes that callable for every physical connection attempt, which ensures
    connections created after token rotation never reuse an expired token.
    Transaction bodies are deliberately not retried here: a generic connection
    layer cannot know whether a failed commit reached the server.
    """

    def __init__(
        self,
        config: LakebaseConfig,
        *,
        credential_provider: DatabaseCredentialProvider | None = None,
        pool_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.config = config
        self._credential_provider = credential_provider or _credential_provider(config)
        self._pool = (pool_factory or _default_pool_factory)(
            conninfo="",
            kwargs=self._connection_kwargs,
            min_size=config.pool_min_size,
            max_size=config.pool_max_size,
            open=False,
            timeout=config.pool_acquire_timeout_seconds,
            max_lifetime=config.pool_max_lifetime_seconds,
            max_idle=config.pool_max_idle_seconds,
            reconnect_timeout=config.pool_reconnect_timeout_seconds,
            name="retrieval-lakebase",
        )
        self._lifecycle_lock = asyncio.Lock()
        self._opened = False
        self._closed = False

    async def _connection_kwargs(self) -> dict[str, Any]:
        password = await self._credential_provider.get_password()
        if not password:
            raise RuntimeError("database credential provider returned an empty credential")
        statement_ms = math.ceil(self.config.statement_timeout_seconds * 1_000)
        lock_ms = math.ceil(self.config.lock_timeout_seconds * 1_000)
        return {
            "host": self.config.host,
            "port": self.config.port,
            "dbname": self.config.database,
            "user": self.config.user,
            "password": password,
            "sslmode": self.config.sslmode,
            "connect_timeout": math.ceil(self.config.connect_timeout_seconds),
            "application_name": self.config.application_name,
            # Applying these as libpq startup options covers every statement,
            # including the first statement executed on a new connection.
            "options": (f"-c statement_timeout={statement_ms}ms -c lock_timeout={lock_ms}ms"),
            "row_factory": _dict_row_factory(),
        }

    async def open(self) -> None:
        """Start the pool and wait for its minimum connections to be ready."""

        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("a closed Lakebase pool cannot be reopened")
            if not self._opened:
                await self._pool.open(wait=False)
                self._opened = True
        await self.wait()

    async def wait(self) -> None:
        if not self._opened or self._closed:
            raise RuntimeError("Lakebase pool is not open")
        await self._pool.wait(timeout=self.config.pool_open_timeout_seconds)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        """Acquire a checked connection inside a bounded wait."""

        if not self._opened or self._closed:
            raise RuntimeError("Lakebase pool must be opened before use")
        async with self._pool.connection(
            timeout=self.config.pool_acquire_timeout_seconds
        ) as connection:
            yield connection

    async def check(self) -> PoolHealth:
        """Check idle pool members and execute one bounded round trip."""

        if not self._opened or self._closed:
            raise LakebaseHealthCheckError("Lakebase pool is not open")
        started = monotonic()
        try:
            async with asyncio.timeout(self.config.health_check_timeout_seconds):
                await self._pool.check()
                async with self.connection() as connection:
                    cursor = await connection.execute("SELECT 1 AS healthy")
                    row = await cursor.fetchone()
                    if row is None:
                        raise RuntimeError("health query returned no row")
        except Exception as exc:
            raise LakebaseHealthCheckError("Lakebase health check failed") from exc
        return PoolHealth(
            checked_at=datetime.now(UTC),
            latency_ms=(monotonic() - started) * 1_000,
            stats=dict(self._pool.get_stats()),
        )

    def get_stats(self) -> Mapping[str, int]:
        return dict(self._pool.get_stats())

    async def aclose(self) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            if self._opened:
                await self._pool.close(timeout=min(self.config.pool_open_timeout_seconds, 10.0))

    async def __aenter__(self) -> LakebaseConnectionProvider:
        await self.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


def _credential_provider(config: LakebaseConfig) -> DatabaseCredentialProvider:
    if config.endpoint is not None:
        return DatabricksOAuthCredentialProvider(config.endpoint)
    assert config.password is not None  # Enforced by LakebaseConfig.
    return StaticPasswordCredentialProvider(config.password)


def _default_workspace_client_factory() -> Any:
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:  # pragma: no cover - dependency error is environment-specific
        raise LakebaseDependencyError(
            "Databricks OAuth requires the optional databricks-sdk dependency"
        ) from exc
    return WorkspaceClient()


def _dict_row_factory() -> Any:
    try:
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - dependency error is environment-specific
        raise LakebaseDependencyError("Lakebase requires the optional psycopg dependency") from exc
    return dict_row


def _default_pool_factory(**kwargs: Any) -> Any:
    try:
        from psycopg_pool import AsyncConnectionPool
    except ImportError as exc:  # pragma: no cover - dependency error is environment-specific
        raise LakebaseDependencyError("Lakebase pooling requires psycopg_pool>=3.3") from exc
    kwargs["check"] = AsyncConnectionPool.check_connection
    return AsyncConnectionPool(**kwargs)
