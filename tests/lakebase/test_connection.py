from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

import retrieval.lakebase.connection as connection_module
from retrieval.lakebase.config import LakebaseConfig
from retrieval.lakebase.connection import (
    DatabricksOAuthCredentialProvider,
    LakebaseConnectionProvider,
    LakebaseHealthCheckError,
    StaticPasswordCredentialProvider,
)


class RotatingCredential:
    def __init__(self) -> None:
        self.calls = 0

    async def get_password(self) -> str:
        self.calls += 1
        return f"fresh-token-{self.calls}"


class FakeCursor:
    async def fetchone(self):
        return {"healthy": 1}


class FakeConnection:
    async def execute(self, sql: str):
        assert sql == "SELECT 1 AS healthy"
        return FakeCursor()


class FakePool:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.open_calls: list[tuple[bool, float | None]] = []
        self.wait_calls: list[float] = []
        self.close_calls: list[float] = []
        self.check_calls = 0
        self.fail_check = False

    async def open(
        self,
        *,
        wait: bool = False,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> None:
        self.open_calls.append((wait, timeout))

    async def wait(
        self,
        *,
        timeout: float,  # noqa: ASYNC109 - mirrors third-party API
    ) -> None:
        self.wait_calls.append(timeout)

    @asynccontextmanager
    async def connection(
        self,
        *,
        timeout: float,  # noqa: ASYNC109 - mirrors third-party API
    ):
        assert timeout == self.kwargs["timeout"]
        yield FakeConnection()

    async def check(self) -> None:
        self.check_calls += 1
        if self.fail_check:
            raise OSError("database unavailable")

    def get_stats(self) -> dict[str, int]:
        return {"pool_size": 1, "requests_waiting": 0}

    async def close(
        self,
        *,
        timeout: float,  # noqa: ASYNC109 - mirrors third-party API
    ) -> None:
        self.close_calls.append(timeout)


def local_config() -> LakebaseConfig:
    return LakebaseConfig(
        host="localhost",
        database="retrieval",
        user="developer",
        password="not-logged",
        pool_min_size=1,
        pool_max_size=3,
    )


@pytest.mark.asyncio
async def test_pool_gets_fresh_credential_kwargs_for_every_new_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool: FakePool | None = None

    def pool_factory(**kwargs):
        nonlocal fake_pool
        fake_pool = FakePool(**kwargs)
        return fake_pool

    monkeypatch.setattr(connection_module, "_dict_row_factory", lambda: "dict-row")
    rotating = RotatingCredential()
    provider = LakebaseConnectionProvider(
        local_config(),
        credential_provider=rotating,
        pool_factory=pool_factory,
    )
    assert fake_pool is not None

    first = await fake_pool.kwargs["kwargs"]()
    second = await fake_pool.kwargs["kwargs"]()

    assert first["password"] == "fresh-token-1"
    assert second["password"] == "fresh-token-2"
    assert first["host"] == "localhost"
    assert first["sslmode"] == "require"
    assert first["row_factory"] == "dict-row"
    assert "statement_timeout=30000ms" in first["options"]
    assert "lock_timeout=5000ms" in first["options"]
    assert fake_pool.kwargs["conninfo"] == ""
    assert "not-logged" not in repr(provider.config)


@pytest.mark.asyncio
async def test_pool_lifecycle_is_explicit_checked_and_idempotently_closed() -> None:
    fake_pool: FakePool | None = None

    def pool_factory(**kwargs):
        nonlocal fake_pool
        fake_pool = FakePool(**kwargs)
        return fake_pool

    provider = LakebaseConnectionProvider(local_config(), pool_factory=pool_factory)
    assert fake_pool is not None

    with pytest.raises(RuntimeError, match="must be opened"):
        async with provider.connection():
            pass

    await provider.open()
    await provider.open()
    health = await provider.check()
    await provider.aclose()
    await provider.aclose()

    assert fake_pool.open_calls == [(False, None)]
    # open() always waits, including the idempotent readiness call.
    assert fake_pool.wait_calls == [30.0, 30.0]
    assert fake_pool.check_calls == 1
    assert health.stats["pool_size"] == 1
    assert health.latency_ms >= 0
    assert fake_pool.close_calls == [10.0]
    with pytest.raises(RuntimeError, match="cannot be reopened"):
        await provider.open()


@pytest.mark.asyncio
async def test_health_check_fails_closed() -> None:
    fake_pool: FakePool | None = None

    def pool_factory(**kwargs):
        nonlocal fake_pool
        fake_pool = FakePool(**kwargs)
        return fake_pool

    provider = LakebaseConnectionProvider(local_config(), pool_factory=pool_factory)
    assert fake_pool is not None
    await provider.open()
    fake_pool.fail_check = True

    with pytest.raises(LakebaseHealthCheckError, match="health check failed"):
        await provider.check()


@pytest.mark.asyncio
async def test_databricks_credential_provider_reuses_client_but_not_tokens() -> None:
    class Postgres:
        def __init__(self) -> None:
            self.endpoints: list[str] = []

        def generate_database_credential(self, *, endpoint: str):
            self.endpoints.append(endpoint)
            return SimpleNamespace(token=f"token-{len(self.endpoints)}")

    postgres = Postgres()
    client = SimpleNamespace(postgres=postgres)
    factory_calls = 0

    def factory():
        nonlocal factory_calls
        factory_calls += 1
        return client

    credentials = DatabricksOAuthCredentialProvider(
        "projects/demo/branches/production/endpoints/primary",
        workspace_client_factory=factory,
    )

    assert await credentials.get_password() == "token-1"
    assert await credentials.get_password() == "token-2"
    assert factory_calls == 1
    assert postgres.endpoints == [
        "projects/demo/branches/production/endpoints/primary",
        "projects/demo/branches/production/endpoints/primary",
    ]


def test_static_password_repr_is_redacted() -> None:
    assert "secret" not in repr(StaticPasswordCredentialProvider("secret"))
