from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from retrieval.lakebase.grants import RuntimeRoles, apply_runtime_grants


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: object) -> None:
        return None


class _Connection:
    def __init__(self) -> None:
        self.statements: list[object] = []

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def execute(self, statement: object) -> None:
        self.statements.append(statement)


class _Provider:
    def __init__(self) -> None:
        self.connection_instance = _Connection()

    @asynccontextmanager
    async def connection(self):
        yield self.connection_instance


@pytest.mark.parametrize(
    "role",
    [
        "",
        "   ",
        " leading",
        "trailing ",
        "x\x00y",
        "x" * 64,
        "é" * 32,
    ],
)
def test_runtime_roles_reject_invalid_names(role: str) -> None:
    with pytest.raises(ValueError):
        RuntimeRoles(role, "worker")


def test_runtime_roles_must_be_distinct() -> None:
    with pytest.raises(ValueError, match="distinct"):
        RuntimeRoles("same", "same")


def test_runtime_roles_reject_names_that_only_differ_after_postgres_prefix() -> None:
    with pytest.raises(ValueError, match="63-byte"):
        RuntimeRoles("x" * 63 + "a", "x" * 63 + "b")


def test_runtime_roles_accept_exactly_63_utf8_bytes() -> None:
    roles = RuntimeRoles("a" * 63, "é" * 31 + "x")

    assert len(roles.app_role.encode()) == 63
    assert len(roles.worker_role.encode()) == 63


@pytest.mark.asyncio
async def test_grants_quote_roles_and_keep_app_off_core_dml() -> None:
    provider = _Provider()
    roles = RuntimeRoles('app role"quoted', "worker-role")

    await apply_runtime_grants(provider, roles)

    rendered = [statement.as_string(None) for statement in provider.connection_instance.statements]
    assert len(rendered) == 17
    assert any('TO "app role""quoted"' in statement for statement in rendered)
    assert any('TO "worker-role"' in statement for statement in rendered)
    app_statements = [statement for statement in rendered if 'TO "app role""quoted"' in statement]
    assert not any(
        "INSERT" in statement and "retrieval.stores" in statement for statement in app_statements
    )
    assert any(
        "retrieval.store_users" in statement and "retrieval.retrieval_state" in statement
        for statement in app_statements
    )
    assert any("create_northstar_run" in statement for statement in app_statements)
