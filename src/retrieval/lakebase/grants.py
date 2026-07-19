"""Explicit least-privilege grants for the App and Temporal worker identities."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Any, Protocol


class GrantProvider(Protocol):
    def connection(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class RuntimeRoles:
    app_role: str
    worker_role: str

    def __post_init__(self) -> None:
        for name, value in (
            ("app_role", self.app_role),
            ("worker_role", self.worker_role),
        ):
            if not value or value != value.strip() or "\x00" in value:
                raise ValueError(
                    f"{name} must be a non-empty database role name without surrounding whitespace"
                )
            if len(value.encode("utf-8")) > 63:
                raise ValueError(f"{name} must fit PostgreSQL's 63-byte identifier limit")
        if self.app_role == self.worker_role:
            raise ValueError("App and worker database roles must be distinct")


async def apply_runtime_grants(provider: GrantProvider, roles: RuntimeRoles) -> None:
    """Grant only the steady-state privileges required by each runtime.

    The caller must be the migration owner. Role names are composed as quoted
    identifiers; no role name is interpolated into a raw SQL string.
    """

    try:
        from psycopg import sql
    except ImportError as exc:  # pragma: no cover - optional dependency guard
        raise RuntimeError("runtime grants require psycopg") from exc

    app = sql.Identifier(roles.app_role)
    worker = sql.Identifier(roles.worker_role)
    statements = (
        sql.SQL("GRANT USAGE ON SCHEMA retrieval, retrieval_demo_ui TO {}").format(app),
        sql.SQL(
            "GRANT SELECT ON retrieval.stores, retrieval.store_users, "
            "retrieval.retrieval_state, retrieval.documents, "
            "retrieval.document_chunks, retrieval.schema_migrations TO {}"
        ).format(app),
        sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA retrieval_demo_ui TO {}").format(app),
        sql.SQL(
            "GRANT UPDATE ON retrieval_demo_ui.demo_runs, retrieval_demo_ui.demo_controls TO {}"
        ).format(app),
        sql.SQL(
            "GRANT INSERT, UPDATE ON retrieval_demo_ui.demo_events, "
            "retrieval_demo_ui.demo_operations, retrieval_demo_ui.api_idempotency TO {}"
        ).format(app),
        sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA retrieval_demo_ui TO {}").format(
            app
        ),
        sql.SQL(
            "GRANT EXECUTE ON FUNCTION retrieval_demo_ui.create_northstar_run"
            "(uuid, text, text, bigint, double precision, text, boolean) TO {}"
        ).format(app),
        sql.SQL("GRANT USAGE ON SCHEMA retrieval, retrieval_demo_ui TO {}").format(worker),
        sql.SQL(
            "GRANT SELECT ON retrieval.stores, "
            "retrieval.store_users, retrieval.retrieval_state, retrieval.documents, "
            "retrieval.document_chunks, retrieval.write_receipts TO {}"
        ).format(worker),
        sql.SQL(
            "GRANT INSERT, UPDATE ON retrieval.stores, retrieval.store_users, "
            "retrieval.retrieval_state, retrieval.documents TO {}"
        ).format(worker),
        sql.SQL("GRANT INSERT ON retrieval.document_chunks, retrieval.write_receipts TO {}").format(
            worker
        ),
        sql.SQL(
            "GRANT DELETE ON retrieval.retrieval_state, retrieval.documents, "
            "retrieval.document_chunks TO {}"
        ).format(worker),
        sql.SQL("GRANT SELECT ON retrieval.schema_migrations TO {}").format(worker),
        sql.SQL(
            "GRANT SELECT ON retrieval_demo_ui.demo_runs, retrieval_demo_ui.demo_controls TO {}"
        ).format(worker),
        sql.SQL("GRANT UPDATE ON retrieval_demo_ui.demo_controls TO {}").format(worker),
        sql.SQL("GRANT SELECT, INSERT, UPDATE ON retrieval_demo_ui.demo_events TO {}").format(
            worker
        ),
        sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA retrieval_demo_ui TO {}").format(
            worker
        ),
    )
    async with provider.connection() as connection, connection.transaction():
        for statement in statements:
            await connection.execute(statement)


async def _run_cli(args: argparse.Namespace) -> None:
    from retrieval.lakebase.config import LakebaseConfig
    from retrieval.lakebase.connection import LakebaseConnectionProvider

    roles = RuntimeRoles(args.app_role, args.worker_role)
    provider = LakebaseConnectionProvider(LakebaseConfig.from_env(default_pool_max_size=2))
    await provider.open()
    try:
        await apply_runtime_grants(provider, roles)
    finally:
        await provider.aclose()
    print("Lakebase runtime grants applied to the explicitly named App and worker roles")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Apply least-privilege Lakebase grants after migrations"
    )
    parser.add_argument("--app-role", required=True, help="Databricks App database role")
    parser.add_argument("--worker-role", required=True, help="Temporal worker database role")
    args = parser.parse_args(argv)
    asyncio.run(_run_cli(args))


__all__ = ["RuntimeRoles", "apply_runtime_grants", "main"]
