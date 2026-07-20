from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from retrieval.demo.migrations import DemoMigrationRunner, discover_demo_migrations


class _Cursor:
    def __init__(self, rows=()) -> None:
        self._rows = list(rows)

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return list(self._rows)


class _Transaction:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self):
        self._connection.transaction_entries += 1
        return self

    async def __aexit__(self, *_: object) -> None:
        return None


class _Connection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, object]] = []
        self.applied: list[tuple[int, str, str]] = []
        self.schema_present = False
        self.transaction_entries = 0

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    async def execute(self, sql: str, params=None, *, prepare=None) -> _Cursor:
        normalized = " ".join(sql.split())
        self.calls.append((normalized, params, prepare))
        if "to_regclass('retrieval_demo_ui.schema_migrations')" in normalized:
            table = "retrieval_demo_ui.schema_migrations" if self.schema_present else None
            return _Cursor([{"migration_table": table}])
        if normalized.startswith("CREATE SCHEMA IF NOT EXISTS retrieval_demo_ui;"):
            self.schema_present = True
        if normalized.startswith("SELECT version,name,checksum"):
            return _Cursor(self.applied)
        if normalized.startswith("INSERT INTO retrieval_demo_ui.schema_migrations"):
            version, name, checksum = params
            self.applied.append((version, name, checksum))
        return _Cursor()


class _Provider:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    @asynccontextmanager
    async def connection(self):
        yield self._connection


def test_demo_migration_is_packaged_and_contains_durable_contracts() -> None:
    migrations = discover_demo_migrations()

    assert tuple(item.version for item in migrations) == (1, 2)
    sql = migrations[0].sql
    for table in (
        "demo_runs",
        "demo_controls",
        "demo_events",
        "demo_operations",
        "api_idempotency",
    ):
        assert f"retrieval_demo_ui.{table}" in sql
    assert "SECURITY DEFINER" in sql
    assert "REVOKE ALL" in sql
    assert "UNIQUE (run_id, event_key)" in sql
    google_drive_sql = migrations[1].sql
    assert "create_demo_run" in google_drive_sql
    assert "generation_proof" in google_drive_sql
    assert "gdrive:" in google_drive_sql
    assert "retrieval_demo_ui.preflight_runs" in google_drive_sql


@pytest.mark.asyncio
async def test_first_run_bootstrap_is_inside_the_advisory_lock_transaction() -> None:
    connection = _Connection()

    status = await DemoMigrationRunner(_Provider(connection)).apply()

    assert status.ready
    # apply uses one transaction; the later status call is read-only.
    assert connection.transaction_entries == 1
    statements = [statement for statement, _, _ in connection.calls]
    lock_index = next(i for i, sql in enumerate(statements) if "pg_advisory_xact_lock" in sql)
    bootstrap_index = next(
        i
        for i, sql in enumerate(statements)
        if sql.startswith("CREATE SCHEMA IF NOT EXISTS retrieval_demo_ui;")
    )
    ledger_read_index = next(
        i for i, sql in enumerate(statements) if sql.startswith("SELECT version,name,checksum")
    )
    assert lock_index < bootstrap_index < ledger_read_index
