from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from retrieval.lakebase.migrations import (
    MigrationDriftError,
    MigrationRunner,
    discover_migrations,
)


class Cursor:
    def __init__(self, rows=()) -> None:
        self.rows = list(rows)

    async def fetchone(self):
        return self.rows[0] if self.rows else None

    async def fetchall(self):
        return list(self.rows)


class Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None


class MigrationConnection:
    def __init__(self, *, schema_present: bool = False, applied=()) -> None:
        self.schema_present = schema_present
        self.applied = list(applied)
        self.calls: list[tuple[str, object, object]] = []

    def transaction(self) -> Transaction:
        return Transaction()

    async def execute(self, sql: str, params=None, *, prepare=None) -> Cursor:
        normalized = " ".join(sql.split())
        self.calls.append((normalized, params, prepare))
        if "to_regclass('retrieval.schema_migrations')" in normalized:
            return Cursor(
                [
                    {
                        "migration_table": (
                            "retrieval.schema_migrations" if self.schema_present else None
                        )
                    }
                ]
            )
        if normalized.startswith("CREATE SCHEMA IF NOT EXISTS retrieval;"):
            self.schema_present = True
            return Cursor()
        if normalized.startswith("SELECT version, name, checksum"):
            return Cursor(
                [
                    {"version": version, "name": name, "checksum": checksum}
                    for version, name, checksum in self.applied
                ]
            )
        if normalized.startswith("INSERT INTO retrieval.schema_migrations"):
            version, name, checksum = params
            self.applied.append((version, name, checksum))
            return Cursor()
        return Cursor()


class Provider:
    def __init__(self, connection: MigrationConnection) -> None:
        self.value = connection

    @asynccontextmanager
    async def connection(self):
        yield self.value


def test_packaged_migrations_are_contiguous_and_checksum_verified() -> None:
    migrations = discover_migrations()

    assert [migration.version for migration in migrations] == [1, 2, 3, 4, 5, 6]
    assert [migration.name for migration in migrations] == [
        "retrieval_core",
        "postgres_text_search",
        "allow_repeated_chunk_hashes",
        "harden_public_privileges",
        "lakebase_hybrid_search",
        "google_drive_connector_state",
    ]
    assert all(len(migration.checksum) == 64 for migration in migrations)
    assert "write_receipts" in migrations[0].sql
    assert "GENERATED ALWAYS AS" in migrations[1].sql
    assert "USING gin" in migrations[1].sql
    assert "DROP CONSTRAINT IF EXISTS document_chunks_content_unique" in migrations[2].sql
    assert "REVOKE ALL ON SCHEMA retrieval FROM PUBLIC" in migrations[3].sql
    assert "ALTER DEFAULT PRIVILEGES IN SCHEMA retrieval" in migrations[3].sql
    assert "lakebase_ann" in migrations[4].sql
    assert "lakebase_bm25" in migrations[4].sql
    assert "retrieval_connector.staged_content" in migrations[5].sql


@pytest.mark.asyncio
async def test_apply_serializes_with_advisory_lock_and_records_checksums() -> None:
    connection = MigrationConnection()
    runner = MigrationRunner(Provider(connection))

    status = await runner.apply()

    assert status.ready
    assert status.current_version == 6
    assert status.pending_versions == ()
    lock_index = next(
        index
        for index, (sql, _, _) in enumerate(connection.calls)
        if "pg_advisory_xact_lock" in sql
    )
    first_migration_index = next(
        index
        for index, (sql, _, prepare) in enumerate(connection.calls)
        if "CREATE TABLE IF NOT EXISTS retrieval.stores" in sql and prepare is False
    )
    assert lock_index < first_migration_index
    assert [(version, name) for version, name, _ in connection.applied] == [
        (1, "retrieval_core"),
        (2, "postgres_text_search"),
        (3, "allow_repeated_chunk_hashes"),
        (4, "harden_public_privileges"),
        (5, "lakebase_hybrid_search"),
        (6, "google_drive_connector_state"),
    ]


@pytest.mark.asyncio
async def test_check_on_missing_schema_is_read_only_and_reports_all_pending() -> None:
    connection = MigrationConnection()
    status = await MigrationRunner(Provider(connection)).status()

    assert not status.schema_present
    assert not status.ready
    assert status.pending_versions == (1, 2, 3, 4, 5, 6)
    assert not any(sql.startswith("CREATE SCHEMA") for sql, _, _ in connection.calls)


@pytest.mark.asyncio
async def test_applied_checksum_drift_fails_closed() -> None:
    migrations = discover_migrations()
    connection = MigrationConnection(
        schema_present=True,
        applied=[(1, migrations[0].name, "0" * 64)],
    )

    with pytest.raises(MigrationDriftError, match="drift detected"):
        await MigrationRunner(Provider(connection), migrations).status()


@pytest.mark.asyncio
async def test_applied_versions_must_be_a_contiguous_prefix() -> None:
    migrations = discover_migrations()
    connection = MigrationConnection(
        schema_present=True,
        applied=[(2, migrations[1].name, migrations[1].checksum)],
    )

    with pytest.raises(MigrationDriftError, match="not a contiguous prefix"):
        await MigrationRunner(Provider(connection), migrations).status()
