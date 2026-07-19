"""Checksum-verified forward-only migrations for ``retrieval_demo_ui``."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from importlib.resources import files
from typing import Any, Protocol


class DemoMigrationError(RuntimeError):
    pass


class DemoMigrationDriftError(DemoMigrationError):
    pass


class MigrationProvider(Protocol):
    def connection(self) -> Any: ...


@dataclass(frozen=True)
class DemoMigration:
    version: int
    name: str
    checksum: str
    sql: str


@dataclass(frozen=True)
class DemoMigrationStatus:
    schema_present: bool
    current_version: int
    latest_version: int
    pending_versions: tuple[int, ...]

    @property
    def ready(self) -> bool:
        return self.schema_present and not self.pending_versions


_FILE = re.compile(r"^(?P<version>[0-9]{4})_(?P<name>[a-z0-9_]+)\.sql$")
_LOCK_NAME = "temporal-retrieval-v2:retrieval_demo_ui.schema_migrations"
_BOOTSTRAP = """
CREATE SCHEMA IF NOT EXISTS retrieval_demo_ui;
CREATE TABLE IF NOT EXISTS retrieval_demo_ui.schema_migrations (
    version integer PRIMARY KEY CHECK (version > 0),
    name text NOT NULL,
    checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    actor text NOT NULL DEFAULT current_user
)
"""


def discover_demo_migrations() -> tuple[DemoMigration, ...]:
    migrations: list[DemoMigration] = []
    for resource in files(__package__).iterdir():
        match = _FILE.fullmatch(resource.name)
        if match is None:
            continue
        raw = resource.read_bytes()
        migrations.append(
            DemoMigration(
                version=int(match.group("version")),
                name=match.group("name"),
                checksum=hashlib.sha256(raw).hexdigest(),
                sql=raw.decode("utf-8"),
            )
        )
    migrations.sort(key=lambda item: item.version)
    if [item.version for item in migrations] != list(range(1, len(migrations) + 1)):
        raise DemoMigrationError("demo migration versions must be a contiguous sequence")
    if not migrations:
        raise DemoMigrationError("no packaged demo migrations were found")
    return tuple(migrations)


class DemoMigrationRunner:
    def __init__(self, provider: MigrationProvider) -> None:
        self._provider = provider
        self._migrations = discover_demo_migrations()

    async def status(self) -> DemoMigrationStatus:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "SELECT to_regclass('retrieval_demo_ui.schema_migrations') AS migration_table"
            )
            row = await cursor.fetchone()
            if row is None or _row_value(row, "migration_table", 0) is None:
                return DemoMigrationStatus(
                    False,
                    0,
                    self._migrations[-1].version,
                    tuple(item.version for item in self._migrations),
                )
            applied = await self._load(connection)
            self._validate(applied)
        versions = {item[0] for item in applied}
        return DemoMigrationStatus(
            True,
            max(versions, default=0),
            self._migrations[-1].version,
            tuple(item.version for item in self._migrations if item.version not in versions),
        )

    async def apply(self) -> DemoMigrationStatus:
        async with self._provider.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (_LOCK_NAME,),
                )
                # The transaction-scoped lock must cover first-run schema and
                # ledger creation as well as every migration. Otherwise two
                # initial deploys can race before either reaches the lock.
                await connection.execute(_BOOTSTRAP)
                applied = await self._load(connection)
                self._validate(applied)
                versions = {item[0] for item in applied}
                for migration in self._migrations:
                    if migration.version in versions:
                        continue
                    await connection.execute(migration.sql, prepare=False)
                    await connection.execute(
                        "INSERT INTO retrieval_demo_ui.schema_migrations "
                        "(version,name,checksum,actor) VALUES (%s,%s,%s,current_user)",
                        (migration.version, migration.name, migration.checksum),
                    )
        return await self.status()

    @staticmethod
    async def _load(connection: Any) -> list[tuple[int, str, str]]:
        cursor = await connection.execute(
            "SELECT version,name,checksum FROM retrieval_demo_ui.schema_migrations ORDER BY version"
        )
        return [
            (
                int(_row_value(row, "version", 0)),
                str(_row_value(row, "name", 1)),
                str(_row_value(row, "checksum", 2)),
            )
            for row in await cursor.fetchall()
        ]

    def _validate(self, applied: list[tuple[int, str, str]]) -> None:
        by_version = {item.version: item for item in self._migrations}
        versions = [item[0] for item in applied]
        if versions and versions != list(range(1, max(versions) + 1)):
            raise DemoMigrationDriftError("applied demo migrations are not contiguous")
        for version, name, checksum in applied:
            packaged = by_version.get(version)
            if packaged is None or packaged.name != name or packaged.checksum != checksum:
                raise DemoMigrationDriftError(f"demo migration {version} checksum/name drift")


def _row_value(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        return row[name]
    return row[index]


async def _run_cli(args: argparse.Namespace) -> int:
    from retrieval.lakebase.config import LakebaseConfig
    from retrieval.lakebase.connection import LakebaseConnectionProvider

    provider = LakebaseConnectionProvider(LakebaseConfig.from_env(default_pool_max_size=2))
    await provider.open()
    try:
        runner = DemoMigrationRunner(provider)
        status = await runner.status() if args.check else await runner.apply()
    finally:
        await provider.aclose()
    payload = asdict(status)
    payload["ready"] = status.ready
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        state = "ready" if status.ready else "pending"
        print(
            f"Northstar demo schema: {state}; current={status.current_version} "
            f"latest={status.latest_version} pending={list(status.pending_versions)}"
        )
    return 0 if status.ready else 1


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Verify or apply the Northstar demo UI schema")
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify checksums and fail if migrations are missing; do not apply",
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable status")
    args = parser.parse_args(argv)
    raise SystemExit(asyncio.run(_run_cli(args)))


__all__ = [
    "DemoMigration",
    "DemoMigrationDriftError",
    "DemoMigrationError",
    "DemoMigrationRunner",
    "DemoMigrationStatus",
    "discover_demo_migrations",
    "main",
]
