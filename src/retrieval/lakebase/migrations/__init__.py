"""Forward-only, checksum-verified Lakebase schema migrations."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from importlib.resources import files
from typing import Any, Protocol

from retrieval.lakebase.config import LakebaseConfig
from retrieval.lakebase.connection import LakebaseConnectionProvider


class MigrationError(RuntimeError):
    """Base class for migration safety failures."""


class MigrationDriftError(MigrationError):
    """An applied migration no longer matches the packaged SQL."""


class MigrationProvider(Protocol):
    def connection(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    checksum: str
    sql: str


@dataclass(frozen=True, slots=True)
class MigrationStatus:
    schema_present: bool
    current_version: int
    latest_version: int
    pending_versions: tuple[int, ...]

    @property
    def ready(self) -> bool:
        return self.schema_present and not self.pending_versions


_MIGRATION_FILE = re.compile(r"^(?P<version>[0-9]{4})_(?P<name>[a-z0-9_]+)\.sql$")
_LOCK_NAME = "temporal-retrieval-v2:retrieval.schema_migrations"
_BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS retrieval;
CREATE TABLE IF NOT EXISTS retrieval.schema_migrations (
    version integer PRIMARY KEY CHECK (version > 0),
    name text NOT NULL,
    checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    actor text NOT NULL DEFAULT current_user
)
"""


def discover_migrations() -> tuple[Migration, ...]:
    """Read core migration resources in version order and verify continuity."""

    discovered: list[Migration] = []
    for resource in files(__package__).iterdir():
        match = _MIGRATION_FILE.fullmatch(resource.name)
        if match is None:
            continue
        raw = resource.read_bytes()
        discovered.append(
            Migration(
                version=int(match.group("version")),
                name=match.group("name"),
                checksum=hashlib.sha256(raw).hexdigest(),
                sql=raw.decode("utf-8"),
            )
        )
    discovered.sort(key=lambda migration: migration.version)
    versions = [migration.version for migration in discovered]
    if not versions:
        raise MigrationError("no packaged Lakebase migrations were found")
    expected = list(range(1, len(versions) + 1))
    if versions != expected:
        raise MigrationError(
            f"core migration versions must be contiguous; found {versions}, expected {expected}"
        )
    return tuple(discovered)


class MigrationRunner:
    """Apply packaged migrations under one transaction-scoped advisory lock."""

    def __init__(
        self,
        provider: MigrationProvider,
        migrations: tuple[Migration, ...] | None = None,
    ) -> None:
        self._provider = provider
        self._migrations = migrations or discover_migrations()
        self._by_version = {migration.version: migration for migration in self._migrations}

    async def status(self) -> MigrationStatus:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "SELECT to_regclass('retrieval.schema_migrations') AS migration_table"
            )
            row = await cursor.fetchone()
            if row is None or _row_value(row, "migration_table", 0) is None:
                return MigrationStatus(
                    schema_present=False,
                    current_version=0,
                    latest_version=self._migrations[-1].version,
                    pending_versions=tuple(m.version for m in self._migrations),
                )
            applied = await self._load_applied(connection)
            self._validate_applied(applied)
            applied_versions = {version for version, _, _ in applied}
            return MigrationStatus(
                schema_present=True,
                current_version=max(applied_versions, default=0),
                latest_version=self._migrations[-1].version,
                pending_versions=tuple(
                    migration.version
                    for migration in self._migrations
                    if migration.version not in applied_versions
                ),
            )

    async def apply(self, *, target_version: int | None = None) -> MigrationStatus:
        latest = self._migrations[-1].version
        target = latest if target_version is None else target_version
        if target < 1 or target > latest:
            raise MigrationError(f"target version must be between 1 and {latest}")

        async with self._provider.connection() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (_LOCK_NAME,),
                )
                # The lock also serializes first-run schema/table creation; this
                # avoids relying on concurrent IF NOT EXISTS DDL races.
                await connection.execute(_BOOTSTRAP_SQL)
                applied = await self._load_applied(connection)
                self._validate_applied(applied)
                applied_versions = {version for version, _, _ in applied}
                if applied_versions and max(applied_versions) > target:
                    raise MigrationError(
                        "forward-only migrations cannot target a version older than the database"
                    )
                for migration in self._migrations:
                    if migration.version > target or migration.version in applied_versions:
                        continue
                    # Migration files are trusted package resources, never user input.
                    await connection.execute(migration.sql, prepare=False)
                    await connection.execute(
                        """
                        INSERT INTO retrieval.schema_migrations
                            (version, name, checksum, actor)
                        VALUES (%s, %s, %s, current_user)
                        """,
                        (migration.version, migration.name, migration.checksum),
                    )

        return await self.status()

    async def _load_applied(self, connection: Any) -> list[tuple[int, str, str]]:
        cursor = await connection.execute(
            """
            SELECT version, name, checksum
            FROM retrieval.schema_migrations
            ORDER BY version
            """
        )
        rows = await cursor.fetchall()
        return [
            (
                int(_row_value(row, "version", 0)),
                str(_row_value(row, "name", 1)),
                str(_row_value(row, "checksum", 2)),
            )
            for row in rows
        ]

    def _validate_applied(self, applied: list[tuple[int, str, str]]) -> None:
        versions = [version for version, _, _ in applied]
        if versions and versions != list(range(1, max(versions) + 1)):
            raise MigrationDriftError(f"applied migrations are not a contiguous prefix: {versions}")
        for version, name, checksum in applied:
            packaged = self._by_version.get(version)
            if packaged is None:
                raise MigrationDriftError(f"database has unknown migration version {version}")
            if name != packaged.name or checksum != packaged.checksum:
                raise MigrationDriftError(
                    f"migration {version:04d}_{packaged.name} checksum/name drift detected"
                )


def _row_value(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        return row[name]
    return row[index]


async def _run_cli(args: argparse.Namespace) -> int:
    config = LakebaseConfig.from_env(default_pool_max_size=2)
    provider = LakebaseConnectionProvider(config)
    await provider.open()
    try:
        runner = MigrationRunner(provider)
        status = (
            await runner.status()
            if args.check
            else await runner.apply(target_version=args.target_version)
        )
    finally:
        await provider.aclose()

    payload = asdict(status)
    payload["ready"] = status.ready
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        state = "ready" if status.ready else "pending"
        print(
            f"Lakebase retrieval schema: {state}; "
            f"current={status.current_version} latest={status.latest_version} "
            f"pending={list(status.pending_versions)}"
        )
    return 0 if status.ready else 1


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Verify or apply the forward-only Lakebase retrieval schema"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify checksums and fail if migrations are missing; do not apply",
    )
    parser.add_argument(
        "--target-version",
        type=int,
        help="apply through this core version (forward-only)",
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable status")
    args = parser.parse_args(argv)
    if args.check and args.target_version is not None:
        parser.error("--target-version cannot be combined with --check")
    raise SystemExit(asyncio.run(_run_cli(args)))


__all__ = [
    "Migration",
    "MigrationDriftError",
    "MigrationError",
    "MigrationRunner",
    "MigrationStatus",
    "discover_migrations",
    "main",
]
