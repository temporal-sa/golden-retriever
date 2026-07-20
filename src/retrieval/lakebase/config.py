"""Process-local Lakebase connection configuration.

The Databricks Apps runtime injects the standard ``PG*`` variables.  The
``LAKEBASE_*`` aliases make the same adapter convenient to run in a local
worker without translating every setting.  Canonical ``PG*`` values always
win when both spellings are present.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from os import environ


class LakebaseConfigurationError(ValueError):
    """Raised before any connection is attempted when configuration is unsafe."""


_ENDPOINT_PATTERN = re.compile(r"^projects/[^/]+/branches/[^/]+/endpoints/[^/]+$")
_TLS_MODES = frozenset({"require", "verify-ca", "verify-full"})


@dataclass(frozen=True, slots=True)
class LakebaseConfig:
    """Validated settings shared by the worker, migration CLI, and App.

    ``password`` is intended for local development only.  Deployed processes
    should provide ``endpoint`` and obtain a short-lived OAuth database token
    for every newly-created pooled connection.
    """

    host: str
    database: str
    user: str
    port: int = 5432
    sslmode: str = "require"
    endpoint: str | None = None
    password: str | None = field(default=None, repr=False)
    pool_min_size: int = 1
    pool_max_size: int = 10
    pool_acquire_timeout_seconds: float = 10.0
    pool_open_timeout_seconds: float = 30.0
    pool_max_idle_seconds: float = 600.0
    pool_max_lifetime_seconds: float = 3_300.0
    pool_reconnect_timeout_seconds: float = 30.0
    connect_timeout_seconds: float = 10.0
    statement_timeout_seconds: float = 30.0
    lock_timeout_seconds: float = 5.0
    health_check_timeout_seconds: float = 5.0
    transaction_retry_limit: int = 3
    application_name: str = "temporal-retrieval-v2"

    def __post_init__(self) -> None:
        for name in ("host", "database", "user", "application_name"):
            if not getattr(self, name).strip():
                raise LakebaseConfigurationError(f"{name} must not be empty")
        if not 1 <= self.port <= 65_535:
            raise LakebaseConfigurationError("port must be between 1 and 65535")
        if self.sslmode not in _TLS_MODES:
            raise LakebaseConfigurationError(
                "sslmode must require TLS (require, verify-ca, or verify-full)"
            )
        if self.endpoint and self.password:
            raise LakebaseConfigurationError(
                "configure either LAKEBASE_ENDPOINT OAuth or a local password, not both"
            )
        if not self.endpoint and not self.password:
            raise LakebaseConfigurationError(
                "LAKEBASE_ENDPOINT is required unless a local PG/LAKEBASE password is set"
            )
        if self.endpoint and not _ENDPOINT_PATTERN.fullmatch(self.endpoint):
            raise LakebaseConfigurationError(
                "LAKEBASE_ENDPOINT must be an endpoint resource path "
                "(projects/.../branches/.../endpoints/...)"
            )
        if self.pool_min_size < 0:
            raise LakebaseConfigurationError("pool_min_size must be non-negative")
        if self.pool_max_size <= 0:
            raise LakebaseConfigurationError("pool_max_size must be positive")
        if self.pool_min_size > self.pool_max_size:
            raise LakebaseConfigurationError("pool_min_size must not exceed pool_max_size")
        positive_timeouts = (
            "pool_acquire_timeout_seconds",
            "pool_open_timeout_seconds",
            "pool_max_idle_seconds",
            "pool_max_lifetime_seconds",
            "pool_reconnect_timeout_seconds",
            "connect_timeout_seconds",
            "statement_timeout_seconds",
            "lock_timeout_seconds",
            "health_check_timeout_seconds",
        )
        for name in positive_timeouts:
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise LakebaseConfigurationError(f"{name} must be finite and positive")
        if self.transaction_retry_limit < 0:
            raise LakebaseConfigurationError("transaction_retry_limit must be non-negative")

    @property
    def uses_oauth(self) -> bool:
        return self.endpoint is not None

    @classmethod
    def from_env(
        cls,
        environ_override: Mapping[str, str] | None = None,
        *,
        default_pool_max_size: int = 10,
    ) -> LakebaseConfig:
        """Load configuration without ever constructing a clear-text DSN."""

        source = environ if environ_override is None else environ_override

        def value(canonical: str, alias: str, default: str | None = None) -> str | None:
            canonical_value = source.get(canonical)
            if canonical_value not in {None, ""}:
                return canonical_value
            alias_value = source.get(alias)
            if alias_value not in {None, ""}:
                return alias_value
            return default

        host = _required(value("PGHOST", "LAKEBASE_HOST"), "PGHOST/LAKEBASE_HOST")
        database = _required(
            value("PGDATABASE", "LAKEBASE_DATABASE"),
            "PGDATABASE/LAKEBASE_DATABASE",
        )
        user = _required(value("PGUSER", "LAKEBASE_USER"), "PGUSER/LAKEBASE_USER")
        endpoint = _optional(source.get("LAKEBASE_ENDPOINT"))
        password = _optional(value("PGPASSWORD", "LAKEBASE_PASSWORD"))

        return cls(
            host=host,
            database=database,
            user=user,
            port=_parse_int(
                value("PGPORT", "LAKEBASE_PORT", "5432"),
                "PGPORT/LAKEBASE_PORT",
            ),
            sslmode=(value("PGSSLMODE", "LAKEBASE_SSLMODE", "require") or "require").lower(),
            endpoint=endpoint,
            password=password,
            pool_min_size=_parse_int(
                source.get("LAKEBASE_POOL_MIN_SIZE", "1"),
                "LAKEBASE_POOL_MIN_SIZE",
            ),
            pool_max_size=_parse_int(
                source.get("LAKEBASE_POOL_MAX_SIZE", str(default_pool_max_size)),
                "LAKEBASE_POOL_MAX_SIZE",
            ),
            pool_acquire_timeout_seconds=_parse_float(
                source.get("LAKEBASE_POOL_ACQUIRE_TIMEOUT_SECONDS", "10"),
                "LAKEBASE_POOL_ACQUIRE_TIMEOUT_SECONDS",
            ),
            pool_open_timeout_seconds=_parse_float(
                source.get("LAKEBASE_POOL_OPEN_TIMEOUT_SECONDS", "30"),
                "LAKEBASE_POOL_OPEN_TIMEOUT_SECONDS",
            ),
            pool_max_idle_seconds=_parse_float(
                source.get("LAKEBASE_POOL_MAX_IDLE_SECONDS", "600"),
                "LAKEBASE_POOL_MAX_IDLE_SECONDS",
            ),
            pool_max_lifetime_seconds=_parse_float(
                source.get("LAKEBASE_POOL_MAX_LIFETIME_SECONDS", "3300"),
                "LAKEBASE_POOL_MAX_LIFETIME_SECONDS",
            ),
            pool_reconnect_timeout_seconds=_parse_float(
                source.get("LAKEBASE_POOL_RECONNECT_TIMEOUT_SECONDS", "30"),
                "LAKEBASE_POOL_RECONNECT_TIMEOUT_SECONDS",
            ),
            connect_timeout_seconds=_parse_float(
                source.get("LAKEBASE_CONNECT_TIMEOUT_SECONDS", "10"),
                "LAKEBASE_CONNECT_TIMEOUT_SECONDS",
            ),
            statement_timeout_seconds=_parse_float(
                source.get("LAKEBASE_STATEMENT_TIMEOUT_SECONDS", "30"),
                "LAKEBASE_STATEMENT_TIMEOUT_SECONDS",
            ),
            lock_timeout_seconds=_parse_float(
                source.get("LAKEBASE_LOCK_TIMEOUT_SECONDS", "5"),
                "LAKEBASE_LOCK_TIMEOUT_SECONDS",
            ),
            health_check_timeout_seconds=_parse_float(
                source.get("LAKEBASE_HEALTH_CHECK_TIMEOUT_SECONDS", "5"),
                "LAKEBASE_HEALTH_CHECK_TIMEOUT_SECONDS",
            ),
            transaction_retry_limit=_parse_int(
                source.get("LAKEBASE_TRANSACTION_RETRY_LIMIT", "3"),
                "LAKEBASE_TRANSACTION_RETRY_LIMIT",
            ),
            application_name=(source.get("LAKEBASE_APPLICATION_NAME", "temporal-retrieval-v2")),
        )


def _required(value: str | None, label: str) -> str:
    normalized = _optional(value)
    if normalized is None:
        raise LakebaseConfigurationError(f"{label} is required")
    return normalized


def _optional(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _parse_int(value: str | None, label: str) -> int:
    try:
        return int(value or "")
    except ValueError as exc:
        raise LakebaseConfigurationError(f"{label} must be an integer") from exc


def _parse_float(value: str | None, label: str) -> float:
    try:
        return float(value or "")
    except ValueError as exc:
        raise LakebaseConfigurationError(f"{label} must be a number") from exc
