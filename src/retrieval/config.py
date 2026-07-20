"""Typed, validated environment configuration for retrieval workflows."""

from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import timedelta
from types import MappingProxyType

from retrieval.temporal.models.quota import MAX_QUOTA_PENDING_REQUESTS


class ConfigurationError(ValueError):
    """Raised when a retrieval environment variable is invalid."""


_DURATION_PATTERN = re.compile(
    r"^(?P<value>(?:\d+(?:\.\d*)?|\.\d+))\s*(?P<unit>ms|s|m|h)?$",
    re.IGNORECASE,
)
_DURATION_MULTIPLIERS = MappingProxyType({"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0})
DEFAULT_DEACTIVATION_DRAIN_TIMEOUT = timedelta(minutes=5)


def _parse_int(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    return value


def _parse_optional_float(
    environ: Mapping[str, str], name: str, default: float | None
) -> float | None:
    raw = environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number") from exc
    if not math.isfinite(value):
        raise ConfigurationError(f"{name} must be a finite number")
    return value


def _parse_bool(environ: Mapping[str, str], name: str, default: bool) -> bool:
    raw = environ.get(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} must be one of true/false, yes/no, on/off, or 1/0")


def _parse_duration(environ: Mapping[str, str], name: str, default: timedelta) -> timedelta:
    raw = environ.get(name)
    if raw is None or not raw.strip():
        return default
    match = _DURATION_PATTERN.fullmatch(raw.strip())
    if match is None:
        raise ConfigurationError(f"{name} must be seconds or a duration ending in ms, s, m, or h")
    value = float(match.group("value"))
    unit = (match.group("unit") or "s").lower()
    return timedelta(seconds=value * _DURATION_MULTIPLIERS[unit])


@dataclass(frozen=True)
class RetrievalTemporalConfig:
    """All bounded-concurrency and Temporal workflow feature settings.

    Defaults are deliberately finite.  ``from_env`` is the only place that
    reads process environment, which keeps workflow modules deterministic and
    makes configuration tests independent of global state.
    """

    store_sync_max_active_users: int = 20
    store_sync_user_page_size: int = 100
    round_user_window_size: int = 20
    round_page_slice_size: int = 5
    resource_concurrency: int = 8
    files_page_window_size: int = 5
    files_per_page_concurrency: int = 10
    document_ingestion_concurrency: int = 20
    object_cleanup_batch_size: int = 250
    user_quota_max_in_flight: int = 4
    user_quota_max_pending_requests: int = MAX_QUOTA_PENDING_REQUESTS
    user_quota_dedup_window_size: int = 2_000
    user_quota_continue_as_new_message_count: int = 10_000
    deactivation_drain_timeout: timedelta = DEFAULT_DEACTIVATION_DRAIN_TIMEOUT
    temporal_enable_priority_fairness: bool = False
    temporal_provider_queue_rps: float | None = None
    temporal_fairness_key_rps_default: float | None = None

    def __post_init__(self) -> None:
        positive_integers = (
            "store_sync_max_active_users",
            "store_sync_user_page_size",
            "round_user_window_size",
            "round_page_slice_size",
            "resource_concurrency",
            "files_page_window_size",
            "files_per_page_concurrency",
            "document_ingestion_concurrency",
            "object_cleanup_batch_size",
            "user_quota_max_in_flight",
            "user_quota_max_pending_requests",
            "user_quota_dedup_window_size",
            "user_quota_continue_as_new_message_count",
        )
        for name in positive_integers:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ConfigurationError(f"{name} must be a positive integer")

        if self.deactivation_drain_timeout <= timedelta(0):
            raise ConfigurationError("deactivation_drain_timeout must be greater than zero")
        for name in (
            "temporal_provider_queue_rps",
            "temporal_fairness_key_rps_default",
        ):
            value = getattr(self, name)
            if value is not None and (not math.isfinite(value) or value <= 0):
                raise ConfigurationError(f"{name} must be greater than zero when set")

        if self.user_quota_dedup_window_size < self.user_quota_max_in_flight:
            raise ConfigurationError(
                "user_quota_dedup_window_size must be at least user_quota_max_in_flight"
            )
        if self.user_quota_max_pending_requests < self.user_quota_max_in_flight:
            raise ConfigurationError(
                "user_quota_max_pending_requests must be at least user_quota_max_in_flight"
            )
        if self.user_quota_max_pending_requests > MAX_QUOTA_PENDING_REQUESTS:
            raise ConfigurationError(
                "user_quota_max_pending_requests must not exceed "
                f"{MAX_QUOTA_PENDING_REQUESTS} to keep workflow payloads bounded"
            )

    @property
    def deactivation_drain_timeout_seconds(self) -> float:
        return self.deactivation_drain_timeout.total_seconds()

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> RetrievalTemporalConfig:
        source = os.environ if environ is None else environ
        defaults = cls()
        return cls(
            store_sync_max_active_users=_parse_int(
                source,
                "STORE_SYNC_MAX_ACTIVE_USERS",
                defaults.store_sync_max_active_users,
            ),
            store_sync_user_page_size=_parse_int(
                source,
                "STORE_SYNC_USER_PAGE_SIZE",
                defaults.store_sync_user_page_size,
            ),
            round_user_window_size=_parse_int(
                source,
                "ROUND_USER_WINDOW_SIZE",
                defaults.round_user_window_size,
            ),
            round_page_slice_size=_parse_int(
                source,
                "ROUND_PAGE_SLICE_SIZE",
                defaults.round_page_slice_size,
            ),
            resource_concurrency=_parse_int(
                source, "RESOURCE_CONCURRENCY", defaults.resource_concurrency
            ),
            files_page_window_size=_parse_int(
                source,
                "FILES_PAGE_WINDOW_SIZE",
                defaults.files_page_window_size,
            ),
            files_per_page_concurrency=_parse_int(
                source,
                "FILES_PER_PAGE_CONCURRENCY",
                defaults.files_per_page_concurrency,
            ),
            document_ingestion_concurrency=_parse_int(
                source,
                "DOCUMENT_INGESTION_CONCURRENCY",
                defaults.document_ingestion_concurrency,
            ),
            object_cleanup_batch_size=_parse_int(
                source,
                "OBJECT_CLEANUP_BATCH_SIZE",
                defaults.object_cleanup_batch_size,
            ),
            user_quota_max_in_flight=_parse_int(
                source,
                "USER_QUOTA_MAX_IN_FLIGHT",
                defaults.user_quota_max_in_flight,
            ),
            user_quota_max_pending_requests=_parse_int(
                source,
                "USER_QUOTA_MAX_PENDING_REQUESTS",
                defaults.user_quota_max_pending_requests,
            ),
            user_quota_dedup_window_size=_parse_int(
                source,
                "USER_QUOTA_DEDUP_WINDOW_SIZE",
                defaults.user_quota_dedup_window_size,
            ),
            user_quota_continue_as_new_message_count=_parse_int(
                source,
                "USER_QUOTA_CONTINUE_AS_NEW_MESSAGE_COUNT",
                defaults.user_quota_continue_as_new_message_count,
            ),
            deactivation_drain_timeout=_parse_duration(
                source,
                "DEACTIVATION_DRAIN_TIMEOUT",
                defaults.deactivation_drain_timeout,
            ),
            temporal_enable_priority_fairness=_parse_bool(
                source,
                "TEMPORAL_ENABLE_PRIORITY_FAIRNESS",
                defaults.temporal_enable_priority_fairness,
            ),
            temporal_provider_queue_rps=_parse_optional_float(
                source,
                "TEMPORAL_PROVIDER_QUEUE_RPS",
                defaults.temporal_provider_queue_rps,
            ),
            temporal_fairness_key_rps_default=_parse_optional_float(
                source,
                "TEMPORAL_FAIRNESS_KEY_RPS_DEFAULT",
                defaults.temporal_fairness_key_rps_default,
            ),
        )

    @classmethod
    def environment_variables(cls) -> tuple[str, ...]:
        """Return the complete public environment surface for diagnostics."""

        return tuple(field.name.upper() for field in fields(cls))


# Concise names for callers while retaining an explicit primary type name.
TemporalRetrievalConfig = RetrievalTemporalConfig
RetrievalConfig = RetrievalTemporalConfig


def load_config(environ: Mapping[str, str] | None = None) -> RetrievalTemporalConfig:
    return RetrievalTemporalConfig.from_env(environ)
