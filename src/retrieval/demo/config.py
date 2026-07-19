"""Process-only configuration for the opt-in Northstar demonstration."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass


class DemoConfigurationError(ValueError):
    """A demo setting is invalid or unsafe."""


class DemoModeDisabledError(RuntimeError):
    """Demo-only adapters were requested without the explicit feature flag."""

    status_code = 404
    error_code = "demo_disabled"


def _boolean(source: Mapping[str, str], name: str, default: bool) -> bool:
    raw = source.get(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise DemoConfigurationError(f"{name} must be a boolean")


def _positive_float(source: Mapping[str, str], name: str, default: float) -> float:
    raw = source.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise DemoConfigurationError(f"{name} must be a number") from exc
    if not math.isfinite(value) or value <= 0:
        raise DemoConfigurationError(f"{name} must be greater than zero")
    return value


@dataclass(frozen=True)
class DemoConfig:
    """Settings that must never be placed in Temporal workflow payloads."""

    enabled: bool = False
    scenario_id: str = "northstar-v1"
    hold_timeout_seconds: float = 30.0
    control_poll_seconds: float = 0.25
    store_key_prefix: str = "northstar"

    def __post_init__(self) -> None:
        if not self.scenario_id.strip():
            raise DemoConfigurationError("scenario_id must not be empty")
        if self.store_key_prefix != "northstar":
            raise DemoConfigurationError(
                "store_key_prefix must be 'northstar' to match the database seed boundary"
            )
        if (
            not math.isfinite(self.hold_timeout_seconds)
            or self.hold_timeout_seconds <= 0
            or self.hold_timeout_seconds > 30
        ):
            raise DemoConfigurationError(
                "hold_timeout_seconds must be greater than zero and at most 30 seconds"
            )
        if not math.isfinite(self.control_poll_seconds) or self.control_poll_seconds <= 0:
            raise DemoConfigurationError("control_poll_seconds must be greater than zero")

    def require_enabled(self) -> None:
        if not self.enabled:
            raise DemoModeDisabledError("Northstar demo adapters require RETRIEVAL_DEMO_MODE=true")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> DemoConfig:
        source = os.environ if environ is None else environ
        defaults = cls()
        return cls(
            enabled=_boolean(source, "RETRIEVAL_DEMO_MODE", defaults.enabled),
            scenario_id=source.get("RETRIEVAL_DEMO_SCENARIO", defaults.scenario_id).strip(),
            hold_timeout_seconds=_positive_float(
                source,
                "RETRIEVAL_DEMO_HOLD_TIMEOUT_SECONDS",
                defaults.hold_timeout_seconds,
            ),
            control_poll_seconds=_positive_float(
                source,
                "RETRIEVAL_DEMO_CONTROL_POLL_SECONDS",
                defaults.control_poll_seconds,
            ),
            store_key_prefix=source.get(
                "RETRIEVAL_DEMO_STORE_KEY_PREFIX",
                defaults.store_key_prefix,
            ).strip(),
        )


__all__ = [
    "DemoConfig",
    "DemoConfigurationError",
    "DemoModeDisabledError",
]
