"""Replay-safe, low-cardinality Temporal metric helpers.

Metric emission must never change workflow or Activity behavior.  This module
therefore obtains meters lazily from the active SDK context, falls back to the
SDK no-op meter outside a context, filters attributes to an explicit bounded
schema, and contains exporter/instrument failures.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import Any

from temporalio import activity, workflow
from temporalio.common import MetricMeter

QUOTA_REQUESTS = "retrieval_quota_permit_requests"
QUOTA_GRANTS = "retrieval_quota_permits_granted"
QUOTA_GRANT_SIGNAL_FAILURES = "retrieval_quota_grant_signal_failures"
QUOTA_PENDING = "retrieval_quota_pending_requests"
QUOTA_IN_FLIGHT = "retrieval_quota_in_flight"
QUOTA_BLOCKED = "retrieval_quota_scope_blocked"
QUOTA_WAIT_DURATION = "retrieval_quota_wait_duration_ms"
PROVIDER_REQUESTS = "retrieval_provider_requests"
PROVIDER_QUOTA_EXHAUSTED = "retrieval_provider_quota_exhausted"
LIFECYCLE_TRANSITIONS = "retrieval_lifecycle_transitions"
STALE_GENERATION_REJECTIONS = "retrieval_stale_generation_rejections"
INGESTION_RESULTS = "retrieval_document_ingestion_results"
DEACTIVATION_DRAIN_DURATION = "retrieval_deactivation_drain_duration_ms"


# IDs, hashes, cursors, and operation/request/workflow identities are
# intentionally absent.  These dimensions remain small operational classes.
_ALLOWED_ATTRIBUTE_KEYS = frozenset(
    {
        "mutation",
        "operation",
        "provider",
        "quota_class",
        "reason",
        "status",
        "transition",
        "work_class",
    }
)
_MAX_ATTRIBUTE_VALUE_LENGTH = 64
_OVERFLOW_ATTRIBUTE_VALUE = "other"


def bounded_metric_attributes(
    attributes: Mapping[str, object] | None = None,
    /,
    **additional: object,
) -> dict[str, str | int | float | bool]:
    """Filter metric attributes to the public low-cardinality schema.

    Unknown keys are dropped rather than raising so accidental observability
    changes cannot fail business logic.  Oversized strings collapse into one
    bucket instead of retaining a high-cardinality prefix.
    """

    combined = dict(attributes or {})
    combined.update(additional)
    bounded: dict[str, str | int | float | bool] = {}
    for key, raw_value in combined.items():
        if key not in _ALLOWED_ATTRIBUTE_KEYS or raw_value is None:
            continue
        value = raw_value.value if isinstance(raw_value, Enum) else raw_value
        if isinstance(value, bool):
            bounded[key] = value
        elif isinstance(value, int | float):
            bounded[key] = value
        else:
            text = str(value)
            bounded[key] = (
                text
                if len(text.encode("utf-8")) <= _MAX_ATTRIBUTE_VALUE_LENGTH
                else _OVERFLOW_ATTRIBUTE_VALUE
            )
    return bounded


class SafeMetricEmitter:
    """Small exception-contained facade over a Temporal ``MetricMeter``."""

    def __init__(
        self,
        meter: MetricMeter,
        attributes: Mapping[str, object] | None = None,
    ) -> None:
        self._meter = meter
        self._attributes = bounded_metric_attributes(attributes)

    def _attributes_for(
        self, additional: Mapping[str, object] | None
    ) -> dict[str, str | int | float | bool]:
        return bounded_metric_attributes(self._attributes, **dict(additional or {}))

    def increment(
        self,
        name: str,
        value: int = 1,
        *,
        attributes: Mapping[str, object] | None = None,
        description: str | None = None,
        unit: str | None = None,
    ) -> None:
        try:
            if value < 0:
                return
            self._meter.create_counter(name, description, unit).add(
                value, self._attributes_for(attributes)
            )
        except Exception:
            # Observability is intentionally fail-open.
            return

    def gauge(
        self,
        name: str,
        value: int,
        *,
        attributes: Mapping[str, object] | None = None,
        description: str | None = None,
        unit: str | None = None,
    ) -> None:
        try:
            self._meter.create_gauge(name, description, unit).set(
                value, self._attributes_for(attributes)
            )
        except Exception:
            return

    def histogram(
        self,
        name: str,
        value: int,
        *,
        attributes: Mapping[str, object] | None = None,
        description: str | None = None,
        unit: str | None = None,
    ) -> None:
        try:
            if value < 0:
                return
            self._meter.create_histogram(name, description, unit).record(
                value, self._attributes_for(attributes)
            )
        except Exception:
            return


def _safe_emitter(meter_getter: Any, attributes: Mapping[str, object] | None) -> SafeMetricEmitter:
    try:
        meter = meter_getter()
    except Exception:
        meter = MetricMeter.noop
    return SafeMetricEmitter(meter, attributes)


def workflow_metrics(**attributes: object) -> SafeMetricEmitter:
    """Return an emitter backed by the current replay-aware workflow meter."""

    return _safe_emitter(workflow.metric_meter, attributes)


def activity_metrics(**attributes: object) -> SafeMetricEmitter:
    """Return an emitter backed by the current Activity meter."""

    return _safe_emitter(activity.metric_meter, attributes)


__all__ = [
    "DEACTIVATION_DRAIN_DURATION",
    "INGESTION_RESULTS",
    "LIFECYCLE_TRANSITIONS",
    "PROVIDER_QUOTA_EXHAUSTED",
    "PROVIDER_REQUESTS",
    "QUOTA_BLOCKED",
    "QUOTA_GRANTS",
    "QUOTA_GRANT_SIGNAL_FAILURES",
    "QUOTA_IN_FLIGHT",
    "QUOTA_PENDING",
    "QUOTA_REQUESTS",
    "QUOTA_WAIT_DURATION",
    "STALE_GENERATION_REJECTIONS",
    "SafeMetricEmitter",
    "activity_metrics",
    "bounded_metric_attributes",
    "workflow_metrics",
]
