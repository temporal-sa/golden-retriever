"""Work-class priority/fairness policy with graceful SDK feature gating."""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from ..models.operations import WorkClass
from ..models.quota import QuotaScope
from .ids import opaque_quota_scope_key

WORK_CLASS_PRIORITY: dict[WorkClass, int] = {
    WorkClass.INTERACTIVE: 1,
    WorkClass.RECENT_ACTIVATION: 2,
    WorkClass.INCREMENTAL: 3,
    WorkClass.CLEANUP: 4,
    WorkClass.BACKFILL: 5,
}


@dataclass(frozen=True)
class PriorityCapability:
    enabled: bool
    sdk_supported: bool
    active: bool
    mode: str


def priority_key_for(work_class: WorkClass, *, urgent: bool = False) -> int:
    """Map work to Temporal priority keys (smaller values run first)."""

    if urgent and work_class in (WorkClass.CLEANUP, WorkClass.INTERACTIVE):
        return 1
    try:
        return WORK_CLASS_PRIORITY[work_class]
    except KeyError as exc:
        raise ValueError(f"unsupported work class: {work_class!r}") from exc


@lru_cache(maxsize=1)
def sdk_supports_priority_fairness() -> bool:
    """Return whether the installed SDK accepts Temporal Priority metadata.

    This is intentionally a runtime check instead of an unconditional import,
    allowing the same code to remain on workers that drain old executions with
    an older Temporal SDK.
    """

    try:
        common = importlib.import_module("temporalio.common")
        workflow = importlib.import_module("temporalio.workflow")
        priority_type = common.Priority
        parameters = inspect.signature(workflow.execute_activity).parameters
        return callable(priority_type) and "priority" in parameters
    except (ImportError, AttributeError, TypeError, ValueError):
        return False


def priority_capability(enabled: bool) -> PriorityCapability:
    supported = sdk_supports_priority_fairness()
    if not enabled:
        mode = "disabled_by_configuration"
    elif not supported:
        mode = "disabled_unsupported_sdk"
    else:
        mode = "priority_and_fairness"
    return PriorityCapability(
        enabled=enabled,
        sdk_supported=supported,
        active=enabled and supported,
        mode=mode,
    )


def fairness_key_for(quota_scope: QuotaScope) -> str:
    """Return an opaque fairness key no longer than Temporal's 64-byte limit."""

    return opaque_quota_scope_key(
        quota_scope.provider,
        quota_scope.credential_key,
        quota_scope.quota_class,
    )


def activity_priority_kwargs(
    work_class: WorkClass,
    quota_scope: QuotaScope,
    *,
    enabled: bool,
    urgent: bool = False,
    fairness_weight: float | None = None,
    capability_override: bool | None = None,
    priority_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Build kwargs for ``workflow.execute_activity`` when supported.

    Returning an empty mapping is the compatibility path.  Quota correctness
    is therefore independent of Priority/Fairness availability.
    ``capability_override`` and ``priority_factory`` make startup checks and
    unit tests possible without importing Temporal in callers.
    """

    if not enabled:
        return {}
    supported = (
        sdk_supports_priority_fairness() if capability_override is None else capability_override
    )
    if not supported:
        return {}

    weight = quota_scope.fairness_weight if fairness_weight is None else fairness_weight
    if not 0.001 <= weight <= 1000:
        raise ValueError("fairness weight must be between 0.001 and 1000")

    if priority_factory is None:
        try:
            common = importlib.import_module("temporalio.common")
            priority_factory = common.Priority
        except (ImportError, AttributeError):
            return {}

    priority = priority_factory(
        priority_key=priority_key_for(work_class, urgent=urgent),
        fairness_key=fairness_key_for(quota_scope),
        fairness_weight=weight,
    )
    return {"priority": priority}


# A readable alias for code that schedules provider Activities.
provider_activity_kwargs = activity_priority_kwargs
