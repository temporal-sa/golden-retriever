"""Messages and durable state for the shared quota coordinator."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from .operations import WorkClass


@dataclass(frozen=True)
class QuotaScope:
    """The actual external provider quota scope.

    ``credential_key`` must be an opaque key, never a raw credential.  Multiple
    application users that share this value intentionally share one quota
    workflow.
    """

    provider: str
    credential_key: str
    quota_class: str = "default"
    # Scheduling weight is policy metadata, not external quota identity.  It is
    # deliberately excluded from equality because Workflow ID scope excludes it.
    fairness_weight: float = field(default=1.0, compare=False)


class PermitStatus(StrEnum):
    PENDING = "pending"
    GRANTED = "granted"
    COMPLETED = "completed"
    CANCELED = "canceled"


@dataclass(frozen=True)
class PermitRequest:
    request_id: str
    requester_workflow_id: str
    store_key: str
    lifecycle_generation: int
    quota_scope: QuotaScope
    cost: int = 1
    work_class: WorkClass = WorkClass.INCREMENTAL
    requested_at: datetime | None = None


@dataclass(frozen=True)
class CancelPermit:
    request_id: str
    reason: str | None = None


@dataclass(frozen=True)
class CancelGenerationPermits:
    store_key: str
    lifecycle_generation: int


@dataclass(frozen=True)
class PermitCompleted:
    request_id: str
    permit_id: str
    completed_at: datetime | None = None


@dataclass
class PermitReservation:
    request_id: str
    permit_id: str
    requester_workflow_id: str
    cost: int
    quota_window_id: str
    granted_at: datetime | None = None
    status: PermitStatus = PermitStatus.GRANTED
    store_key: str = ""
    lifecycle_generation: int = 0


@dataclass(frozen=True)
class PermitGrant:
    request_id: str
    permit_id: str
    quota_scope: QuotaScope
    quota_window_id: str
    cost: int


@dataclass(frozen=True)
class QuotaObservation:
    quota_scope: QuotaScope
    request_id: str
    limit: int | None = None
    remaining: int | None = None
    reset_at: datetime | None = None
    retry_after_seconds: float | None = None
    exhausted: bool = False


@dataclass(frozen=True)
class DisableQuotaScope:
    reason: str | None = None
    disabled_at: datetime | None = None


@dataclass
class UserQuotaState:
    quota_scope: QuotaScope
    configured_limit: int | None = None
    remaining: int | None = None
    reset_at: datetime | None = None
    blocked_until: datetime | None = None
    max_in_flight: int = 1
    in_flight: int = 0
    disabled: bool = False
    pending: dict[str, PermitRequest] = field(default_factory=dict)
    pending_order: list[str] = field(default_factory=list)
    reservations: dict[str, PermitReservation] = field(default_factory=dict)
    # Lists keep Continue-As-New payload encoding deterministic.  Temporal's
    # JSON converter serializes sets in hash iteration order.
    recent_terminal_request_ids: list[str] = field(default_factory=list)
    # The companion order is required for deterministic bounded eviction.  A
    # set alone must never be iterated or popped to choose the oldest request.
    recent_terminal_request_order: list[str] = field(default_factory=list)
    quota_window_id: str = "initial"
    processed_message_count: int = 0
    dedup_window_size: int = 2_000
    continue_as_new_message_count: int = 10_000


@dataclass(frozen=True)
class QuotaSnapshot:
    quota_scope: QuotaScope
    configured_limit: int | None
    remaining: int | None
    reset_at: datetime | None
    blocked_until: datetime | None
    max_in_flight: int
    in_flight: int
    disabled: bool
    pending_count: int
    reservation_count: int
    quota_window_id: str


@dataclass(frozen=True)
class QuotaActivityResult:
    """Provider activity result without a large provider payload."""

    request_id: str
    succeeded: bool
    observation: QuotaObservation | None = None
    error_type: str | None = None
    error_message: str | None = None
    retryable: bool = False
