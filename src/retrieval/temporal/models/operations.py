"""Operation commands and results shared by retrieval workflows.

The types in this module deliberately contain data only.  Keeping workflow
messages as dataclasses made up of JSON-converter-friendly values lets them be
used with Temporal's default data converter and carried through
Continue-As-New without custom codecs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class WorkClass(StrEnum):
    """Scheduling class for provider-facing work."""

    INTERACTIVE = "interactive"
    INCREMENTAL = "incremental"
    RECENT_ACTIVATION = "recent_activation"
    BACKFILL = "backfill"
    CLEANUP = "cleanup"


class OperationType(StrEnum):
    SYNC = "sync"
    REMEDIATION = "remediation"
    DEACTIVATION = "deactivation"
    CLEANUP = "cleanup"


class OperationStatus(StrEnum):
    ACCEPTED = "accepted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    REJECTED = "rejected"


class ResultStatus(StrEnum):
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELED = "canceled"
    REJECTED = "rejected"
    STALE_GENERATION = "stale_generation"


@dataclass(frozen=True)
class SyncCommand:
    command_id: str
    store_key: str
    expected_generation: int
    sync_sequence: str
    work_class: WorkClass = WorkClass.INCREMENTAL
    requested_at: datetime | None = None
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class CancelSyncCommand:
    command_id: str
    store_key: str
    operation_id: str
    reason: str | None = None
    requested_at: datetime | None = None


@dataclass(frozen=True)
class StartDeactivationCommand:
    command_id: str
    store_key: str
    expected_generation: int
    requested_at: datetime | None = None


# A shorter public alias is convenient for client code.
DeactivateStoreCommand = StartDeactivationCommand


@dataclass(frozen=True)
class OperationAccepted:
    command_id: str
    operation_id: str
    workflow_id: str
    operation_type: OperationType
    lifecycle_generation: int
    duplicate: bool = False


@dataclass(frozen=True)
class CancellationAccepted:
    command_id: str
    operation_id: str
    accepted: bool = True
    duplicate: bool = False
    reason: str | None = None


@dataclass(frozen=True)
class CommandResult:
    command_id: str
    operation_id: str
    operation_type: OperationType
    status: OperationStatus
    lifecycle_generation: int
    workflow_id: str | None = None
    result_status: ResultStatus | None = None
    message: str | None = None
    completed_at: datetime | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OperationResult:
    operation_id: str
    operation_type: OperationType
    status: ResultStatus
    lifecycle_generation: int
    message: str | None = None
    completed_at: datetime | None = None
    counts: dict[str, int] = field(default_factory=dict)
