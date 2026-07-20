"""Store lifecycle state and generation-fence messages."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from .operations import CommandResult, OperationStatus, ResultStatus


class StoreLifecycleState(StrEnum):
    ACTIVE = "active"
    SYNCING = "syncing"
    DEACTIVATING = "deactivating"
    INACTIVE = "inactive"
    DEACTIVATION_FAILED = "deactivation_failed"


class DeactivationPhase(StrEnum):
    PENDING = "pending"
    FENCING = "fencing"
    CANCELING = "canceling"
    DRAINING = "draining"
    CLEANING_USERS = "cleaning_users"
    REMOVING_OBJECTS = "removing_objects"
    MARKING_INACTIVE = "marking_inactive"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class SyncRegistration:
    operation_id: str
    workflow_id: str
    lifecycle_generation: int
    sync_sequence: str
    status: OperationStatus = OperationStatus.RUNNING
    started_at: datetime | None = None


@dataclass
class RemediationRegistration:
    operation_id: str
    workflow_id: str
    lifecycle_generation: int
    sync_sequence: str
    status: OperationStatus = OperationStatus.RUNNING
    started_at: datetime | None = None


@dataclass
class StoreControllerState:
    store_key: str
    lifecycle_state: StoreLifecycleState = StoreLifecycleState.ACTIVE
    lifecycle_generation: int = 0
    active_syncs: dict[str, SyncRegistration] = field(default_factory=dict)
    active_remediations: dict[str, RemediationRegistration] = field(default_factory=dict)
    active_deactivation_id: str | None = None
    # A list is intentional: set serialization order is process-dependent.
    quota_workflow_ids: list[str] = field(default_factory=list)
    recent_command_results: dict[str, CommandResult] = field(default_factory=dict)
    continue_as_new_requested: bool = False
    command_dedup_window_size: int = 2_000
    deactivation_drain_timeout_seconds: float = 300.0
    object_cleanup_batch_size: int = 250
    enable_search_attributes: bool = False
    active_deactivation_fenced: bool = False
    authority_initialized: bool = False


@dataclass(frozen=True)
class StoreControllerSnapshot:
    store_key: str
    lifecycle_state: StoreLifecycleState
    lifecycle_generation: int
    active_sync_ids: tuple[str, ...] = ()
    active_remediation_ids: tuple[str, ...] = ()
    active_deactivation_id: str | None = None
    recent_command_count: int = 0


@dataclass(frozen=True)
class BeginStoreDeactivation:
    store_key: str
    expected_generation: int


@dataclass(frozen=True)
class ResumeStoreDeactivation:
    store_key: str
    lifecycle_generation: int


@dataclass(frozen=True)
class NewGeneration:
    store_key: str
    previous_generation: int
    lifecycle_generation: int
    lifecycle_state: StoreLifecycleState = StoreLifecycleState.DEACTIVATING
    transitioned_at: datetime | None = None


@dataclass(frozen=True)
class LifecycleFence:
    store_key: str
    expected_generation: int
    allowed_states: tuple[StoreLifecycleState, ...] = (
        StoreLifecycleState.ACTIVE,
        StoreLifecycleState.SYNCING,
    )


@dataclass(frozen=True)
class LifecycleMutationResult:
    store_key: str
    expected_generation: int
    authoritative_generation: int
    status: ResultStatus
    lifecycle_state: StoreLifecycleState
    message: str | None = None


@dataclass(frozen=True)
class DeactivationInput:
    store_key: str
    expected_generation: int
    command_id: str
    operation_id: str
    drain_timeout_seconds: float | None = None
    object_cleanup_batch_size: int = 250
    controller_workflow_id: str | None = None
    sync_workflow_ids: tuple[str, ...] = ()
    remediation_workflow_ids: tuple[str, ...] = ()
    quota_workflow_ids: tuple[str, ...] = ()
    enable_search_attributes: bool = False
    resume_same_generation: bool = False


@dataclass(frozen=True)
class DeactivationResult:
    store_key: str
    operation_id: str
    lifecycle_generation: int
    status: ResultStatus
    phase: DeactivationPhase
    message: str | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True)
class OperationStatusEvent:
    operation_id: str
    workflow_id: str
    lifecycle_generation: int
    status: OperationStatus
    result_status: ResultStatus | None = None
    message: str | None = None


@dataclass(frozen=True)
class RemediationStatusEvent:
    operation_id: str
    workflow_id: str
    lifecycle_generation: int
    sync_sequence: str
    status: OperationStatus
    result_status: ResultStatus | None = None
    message: str | None = None


@dataclass(frozen=True)
class DeactivationFencedEvent:
    operation_id: str
    workflow_id: str
    lifecycle_generation: int


@dataclass(frozen=True)
class OperationDrained:
    operation_id: str
    workflow_id: str


@dataclass(frozen=True)
class CleanupWorkflowInput:
    store_key: str
    lifecycle_generation: int
    user_keys: tuple[str, ...] = ()
    user_concurrency: int = 20
    object_batch_size: int = 250
    object_batch_index: int = 0
    documents_deleted: int = 0
    chunks_deleted: int = 0
