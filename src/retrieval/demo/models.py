"""Small process-local models for the Northstar demo and its presentation API."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from retrieval.temporal.activities.repositories import StoreSnapshot


class DemoError(RuntimeError):
    """Base error safe for an API adapter to translate."""

    status_code = 500
    error_code = "demo_error"


class DemoNotFoundError(DemoError):
    status_code = 404
    error_code = "not_found"


class DemoConflictError(DemoError):
    status_code = 409
    error_code = "conflict"


class DemoIdempotencyConflictError(DemoConflictError):
    error_code = "idempotency_conflict"


class DemoUnavailableError(DemoError):
    status_code = 503
    error_code = "unavailable"


class DemoRunStatus(StrEnum):
    READY = "ready"
    SYNCING = "syncing"
    DEACTIVATING = "deactivating"
    COMPLETED = "completed"
    FAILED = "failed"


class DemoOperationType(StrEnum):
    CREATE_RUN = "create_run"
    SYNC = "sync"
    DEACTIVATION = "deactivation"
    HOLD = "hold"
    RELEASE = "release"
    ASK = "ask"


class DemoOperationStatus(StrEnum):
    ACCEPTED = "accepted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    REJECTED = "rejected"


@dataclass(frozen=True)
class DemoRun:
    run_id: str
    store_key: str
    display_name: str
    baseline_generation: int
    status: DemoRunStatus = DemoRunStatus.READY
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None


@dataclass(frozen=True)
class DemoControls:
    run_id: str
    quota_once_pending: bool
    quota_retry_after_seconds: float
    held_document_key: str
    hold_before_commit: bool
    release_requested: bool
    control_version: int = 0
    quota_wait_request_id: str | None = None
    quota_wait_operation: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class QuotaConsumption:
    injected: bool
    retry_after_seconds: float
    waiting_request_id: str | None = None


@dataclass(frozen=True)
class DemoEvent:
    event_id: int | None
    event_key: str
    run_id: str
    store_key: str
    event_type: str
    operation_id: str | None = None
    workflow_id: str | None = None
    document_key: str | None = None
    expected_generation: int | None = None
    actual_generation: int | None = None
    details: Mapping[str, str | int | float | bool | None] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        object.__setattr__(self, "details", dict(self.details))


@dataclass(frozen=True)
class DemoOperation:
    operation_id: str
    run_id: str
    store_key: str
    operation_type: DemoOperationType
    status: DemoOperationStatus
    command_id: str
    workflow_id: str | None = None
    lifecycle_generation: int | None = None
    result: Mapping[str, Any] = field(default_factory=dict)
    message: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        object.__setattr__(self, "result", dict(self.result))


@dataclass(frozen=True)
class ApiIdempotencyReceipt:
    scope: str
    idempotency_key_hash: str
    request_hash: str
    status_code: int
    response: Mapping[str, Any]
    operation_id: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        object.__setattr__(self, "response", dict(self.response))


@dataclass(frozen=True)
class DemoSearchHit:
    document_key: str
    chunk_ordinal: int
    title: str
    text: str
    score: float
    source_uri: str | None = None
    committed_generation: int | None = None
    keyword_rank: int | None = None
    vector_rank: int | None = None


@dataclass(frozen=True)
class EvidenceCitation:
    citation_id: str
    document_key: str
    chunk_ordinal: int
    title: str
    source_uri: str | None = None


@dataclass(frozen=True)
class EvidenceAnswer:
    question: str
    answer: str
    citations: tuple[EvidenceCitation, ...]
    hits: tuple[DemoSearchHit, ...]
    backend: str
    lifecycle_generation: int


@dataclass(frozen=True)
class DemoSnapshot:
    run: DemoRun
    store: StoreSnapshot
    controls: DemoControls
    events: tuple[DemoEvent, ...] = ()
    controller: Mapping[str, Any] | None = None
    temporal_available: bool = True
    temporal_warning: str | None = None
    story_state: str = "ready"


@dataclass(frozen=True)
class DemoReadiness:
    ready: bool
    database_ready: bool
    temporal_ready: bool
    migrations_ready: bool
    search_ready: bool = True
    embeddings_ready: bool = True
    details: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "details", dict(self.details))


__all__ = [
    "ApiIdempotencyReceipt",
    "DemoConflictError",
    "DemoControls",
    "DemoError",
    "DemoEvent",
    "DemoIdempotencyConflictError",
    "DemoNotFoundError",
    "DemoOperation",
    "DemoOperationStatus",
    "DemoOperationType",
    "DemoReadiness",
    "DemoRun",
    "DemoRunStatus",
    "DemoSearchHit",
    "DemoSnapshot",
    "DemoUnavailableError",
    "EvidenceAnswer",
    "EvidenceCitation",
    "QuotaConsumption",
]
