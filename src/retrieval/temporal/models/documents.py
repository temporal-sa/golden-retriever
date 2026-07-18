"""Reference-only document messages safe for Workflow Event History."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from .operations import ResultStatus, WorkClass
from .quota import QuotaScope


@dataclass(frozen=True)
class DocumentRef:
    document_key: str
    source_version: str
    staging_uri: str
    content_hash: str


class DocumentMutation(StrEnum):
    UPSERT = "upsert"
    DELETE = "delete"


@dataclass(frozen=True)
class DocumentIngestionInput:
    store_key: str
    lifecycle_generation: int
    document: DocumentRef
    idempotency_key: str
    sync_sequence: str = ""
    user_key: str = ""
    resource_key: str = ""
    quota_scope: QuotaScope | None = None
    work_class: WorkClass = WorkClass.INCREMENTAL
    mutation: DocumentMutation = DocumentMutation.UPSERT


@dataclass(frozen=True)
class DocumentIngestionResult:
    document_key: str
    source_version: str
    status: ResultStatus
    lifecycle_generation: int
    content_hash: str | None = None
    message: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
