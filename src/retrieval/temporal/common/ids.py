"""Central deterministic, PII-safe identifiers for Temporal executions.

Only static type prefixes and non-sensitive generation numbers are readable in
the resulting IDs.  Every business identifier is represented by a stable
SHA-256 digest encoded with the URL/Workflow-ID-safe base32 alphabet.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any

DEFAULT_OPAQUE_KEY_LENGTH = 26


def _json_default(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__bytes__": base64.urlsafe_b64encode(value).decode("ascii")}
    if isinstance(value, Enum):
        return {"__enum__": value.value}
    if isinstance(value, (date, datetime)):
        return {"__datetime__": value.isoformat()}
    if is_dataclass(value):
        return {"__dataclass__": asdict(value)}
    raise TypeError(f"unsupported deterministic ID component: {type(value).__name__}")


def opaque_key(
    *parts: Any,
    namespace: str = "retrieval-v2",
    length: int = DEFAULT_OPAQUE_KEY_LENGTH,
) -> str:
    """Return a deterministic opaque key for a typed tuple of components.

    Canonical JSON provides unambiguous component boundaries, so values such as
    ``("ab", "c")`` cannot collide structurally with ``("a", "bc")``.
    ``namespace`` domain-separates identifiers used for different purposes.
    """

    if not parts:
        raise ValueError("at least one ID component is required")
    if not namespace:
        raise ValueError("namespace must not be empty")
    if not 8 <= length <= 52:
        raise ValueError("opaque key length must be between 8 and 52")
    payload = json.dumps(
        {"namespace": namespace, "parts": parts},
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    return base64.b32encode(digest).decode("ascii").rstrip("=").lower()[:length]


# Compatibility-friendly descriptive alias.
deterministic_opaque_key = opaque_key


def _generation(value: int) -> str:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("lifecycle generation must be a non-negative integer")
    return str(value)


def _component(value: Any, *, namespace: str) -> str:
    return opaque_key(value, namespace=namespace)


def opaque_store_key(store_key: str) -> str:
    return _component(store_key, namespace="store")


def opaque_quota_scope_key(provider: str, credential_key: str, quota_class: str = "default") -> str:
    return opaque_key(
        provider,
        credential_key,
        quota_class,
        namespace="quota-scope",
    )


def store_controller_workflow_id(store_key: str) -> str:
    return f"store-controller/{opaque_store_key(store_key)}"


def store_sync_workflow_id(store_key: str, lifecycle_generation: int, sync_sequence: Any) -> str:
    return "/".join(
        (
            "store-sync",
            opaque_store_key(store_key),
            _generation(lifecycle_generation),
            _component(sync_sequence, namespace="sync-sequence"),
        )
    )


def failed_user_remediation_workflow_id(
    store_key: str,
    lifecycle_generation: int,
    sync_sequence: Any,
    partition: Any | None = None,
) -> str:
    identity = sync_sequence if partition is None else (sync_sequence, partition)
    return "/".join(
        (
            "failed-user-remediation",
            opaque_store_key(store_key),
            _generation(lifecycle_generation),
            _component(identity, namespace="sync-sequence"),
        )
    )


def user_sync_workflow_id(
    store_key: str,
    lifecycle_generation: int,
    sync_sequence: Any,
    user_key: str,
) -> str:
    return "/".join(
        (
            "user-sync",
            opaque_store_key(store_key),
            _generation(lifecycle_generation),
            _component(sync_sequence, namespace="sync-sequence"),
            _component(user_key, namespace="user"),
        )
    )


def resource_sync_workflow_id(
    store_key: str,
    lifecycle_generation: int,
    sync_sequence: Any,
    user_key: str,
    resource_key: str,
) -> str:
    return "/".join(
        (
            "resource-sync",
            opaque_store_key(store_key),
            _generation(lifecycle_generation),
            _component(sync_sequence, namespace="sync-sequence"),
            _component(user_key, namespace="user"),
            _component(resource_key, namespace="resource"),
        )
    )


def files_page_workflow_id(resource_workflow_id: str, page_key: Any) -> str:
    return "/".join(
        (
            "files-page",
            _component(resource_workflow_id, namespace="resource-workflow"),
            _component(page_key, namespace="page"),
        )
    )


def document_ingest_workflow_id(
    store_key: str,
    lifecycle_generation: int,
    document_key: str,
    source_version: str,
) -> str:
    return "/".join(
        (
            "document-ingest",
            opaque_store_key(store_key),
            _generation(lifecycle_generation),
            _component(document_key, namespace="document"),
            _component(source_version, namespace="source-version"),
        )
    )


def store_deactivation_workflow_id(store_key: str, lifecycle_generation: int) -> str:
    return "/".join(
        (
            "store-deactivation",
            opaque_store_key(store_key),
            _generation(lifecycle_generation),
        )
    )


def user_quota_workflow_id(provider: str, credential_key: str, quota_class: str = "default") -> str:
    """Return one Workflow ID per actual provider credential quota scope."""

    return f"user-quota/{opaque_quota_scope_key(provider, credential_key, quota_class)}"


def permit_request_id(
    *,
    store_key: str,
    lifecycle_generation: int,
    sync_sequence: Any,
    user_key: str,
    resource_key: str,
    cursor: Any,
    operation: str,
    quota_class: str,
) -> str:
    """Build the stable request identity required by the quota protocol."""

    return "permit-request/" + opaque_key(
        store_key,
        _generation(lifecycle_generation),
        sync_sequence,
        user_key,
        resource_key,
        cursor,
        operation,
        quota_class,
        namespace="permit-request",
    )


def permit_id(request_id: str, quota_window_id: str) -> str:
    """Build a deterministic reservation ID inside a quota window."""

    return "permit/" + opaque_key(
        request_id,
        quota_window_id,
        namespace="permit-reservation",
    )


# Short aliases used by workflow code without repeating the ``workflow`` noun.
store_controller_id = store_controller_workflow_id
store_sync_id = store_sync_workflow_id
failed_user_remediation_id = failed_user_remediation_workflow_id
user_sync_id = user_sync_workflow_id
resource_sync_id = resource_sync_workflow_id
files_page_id = files_page_workflow_id
document_ingest_id = document_ingest_workflow_id
store_deactivation_id = store_deactivation_workflow_id
user_quota_id = user_quota_workflow_id
