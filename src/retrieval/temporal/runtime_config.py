"""Process-only Temporal connection and worker deployment settings."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


def _bool(source: Mapping[str, str], name: str, default: bool = False) -> bool:
    value = source.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


@dataclass(frozen=True)
class TemporalRuntimeConfig:
    address: str = "localhost:7233"
    namespace: str = "default"
    retrieval_task_queue: str = "retrieval-v2"
    provider_task_queue: str = "retrieval-provider-v2"
    api_key: str | None = None
    tls: bool = False
    deployment_name: str = "retrieval-v2"
    build_id: str = "local"
    use_worker_versioning: bool = False
    register_legacy_drain_types: bool = False
    server_priority_fairness_supported: bool = False
    enable_search_attributes: bool = False
    allow_unsafe_in_memory_adapters: bool = False
    adapter_bundle_factory: str | None = None
    repository_factory: str | None = None
    staging_store_factory: str | None = None
    provider_gateway_factory: str | None = None

    def __post_init__(self) -> None:
        if self.api_key and not self.tls:
            raise ValueError("TEMPORAL_API_KEY requires TEMPORAL_TLS=true")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> TemporalRuntimeConfig:
        source = os.environ if environ is None else environ
        api_key = source.get("TEMPORAL_API_KEY") or None
        return cls(
            address=source.get("TEMPORAL_ADDRESS", "localhost:7233"),
            namespace=source.get("TEMPORAL_NAMESPACE", "default"),
            retrieval_task_queue=source.get("TEMPORAL_RETRIEVAL_TASK_QUEUE", "retrieval-v2"),
            provider_task_queue=source.get("TEMPORAL_PROVIDER_TASK_QUEUE", "retrieval-provider-v2"),
            api_key=api_key,
            tls=_bool(source, "TEMPORAL_TLS", bool(api_key)),
            deployment_name=source.get("TEMPORAL_DEPLOYMENT_NAME", "retrieval-v2"),
            build_id=source.get("TEMPORAL_BUILD_ID", "local"),
            use_worker_versioning=_bool(source, "TEMPORAL_USE_WORKER_VERSIONING"),
            register_legacy_drain_types=_bool(source, "TEMPORAL_REGISTER_LEGACY_DRAIN_TYPES"),
            server_priority_fairness_supported=_bool(
                source, "TEMPORAL_SERVER_PRIORITY_FAIRNESS_SUPPORTED"
            ),
            enable_search_attributes=_bool(source, "TEMPORAL_ENABLE_SEARCH_ATTRIBUTES"),
            allow_unsafe_in_memory_adapters=_bool(
                source, "RETRIEVAL_ALLOW_UNSAFE_IN_MEMORY_ADAPTERS"
            ),
            adapter_bundle_factory=source.get("RETRIEVAL_ADAPTER_BUNDLE_FACTORY") or None,
            repository_factory=source.get("RETRIEVAL_REPOSITORY_FACTORY") or None,
            staging_store_factory=source.get("RETRIEVAL_STAGING_STORE_FACTORY") or None,
            provider_gateway_factory=source.get("RETRIEVAL_PROVIDER_GATEWAY_FACTORY") or None,
        )
