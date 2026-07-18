"""Typed Search Attribute names and bounded value builders."""

from __future__ import annotations

from temporalio.common import SearchAttributeKey, TypedSearchAttributes

from retrieval.temporal.common.ids import opaque_key, opaque_store_key
from retrieval.temporal.models.operations import OperationType, WorkClass
from retrieval.temporal.models.quota import QuotaScope

STORE_KEY_HASH = SearchAttributeKey.for_keyword("StoreKeyHash")
LIFECYCLE_GENERATION = SearchAttributeKey.for_int("LifecycleGeneration")
OPERATION_TYPE = SearchAttributeKey.for_keyword("OperationType")
SYNC_SEQUENCE = SearchAttributeKey.for_keyword("SyncSequence")
PROVIDER = SearchAttributeKey.for_keyword("Provider")
QUOTA_SCOPE_HASH = SearchAttributeKey.for_keyword("QuotaScopeHash")
WORK_CLASS = SearchAttributeKey.for_keyword("WorkClass")
CURRENT_PHASE = SearchAttributeKey.for_keyword("CurrentPhase")
RESULT_STATUS = SearchAttributeKey.for_keyword("ResultStatus")


def operation_search_attributes(
    *,
    store_key: str,
    lifecycle_generation: int,
    operation_type: OperationType,
    sync_sequence: str | None = None,
    quota_scope: QuotaScope | None = None,
    work_class: WorkClass | None = None,
    current_phase: str = "accepted",
) -> TypedSearchAttributes:
    pairs = [
        STORE_KEY_HASH.value_set(opaque_store_key(store_key)),
        LIFECYCLE_GENERATION.value_set(lifecycle_generation),
        OPERATION_TYPE.value_set(operation_type.value),
        CURRENT_PHASE.value_set(current_phase),
    ]
    if sync_sequence is not None:
        pairs.append(
            SYNC_SEQUENCE.value_set(opaque_key(sync_sequence, namespace="search-sync-sequence"))
        )
    if quota_scope is not None:
        pairs.extend(
            [
                PROVIDER.value_set(quota_scope.provider),
                QUOTA_SCOPE_HASH.value_set(
                    opaque_key(
                        quota_scope.provider,
                        quota_scope.credential_key,
                        quota_scope.quota_class,
                        namespace="search-quota-scope",
                    )
                ),
            ]
        )
    if work_class is not None:
        pairs.append(WORK_CLASS.value_set(work_class.value))
    return TypedSearchAttributes(pairs)
