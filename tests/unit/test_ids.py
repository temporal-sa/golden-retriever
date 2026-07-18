from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from temporalio.converter import DataConverter

from retrieval.temporal.common.ids import (
    document_ingest_workflow_id,
    failed_user_remediation_workflow_id,
    opaque_key,
    permit_id,
    permit_request_id,
    store_controller_workflow_id,
    store_deactivation_workflow_id,
    store_sync_workflow_id,
    user_quota_workflow_id,
)
from retrieval.temporal.common.priorities import (
    activity_priority_kwargs,
    fairness_key_for,
    priority_key_for,
)
from retrieval.temporal.models import (
    PermitRequest,
    QuotaScope,
    UserQuotaState,
    WorkClass,
)


def test_opaque_key_is_deterministic_and_component_boundaries_are_distinct() -> None:
    assert opaque_key("store", 7) == opaque_key("store", 7)
    assert opaque_key("ab", "c") != opaque_key("a", "bc")
    assert opaque_key(1) != opaque_key("1")


def test_workflow_ids_hide_business_values_and_keep_expected_prefixes() -> None:
    store = "customer@example.com"
    credential = "secret-api-token"
    ids = (
        store_controller_workflow_id(store),
        store_sync_workflow_id(store, 4, "sync-123"),
        failed_user_remediation_workflow_id(store, 4, "sync-123"),
        document_ingest_workflow_id(store, 4, "invoice.pdf", "v17"),
        store_deactivation_workflow_id(store, 5),
        user_quota_workflow_id("provider", credential, "reads"),
    )
    assert ids[0].startswith("store-controller/")
    assert ids[1].startswith("store-sync/")
    assert ids[2].startswith("failed-user-remediation/")
    assert ids[3].startswith("document-ingest/")
    assert ids[4].startswith("store-deactivation/")
    assert ids[5].startswith("user-quota/")
    assert all(store not in workflow_id for workflow_id in ids)
    assert all(credential not in workflow_id for workflow_id in ids)


def test_permit_request_identity_uses_all_stable_business_context() -> None:
    context = {
        "store_key": "store",
        "lifecycle_generation": 3,
        "sync_sequence": "42",
        "user_key": "user",
        "resource_key": "files",
        "cursor": "page-2",
        "operation": "list-files",
        "quota_class": "reads",
    }
    request_id = permit_request_id(**context)
    assert request_id == permit_request_id(**context)
    assert request_id != permit_request_id(**(context | {"cursor": "page-3"}))
    assert permit_id(request_id, "window-1") == permit_id(request_id, "window-1")
    assert permit_id(request_id, "window-1") != permit_id(request_id, "window-2")


def test_negative_generation_is_rejected() -> None:
    try:
        store_deactivation_workflow_id("store", -1)
    except ValueError as exc:
        assert "generation" in str(exc)
    else:
        raise AssertionError("negative generation was accepted")


def test_work_class_priority_mapping_and_urgent_cleanup() -> None:
    assert priority_key_for(WorkClass.INTERACTIVE) == 1
    assert priority_key_for(WorkClass.RECENT_ACTIVATION) == 2
    assert priority_key_for(WorkClass.INCREMENTAL) == 3
    assert priority_key_for(WorkClass.CLEANUP) == 4
    assert priority_key_for(WorkClass.BACKFILL) == 5
    assert priority_key_for(WorkClass.CLEANUP, urgent=True) == 1


def test_priority_kwargs_are_capability_safe_and_use_opaque_fairness_key() -> None:
    scope = QuotaScope(provider="provider", credential_key="credential-secret", quota_class="reads")
    assert (
        activity_priority_kwargs(
            WorkClass.INCREMENTAL,
            scope,
            enabled=True,
            capability_override=False,
        )
        == {}
    )

    captured: dict[str, object] = {}

    def fake_priority(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return kwargs

    result = activity_priority_kwargs(
        WorkClass.BACKFILL,
        scope,
        enabled=True,
        capability_override=True,
        priority_factory=fake_priority,
    )
    assert result["priority"] == captured
    assert captured["priority_key"] == 5
    assert captured["fairness_key"] == fairness_key_for(scope)
    assert "credential-secret" not in str(captured["fairness_key"])
    assert len(str(captured["fairness_key"]).encode()) <= 64


def test_quota_models_round_trip_through_temporal_default_converter() -> None:
    scope = QuotaScope("provider", "opaque-credential", "reads")
    request = PermitRequest(
        request_id="request-1",
        requester_workflow_id="workflow-1",
        store_key="opaque-store",
        lifecycle_generation=2,
        quota_scope=scope,
        requested_at=datetime(2026, 7, 18, tzinfo=UTC),
    )
    state = UserQuotaState(
        quota_scope=scope,
        pending={request.request_id: request},
        pending_order=[request.request_id],
        recent_terminal_request_ids=["old-request"],
        recent_terminal_request_order=["old-request"],
    )

    async def round_trip() -> UserQuotaState:
        converter = DataConverter.default
        payloads = await converter.encode([state])
        return (await converter.decode(payloads, [UserQuotaState]))[0]

    decoded = asyncio.run(round_trip())
    assert decoded == state
    assert decoded.pending[request.request_id].requested_at == request.requested_at
