from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from retrieval.config import RetrievalTemporalConfig
from retrieval.temporal import client as client_module
from retrieval.temporal.client import RetrievalClient
from retrieval.temporal.models.operations import SyncCommand
from retrieval.temporal.runtime_config import TemporalRuntimeConfig
from retrieval.temporal.workflows._policies import ingestion_activity_options


@pytest.mark.asyncio
async def test_from_runtime_requires_both_sdk_and_server_fairness_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        client_module,
        "priority_capability",
        lambda enabled: SimpleNamespace(active=enabled),
    )
    config = RetrievalTemporalConfig(
        temporal_enable_priority_fairness=True,
        deactivation_drain_timeout=timedelta(seconds=47),
    )
    runtime = TemporalRuntimeConfig(
        retrieval_task_queue="retrieval-custom",
        provider_task_queue="provider-custom",
        server_priority_fairness_supported=False,
        enable_search_attributes=True,
    )

    retrieval = RetrievalClient.from_runtime(cast(Any, object()), runtime=runtime, config=config)
    command = retrieval._apply_sync_policy(
        SyncCommand(
            command_id="command",
            store_key="store",
            expected_generation=0,
            sync_sequence="sequence",
        )
    )

    assert retrieval._task_queue == "retrieval-custom"
    assert retrieval._provider_task_queue == "provider-custom"
    assert retrieval._enable_search_attributes is True
    assert command.metadata["priority_fairness_enabled"] == "false"
    controller_start = retrieval._controller_start("store", 0)
    initial_state = controller_start._start_workflow_input.args[0]
    assert initial_state.deactivation_drain_timeout_seconds == 47
    assert initial_state.enable_search_attributes is True


def test_from_runtime_enables_fairness_only_after_both_gates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        client_module,
        "priority_capability",
        lambda enabled: SimpleNamespace(active=enabled),
    )
    retrieval = RetrievalClient.from_runtime(
        cast(Any, object()),
        runtime=TemporalRuntimeConfig(server_priority_fairness_supported=True),
        config=RetrievalTemporalConfig(temporal_enable_priority_fairness=True),
    )

    assert retrieval._priority_fairness_active is True


def test_ingestion_activity_has_heartbeat_timeout() -> None:
    assert ingestion_activity_options()["heartbeat_timeout"] == timedelta(seconds=45)
