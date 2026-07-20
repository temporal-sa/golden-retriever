from __future__ import annotations

import pytest
from temporalio import workflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from retrieval.temporal.runtime_config import TemporalRuntimeConfig
from retrieval.temporal.worker import V2_WORKFLOW_TYPES, _load_adapters
from retrieval.temporal.workflows.legacy import LEGACY_DRAIN_WORKFLOWS

EXPECTED_V2_TYPES = {
    "ActivateUserWorkflow",
    "CleanupUsersWorkflow",
    "CommentsResyncWorkflow",
    "DeactivateAllUsersWorkflow",
    "DeactivateOneUserWorkflow",
    "DeactivateStoreWorkflow",
    "DeactivateUserWorkflow",
    "DocumentIngestionWorkflow",
    "FailedUserRemediationWorkflow",
    "FilesPageWorkflow",
    "RemoveObjectsWorkflow",
    "ResourcePagesWorkflow",
    "ResourceSyncWorkflow",
    "RootSyncWorkflow",
    "StoreControllerWorkflow",
    "UserQuotaWorkflow",
    "UserSyncWorkflow",
}


def test_final_inventory_has_exactly_seventeen_v2_workflow_types() -> None:
    names = {
        workflow._Definition.must_from_class(workflow_type).name
        for workflow_type in V2_WORKFLOW_TYPES
    }

    assert len(V2_WORKFLOW_TYPES) == 17
    assert names == EXPECTED_V2_TYPES
    assert "QuotaWaitWorkflow" not in names
    assert "AccessioningWorkflow" not in names


@pytest.mark.parametrize("workflow_type", V2_WORKFLOW_TYPES)
@pytest.mark.asyncio
async def test_every_v2_workflow_prepares_in_temporal_sandbox(
    workflow_type: type,
) -> None:
    definition = workflow._Definition.must_from_class(workflow_type)
    SandboxedWorkflowRunner().prepare_workflow(definition)


def test_only_two_legacy_drain_types_are_retained() -> None:
    names = {
        workflow._Definition.must_from_class(workflow_type).name
        for workflow_type in LEGACY_DRAIN_WORKFLOWS
    }
    assert names == {"QuotaWaitWorkflow", "AccessioningWorkflow"}


@pytest.mark.asyncio
async def test_partial_production_adapter_configuration_always_fails_closed() -> None:
    runtime = TemporalRuntimeConfig(
        repository_factory="example.adapters:repository",
        allow_unsafe_in_memory_adapters=True,
    )

    with pytest.raises(RuntimeError, match="all-or-nothing"):
        await _load_adapters(runtime)
