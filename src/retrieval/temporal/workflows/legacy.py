"""Optional drain-only names for deployments with existing workflow histories.

Normal execution never starts these placeholders. They cannot replay Event Histories created by
a different implementation, so those executions must remain pinned to a compatible worker build.
"""

from __future__ import annotations

from temporalio import workflow


@workflow.defn(name="QuotaWaitWorkflow")
class QuotaWaitWorkflow:
    @workflow.run
    async def run(self, payload: dict[str, object]) -> dict[str, object]:
        return {**payload, "legacy_drain_only": True}


@workflow.defn(name="AccessioningWorkflow")
class AccessioningWorkflow:
    @workflow.run
    async def run(self, payload: dict[str, object]) -> dict[str, object]:
        return {**payload, "legacy_drain_only": True}


LEGACY_DRAIN_WORKFLOWS = (QuotaWaitWorkflow, AccessioningWorkflow)
