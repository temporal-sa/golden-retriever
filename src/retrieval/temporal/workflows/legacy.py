"""Drain-only names for deployments that still host legacy executions.

These greenfield placeholders are never started by V2. Real migrations must keep the
original worker build pinned until its real histories drain; a placeholder cannot replay
code that was not supplied with this repository.
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
