"""Bounded provider preflight with compact, replay-safe results."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityCancellationType

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.activities.provider_api import (
        ProviderPreflightRequest,
        ProviderPreflightResult,
    )


@workflow.defn(name="ProviderPreflightWorkflow")
class ProviderPreflightWorkflow:
    @workflow.run
    async def run(self, request: ProviderPreflightRequest) -> ProviderPreflightResult:
        return await workflow.execute_activity(
            "provider_preflight",
            request,
            result_type=ProviderPreflightResult,
            task_queue=request.provider_task_queue,
            start_to_close_timeout=timedelta(minutes=2),
            schedule_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(seconds=30),
            cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=15),
                maximum_attempts=5,
                non_retryable_error_types=[
                    "InvalidCredentials",
                    "GoogleDriveScopeUnavailable",
                    "GoogleDriveRequestRejected",
                ],
            ),
        )


__all__ = ["ProviderPreflightWorkflow"]
