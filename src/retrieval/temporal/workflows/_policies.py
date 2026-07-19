"""Explicit timeout, retry, child ownership, and dispatch policies."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import Priority, RetryPolicy

from retrieval.temporal.common.priorities import (
    activity_priority_kwargs,
    priority_key_for,
    sdk_supports_priority_fairness,
)
from retrieval.temporal.models.operations import WorkClass
from retrieval.temporal.models.quota import QuotaScope

NON_RETRYABLE_PROVIDER_ERRORS = [
    "InvalidCredentials",
    "InvalidDocumentPayload",
    "StaleLifecycleGenerationError",
    "ProviderQuotaExhausted",
]


def short_client_activity_options() -> dict[str, Any]:
    return {
        "start_to_close_timeout": timedelta(seconds=10),
        "schedule_to_close_timeout": timedelta(seconds=30),
        "retry_policy": RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=5),
            maximum_attempts=5,
        ),
    }


def metadata_activity_options() -> dict[str, Any]:
    return {
        "start_to_close_timeout": timedelta(seconds=30),
        "schedule_to_close_timeout": timedelta(minutes=2),
        "retry_policy": RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=10),
            maximum_attempts=5,
            non_retryable_error_types=[
                "StaleLifecycleGenerationError",
                "LifecycleStateRejectedError",
            ],
        ),
    }


def ingestion_activity_options() -> dict[str, Any]:
    return {
        "start_to_close_timeout": timedelta(minutes=15),
        "schedule_to_close_timeout": timedelta(minutes=30),
        # Demo holds are capped at 30 seconds. Keep a margin so a cancellation
        # delivered at the boundary cannot race an Activity heartbeat timeout
        # and start an unnecessary retry before the bounded hold resolves.
        "heartbeat_timeout": timedelta(seconds=45),
        "retry_policy": RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(minutes=1),
            maximum_attempts=5,
            non_retryable_error_types=[
                "InvalidDocumentPayload",
                "StaleLifecycleGenerationError",
            ],
        ),
    }


def provider_activity_options(
    *,
    task_queue: str,
    work_class: WorkClass,
    quota_scope: QuotaScope | None,
    priority_fairness_enabled: bool,
) -> dict[str, Any]:
    options: dict[str, Any] = {
        "task_queue": task_queue,
        "start_to_close_timeout": timedelta(seconds=45),
        "schedule_to_close_timeout": timedelta(minutes=3),
        "retry_policy": RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=20),
            maximum_attempts=5,
            non_retryable_error_types=NON_RETRYABLE_PROVIDER_ERRORS,
        ),
    }
    if quota_scope is not None:
        options.update(
            activity_priority_kwargs(
                work_class,
                quota_scope,
                enabled=priority_fairness_enabled,
            )
        )
    elif priority_fairness_enabled and sdk_supports_priority_fairness():
        options["priority"] = Priority(priority_key=priority_key_for(work_class))
    return options


def current_workflow_id() -> str:
    return workflow.info().workflow_id
