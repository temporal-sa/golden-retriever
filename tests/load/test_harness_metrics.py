"""Offline checks for load measurements; these do not generate Temporal load."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from temporalio.api.enums.v1 import EventType
from temporalio.api.history.v1 import HistoryEvent
from temporalio.client import WorkflowHistory

from .harness import DispatchSample, measure_fairness, measure_history


def _event(event_id: int, event_type: int, when: datetime) -> HistoryEvent:
    event = HistoryEvent(event_id=event_id, event_type=event_type)
    event.event_time.FromDatetime(when)
    return event


def test_history_measurement_counts_signals_and_resume_latency() -> None:
    started_at = datetime(2026, 1, 1, tzinfo=UTC)
    started = _event(
        1,
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        started_at,
    )
    started.workflow_execution_started_event_attributes.original_execution_run_id = "run-1"
    signal = _event(
        2,
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
        started_at + timedelta(seconds=1),
    )
    signal.workflow_execution_signaled_event_attributes.signal_name = "wake"
    workflow_task_started = _event(
        3,
        EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED,
        started_at + timedelta(seconds=1.25),
    )
    activity_scheduled = _event(
        4,
        EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        started_at + timedelta(seconds=2),
    )
    activity_started = _event(
        5,
        EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
        started_at + timedelta(seconds=2.5),
    )
    activity_started.activity_task_started_event_attributes.scheduled_event_id = 4
    history = WorkflowHistory(
        "workflow-1",
        (
            started,
            signal,
            workflow_task_started,
            activity_scheduled,
            activity_started,
        ),
    )

    measurement = measure_history(history)

    assert measurement.run_id == "run-1"
    assert measurement.event_count == 5
    assert measurement.signal_counts == {"wake": 1}
    assert measurement.signal_resume_latency_seconds == pytest.approx((0.25,))
    assert measurement.activity_schedule_to_start_seconds == pytest.approx((0.5,))
    assert measurement.approximate_history_bytes > 0


def test_fairness_measurement_reports_order_and_weighted_distribution() -> None:
    samples = [
        DispatchSample("large", 0, 1.0),
        DispatchSample("small", 0, 2.0),
        DispatchSample("large", 1, 3.0),
        DispatchSample("small", 1, 4.0),
        DispatchSample("large", 2, 5.0),
    ]

    measurement = measure_fairness(samples, weights={"large": 1.0, "small": 1.0})

    assert measurement.total_dispatches == 5
    assert measurement.dispatch_counts == {"large": 3, "small": 2}
    assert measurement.completion_rank_by_key == {"large": 5, "small": 4}
    assert measurement.max_consecutive_dispatches == {"large": 1, "small": 1}
    assert measurement.weighted_jain_index == pytest.approx(25 / 26)
