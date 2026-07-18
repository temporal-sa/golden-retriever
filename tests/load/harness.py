"""Reusable measurements for opt-in Temporal retrieval load scenarios."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from math import fsum
from typing import Any

from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowHistory


def _event_time(event: Any) -> datetime:
    return event.event_time.ToDatetime(tzinfo=UTC)


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = round((len(ordered) - 1) * percentile)
    return ordered[index]


@dataclass(frozen=True)
class HistoryMeasurement:
    workflow_id: str
    run_id: str
    event_count: int
    approximate_history_bytes: int
    event_type_counts: dict[str, int]
    signal_counts: dict[str, int]
    signal_resume_latency_seconds: tuple[float, ...]
    activity_schedule_to_start_seconds: tuple[float, ...]

    @property
    def signal_count(self) -> int:
        return sum(self.signal_counts.values())

    def summary(self) -> dict[str, Any]:
        result = asdict(self)
        result["signal_count"] = self.signal_count
        result["signal_resume_p50_seconds"] = _percentile(self.signal_resume_latency_seconds, 0.50)
        result["signal_resume_p95_seconds"] = _percentile(self.signal_resume_latency_seconds, 0.95)
        result["activity_schedule_to_start_p95_seconds"] = _percentile(
            self.activity_schedule_to_start_seconds, 0.95
        )
        return result


@dataclass(frozen=True)
class DispatchSample:
    fairness_key: str
    ordinal: int
    started_at: float


@dataclass(frozen=True)
class FairnessMeasurement:
    total_dispatches: int
    dispatch_counts: dict[str, int]
    weighted_jain_index: float
    completion_rank_by_key: dict[str, int]
    max_consecutive_dispatches: dict[str, int]

    def summary(self) -> dict[str, Any]:
        return asdict(self)


def measure_history(history: WorkflowHistory) -> HistoryMeasurement:
    """Measure history growth, signals, and server-recorded dispatch latency."""

    type_counts: Counter[str] = Counter()
    signal_counts: Counter[str] = Counter()
    pending_signal_times: list[datetime] = []
    signal_resume_latencies: list[float] = []
    activity_scheduled_at: dict[int, datetime] = {}
    activity_latencies: list[float] = []

    for event in history.events:
        event_name = EventType.Name(event.event_type)
        type_counts[event_name] += 1

        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
            attributes = event.workflow_execution_signaled_event_attributes
            signal_counts[attributes.signal_name] += 1
            pending_signal_times.append(_event_time(event))
        elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED:
            started_at = _event_time(event)
            signal_resume_latencies.extend(
                max(0.0, (started_at - signaled_at).total_seconds())
                for signaled_at in pending_signal_times
            )
            pending_signal_times.clear()
        elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            activity_scheduled_at[event.event_id] = _event_time(event)
        elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            scheduled_event_id = event.activity_task_started_event_attributes.scheduled_event_id
            scheduled_at = activity_scheduled_at.get(scheduled_event_id)
            if scheduled_at is not None:
                activity_latencies.append(
                    max(0.0, (_event_time(event) - scheduled_at).total_seconds())
                )

    return HistoryMeasurement(
        workflow_id=history.workflow_id,
        run_id=history.run_id,
        event_count=len(history.events),
        approximate_history_bytes=sum(event.ByteSize() for event in history.events),
        event_type_counts=dict(sorted(type_counts.items())),
        signal_counts=dict(sorted(signal_counts.items())),
        signal_resume_latency_seconds=tuple(signal_resume_latencies),
        activity_schedule_to_start_seconds=tuple(activity_latencies),
    )


def measure_fairness(
    samples: Iterable[DispatchSample],
    *,
    weights: Mapping[str, float] | None = None,
) -> FairnessMeasurement:
    """Summarize observed dispatch order without asserting a global ordering guarantee.

    Temporal Fairness is approximate across Task Queue partitions.  The weighted Jain
    index normalizes dispatch counts by configured weight; completion rank and maximum
    consecutive dispatches make starvation or long monopolizing runs visible.
    """

    ordered = sorted(samples, key=lambda sample: (sample.started_at, sample.ordinal))
    counts = Counter(sample.fairness_key for sample in ordered)
    effective_weights = {key: (weights or {}).get(key, 1.0) for key in counts}
    if any(weight <= 0 for weight in effective_weights.values()):
        raise ValueError("fairness weights must be greater than zero")
    normalized = [count / effective_weights[key] for key, count in sorted(counts.items())]
    squared_sum = fsum(value * value for value in normalized)
    jain = (
        (fsum(normalized) ** 2) / (len(normalized) * squared_sum)
        if normalized and squared_sum
        else 1.0
    )

    completion_rank: dict[str, int] = {}
    max_consecutive: Counter[str] = Counter()
    previous_key: str | None = None
    current_run = 0
    for rank, sample in enumerate(ordered, start=1):
        completion_rank[sample.fairness_key] = rank
        if sample.fairness_key == previous_key:
            current_run += 1
        else:
            previous_key = sample.fairness_key
            current_run = 1
        max_consecutive[sample.fairness_key] = max(
            max_consecutive[sample.fairness_key], current_run
        )

    return FairnessMeasurement(
        total_dispatches=len(ordered),
        dispatch_counts=dict(sorted(counts.items())),
        weighted_jain_index=jain,
        completion_rank_by_key=dict(sorted(completion_rank.items())),
        max_consecutive_dispatches=dict(sorted(max_consecutive.items())),
    )


class TemporalLoadHarness:
    """Read-only collector for histories produced by a load scenario."""

    def __init__(self, client: Client) -> None:
        self._client = client

    async def collect_history(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
    ) -> HistoryMeasurement:
        history = await self._client.get_workflow_handle(
            workflow_id,
            run_id=run_id,
        ).fetch_history()
        return measure_history(history)


__all__ = [
    "DispatchSample",
    "FairnessMeasurement",
    "HistoryMeasurement",
    "TemporalLoadHarness",
    "measure_fairness",
    "measure_history",
]
