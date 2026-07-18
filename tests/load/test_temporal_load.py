"""Opt-in signal/history/fairness load harness.

This module never starts a server or generates load unless ``RUN_TEMPORAL_LOAD=1``.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import timedelta
from time import monotonic
from uuid import uuid4

import pytest
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import Priority
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from .harness import (
    DispatchSample,
    TemporalLoadHarness,
    measure_fairness,
)

RUN_LOAD = os.getenv("RUN_TEMPORAL_LOAD") == "1"
pytestmark = [
    pytest.mark.load,
    pytest.mark.skipif(
        not RUN_LOAD,
        reason="set RUN_TEMPORAL_LOAD=1 to run the Temporal load harness",
    ),
]


@dataclass(frozen=True)
class SignalLoadInput:
    expected_signals: int


@workflow.defn(name="SignalLoadWorkflow")
class SignalLoadWorkflow:
    def __init__(self) -> None:
        self._received = 0

    @workflow.signal(name="wake")
    def wake(self, _sequence: int) -> None:
        self._received += 1

    @workflow.run
    async def run(self, input: SignalLoadInput) -> int:
        await workflow.wait_condition(lambda: self._received >= input.expected_signals)
        return self._received


@dataclass(frozen=True)
class FairnessLoadInput:
    fairness_key: str
    fairness_weight: float
    activity_count: int


@dataclass(frozen=True)
class DispatchActivityInput:
    fairness_key: str
    ordinal: int


@workflow.defn(name="FairnessLoadWorkflow")
class FairnessLoadWorkflow:
    @workflow.run
    async def run(self, input: FairnessLoadInput) -> int:
        handles = [
            workflow.start_activity(
                "record_load_dispatch",
                DispatchActivityInput(input.fairness_key, ordinal),
                result_type=int,
                start_to_close_timeout=timedelta(seconds=30),
                priority=Priority(
                    priority_key=3,
                    fairness_key=input.fairness_key,
                    fairness_weight=input.fairness_weight,
                ),
            )
            for ordinal in range(input.activity_count)
        ]
        await asyncio.gather(*handles)
        return len(handles)


class DispatchRecorder:
    def __init__(self) -> None:
        self.samples: list[DispatchSample] = []

    @activity.defn(name="record_load_dispatch")
    async def record(self, input: DispatchActivityInput) -> int:
        self.samples.append(
            DispatchSample(
                fairness_key=input.fairness_key,
                ordinal=input.ordinal,
                started_at=monotonic(),
            )
        )
        # Keep one Worker slot occupied briefly so a ready backlog forms.
        await asyncio.sleep(0.002)
        return input.ordinal


@asynccontextmanager
async def _load_environment() -> AsyncIterator[WorkflowEnvironment]:
    address = os.getenv("TEMPORAL_LOAD_ADDRESS")
    if address:
        api_key = os.getenv("TEMPORAL_LOAD_API_KEY")
        client = await Client.connect(
            address,
            namespace=os.getenv("TEMPORAL_LOAD_NAMESPACE", "default"),
            api_key=api_key,
            tls=bool(api_key),
        )
        yield WorkflowEnvironment.from_client(client)
        return
    async with await WorkflowEnvironment.start_local() as environment:
        yield environment


async def test_signal_history_resume_latency_and_fairness_load() -> None:
    suffix = uuid4().hex
    task_queue = f"retrieval-load-{suffix}"
    signal_count = int(os.getenv("TEMPORAL_LOAD_SIGNAL_COUNT", "100"))
    large_count = int(os.getenv("TEMPORAL_LOAD_LARGE_SCOPE_COUNT", "80"))
    small_count = int(os.getenv("TEMPORAL_LOAD_SMALL_SCOPE_COUNT", "20"))
    if min(signal_count, large_count, small_count) <= 0:
        pytest.fail("all Temporal load counts must be positive")

    recorder = DispatchRecorder()
    async with _load_environment() as environment:
        async with Worker(
            environment.client,
            task_queue=task_queue,
            workflows=[SignalLoadWorkflow, FairnessLoadWorkflow],
            activities=[recorder.record],
            max_concurrent_activities=1,
            max_task_queue_activities_per_second=float(os.getenv("TEMPORAL_LOAD_QUEUE_RPS", "500")),
        ):
            signal_workflow_id = f"signal-load-{suffix}"
            signal_handle = await environment.client.start_workflow(
                SignalLoadWorkflow.run,
                SignalLoadInput(expected_signals=signal_count),
                id=signal_workflow_id,
                task_queue=task_queue,
            )
            await asyncio.gather(
                *(
                    signal_handle.signal(SignalLoadWorkflow.wake, sequence)
                    for sequence in range(signal_count)
                )
            )
            assert await signal_handle.result() == signal_count

            load_inputs = (
                FairnessLoadInput("large-scope", 1.0, large_count),
                FairnessLoadInput("small-scope", 1.0, small_count),
            )
            fairness_handles = await asyncio.gather(
                *(
                    environment.client.start_workflow(
                        FairnessLoadWorkflow.run,
                        load_input,
                        id=f"fairness-load-{load_input.fairness_key}-{suffix}",
                        task_queue=task_queue,
                    )
                    for load_input in load_inputs
                )
            )
            assert await asyncio.gather(*(handle.result() for handle in fairness_handles)) == [
                large_count,
                small_count,
            ]

            collector = TemporalLoadHarness(environment.client)
            signal_measurement = await collector.collect_history(signal_workflow_id)
            fairness_histories = await asyncio.gather(
                *(collector.collect_history(handle.id) for handle in fairness_handles)
            )
            fairness_measurement = measure_fairness(
                recorder.samples,
                weights={item.fairness_key: item.fairness_weight for item in load_inputs},
            )

    assert signal_measurement.signal_count == signal_count
    assert signal_measurement.event_count > signal_count
    assert signal_measurement.signal_resume_latency_seconds
    assert fairness_measurement.total_dispatches == large_count + small_count
    assert set(fairness_measurement.dispatch_counts) == {
        "large-scope",
        "small-scope",
    }

    # Emit one machine-readable report under pytest -s / captured test logs.  No result
    # files are written implicitly, keeping repeat runs and CI worktrees clean.
    report = {
        "signal_history": signal_measurement.summary(),
        "fairness_histories": [measurement.summary() for measurement in fairness_histories],
        "fairness": fairness_measurement.summary(),
        "configured_inputs": [asdict(item) for item in load_inputs],
    }
    print(json.dumps(report, sort_keys=True))
