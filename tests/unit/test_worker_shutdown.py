from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable
from typing import Any

import pytest

from retrieval.config import RetrievalTemporalConfig
from retrieval.temporal.runtime_config import TemporalRuntimeConfig
from retrieval.temporal.worker import (
    WORKER_GRACEFUL_SHUTDOWN_TIMEOUT,
    _install_signal_handlers,
    build_workers,
    run_worker,
)


def test_both_workers_have_a_bounded_graceful_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_options: list[dict[str, Any]] = []

    class _CapturedWorker:
        def __init__(self, *_args: object, **kwargs: Any) -> None:
            worker_options.append(kwargs)

    monkeypatch.setattr("retrieval.temporal.worker.Worker", _CapturedWorker)

    build_workers(
        object(),  # type: ignore[arg-type]
        runtime=TemporalRuntimeConfig(),
        config=RetrievalTemporalConfig(),
        repository=object(),  # type: ignore[arg-type]
        staging_store=object(),  # type: ignore[arg-type]
        provider_gateway=object(),  # type: ignore[arg-type]
    )

    assert len(worker_options) == 2
    assert all(
        options["graceful_shutdown_timeout"] == WORKER_GRACEFUL_SHUTDOWN_TIMEOUT
        for options in worker_options
    )
    assert WORKER_GRACEFUL_SHUTDOWN_TIMEOUT.total_seconds() > 30


def test_sigint_and_sigterm_request_coordinated_shutdown() -> None:
    callbacks: dict[signal.Signals, tuple[Callable[..., None], tuple[object, ...]]] = {}
    removed: list[signal.Signals] = []

    class _Loop:
        def add_signal_handler(
            self,
            received: signal.Signals,
            callback: Callable[..., None],
            *args: object,
        ) -> None:
            callbacks[received] = (callback, args)

        def remove_signal_handler(self, received: signal.Signals) -> bool:
            removed.append(received)
            return True

    shutdown_requested = asyncio.Event()
    remove = _install_signal_handlers(
        shutdown_requested,
        loop=_Loop(),  # type: ignore[arg-type]
    )

    assert set(callbacks) == {signal.SIGINT, signal.SIGTERM}
    callback, args = callbacks[signal.SIGTERM]
    callback(*args)
    assert shutdown_requested.is_set()

    remove()
    assert set(removed) == {signal.SIGINT, signal.SIGTERM}


@pytest.mark.asyncio
async def test_fatal_poller_is_propagated_only_after_both_workers_are_drained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    both_started = asyncio.Event()
    start_count = 0
    fatal = RuntimeError("fatal provider poller")

    class _Worker:
        def __init__(self, name: str, *, fails: bool = False) -> None:
            self.name = name
            self.fails = fails
            self.stop_requested = asyncio.Event()
            self.run_complete = asyncio.Event()

        async def run(self) -> None:
            nonlocal start_count
            events.append(f"{self.name}:run-started")
            start_count += 1
            if start_count == 2:
                both_started.set()
            await both_started.wait()
            try:
                if self.fails:
                    events.append(f"{self.name}:fatal")
                    raise fatal
                await self.stop_requested.wait()
                events.append(f"{self.name}:polling-stopped")
            finally:
                self.run_complete.set()

        async def shutdown(self) -> None:
            events.append(f"{self.name}:shutdown-requested")
            self.stop_requested.set()
            await self.run_complete.wait()
            events.append(f"{self.name}:shutdown-complete")

    retrieval_worker = _Worker("retrieval")
    provider_worker = _Worker("provider", fails=True)

    class _Adapters:
        repository = object()
        staging_store = object()
        provider_gateway = object()
        before_document_commit = None
        ingestion_event_sink = None

        async def aclose(self) -> None:
            assert retrieval_worker.run_complete.is_set()
            assert provider_worker.run_complete.is_set()
            assert "retrieval:shutdown-complete" in events
            assert "provider:shutdown-complete" in events
            events.append("adapters:closed")

    async def connect(*_args: object, **_kwargs: object) -> object:
        return object()

    monkeypatch.setattr("retrieval.temporal.worker.Client.connect", connect)
    monkeypatch.setattr(
        "retrieval.temporal.worker.TemporalRuntimeConfig.from_env",
        lambda: TemporalRuntimeConfig(allow_unsafe_in_memory_adapters=True),
    )
    monkeypatch.setattr(
        "retrieval.temporal.worker.RetrievalTemporalConfig.from_env",
        lambda: RetrievalTemporalConfig(),
    )
    monkeypatch.setattr(
        "retrieval.temporal.worker._load_adapters",
        lambda _runtime: asyncio.sleep(0, result=_Adapters()),
    )
    monkeypatch.setattr(
        "retrieval.temporal.worker.build_workers",
        lambda *_args, **_kwargs: (retrieval_worker, provider_worker),
    )
    monkeypatch.setattr(
        "retrieval.temporal.worker._install_signal_handlers",
        lambda _event: lambda: None,
    )

    with pytest.raises(RuntimeError, match="fatal provider poller") as caught:
        await run_worker()

    assert caught.value is fatal
    assert events[-1] == "adapters:closed"
    assert events.index("retrieval:polling-stopped") < events.index("adapters:closed")
    assert events.index("retrieval:shutdown-complete") < events.index("adapters:closed")
    assert events.index("provider:shutdown-complete") < events.index("adapters:closed")
