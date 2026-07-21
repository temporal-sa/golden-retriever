"""Durable demo-control boundary with an in-memory reference implementation."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Protocol

from .models import (
    ApiIdempotencyReceipt,
    DemoControls,
    DemoEvent,
    DemoIdempotencyConflictError,
    DemoNotFoundError,
    DemoOperation,
    DemoOperationStatus,
    DemoOperationType,
    DemoRun,
    DemoRunStatus,
    QuotaConsumption,
)

REQUIRED_EVENT_TYPES = frozenset(
    {
        "run_created",
        "quota_injected",
        "quota_wait_started",
        "quota_wait_completed",
        "document_commit_held",
        "document_committed",
        "deactivation_fenced",
        "held_commit_released",
        "stale_generation_rejected",
        "cleanup_batch_completed",
        "store_inactive",
    }
)

_TERMINAL_OPERATION_STATUSES = frozenset(
    {
        DemoOperationStatus.COMPLETED,
        DemoOperationStatus.FAILED,
        DemoOperationStatus.CANCELED,
        DemoOperationStatus.REJECTED,
    }
)


def _operation_update_allowed(
    current: DemoOperation,
    status: DemoOperationStatus,
    workflow_id: str | None,
) -> bool:
    if current.status in _TERMINAL_OPERATION_STATUSES:
        # A command submission that failed before receiving a Workflow ID may
        # be retried with the same deterministic identity. Workflow terminal
        # results, which always have an ID, are immutable.
        return (
            current.status is DemoOperationStatus.FAILED
            and current.workflow_id is None
            and (
                (status is DemoOperationStatus.ACCEPTED and workflow_id is not None)
                or status is DemoOperationStatus.REJECTED
            )
        )
    return not (
        current.status is DemoOperationStatus.RUNNING and status is DemoOperationStatus.ACCEPTED
    )


def _run_update_allowed(current: DemoRunStatus, status: DemoRunStatus) -> bool:
    if current is DemoRunStatus.COMPLETED:
        return False
    if current is DemoRunStatus.FAILED:
        return status in {DemoRunStatus.DEACTIVATING, DemoRunStatus.COMPLETED}
    return not (
        current is DemoRunStatus.DEACTIVATING
        and status in {DemoRunStatus.READY, DemoRunStatus.SYNCING}
    )


class DemoStateStore(Protocol):
    async def start(self) -> None: ...

    async def ready(self) -> bool: ...

    async def aclose(self) -> None: ...

    async def create_run(self, run: DemoRun, controls: DemoControls) -> DemoRun: ...

    async def get_run(self, run_id: str) -> DemoRun: ...

    async def get_run_by_store(self, store_key: str) -> DemoRun: ...

    async def update_run_status(
        self, run_id: str, status: DemoRunStatus, *, finished: bool = False
    ) -> DemoRun: ...

    async def get_controls(self, run_id: str) -> DemoControls: ...

    async def set_hold(self, run_id: str, *, enabled: bool) -> DemoControls: ...

    async def request_release(self, run_id: str) -> DemoControls: ...

    async def wait_for_release(self, run_id: str, *, timeout_seconds: float) -> bool: ...

    async def consume_quota_once(
        self, run_id: str, *, request_id: str, operation: str
    ) -> QuotaConsumption: ...

    async def complete_quota_wait(self, run_id: str, *, operation: str) -> str | None: ...

    async def append_event(self, event: DemoEvent) -> DemoEvent: ...

    async def list_events(
        self, run_id: str, *, after_event_id: int = 0, limit: int = 200
    ) -> tuple[DemoEvent, ...]: ...

    async def put_operation(self, operation: DemoOperation) -> DemoOperation: ...

    async def get_operation(self, operation_id: str) -> DemoOperation: ...

    async def update_operation(
        self,
        operation_id: str,
        status: DemoOperationStatus,
        *,
        workflow_id: str | None = None,
        lifecycle_generation: int | None = None,
        result: Mapping[str, Any] | None = None,
        message: str | None = None,
        require_workflow_id_absent: bool = False,
    ) -> DemoOperation: ...

    async def get_idempotency_receipt(
        self, scope: str, idempotency_key_hash: str
    ) -> ApiIdempotencyReceipt | None: ...

    async def put_idempotency_receipt(
        self, receipt: ApiIdempotencyReceipt
    ) -> ApiIdempotencyReceipt: ...

    async def generation_proof(self, store_key: str) -> Mapping[str, Any]: ...

    async def put_preflight(
        self,
        *,
        request_id: str,
        workflow_id: str,
        status: str,
        result: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]: ...

    async def get_preflight(self, workflow_id: str) -> Mapping[str, Any]: ...


def validate_event(event: DemoEvent) -> None:
    if event.event_type not in REQUIRED_EVENT_TYPES:
        raise ValueError(f"unsupported demo event type {event.event_type!r}")
    if not event.event_key or len(event.event_key) > 300:
        raise ValueError("event_key must contain at most 300 characters")
    redacted_details: dict[str, object] = {}
    for key, value in event.details.items():
        if not isinstance(key, str) or not key or len(key) > 80:
            raise ValueError("event detail keys must be short non-empty strings")
        if not isinstance(value, (str, int, float, bool, type(None))):
            raise ValueError("event details may contain only scalar values")
        if isinstance(value, str) and len(value) > 500:
            raise ValueError("event detail strings must contain at most 500 characters")
        redacted_details[key] = value
    if len(json.dumps(redacted_details, separators=(",", ":"))) > 4_096:
        raise ValueError("event details exceed the 4 KiB presentation-event limit")


class InMemoryDemoStateStore:
    """Single-process reference adapter with the same idempotency rules as Postgres."""

    def __init__(self) -> None:
        self._runs: dict[str, DemoRun] = {}
        self._run_by_store: dict[str, str] = {}
        self._controls: dict[str, DemoControls] = {}
        self._events: dict[str, list[DemoEvent]] = {}
        self._events_by_key: dict[tuple[str, str], DemoEvent] = {}
        self._operations: dict[str, DemoOperation] = {}
        self._receipts: dict[tuple[str, str], ApiIdempotencyReceipt] = {}
        self._preflights: dict[str, dict[str, Any]] = {}
        self._release_events: dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()
        self._started = False

    async def start(self) -> None:
        self._started = True

    async def ready(self) -> bool:
        return self._started

    async def aclose(self) -> None:
        self._started = False

    async def create_run(self, run: DemoRun, controls: DemoControls) -> DemoRun:
        if run.run_id != controls.run_id:
            raise ValueError("run and controls must share run_id")
        async with self._lock:
            existing = self._runs.get(run.run_id)
            if existing is not None:
                if (
                    existing.store_key != run.store_key
                    or existing.display_name != run.display_name
                    or existing.baseline_generation != run.baseline_generation
                    or self._controls[run.run_id].held_document_key != controls.held_document_key
                    or self._controls[run.run_id].quota_retry_after_seconds
                    != controls.quota_retry_after_seconds
                ):
                    raise DemoIdempotencyConflictError(
                        "run_id was reused with different seed attributes"
                    )
                return existing
            other_run_id = self._run_by_store.get(run.store_key)
            if other_run_id is not None:
                raise DemoIdempotencyConflictError("store_key already belongs to another run")
            self._runs[run.run_id] = run
            self._run_by_store[run.store_key] = run.run_id
            self._controls[run.run_id] = controls
            self._events[run.run_id] = []
            self._release_events[run.run_id] = asyncio.Event()
            return run

    async def get_run(self, run_id: str) -> DemoRun:
        async with self._lock:
            return self._require_run(run_id)

    async def get_run_by_store(self, store_key: str) -> DemoRun:
        async with self._lock:
            try:
                run_id = self._run_by_store[store_key]
            except KeyError as exc:
                raise DemoNotFoundError(f"unknown demo store {store_key!r}") from exc
            return self._runs[run_id]

    async def update_run_status(
        self, run_id: str, status: DemoRunStatus, *, finished: bool = False
    ) -> DemoRun:
        async with self._lock:
            current = self._require_run(run_id)
            if not _run_update_allowed(current.status, status):
                return current
            updated = replace(
                current,
                status=status,
                finished_at=(
                    (current.finished_at or datetime.now(UTC)) if finished else current.finished_at
                ),
            )
            self._runs[run_id] = updated
            return updated

    async def get_controls(self, run_id: str) -> DemoControls:
        async with self._lock:
            self._require_run(run_id)
            return self._controls[run_id]

    async def set_hold(self, run_id: str, *, enabled: bool) -> DemoControls:
        async with self._lock:
            self._require_run(run_id)
            current = self._controls[run_id]
            changed = current.hold_before_commit != enabled or (
                enabled and current.release_requested
            )
            updated = replace(
                current,
                hold_before_commit=enabled,
                release_requested=False if enabled else current.release_requested,
                control_version=current.control_version + changed,
                updated_at=datetime.now(UTC) if changed else current.updated_at,
            )
            self._controls[run_id] = updated
            if enabled:
                self._release_events[run_id].clear()
            return updated

    async def request_release(self, run_id: str) -> DemoControls:
        async with self._lock:
            self._require_run(run_id)
            current = self._controls[run_id]
            changed = not current.release_requested
            updated = replace(
                current,
                release_requested=True,
                control_version=current.control_version + changed,
                updated_at=datetime.now(UTC) if changed else current.updated_at,
            )
            self._controls[run_id] = updated
            self._release_events[run_id].set()
            return updated

    async def wait_for_release(self, run_id: str, *, timeout_seconds: float) -> bool:
        async with self._lock:
            self._require_run(run_id)
            if self._controls[run_id].release_requested:
                return True
            release_event = self._release_events[run_id]
        try:
            await asyncio.wait_for(release_event.wait(), timeout=timeout_seconds)
        except TimeoutError:
            return False
        return True

    async def consume_quota_once(
        self, run_id: str, *, request_id: str, operation: str
    ) -> QuotaConsumption:
        async with self._lock:
            run = self._require_run(run_id)
            current = self._controls[run_id]
            if not current.quota_once_pending:
                return QuotaConsumption(
                    injected=False,
                    retry_after_seconds=current.quota_retry_after_seconds,
                    waiting_request_id=current.quota_wait_request_id,
                )
            updated = replace(
                current,
                quota_once_pending=False,
                quota_wait_request_id=request_id,
                quota_wait_operation=operation,
                control_version=current.control_version + 1,
                updated_at=datetime.now(UTC),
            )
            self._controls[run_id] = updated
            self._append_locked(
                DemoEvent(
                    event_id=None,
                    event_key="quota:injected",
                    run_id=run_id,
                    store_key=run.store_key,
                    event_type="quota_injected",
                    operation_id=request_id,
                    details={
                        "provider_operation": operation,
                        "retry_after_seconds": current.quota_retry_after_seconds,
                    },
                )
            )
            self._append_locked(
                DemoEvent(
                    event_id=None,
                    event_key=f"quota:wait:{request_id}:started",
                    run_id=run_id,
                    store_key=run.store_key,
                    event_type="quota_wait_started",
                    operation_id=request_id,
                    details={"provider_operation": operation},
                )
            )
            return QuotaConsumption(
                injected=True,
                retry_after_seconds=current.quota_retry_after_seconds,
                waiting_request_id=request_id,
            )

    async def complete_quota_wait(self, run_id: str, *, operation: str) -> str | None:
        async with self._lock:
            run = self._require_run(run_id)
            current = self._controls[run_id]
            request_id = current.quota_wait_request_id
            if request_id is None or current.quota_wait_operation != operation:
                return None
            self._controls[run_id] = replace(
                current,
                quota_wait_request_id=None,
                quota_wait_operation=None,
                control_version=current.control_version + 1,
                updated_at=datetime.now(UTC),
            )
            self._append_locked(
                DemoEvent(
                    event_id=None,
                    event_key=f"quota:wait:{request_id}:completed",
                    run_id=run_id,
                    store_key=run.store_key,
                    event_type="quota_wait_completed",
                    operation_id=request_id,
                    details={"provider_operation": operation},
                )
            )
            return request_id

    async def append_event(self, event: DemoEvent) -> DemoEvent:
        async with self._lock:
            self._require_run(event.run_id)
            if self._runs[event.run_id].store_key != event.store_key:
                raise ValueError("event store_key does not belong to run_id")
            return self._append_locked(event)

    async def list_events(
        self, run_id: str, *, after_event_id: int = 0, limit: int = 200
    ) -> tuple[DemoEvent, ...]:
        if limit <= 0 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        async with self._lock:
            self._require_run(run_id)
            return tuple(
                event
                for event in self._events[run_id]
                if event.event_id is not None and event.event_id > after_event_id
            )[:limit]

    async def put_operation(self, operation: DemoOperation) -> DemoOperation:
        async with self._lock:
            run = self._require_run(operation.run_id)
            if run.store_key != operation.store_key:
                raise ValueError("operation store_key does not belong to run_id")
            existing = self._operations.get(operation.operation_id)
            if existing is not None:
                if (
                    existing.run_id != operation.run_id
                    or existing.store_key != operation.store_key
                    or existing.operation_type is not operation.operation_type
                    or existing.command_id != operation.command_id
                ):
                    raise DemoIdempotencyConflictError(
                        "operation_id was reused for another command"
                    )
                return existing
            self._operations[operation.operation_id] = operation
            return operation

    async def get_operation(self, operation_id: str) -> DemoOperation:
        async with self._lock:
            try:
                return self._operations[operation_id]
            except KeyError as exc:
                raise DemoNotFoundError(f"unknown operation {operation_id!r}") from exc

    async def update_operation(
        self,
        operation_id: str,
        status: DemoOperationStatus,
        *,
        workflow_id: str | None = None,
        lifecycle_generation: int | None = None,
        result: Mapping[str, Any] | None = None,
        message: str | None = None,
        require_workflow_id_absent: bool = False,
    ) -> DemoOperation:
        async with self._lock:
            try:
                current = self._operations[operation_id]
            except KeyError as exc:
                raise DemoNotFoundError(f"unknown operation {operation_id!r}") from exc
            if require_workflow_id_absent and current.workflow_id is not None:
                return current
            if not _operation_update_allowed(current, status, workflow_id):
                return current
            updated = replace(
                current,
                status=status,
                workflow_id=workflow_id if workflow_id is not None else current.workflow_id,
                lifecycle_generation=(
                    lifecycle_generation
                    if lifecycle_generation is not None
                    else current.lifecycle_generation
                ),
                result=dict(result) if result is not None else current.result,
                message=message,
                updated_at=datetime.now(UTC),
            )
            self._operations[operation_id] = updated
            return updated

    async def get_idempotency_receipt(
        self, scope: str, idempotency_key_hash: str
    ) -> ApiIdempotencyReceipt | None:
        async with self._lock:
            return self._receipts.get((scope, idempotency_key_hash))

    async def put_idempotency_receipt(
        self, receipt: ApiIdempotencyReceipt
    ) -> ApiIdempotencyReceipt:
        async with self._lock:
            key = (receipt.scope, receipt.idempotency_key_hash)
            existing = self._receipts.get(key)
            if existing is not None:
                if existing.request_hash != receipt.request_hash:
                    raise DemoIdempotencyConflictError(
                        "idempotency key was reused with a different request"
                    )
                return existing
            self._receipts[key] = receipt
            return receipt

    async def generation_proof(self, store_key: str) -> Mapping[str, Any]:
        async with self._lock:
            run_id = self._run_by_store.get(store_key)
            if run_id is None:
                raise DemoNotFoundError(f"unknown demo store {store_key!r}")
            return {
                "store_key": store_key,
                "durable_demo_events": len(self._events[run_id]),
                "durable_api_receipts": len(self._receipts),
                "backend": "in_memory",
            }

    async def put_preflight(
        self,
        *,
        request_id: str,
        workflow_id: str,
        status: str,
        result: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        async with self._lock:
            existing = self._preflights.get(workflow_id)
            if existing is not None and existing["request_id"] != request_id:
                raise DemoIdempotencyConflictError("preflight workflow ID was reused")
            if existing is not None and existing["status"] != "running":
                return dict(existing)
            record = {
                "request_id": request_id,
                "workflow_id": workflow_id,
                "status": status,
                "result": None if result is None else dict(result),
            }
            self._preflights[workflow_id] = record
            return dict(record)

    async def get_preflight(self, workflow_id: str) -> Mapping[str, Any]:
        async with self._lock:
            try:
                return dict(self._preflights[workflow_id])
            except KeyError as exc:
                raise DemoNotFoundError(f"unknown preflight {workflow_id!r}") from exc

    def _require_run(self, run_id: str) -> DemoRun:
        try:
            return self._runs[run_id]
        except KeyError as exc:
            raise DemoNotFoundError(f"unknown demo run {run_id!r}") from exc

    def _append_locked(self, event: DemoEvent) -> DemoEvent:
        validate_event(event)
        key = (event.run_id, event.event_key)
        existing = self._events_by_key.get(key)
        if existing is not None:
            return existing
        persisted = replace(event, event_id=len(self._events[event.run_id]) + 1)
        self._events[event.run_id].append(persisted)
        self._events_by_key[key] = persisted
        return persisted


class AsyncConnectionProvider(Protocol):
    def connection(self) -> AbstractAsyncContextManager[Any]: ...

    async def open(self) -> None: ...

    async def wait(self) -> None: ...

    async def check(self) -> None: ...

    async def aclose(self) -> None: ...


class PostgresDemoStateStore:
    """Psycopg-compatible durable implementation backed by ``retrieval_demo_ui``."""

    def __init__(self, provider: AsyncConnectionProvider, *, owns_provider: bool = False) -> None:
        self._provider = provider
        self._owns_provider = owns_provider

    async def start(self) -> None:
        await self._provider.open()
        await self._provider.wait()

    async def ready(self) -> bool:
        try:
            await self._provider.check()
            async with self._provider.connection() as connection:
                cursor = await connection.execute(
                    "SELECT to_regclass('retrieval_demo_ui.demo_runs') IS NOT NULL AS ready"
                )
                row = await cursor.fetchone()
            return bool(row and _row_value(row, "ready", 0))
        except Exception:
            return False

    async def aclose(self) -> None:
        if self._owns_provider:
            await self._provider.aclose()

    async def generation_proof(self, store_key: str) -> Mapping[str, Any]:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM retrieval_demo_ui.generation_proof(%s)",
                (store_key,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise DemoNotFoundError(f"unknown demo store {store_key!r}")
        names = (
            "lifecycle_state",
            "lifecycle_generation",
            "physical_documents",
            "physical_chunks",
            "durable_write_receipts",
            "visible_documents",
            "visible_chunks",
        )
        values = {name: _row_value(row, name, index) for index, name in enumerate(names)}
        return {
            "store_key": store_key,
            "lifecycle_state": str(values["lifecycle_state"]),
            "lifecycle_generation": int(values["lifecycle_generation"]),
            **{name: int(values[name]) for name in names[2:]},
            "visibility_rule": ("state IN (active,syncing) AND row.generation = store.generation"),
            "backend": "lakebase",
        }

    async def put_preflight(
        self,
        *,
        request_id: str,
        workflow_id: str,
        status: str,
        result: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        encoded = None if result is None else json.dumps(dict(result), sort_keys=True)
        async with self._provider.connection() as connection:
            await connection.execute(
                """
                INSERT INTO retrieval_demo_ui.preflight_runs (
                    workflow_id, request_id, status, result
                ) VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (workflow_id) DO UPDATE
                SET status = CASE
                        WHEN retrieval_demo_ui.preflight_runs.status = 'running'
                        THEN EXCLUDED.status
                        ELSE retrieval_demo_ui.preflight_runs.status
                    END,
                    result = CASE
                        WHEN retrieval_demo_ui.preflight_runs.status = 'running'
                        THEN COALESCE(EXCLUDED.result, retrieval_demo_ui.preflight_runs.result)
                        ELSE retrieval_demo_ui.preflight_runs.result
                    END,
                    updated_at = clock_timestamp()
                WHERE retrieval_demo_ui.preflight_runs.request_id = EXCLUDED.request_id
                """,
                (workflow_id, request_id, status, encoded),
            )
        persisted = await self.get_preflight(workflow_id)
        if persisted["request_id"] != request_id:
            raise DemoIdempotencyConflictError("preflight workflow ID was reused")
        return persisted

    async def get_preflight(self, workflow_id: str) -> Mapping[str, Any]:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT request_id, workflow_id, status, result
                FROM retrieval_demo_ui.preflight_runs
                WHERE workflow_id = %s
                """,
                (workflow_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise DemoNotFoundError(f"unknown preflight {workflow_id!r}")
        raw_result = _row_value(row, "result", 3)
        return {
            "request_id": str(_row_value(row, "request_id", 0)),
            "workflow_id": str(_row_value(row, "workflow_id", 1)),
            "status": str(_row_value(row, "status", 2)),
            "result": (json.loads(raw_result) if isinstance(raw_result, str) else raw_result),
        }

    async def create_run(self, run: DemoRun, controls: DemoControls) -> DemoRun:
        async with self._provider.connection() as connection, connection.transaction():
            await connection.execute(
                "SELECT retrieval_demo_ui.create_demo_run(%s,%s,%s,%s,%s,%s,%s)",
                (
                    run.run_id,
                    run.store_key,
                    run.display_name,
                    run.baseline_generation,
                    controls.quota_retry_after_seconds,
                    controls.held_document_key,
                    controls.hold_before_commit,
                ),
            )
        return await self.get_run(run.run_id)

    async def get_run(self, run_id: str) -> DemoRun:
        return await self._select_run("run_id = %s", (run_id,))

    async def get_run_by_store(self, store_key: str) -> DemoRun:
        return await self._select_run("store_key = %s", (store_key,))

    async def _select_run(self, predicate: str, parameters: tuple[object, ...]) -> DemoRun:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "SELECT run_id::text,store_key,display_name,baseline_generation,status,"
                "created_at,finished_at FROM retrieval_demo_ui.demo_runs WHERE " + predicate,
                parameters,
            )
            row = await cursor.fetchone()
        if row is None:
            raise DemoNotFoundError("unknown demo run")
        return DemoRun(
            run_id=str(_row_value(row, "run_id", 0)),
            store_key=str(_row_value(row, "store_key", 1)),
            display_name=str(_row_value(row, "display_name", 2)),
            baseline_generation=int(_row_value(row, "baseline_generation", 3)),
            status=DemoRunStatus(str(_row_value(row, "status", 4))),
            created_at=_row_value(row, "created_at", 5),
            finished_at=_row_value(row, "finished_at", 6),
        )

    async def update_run_status(
        self, run_id: str, status: DemoRunStatus, *, finished: bool = False
    ) -> DemoRun:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "UPDATE retrieval_demo_ui.demo_runs SET status=%s,"
                "finished_at=CASE WHEN %s THEN COALESCE(finished_at,now()) "
                "ELSE finished_at END "
                "WHERE run_id=%s AND status<>'completed' "
                "AND (status<>'failed' OR %s IN ('deactivating','completed')) "
                "AND NOT (status='deactivating' AND %s IN ('ready','syncing')) "
                "RETURNING run_id",
                (status.value, finished, run_id, status.value, status.value),
            )
            row = await cursor.fetchone()
        if row is None:
            return await self.get_run(run_id)
        return await self.get_run(run_id)

    async def get_controls(self, run_id: str) -> DemoControls:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "SELECT run_id::text,quota_once_pending,quota_retry_after_seconds,"
                "held_document_key,hold_before_commit,release_requested,control_version,"
                "quota_wait_request_id,quota_wait_operation,updated_at "
                "FROM retrieval_demo_ui.demo_controls WHERE run_id=%s",
                (run_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise DemoNotFoundError(f"unknown demo run {run_id!r}")
        return self._controls_from_row(row)

    async def set_hold(self, run_id: str, *, enabled: bool) -> DemoControls:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "UPDATE retrieval_demo_ui.demo_controls SET hold_before_commit=%s,"
                "release_requested=CASE WHEN %s THEN false ELSE release_requested END,"
                "control_version=control_version+CASE "
                "WHEN hold_before_commit IS DISTINCT FROM %s "
                "OR (%s AND release_requested) THEN 1 ELSE 0 END,"
                "updated_at=CASE WHEN hold_before_commit IS DISTINCT FROM %s "
                "OR (%s AND release_requested) THEN now() ELSE updated_at END "
                "WHERE run_id=%s "
                "RETURNING run_id::text,quota_once_pending,quota_retry_after_seconds,"
                "held_document_key,hold_before_commit,release_requested,control_version,"
                "quota_wait_request_id,quota_wait_operation,updated_at",
                (enabled, enabled, enabled, enabled, enabled, enabled, run_id),
            )
            row = await cursor.fetchone()
        if row is None:
            raise DemoNotFoundError(f"unknown demo run {run_id!r}")
        return self._controls_from_row(row)

    async def request_release(self, run_id: str) -> DemoControls:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "UPDATE retrieval_demo_ui.demo_controls SET release_requested=true,"
                "control_version=control_version+CASE WHEN release_requested THEN 0 ELSE 1 END,"
                "updated_at=CASE WHEN release_requested THEN updated_at ELSE now() END "
                "WHERE run_id=%s "
                "RETURNING run_id::text,quota_once_pending,quota_retry_after_seconds,"
                "held_document_key,hold_before_commit,release_requested,control_version,"
                "quota_wait_request_id,quota_wait_operation,updated_at",
                (run_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise DemoNotFoundError(f"unknown demo run {run_id!r}")
        return self._controls_from_row(row)

    async def wait_for_release(self, run_id: str, *, timeout_seconds: float) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        while loop.time() < deadline:
            if (await self.get_controls(run_id)).release_requested:
                return True
            await asyncio.sleep(min(0.25, max(0.01, deadline - loop.time())))
        return False

    async def consume_quota_once(
        self, run_id: str, *, request_id: str, operation: str
    ) -> QuotaConsumption:
        async with self._provider.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "UPDATE retrieval_demo_ui.demo_controls SET quota_once_pending=false,"
                "quota_wait_request_id=%s,quota_wait_operation=%s,"
                "control_version=control_version+1,updated_at=now() "
                "WHERE run_id=%s AND quota_once_pending=true "
                "RETURNING quota_retry_after_seconds",
                (request_id, operation, run_id),
            )
            row = await cursor.fetchone()
            if row is None:
                current = await connection.execute(
                    "SELECT quota_retry_after_seconds,quota_wait_request_id "
                    "FROM retrieval_demo_ui.demo_controls WHERE run_id=%s",
                    (run_id,),
                )
                existing = await current.fetchone()
                if existing is None:
                    raise DemoNotFoundError(f"unknown demo run {run_id!r}")
                return QuotaConsumption(
                    False,
                    float(_row_value(existing, "quota_retry_after_seconds", 0)),
                    _row_value(existing, "quota_wait_request_id", 1),
                )
            run = await self._run_on_connection(connection, run_id)
            await self._insert_event_on_connection(
                connection,
                DemoEvent(
                    event_id=None,
                    event_key="quota:injected",
                    run_id=run_id,
                    store_key=run.store_key,
                    event_type="quota_injected",
                    operation_id=request_id,
                    details={
                        "provider_operation": operation,
                        "retry_after_seconds": float(
                            _row_value(row, "quota_retry_after_seconds", 0)
                        ),
                    },
                ),
            )
            await self._insert_event_on_connection(
                connection,
                DemoEvent(
                    event_id=None,
                    event_key=f"quota:wait:{request_id}:started",
                    run_id=run_id,
                    store_key=run.store_key,
                    event_type="quota_wait_started",
                    operation_id=request_id,
                    details={"provider_operation": operation},
                ),
            )
            return QuotaConsumption(
                True,
                float(_row_value(row, "quota_retry_after_seconds", 0)),
                request_id,
            )

    async def complete_quota_wait(self, run_id: str, *, operation: str) -> str | None:
        async with self._provider.connection() as connection, connection.transaction():
            cursor = await connection.execute(
                "WITH pending AS ("
                " SELECT run_id,quota_wait_request_id FROM retrieval_demo_ui.demo_controls"
                " WHERE run_id=%s AND quota_wait_operation=%s"
                " AND quota_wait_request_id IS NOT NULL FOR UPDATE"
                ") UPDATE retrieval_demo_ui.demo_controls AS controls"
                " SET quota_wait_request_id=NULL,quota_wait_operation=NULL,"
                " control_version=controls.control_version+1,updated_at=now()"
                " FROM pending WHERE controls.run_id=pending.run_id"
                " RETURNING pending.quota_wait_request_id",
                (run_id, operation),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            request_id = str(_row_value(row, "quota_wait_request_id", 0))
            run = await self._run_on_connection(connection, run_id)
            await self._insert_event_on_connection(
                connection,
                DemoEvent(
                    event_id=None,
                    event_key=f"quota:wait:{request_id}:completed",
                    run_id=run_id,
                    store_key=run.store_key,
                    event_type="quota_wait_completed",
                    operation_id=request_id,
                    details={"provider_operation": operation},
                ),
            )
            return request_id

    async def append_event(self, event: DemoEvent) -> DemoEvent:
        validate_event(event)
        async with self._provider.connection() as connection:
            return await self._insert_event_on_connection(connection, event)

    async def _insert_event_on_connection(self, connection: Any, event: DemoEvent) -> DemoEvent:
        cursor = await connection.execute(
            "INSERT INTO retrieval_demo_ui.demo_events "
            "(event_key,run_id,store_key,event_type,operation_id,workflow_id,document_key,"
            "expected_generation,actual_generation,details) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) "
            "ON CONFLICT (run_id,event_key) DO UPDATE SET event_key=EXCLUDED.event_key "
            "RETURNING event_id,event_key,run_id::text,store_key,event_type,operation_id,"
            "workflow_id,document_key,expected_generation,actual_generation,details,created_at",
            (
                event.event_key,
                event.run_id,
                event.store_key,
                event.event_type,
                event.operation_id,
                event.workflow_id,
                event.document_key,
                event.expected_generation,
                event.actual_generation,
                json.dumps(dict(event.details), separators=(",", ":")),
            ),
        )
        return self._event_from_row(await cursor.fetchone())

    async def list_events(
        self, run_id: str, *, after_event_id: int = 0, limit: int = 200
    ) -> tuple[DemoEvent, ...]:
        if limit <= 0 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "SELECT event_id,event_key,run_id::text,store_key,event_type,operation_id,"
                "workflow_id,document_key,expected_generation,actual_generation,details,created_at "
                "FROM retrieval_demo_ui.demo_events WHERE run_id=%s AND event_id>%s "
                "ORDER BY event_id LIMIT %s",
                (run_id, after_event_id, limit),
            )
            rows = await cursor.fetchall()
        return tuple(self._event_from_row(row) for row in rows)

    async def put_operation(self, operation: DemoOperation) -> DemoOperation:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "INSERT INTO retrieval_demo_ui.demo_operations "
                "(operation_id,run_id,store_key,operation_type,status,command_id,workflow_id,"
                "lifecycle_generation,result,message) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s) "
                "ON CONFLICT (operation_id) DO UPDATE SET operation_id=EXCLUDED.operation_id "
                "RETURNING operation_id,run_id::text,store_key,operation_type,status,command_id,"
                "workflow_id,lifecycle_generation,result,message,created_at,updated_at",
                (
                    operation.operation_id,
                    operation.run_id,
                    operation.store_key,
                    operation.operation_type.value,
                    operation.status.value,
                    operation.command_id,
                    operation.workflow_id,
                    operation.lifecycle_generation,
                    json.dumps(dict(operation.result), separators=(",", ":")),
                    operation.message,
                ),
            )
            row = await cursor.fetchone()
        persisted = self._operation_from_row(row)
        if (
            persisted.run_id != operation.run_id
            or persisted.store_key != operation.store_key
            or persisted.operation_type is not operation.operation_type
            or persisted.command_id != operation.command_id
        ):
            raise DemoIdempotencyConflictError("operation_id was reused for another command")
        return persisted

    async def get_operation(self, operation_id: str) -> DemoOperation:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "SELECT operation_id,run_id::text,store_key,operation_type,status,command_id,"
                "workflow_id,lifecycle_generation,result,message,created_at,updated_at "
                "FROM retrieval_demo_ui.demo_operations WHERE operation_id=%s",
                (operation_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise DemoNotFoundError(f"unknown operation {operation_id!r}")
        return self._operation_from_row(row)

    async def update_operation(
        self,
        operation_id: str,
        status: DemoOperationStatus,
        *,
        workflow_id: str | None = None,
        lifecycle_generation: int | None = None,
        result: Mapping[str, Any] | None = None,
        message: str | None = None,
        require_workflow_id_absent: bool = False,
    ) -> DemoOperation:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "UPDATE retrieval_demo_ui.demo_operations SET status=%s,"
                "workflow_id=COALESCE(%s,workflow_id),"
                "lifecycle_generation=COALESCE(%s,lifecycle_generation),"
                "result=COALESCE(%s::jsonb,result),message=%s,updated_at=now() "
                "WHERE operation_id=%s AND (NOT %s OR workflow_id IS NULL) AND ((status NOT IN "
                "('completed','failed','canceled','rejected') "
                "AND NOT (status='running' AND %s='accepted')) "
                "OR (status='failed' AND workflow_id IS NULL AND "
                "((%s='accepted' AND %s::text IS NOT NULL) OR %s='rejected'))) "
                "RETURNING operation_id,run_id::text,store_key,"
                "operation_type,status,command_id,workflow_id,lifecycle_generation,result,message,"
                "created_at,updated_at",
                (
                    status.value,
                    workflow_id,
                    lifecycle_generation,
                    json.dumps(dict(result), separators=(",", ":")) if result is not None else None,
                    message,
                    operation_id,
                    require_workflow_id_absent,
                    status.value,
                    status.value,
                    workflow_id,
                    status.value,
                ),
            )
            row = await cursor.fetchone()
        if row is None:
            return await self.get_operation(operation_id)
        return self._operation_from_row(row)

    async def get_idempotency_receipt(
        self, scope: str, idempotency_key_hash: str
    ) -> ApiIdempotencyReceipt | None:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "SELECT scope,idempotency_key_hash,request_hash,status_code,response,operation_id,"
                "created_at FROM retrieval_demo_ui.api_idempotency "
                "WHERE scope=%s AND idempotency_key_hash=%s",
                (scope, idempotency_key_hash),
            )
            row = await cursor.fetchone()
        return None if row is None else self._receipt_from_row(row)

    async def put_idempotency_receipt(
        self, receipt: ApiIdempotencyReceipt
    ) -> ApiIdempotencyReceipt:
        async with self._provider.connection() as connection:
            cursor = await connection.execute(
                "INSERT INTO retrieval_demo_ui.api_idempotency "
                "(scope,idempotency_key_hash,request_hash,status_code,response,operation_id) "
                "VALUES (%s,%s,%s,%s,%s::jsonb,%s) "
                "ON CONFLICT (scope,idempotency_key_hash) DO UPDATE SET scope=EXCLUDED.scope "
                "RETURNING scope,idempotency_key_hash,request_hash,status_code,response,"
                "operation_id,created_at",
                (
                    receipt.scope,
                    receipt.idempotency_key_hash,
                    receipt.request_hash,
                    receipt.status_code,
                    json.dumps(dict(receipt.response), separators=(",", ":")),
                    receipt.operation_id,
                ),
            )
            row = await cursor.fetchone()
        persisted = self._receipt_from_row(row)
        if persisted.request_hash != receipt.request_hash:
            raise DemoIdempotencyConflictError(
                "idempotency key was reused with a different request"
            )
        return persisted

    async def _run_on_connection(self, connection: Any, run_id: str) -> DemoRun:
        cursor = await connection.execute(
            "SELECT run_id::text,store_key,display_name,baseline_generation,status,"
            "created_at,finished_at FROM retrieval_demo_ui.demo_runs WHERE run_id=%s",
            (run_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise DemoNotFoundError(f"unknown demo run {run_id!r}")
        return DemoRun(
            str(_row_value(row, "run_id", 0)),
            str(_row_value(row, "store_key", 1)),
            str(_row_value(row, "display_name", 2)),
            int(_row_value(row, "baseline_generation", 3)),
            DemoRunStatus(str(_row_value(row, "status", 4))),
            _row_value(row, "created_at", 5),
            _row_value(row, "finished_at", 6),
        )

    @staticmethod
    def _controls_from_row(row: Any) -> DemoControls:
        return DemoControls(
            run_id=str(_row_value(row, "run_id", 0)),
            quota_once_pending=bool(_row_value(row, "quota_once_pending", 1)),
            quota_retry_after_seconds=float(_row_value(row, "quota_retry_after_seconds", 2)),
            held_document_key=str(_row_value(row, "held_document_key", 3)),
            hold_before_commit=bool(_row_value(row, "hold_before_commit", 4)),
            release_requested=bool(_row_value(row, "release_requested", 5)),
            control_version=int(_row_value(row, "control_version", 6)),
            quota_wait_request_id=_row_value(row, "quota_wait_request_id", 7),
            quota_wait_operation=_row_value(row, "quota_wait_operation", 8),
            updated_at=_row_value(row, "updated_at", 9),
        )

    @staticmethod
    def _event_from_row(row: Any) -> DemoEvent:
        raw_details = _row_value(row, "details", 10)
        details = raw_details if isinstance(raw_details, dict) else json.loads(raw_details)
        return DemoEvent(
            event_id=int(_row_value(row, "event_id", 0)),
            event_key=str(_row_value(row, "event_key", 1)),
            run_id=str(_row_value(row, "run_id", 2)),
            store_key=str(_row_value(row, "store_key", 3)),
            event_type=str(_row_value(row, "event_type", 4)),
            operation_id=_row_value(row, "operation_id", 5),
            workflow_id=_row_value(row, "workflow_id", 6),
            document_key=_row_value(row, "document_key", 7),
            expected_generation=_row_value(row, "expected_generation", 8),
            actual_generation=_row_value(row, "actual_generation", 9),
            details=details,
            created_at=_row_value(row, "created_at", 11),
        )

    @staticmethod
    def _operation_from_row(row: Any) -> DemoOperation:
        raw_result = _row_value(row, "result", 8)
        result = raw_result if isinstance(raw_result, dict) else json.loads(raw_result)
        return DemoOperation(
            operation_id=str(_row_value(row, "operation_id", 0)),
            run_id=str(_row_value(row, "run_id", 1)),
            store_key=str(_row_value(row, "store_key", 2)),
            operation_type=DemoOperationType(str(_row_value(row, "operation_type", 3))),
            status=DemoOperationStatus(str(_row_value(row, "status", 4))),
            command_id=str(_row_value(row, "command_id", 5)),
            workflow_id=_row_value(row, "workflow_id", 6),
            lifecycle_generation=_row_value(row, "lifecycle_generation", 7),
            result=result,
            message=_row_value(row, "message", 9),
            created_at=_row_value(row, "created_at", 10),
            updated_at=_row_value(row, "updated_at", 11),
        )

    @staticmethod
    def _receipt_from_row(row: Any) -> ApiIdempotencyReceipt:
        raw_response = _row_value(row, "response", 4)
        response = raw_response if isinstance(raw_response, dict) else json.loads(raw_response)
        return ApiIdempotencyReceipt(
            scope=str(_row_value(row, "scope", 0)),
            idempotency_key_hash=str(_row_value(row, "idempotency_key_hash", 1)),
            request_hash=str(_row_value(row, "request_hash", 2)),
            status_code=int(_row_value(row, "status_code", 3)),
            response=response,
            operation_id=_row_value(row, "operation_id", 5),
            created_at=_row_value(row, "created_at", 6),
        )


def _row_value(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        return row[name]
    return row[index]


async def iter_events(
    store: DemoStateStore,
    run_id: str,
    *,
    after_event_id: int = 0,
    limit: int = 200,
) -> AsyncIterator[DemoEvent]:
    for event in await store.list_events(run_id, after_event_id=after_event_id, limit=limit):
        yield event


__all__ = [
    "REQUIRED_EVENT_TYPES",
    "AsyncConnectionProvider",
    "DemoStateStore",
    "InMemoryDemoStateStore",
    "PostgresDemoStateStore",
    "iter_events",
    "validate_event",
]
