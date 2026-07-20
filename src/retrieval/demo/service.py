"""Application-facing Northstar service and deterministic evidence answerer."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from temporalio.exceptions import ApplicationError

from retrieval.temporal.activities.provider_api import ProviderPreflightRequest
from retrieval.temporal.activities.repositories import (
    InMemoryRetrievalRepository,
    RetrievalRepository,
)
from retrieval.temporal.client import RetrievalClient
from retrieval.temporal.common.ids import (
    store_controller_workflow_id,
    user_quota_workflow_id,
)
from retrieval.temporal.models.lifecycle import StoreLifecycleState
from retrieval.temporal.models.operations import (
    CommandResult,
    OperationAccepted,
    OperationStatus,
    StartDeactivationCommand,
    SyncCommand,
    WorkClass,
)

from .config import DemoConfig
from .controls import DemoControlsManager
from .fixtures import NorthstarScenario, load_northstar_scenario
from .models import (
    ApiIdempotencyReceipt,
    DemoConflictError,
    DemoControls,
    DemoEvent,
    DemoIdempotencyConflictError,
    DemoNotFoundError,
    DemoOperation,
    DemoOperationStatus,
    DemoOperationType,
    DemoReadiness,
    DemoRun,
    DemoRunStatus,
    DemoSearchHit,
    DemoSnapshot,
    DemoUnavailableError,
    EvidenceAnswer,
    EvidenceCitation,
)
from .store import DemoStateStore, PostgresDemoStateStore

_RUN_NAMESPACE = uuid.UUID("dbde145b-0721-4b44-8595-7d26d75b82c6")
_OPERATION_NAMESPACE = uuid.UUID("e70d7b93-6988-4fcf-965d-5106dc45b4f5")
_TOKEN = re.compile(r"[a-z0-9]+")
_CONTROLLER_CONFLICT_TYPES = frozenset(
    {
        "RemediationAlreadyRunning",
        "StaleLifecycleGeneration",
        "StoreNotSyncable",
        "SyncAlreadyRunning",
    }
)


class SearchAdapter(Protocol):
    backend: str

    async def search(self, store_key: str, query: str, limit: int = 8) -> tuple[Any, ...]: ...


class StoreCreationBoundary(Protocol):
    async def create(self, run: DemoRun, controls: DemoControls) -> DemoRun: ...


class RepositoryStoreCreationBoundary:
    """Local/test boundary using the repository's ordinary create permission."""

    def __init__(
        self,
        repository: RetrievalRepository,
        state_store: DemoStateStore,
    ) -> None:
        self._repository = repository
        self._state_store = state_store

    async def create(self, run: DemoRun, controls: DemoControls) -> DemoRun:
        await self._repository.create_store(
            run.store_key,
            run.display_name,
            generation=run.baseline_generation,
            state=StoreLifecycleState.ACTIVE,
        )
        return await self._state_store.create_run(run, controls)


class PostgresNorthstarStoreCreationBoundary:
    """Least-privilege App boundary backed by the constrained demo seed function."""

    def __init__(self, state_store: PostgresDemoStateStore) -> None:
        self._state_store = state_store

    async def create(self, run: DemoRun, controls: DemoControls) -> DemoRun:
        # PostgresDemoStateStore invokes the migration-owned, fixed-purpose
        # retrieval_demo_ui.create_demo_run SECURITY DEFINER function.
        return await self._state_store.create_run(run, controls)


class DemoCommandGateway(Protocol):
    async def start(self) -> None: ...

    async def ready(self) -> bool: ...

    async def aclose(self) -> None: ...

    async def request_sync(self, command: SyncCommand) -> OperationAccepted: ...

    async def start_deactivation(self, command: StartDeactivationCommand) -> OperationAccepted: ...

    async def get_status(self, store_key: str) -> Any: ...

    async def get_operation_result(
        self,
        store_key: str,
        operation_id: str,
    ) -> CommandResult | None: ...

    async def start_preflight(self, request: ProviderPreflightRequest) -> str: ...

    async def get_preflight(self, workflow_id: str) -> Mapping[str, Any]: ...


class LazyTemporalCommandGateway:
    """Connect during service startup, never during module import or factory construction."""

    def __init__(self, runtime: Any, retrieval_config: Any) -> None:
        self._runtime = runtime
        self._retrieval_config = retrieval_config
        self._client: RetrievalClient | None = None
        self._raw_client: Any | None = None

    async def start(self) -> None:
        if self._client is not None:
            return
        from temporalio.client import Client

        client = await Client.connect(
            self._runtime.address,
            namespace=self._runtime.namespace,
            api_key=self._runtime.api_key,
            tls=self._runtime.tls,
        )
        self._client = RetrievalClient.from_runtime(
            client,
            runtime=self._runtime,
            config=self._retrieval_config,
        )
        self._raw_client = client

    async def ready(self) -> bool:
        if self._raw_client is None:
            return False
        try:
            await self._raw_client.service_client.check_health()
            from temporalio.api.enums.v1 import TaskQueueType
            from temporalio.api.taskqueue.v1 import TaskQueue
            from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

            queue_checks = (
                (self._runtime.retrieval_task_queue, "TASK_QUEUE_TYPE_WORKFLOW"),
                (self._runtime.retrieval_task_queue, "TASK_QUEUE_TYPE_ACTIVITY"),
                (self._runtime.provider_task_queue, "TASK_QUEUE_TYPE_ACTIVITY"),
            )
            for task_queue, queue_type in queue_checks:
                response = await self._raw_client.workflow_service.describe_task_queue(
                    DescribeTaskQueueRequest(
                        namespace=self._runtime.namespace,
                        task_queue=TaskQueue(name=task_queue),
                        task_queue_type=TaskQueueType.Value(queue_type),
                        report_pollers=True,
                    )
                )
                if not response.pollers:
                    return False
        except Exception:
            return False
        return True

    async def aclose(self) -> None:
        self._client = None
        self._raw_client = None

    async def request_sync(self, command: SyncCommand) -> OperationAccepted:
        return await self._require().request_sync(command)

    async def start_deactivation(self, command: StartDeactivationCommand) -> OperationAccepted:
        return await self._require().start_deactivation(command)

    async def get_status(self, store_key: str) -> Any:
        return await self._require().get_status(store_key)

    async def get_operation_result(
        self,
        store_key: str,
        operation_id: str,
    ) -> CommandResult | None:
        return await self._require().get_operation_result(store_key, operation_id)

    async def start_preflight(self, request: ProviderPreflightRequest) -> str:
        if self._raw_client is None:
            raise DemoUnavailableError("Temporal client is not connected")
        from temporalio.client import WorkflowAlreadyStartedError

        from retrieval.temporal.workflows.provider_preflight import ProviderPreflightWorkflow

        workflow_id = f"retrieval-preflight-{request.request_id}"
        try:
            await self._raw_client.start_workflow(
                ProviderPreflightWorkflow.run,
                request,
                id=workflow_id,
                task_queue=self._runtime.retrieval_task_queue,
                execution_timeout=timedelta(minutes=6),
            )
        except WorkflowAlreadyStartedError:
            pass
        return workflow_id

    async def get_preflight(self, workflow_id: str) -> Mapping[str, Any]:
        if self._raw_client is None:
            raise DemoUnavailableError("Temporal client is not connected")
        handle = self._raw_client.get_workflow_handle(workflow_id)
        description = await handle.describe()
        status = description.status.name.lower()
        payload: dict[str, Any] = {"workflow_id": workflow_id, "status": status}
        if status == "completed":
            result = await handle.result()
            payload["result"] = asdict(result)
        return payload

    def _require(self) -> RetrievalClient:
        if self._client is None:
            raise DemoUnavailableError("Temporal client is not connected")
        return self._client


class InMemoryTextSearch:
    """Stable OR-token search for a local rehearsal using the reference repository."""

    backend = "in_memory_text"

    def __init__(self, repository: InMemoryRetrievalRepository) -> None:
        self._repository = repository

    async def search(self, store_key: str, query: str, limit: int = 8) -> tuple[DemoSearchHit, ...]:
        if not 1 <= limit <= 50:
            raise ValueError("search limit must be between 1 and 50")
        record = await self._repository.inspect_store(store_key)
        if record.lifecycle_state not in {
            StoreLifecycleState.ACTIVE,
            StoreLifecycleState.SYNCING,
        }:
            return ()
        query_terms = set(_TOKEN.findall(query.lower()))
        ignored = {"a", "an", "and", "before", "for", "or", "should", "the", "to", "what"}
        query_terms.difference_update(ignored)
        ranked: list[DemoSearchHit] = []
        for document_key, document in record.documents.items():
            for chunk in document.chunks:
                tokens = set(_TOKEN.findall(chunk.text.lower()))
                matches = query_terms.intersection(tokens)
                if not matches:
                    continue
                ranked.append(
                    DemoSearchHit(
                        document_key=document_key,
                        chunk_ordinal=chunk.ordinal,
                        title=document.title,
                        text=chunk.text,
                        score=float(len(matches)),
                        source_uri=document.source_uri,
                    )
                )
        ranked.sort(key=lambda hit: (-hit.score, hit.document_key, hit.chunk_ordinal))
        return tuple(ranked[:limit])


class DemoService:
    """One process-local composition root shared by the API and headless rehearsal."""

    def __init__(
        self,
        *,
        config: DemoConfig,
        scenario: NorthstarScenario,
        state_store: DemoStateStore,
        repository: RetrievalRepository,
        search_adapter: SearchAdapter,
        command_gateway: DemoCommandGateway | None,
        store_creation: StoreCreationBoundary | None = None,
        migrations_ready: Callable[[], Awaitable[bool]] | None = None,
        sync_metadata: Mapping[str, str] | None = None,
        held_document_key: str | None = None,
        display_name: str | None = None,
    ) -> None:
        self._config = config
        self._scenario = scenario
        self._state_store = state_store
        self._repository = repository
        self._search = search_adapter
        self._commands = command_gateway
        self._store_creation = store_creation or RepositoryStoreCreationBoundary(
            repository,
            state_store,
        )
        self._migrations_ready = migrations_ready
        self._sync_metadata = dict(sync_metadata or {})
        self._held_document_key = held_document_key or scenario.held_document_key
        self._display_name = display_name or scenario.display_name
        self._controls = DemoControlsManager(state_store, repository)
        self._started = False

    async def start(self) -> None:
        self._config.require_enabled()
        if self._started:
            return
        try:
            await self._state_store.start()
            if self._commands is not None:
                await self._commands.start()
        except BaseException:
            await self._state_store.aclose()
            raise
        self._started = True

    async def aclose(self) -> None:
        seen: set[int] = set()
        for resource in (self._commands, self._search, self._state_store, self._repository):
            if resource is None or id(resource) in seen:
                continue
            seen.add(id(resource))
            close = getattr(resource, "aclose", None) or getattr(resource, "close", None)
            if close is None:
                continue
            result = close()
            if inspect.isawaitable(result):
                await result
        self._started = False

    async def create_run(self, *, idempotency_key: str) -> DemoRun:
        self._require_started()
        scope = "demo:runs:create"
        request_hash = _payload_hash(
            {
                "scenario_id": self._scenario.scenario_id,
                "display_name": self._display_name,
            }
        )
        key_hash = _idempotency_hash(idempotency_key)
        existing = await self._checked_receipt(scope, key_hash, request_hash)
        if existing is not None:
            return _run_from_payload(existing.response)

        run_uuid = uuid.uuid5(_RUN_NAMESPACE, f"{scope}:{key_hash}")
        run = DemoRun(
            run_id=str(run_uuid),
            store_key=f"{self._config.store_key_prefix}-{run_uuid.hex[:12]}",
            display_name=self._display_name,
            baseline_generation=self._scenario.baseline_generation,
        )
        controls = DemoControls(
            run_id=run.run_id,
            quota_once_pending=True,
            quota_retry_after_seconds=self._scenario.quota_retry_after_seconds,
            held_document_key=self._held_document_key,
            hold_before_commit=True,
            release_requested=False,
        )
        persisted = await self._store_creation.create(run, controls)
        await self._state_store.append_event(
            DemoEvent(
                event_id=None,
                event_key="run:created",
                run_id=run.run_id,
                store_key=run.store_key,
                event_type="run_created",
                expected_generation=run.baseline_generation,
                actual_generation=run.baseline_generation,
                details={"display_name": run.display_name},
            )
        )
        receipt = await self._state_store.put_idempotency_receipt(
            ApiIdempotencyReceipt(
                scope=scope,
                idempotency_key_hash=key_hash,
                request_hash=request_hash,
                status_code=201,
                response=_run_payload(persisted),
            )
        )
        return _run_from_payload(receipt.response)

    async def start_preflight(self, *, idempotency_key: str) -> Mapping[str, Any]:
        self._require_started()
        if self._commands is None:
            raise DemoUnavailableError("Temporal command gateway is not configured")
        readiness = await self.ready()
        if not readiness.ready:
            raise DemoUnavailableError("platform dependencies are not ready for Drive preflight")
        request_id = _idempotency_hash(idempotency_key)[:32]
        request = ProviderPreflightRequest(
            request_id=request_id,
            provider_task_queue=getattr(
                getattr(self._commands, "_runtime", None),
                "provider_task_queue",
                "retrieval-provider-v2",
            ),
        )
        workflow_id = await self._commands.start_preflight(request)
        return await self._state_store.put_preflight(
            request_id=request_id,
            workflow_id=workflow_id,
            status="running",
        )

    async def get_preflight(self, workflow_id: str) -> Mapping[str, Any]:
        self._require_started()
        if self._commands is None:
            raise DemoUnavailableError("Temporal command gateway is not configured")
        persisted = await self._state_store.get_preflight(workflow_id)
        try:
            current = await self._commands.get_preflight(workflow_id)
        except Exception:
            return persisted
        return await self._state_store.put_preflight(
            request_id=str(persisted["request_id"]),
            workflow_id=workflow_id,
            status=str(current["status"]),
            result=(current.get("result") if isinstance(current.get("result"), Mapping) else None),
        )

    async def get_proof(self, run_id: str) -> Mapping[str, Any]:
        self._require_started()
        run = await self._state_store.get_run(run_id)
        proof = dict(await self._state_store.generation_proof(run.store_key))
        proof["baseline_generation"] = run.baseline_generation
        proof["late_writer_generation"] = run.baseline_generation
        proof["fence_generation"] = run.baseline_generation + 1
        quoted_store_key = run.store_key.replace("'", "''")
        proof["proof_query"] = (
            f"SELECT * FROM retrieval_demo_ui.generation_proof('{quoted_store_key}');"
        )
        return proof

    async def get_run(self, run_id: str) -> DemoRun:
        self._require_started()
        return await self._state_store.get_run(run_id)

    async def get_snapshot(self, run_id: str) -> DemoSnapshot:
        self._require_started()
        run = await self._state_store.get_run(run_id)
        store = await self._repository.get_store(run.store_key)
        controls = await self._state_store.get_controls(run_id)
        await self._reconcile_lifecycle_events(run, store)
        run = await self._state_store.get_run(run_id)
        events = await self._state_store.list_events(run_id)
        controller: Mapping[str, Any] | None = {
            "controller_workflow_id": store_controller_workflow_id(run.store_key),
            "quota_workflow_ids": (
                user_quota_workflow_id(
                    self._sync_metadata.get("provider", "northstar-scripted"),
                    self._sync_metadata.get(
                        "credential_key",
                        _quota_credential_key(run.run_id),
                    ),
                    self._sync_metadata.get("quota_class", "demo"),
                ),
            ),
        }
        temporal_available = self._commands is not None
        warning: str | None = None
        if self._commands is not None:
            try:
                raw_controller = await self._commands.get_status(run.store_key)
                current_controller = (
                    asdict(raw_controller)
                    if hasattr(raw_controller, "__dataclass_fields__")
                    else raw_controller
                )
                controller = {**controller, **current_controller}
            except Exception as exc:
                temporal_available = False
                warning = f"Temporal status temporarily unavailable ({type(exc).__name__})"
        return DemoSnapshot(
            run=run,
            store=store,
            controls=controls,
            events=events,
            controller=controller,
            temporal_available=temporal_available,
            temporal_warning=warning,
            story_state=_story_state(run, store, events),
        )

    async def start_sync(self, run_id: str, *, idempotency_key: str) -> DemoOperation:
        return await self._start_temporal_operation(
            run_id,
            idempotency_key=idempotency_key,
            operation_type=DemoOperationType.SYNC,
        )

    async def start_deactivation(self, run_id: str, *, idempotency_key: str) -> DemoOperation:
        return await self._start_temporal_operation(
            run_id,
            idempotency_key=idempotency_key,
            operation_type=DemoOperationType.DEACTIVATION,
        )

    async def hold_late_document(self, run_id: str, *, idempotency_key: str) -> DemoControls:
        return await self._mutate_control(run_id, idempotency_key, release=False)

    async def release_late_document(self, run_id: str, *, idempotency_key: str) -> DemoControls:
        return await self._mutate_control(run_id, idempotency_key, release=True)

    async def get_operation(self, operation_id: str) -> DemoOperation:
        self._require_started()
        operation = await self._state_store.get_operation(operation_id)
        if operation.status in {
            DemoOperationStatus.COMPLETED,
            DemoOperationStatus.FAILED,
            DemoOperationStatus.CANCELED,
            DemoOperationStatus.REJECTED,
        }:
            return operation
        run = await self._state_store.get_run(operation.run_id)
        store = await self._repository.get_store(operation.store_key)
        await self._reconcile_lifecycle_events(run, store)

        terminal = await self._controller_operation_result(operation)
        if terminal is not None and terminal.status in {
            OperationStatus.COMPLETED,
            OperationStatus.FAILED,
            OperationStatus.CANCELED,
            OperationStatus.REJECTED,
        }:
            next_status = DemoOperationStatus(terminal.status.value)
            operation = await self._state_store.update_operation(
                operation_id,
                next_status,
                lifecycle_generation=terminal.lifecycle_generation,
                result={
                    "result_status": (
                        terminal.result_status.value if terminal.result_status is not None else None
                    ),
                    **terminal.details,
                },
                message=terminal.message,
            )
            if next_status is DemoOperationStatus.COMPLETED:
                await self._state_store.update_run_status(
                    run.run_id,
                    (
                        DemoRunStatus.READY
                        if operation.operation_type is DemoOperationType.SYNC
                        else DemoRunStatus.COMPLETED
                    ),
                    finished=operation.operation_type is DemoOperationType.DEACTIVATION,
                )
            elif next_status in {
                DemoOperationStatus.FAILED,
                DemoOperationStatus.REJECTED,
            }:
                await self._state_store.update_run_status(run.run_id, DemoRunStatus.FAILED)
            return operation

        next_status = operation.status
        if operation.operation_type is DemoOperationType.DEACTIVATION:
            if store.lifecycle_state is StoreLifecycleState.INACTIVE:
                next_status = DemoOperationStatus.COMPLETED
                await self._state_store.update_run_status(
                    run.run_id,
                    DemoRunStatus.COMPLETED,
                    finished=True,
                )
            elif store.lifecycle_state is StoreLifecycleState.DEACTIVATION_FAILED:
                next_status = DemoOperationStatus.FAILED
                await self._state_store.update_run_status(run.run_id, DemoRunStatus.FAILED)
            elif store.lifecycle_state is StoreLifecycleState.DEACTIVATING:
                next_status = DemoOperationStatus.RUNNING
        elif operation.operation_type is DemoOperationType.SYNC and self._commands is not None:
            try:
                controller = await self._commands.get_status(operation.store_key)
            except Exception:
                return operation
            active_ids = set(controller.active_sync_ids)
            if operation.workflow_id in active_ids:
                next_status = DemoOperationStatus.RUNNING
            elif controller.lifecycle_state in {
                StoreLifecycleState.DEACTIVATING,
                StoreLifecycleState.INACTIVE,
            }:
                next_status = DemoOperationStatus.CANCELED

        if next_status is operation.status:
            return operation
        return await self._state_store.update_operation(
            operation_id,
            next_status,
            lifecycle_generation=store.lifecycle_generation,
        )

    async def _controller_operation_result(
        self,
        operation: DemoOperation,
    ) -> CommandResult | None:
        if self._commands is None or operation.workflow_id is None:
            return None
        try:
            return await self._commands.get_operation_result(
                operation.store_key,
                operation.workflow_id,
            )
        except Exception:
            # Lakebase operation state remains available while a Temporal
            # query is temporarily unavailable. Never infer success merely
            # from an operation disappearing from the active-ID set.
            return None

    async def list_events(
        self, run_id: str, *, after_event_id: int = 0, limit: int = 200
    ) -> tuple[DemoEvent, ...]:
        self._require_started()
        return await self._state_store.list_events(
            run_id, after_event_id=after_event_id, limit=limit
        )

    async def search(self, run_id: str, query: str, *, limit: int = 8) -> tuple[DemoSearchHit, ...]:
        self._require_started()
        run = await self._state_store.get_run(run_id)
        snapshot = await self._repository.get_store(run.store_key)
        if snapshot.lifecycle_state not in {
            StoreLifecycleState.ACTIVE,
            StoreLifecycleState.SYNCING,
        }:
            raise DemoConflictError("search is disabled after deactivation begins")
        raw_hits = await self._search.search(run.store_key, query, limit)
        return tuple(_normalize_hit(hit) for hit in raw_hits)

    async def ask(
        self,
        run_id: str,
        question: str,
        *,
        idempotency_key: str,
    ) -> EvidenceAnswer:
        self._require_started()
        normalized_question = " ".join(question.split())
        if not normalized_question or len(normalized_question) > 1_000:
            raise ValueError("question must contain between 1 and 1000 characters")
        scope = f"demo:runs:{run_id}:ask"
        request_hash = _payload_hash({"question": normalized_question})
        key_hash = _idempotency_hash(idempotency_key)
        existing = await self._checked_receipt(scope, key_hash, request_hash)
        if existing is not None:
            return _answer_from_payload(existing.response)

        run = await self._state_store.get_run(run_id)
        snapshot = await self._repository.get_store(run.store_key)
        if snapshot.lifecycle_state not in {
            StoreLifecycleState.ACTIVE,
            StoreLifecycleState.SYNCING,
        }:
            raise DemoConflictError("answers are disabled after deactivation begins")
        hits = await self.search(run_id, normalized_question, limit=12)
        answer = _evidence_answer(
            normalized_question,
            hits,
            backend=self._search.backend,
            generation=snapshot.lifecycle_generation,
        )
        receipt = await self._state_store.put_idempotency_receipt(
            ApiIdempotencyReceipt(
                scope=scope,
                idempotency_key_hash=key_hash,
                request_hash=request_hash,
                status_code=200,
                response=_answer_payload(answer),
            )
        )
        return _answer_from_payload(receipt.response)

    async def ready(self) -> DemoReadiness:
        database_ready = await self._state_store.ready()
        temporal_ready = await self._commands.ready() if self._commands is not None else False
        search_ready = True
        embeddings_ready = True
        search_probe = getattr(self._search, "readiness", None)
        if search_probe is not None:
            try:
                search_status = await search_probe()
                search_ready = bool(search_status.get("search"))
                embeddings_ready = bool(search_status.get("embeddings"))
            except Exception:
                search_ready = False
                embeddings_ready = False
        readiness_details = {
            "search_backend": self._search.backend,
            "provider": self._sync_metadata.get("provider", self._scenario.scenario_id),
        }
        try:
            migrations_ready = (
                await self._migrations_ready()
                if database_ready and self._migrations_ready is not None
                else database_ready
            )
        except Exception as exc:
            migrations_ready = False
            readiness_details["migration_error"] = type(exc).__name__
        ready = (
            self._started
            and database_ready
            and temporal_ready
            and migrations_ready
            and search_ready
            and embeddings_ready
        )
        return DemoReadiness(
            ready=ready,
            database_ready=database_ready,
            temporal_ready=temporal_ready,
            migrations_ready=migrations_ready,
            search_ready=search_ready,
            embeddings_ready=embeddings_ready,
            details=readiness_details,
        )

    async def _start_temporal_operation(
        self,
        run_id: str,
        *,
        idempotency_key: str,
        operation_type: DemoOperationType,
    ) -> DemoOperation:
        self._require_started()
        if self._commands is None:
            raise DemoUnavailableError("Temporal command gateway is not configured")
        run = await self._state_store.get_run(run_id)
        scope = f"demo:runs:{run_id}:{operation_type.value}"
        request_hash = _payload_hash({"run_id": run_id, "operation_type": operation_type.value})
        key_hash = _idempotency_hash(idempotency_key)
        existing = await self._checked_receipt(scope, key_hash, request_hash)
        if existing is not None:
            return _operation_from_payload(existing.response)
        deterministic = uuid.uuid5(_OPERATION_NAMESPACE, f"{scope}:{key_hash}")
        operation_id = f"demo-{operation_type.value}-{deterministic}"
        command_id = f"demo-command-{deterministic}"
        try:
            operation = await self._state_store.get_operation(operation_id)
        except DemoNotFoundError:
            store = await self._repository.get_store(run.store_key)
            allowed_states = {
                StoreLifecycleState.ACTIVE,
                StoreLifecycleState.SYNCING,
            }
            if operation_type is DemoOperationType.DEACTIVATION:
                allowed_states.add(StoreLifecycleState.DEACTIVATION_FAILED)
            if store.lifecycle_state not in allowed_states:
                raise DemoConflictError(
                    f"{operation_type.value} cannot start while the store is "
                    f"{store.lifecycle_state.value}"
                ) from None
            operation = await self._state_store.put_operation(
                DemoOperation(
                    operation_id=operation_id,
                    run_id=run_id,
                    store_key=run.store_key,
                    operation_type=operation_type,
                    status=DemoOperationStatus.ACCEPTED,
                    command_id=command_id,
                    lifecycle_generation=store.lifecycle_generation,
                )
            )
        if (
            operation.run_id != run_id
            or operation.store_key != run.store_key
            or operation.operation_type is not operation_type
            or operation.command_id != command_id
            or operation.lifecycle_generation is None
        ):
            raise DemoIdempotencyConflictError(
                "deterministic operation identity belongs to another request"
            )
        if operation.status is DemoOperationStatus.REJECTED:
            raise DemoConflictError(operation.message or "Temporal controller rejected command")

        # The operation is durable before the external command. A retry after
        # Temporal acceptance but before the API receipt commits can therefore
        # repair the receipt without being rejected by the new lifecycle state.
        # If acceptance happened before the operation update, re-submitting the
        # same deterministic controller command is safe and returns a duplicate.
        if operation.workflow_id is not None:
            return await self._put_operation_receipt(
                scope=scope,
                key_hash=key_hash,
                request_hash=request_hash,
                operation=operation,
            )

        expected_generation = operation.lifecycle_generation
        try:
            if operation_type is DemoOperationType.SYNC:
                accepted = await self._commands.request_sync(
                    SyncCommand(
                        command_id=command_id,
                        store_key=run.store_key,
                        expected_generation=expected_generation,
                        sync_sequence=f"northstar-{deterministic.hex[:12]}",
                        work_class=WorkClass.INTERACTIVE,
                        requested_at=datetime.now(UTC),
                        metadata={
                            "demo_run_id": run_id,
                            "provider": self._sync_metadata.get("provider", "northstar-scripted"),
                            "credential_key": self._sync_metadata.get(
                                "credential_key", _quota_credential_key(run_id)
                            ),
                            "quota_class": self._sync_metadata.get("quota_class", "demo"),
                            "resource_types": "files",
                            **self._sync_metadata,
                        },
                    )
                )
            else:
                accepted = await self._commands.start_deactivation(
                    StartDeactivationCommand(
                        command_id=command_id,
                        store_key=run.store_key,
                        expected_generation=expected_generation,
                        requested_at=datetime.now(UTC),
                    )
                )
            operation = await self._state_store.update_operation(
                operation_id,
                DemoOperationStatus.ACCEPTED,
                workflow_id=accepted.workflow_id,
                lifecycle_generation=accepted.lifecycle_generation,
                result={"duplicate": accepted.duplicate},
            )
            if operation.status in {
                DemoOperationStatus.ACCEPTED,
                DemoOperationStatus.RUNNING,
            }:
                await self._state_store.update_run_status(
                    run_id,
                    (
                        DemoRunStatus.SYNCING
                        if operation_type is DemoOperationType.SYNC
                        else DemoRunStatus.DEACTIVATING
                    ),
                )
        except Exception as exc:
            rejection_type = _controller_rejection_type(exc)
            if rejection_type is not None:
                rejected = await self._state_store.update_operation(
                    operation_id,
                    DemoOperationStatus.REJECTED,
                    message=rejection_type,
                    require_workflow_id_absent=True,
                )
                if rejected.workflow_id is not None:
                    return await self._put_operation_receipt(
                        scope=scope,
                        key_hash=key_hash,
                        request_hash=request_hash,
                        operation=rejected,
                    )
                raise DemoConflictError(
                    f"Temporal controller rejected the command ({rejection_type})"
                ) from exc
            failed = await self._state_store.update_operation(
                operation_id,
                DemoOperationStatus.FAILED,
                message=type(exc).__name__,
                require_workflow_id_absent=True,
            )
            if failed.workflow_id is not None:
                return await self._put_operation_receipt(
                    scope=scope,
                    key_hash=key_hash,
                    request_hash=request_hash,
                    operation=failed,
                )
            raise DemoUnavailableError("Temporal command submission failed") from exc
        return await self._put_operation_receipt(
            scope=scope,
            key_hash=key_hash,
            request_hash=request_hash,
            operation=operation,
        )

    async def _put_operation_receipt(
        self,
        *,
        scope: str,
        key_hash: str,
        request_hash: str,
        operation: DemoOperation,
    ) -> DemoOperation:
        receipt = await self._state_store.put_idempotency_receipt(
            ApiIdempotencyReceipt(
                scope=scope,
                idempotency_key_hash=key_hash,
                request_hash=request_hash,
                status_code=202,
                response=_operation_payload(operation),
                operation_id=operation.operation_id,
            )
        )
        return _operation_from_payload(receipt.response)

    async def _mutate_control(
        self, run_id: str, idempotency_key: str, *, release: bool
    ) -> DemoControls:
        self._require_started()
        run = await self._state_store.get_run(run_id)
        action = DemoOperationType.RELEASE if release else DemoOperationType.HOLD
        scope = f"demo:runs:{run_id}:controls:{action.value}"
        request_hash = _payload_hash({"action": action.value})
        key_hash = _idempotency_hash(idempotency_key)
        existing = await self._checked_receipt(scope, key_hash, request_hash)
        if existing is not None:
            return _controls_from_payload(existing.response)
        deterministic = uuid.uuid5(_OPERATION_NAMESPACE, f"{scope}:{key_hash}")
        operation = await self._state_store.put_operation(
            DemoOperation(
                operation_id=f"demo-{action.value}-{deterministic}",
                run_id=run_id,
                store_key=run.store_key,
                operation_type=action,
                status=DemoOperationStatus.RUNNING,
                command_id=f"demo-command-{deterministic}",
            )
        )
        controls = (
            await self._controls.release(run_id, operation_id=operation.operation_id)
            if release
            else await self._controls.hold(run_id)
        )
        await self._state_store.update_operation(
            operation.operation_id,
            DemoOperationStatus.COMPLETED,
            result={"control_version": controls.control_version},
        )
        receipt = await self._state_store.put_idempotency_receipt(
            ApiIdempotencyReceipt(
                scope=scope,
                idempotency_key_hash=key_hash,
                request_hash=request_hash,
                status_code=200,
                response=_controls_payload(controls),
                operation_id=operation.operation_id,
            )
        )
        return _controls_from_payload(receipt.response)

    async def _checked_receipt(
        self, scope: str, key_hash: str, request_hash: str
    ) -> ApiIdempotencyReceipt | None:
        receipt = await self._state_store.get_idempotency_receipt(scope, key_hash)
        if receipt is not None and receipt.request_hash != request_hash:
            raise DemoIdempotencyConflictError(
                "idempotency key was reused with a different request"
            )
        return receipt

    async def _reconcile_lifecycle_events(self, run: DemoRun, store: Any) -> None:
        if store.lifecycle_generation >= run.baseline_generation + 1:
            await self._state_store.append_event(
                DemoEvent(
                    event_id=None,
                    event_key="lifecycle:fence:8",
                    run_id=run.run_id,
                    store_key=run.store_key,
                    event_type="deactivation_fenced",
                    expected_generation=run.baseline_generation,
                    actual_generation=store.lifecycle_generation,
                    details={"state": store.lifecycle_state.value},
                )
            )
        if store.lifecycle_state is StoreLifecycleState.INACTIVE:
            await self._state_store.update_run_status(
                run.run_id,
                DemoRunStatus.COMPLETED,
                finished=True,
            )
            if store.document_count == 0 and store.chunk_count == 0:
                await self._state_store.append_event(
                    DemoEvent(
                        event_id=None,
                        event_key="cleanup:authoritative-zero:8",
                        run_id=run.run_id,
                        store_key=run.store_key,
                        event_type="cleanup_batch_completed",
                        expected_generation=run.baseline_generation + 1,
                        actual_generation=store.lifecycle_generation,
                        details={
                            "deleted_documents": 0,
                            "deleted_chunks": 0,
                            "remaining": False,
                            "inferred_from_terminal_snapshot": True,
                        },
                    )
                )
            await self._state_store.append_event(
                DemoEvent(
                    event_id=None,
                    event_key="lifecycle:inactive:8",
                    run_id=run.run_id,
                    store_key=run.store_key,
                    event_type="store_inactive",
                    expected_generation=run.baseline_generation + 1,
                    actual_generation=store.lifecycle_generation,
                    details={
                        "document_count": store.document_count,
                        "chunk_count": store.chunk_count,
                    },
                )
            )
        elif store.lifecycle_state is StoreLifecycleState.DEACTIVATION_FAILED:
            await self._state_store.update_run_status(run.run_id, DemoRunStatus.FAILED)

    def _require_started(self) -> None:
        self._config.require_enabled()
        if not self._started:
            raise DemoUnavailableError("demo service has not been started")


def _idempotency_hash(value: str) -> str:
    normalized = value.strip()
    if not normalized or len(normalized) > 500:
        raise ValueError("idempotency key must contain between 1 and 500 characters")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _story_state(run: DemoRun, store: Any, events: tuple[DemoEvent, ...]) -> str:
    event_types = {event.event_type for event in events}
    if store.lifecycle_state is StoreLifecycleState.DEACTIVATION_FAILED:
        return "failed"
    if (
        store.lifecycle_state is StoreLifecycleState.INACTIVE
        and "stale_generation_rejected" in event_types
    ):
        return "complete"
    if "stale_generation_rejected" in event_types:
        return "late_write_rejected"
    if store.lifecycle_generation >= run.baseline_generation + 1:
        return "fenced"
    if store.lifecycle_state is StoreLifecycleState.DEACTIVATING:
        return "deactivating"
    if "document_commit_held" in event_types:
        return "retrievable" if store.document_count > 0 else "held"
    if (
        store.lifecycle_state is StoreLifecycleState.SYNCING
        or {
            "quota_wait_started",
            "quota_wait_completed",
        }
        & event_types
    ):
        return "syncing"
    return "ready"


def _quota_credential_key(run_id: str) -> str:
    return f"northstar-demo-run:{run_id}"


def _payload_hash(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _controller_rejection_type(exc: BaseException) -> str | None:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ApplicationError) and current.type in _CONTROLLER_CONFLICT_TYPES:
            return current.type
        current = current.__cause__ or current.__context__
    return None


def _run_payload(run: DemoRun) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "store_key": run.store_key,
        "display_name": run.display_name,
        "baseline_generation": run.baseline_generation,
        "status": run.status.value,
        "created_at": run.created_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at is not None else None,
    }


def _run_from_payload(payload: Mapping[str, Any]) -> DemoRun:
    finished_at = payload.get("finished_at")
    return DemoRun(
        run_id=str(payload["run_id"]),
        store_key=str(payload["store_key"]),
        display_name=str(payload["display_name"]),
        baseline_generation=int(payload["baseline_generation"]),
        status=DemoRunStatus(str(payload["status"])),
        created_at=datetime.fromisoformat(str(payload["created_at"])),
        finished_at=(datetime.fromisoformat(str(finished_at)) if finished_at is not None else None),
    )


def _controls_payload(controls: DemoControls) -> dict[str, Any]:
    return {
        "run_id": controls.run_id,
        "quota_once_pending": controls.quota_once_pending,
        "quota_retry_after_seconds": controls.quota_retry_after_seconds,
        "held_document_key": controls.held_document_key,
        "hold_before_commit": controls.hold_before_commit,
        "release_requested": controls.release_requested,
        "control_version": controls.control_version,
        "quota_wait_request_id": controls.quota_wait_request_id,
        "quota_wait_operation": controls.quota_wait_operation,
        "updated_at": controls.updated_at.isoformat(),
    }


def _controls_from_payload(payload: Mapping[str, Any]) -> DemoControls:
    return DemoControls(
        run_id=str(payload["run_id"]),
        quota_once_pending=bool(payload["quota_once_pending"]),
        quota_retry_after_seconds=float(payload["quota_retry_after_seconds"]),
        held_document_key=str(payload["held_document_key"]),
        hold_before_commit=bool(payload["hold_before_commit"]),
        release_requested=bool(payload["release_requested"]),
        control_version=int(payload["control_version"]),
        quota_wait_request_id=(
            str(payload["quota_wait_request_id"])
            if payload.get("quota_wait_request_id") is not None
            else None
        ),
        quota_wait_operation=(
            str(payload["quota_wait_operation"])
            if payload.get("quota_wait_operation") is not None
            else None
        ),
        updated_at=datetime.fromisoformat(str(payload["updated_at"])),
    )


def _operation_payload(operation: DemoOperation) -> dict[str, Any]:
    return {
        "operation_id": operation.operation_id,
        "run_id": operation.run_id,
        "store_key": operation.store_key,
        "operation_type": operation.operation_type.value,
        "status": operation.status.value,
        "command_id": operation.command_id,
        "workflow_id": operation.workflow_id,
        "lifecycle_generation": operation.lifecycle_generation,
        "result": dict(operation.result),
        "message": operation.message,
        "created_at": operation.created_at.isoformat(),
        "updated_at": operation.updated_at.isoformat(),
    }


def _operation_from_payload(payload: Mapping[str, Any]) -> DemoOperation:
    workflow_id = payload.get("workflow_id")
    generation = payload.get("lifecycle_generation")
    message = payload.get("message")
    raw_result = payload.get("result", {})
    return DemoOperation(
        operation_id=str(payload["operation_id"]),
        run_id=str(payload["run_id"]),
        store_key=str(payload["store_key"]),
        operation_type=DemoOperationType(str(payload["operation_type"])),
        status=DemoOperationStatus(str(payload["status"])),
        command_id=str(payload["command_id"]),
        workflow_id=str(workflow_id) if workflow_id is not None else None,
        lifecycle_generation=int(generation) if generation is not None else None,
        result=dict(raw_result) if isinstance(raw_result, Mapping) else {},
        message=str(message) if message is not None else None,
        created_at=datetime.fromisoformat(str(payload["created_at"])),
        updated_at=datetime.fromisoformat(str(payload["updated_at"])),
    )


def _normalize_hit(hit: Any) -> DemoSearchHit:
    if isinstance(hit, DemoSearchHit):
        return hit
    return DemoSearchHit(
        document_key=hit.document_key,
        chunk_ordinal=hit.chunk_ordinal,
        title=hit.title,
        text=getattr(hit, "excerpt", getattr(hit, "text", "")),
        score=float(hit.score),
        source_uri=hit.source_uri,
        committed_generation=getattr(hit, "committed_generation", None),
        keyword_rank=getattr(hit, "keyword_rank", None),
        vector_rank=getattr(hit, "vector_rank", None),
    )


def _evidence_answer(
    question: str,
    hits: tuple[DemoSearchHit, ...],
    *,
    backend: str,
    generation: int,
) -> EvidenceAnswer:
    by_document: dict[str, DemoSearchHit] = {}
    for hit in hits:
        by_document.setdefault(hit.document_key, hit)
    ordered = list(by_document.values())[:4]
    if ordered:
        evidence = []
        for hit in ordered:
            excerpt = re.sub(r"\[\[/?HIT\]\]", "", hit.text)
            excerpt = " ".join(excerpt.split())
            if len(excerpt) > 240:
                excerpt = excerpt[:237].rstrip() + "..."
            evidence.append(f"{hit.title}: {excerpt}")
        answer = "Strongest committed evidence: " + " ".join(evidence)
    else:
        answer = "No committed evidence matched the question."
    citations = tuple(
        EvidenceCitation(
            citation_id=f"{hit.document_key}#chunk-{hit.chunk_ordinal}",
            document_key=hit.document_key,
            chunk_ordinal=hit.chunk_ordinal,
            title=hit.title,
            source_uri=hit.source_uri,
        )
        for hit in ordered
    )
    return EvidenceAnswer(
        question=question,
        answer=answer,
        citations=citations,
        hits=tuple(ordered),
        backend=backend,
        lifecycle_generation=generation,
    )


def _answer_payload(answer: EvidenceAnswer) -> dict[str, Any]:
    return {
        "question": answer.question,
        "answer": answer.answer,
        "citations": [asdict(item) for item in answer.citations],
        "hits": [asdict(item) for item in answer.hits],
        "backend": answer.backend,
        "lifecycle_generation": answer.lifecycle_generation,
    }


def _answer_from_payload(payload: Mapping[str, Any]) -> EvidenceAnswer:
    return EvidenceAnswer(
        question=str(payload["question"]),
        answer=str(payload["answer"]),
        citations=tuple(EvidenceCitation(**item) for item in payload["citations"]),
        hits=tuple(DemoSearchHit(**item) for item in payload["hits"]),
        backend=str(payload["backend"]),
        lifecycle_generation=int(payload["lifecycle_generation"]),
    )


async def create_service_from_env() -> DemoService:
    """Construct an unstarted production service without network access.

    The Databricks App lifespan owns ``start``/``aclose``. Construction validates environment
    shape and allocates closed clients/pools only; it does not contact Lakebase or Temporal.
    """

    config = DemoConfig.from_env()
    config.require_enabled()
    scenario = load_northstar_scenario()
    if scenario.scenario_id != config.scenario_id:
        raise ValueError(f"unsupported configured scenario {config.scenario_id!r}")

    from retrieval.config import RetrievalTemporalConfig
    from retrieval.lakebase.config import LakebaseConfig
    from retrieval.lakebase.connection import LakebaseConnectionProvider
    from retrieval.lakebase.migrations import MigrationRunner
    from retrieval.lakebase.repository import LakebaseRetrievalRepository
    from retrieval.lakebase.search import create_search
    from retrieval.temporal.runtime_config import TemporalRuntimeConfig

    from .migrations import DemoMigrationRunner

    provider = LakebaseConnectionProvider(
        LakebaseConfig.from_env(default_pool_max_size=10),
    )
    try:
        state_store = PostgresDemoStateStore(provider, owns_provider=True)
        core_migrations = MigrationRunner(provider)
        demo_migrations = DemoMigrationRunner(provider)
        search_adapter = await create_search(provider)
        runtime = TemporalRuntimeConfig.from_env()
        retrieval_config = RetrievalTemporalConfig.from_env()
    except BaseException:
        await provider.aclose()
        raise

    async def migrations_ready() -> bool:
        core, demo = await core_migrations.status(), await demo_migrations.status()
        return core.ready and demo.ready

    return DemoService(
        config=config,
        scenario=scenario,
        state_store=state_store,
        repository=LakebaseRetrievalRepository(provider),
        search_adapter=search_adapter,
        command_gateway=LazyTemporalCommandGateway(
            runtime,
            retrieval_config,
        ),
        store_creation=PostgresNorthstarStoreCreationBoundary(state_store),
        migrations_ready=migrations_ready,
        sync_metadata=_production_sync_metadata_from_env(),
        held_document_key=os.environ.get("RETRIEVAL_DEMO_HELD_DOCUMENT_KEY") or None,
        display_name=os.environ.get("RETRIEVAL_DEMO_DISPLAY_NAME", "Drive retrieval demo").strip(),
    )


def _production_sync_metadata_from_env() -> dict[str, str]:
    provider = os.environ.get("RETRIEVAL_DEMO_PROVIDER", "google-drive").strip()
    credential_key = os.environ.get("RETRIEVAL_DEMO_CREDENTIAL_KEY", "drive-demo").strip()
    return {
        "provider": provider,
        "credential_key": credential_key,
        "quota_class": os.environ.get("RETRIEVAL_DEMO_QUOTA_CLASS", "drive-api-v3").strip(),
        "resource_types": "files",
        "refresh_search_index": "true",
    }


__all__ = [
    "DemoCommandGateway",
    "DemoService",
    "InMemoryTextSearch",
    "LazyTemporalCommandGateway",
    "PostgresNorthstarStoreCreationBoundary",
    "RepositoryStoreCreationBoundary",
    "SearchAdapter",
    "StoreCreationBoundary",
    "create_service_from_env",
]
