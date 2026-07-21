"""FastAPI command/read gateway for the Lakebase + Temporal demonstration.

Importing this module is intentionally side-effect free: configuration is parsed and external
connections are opened only from the application lifespan. Tests can inject a service directly
through :func:`create_app`.
"""

from __future__ import annotations

import inspect
import logging
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote, urlsplit
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from retrieval.environment import inject_environment

STATIC_DIRECTORY = Path(__file__).with_name("static")
MAX_IDEMPOTENCY_KEY_LENGTH = 200
LOGGER = logging.getLogger(__name__)


class AskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str = Field(min_length=3, max_length=500)


class EndWorkflowRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str = Field(min_length=1, max_length=512)


@runtime_checkable
class DemoApplicationService(Protocol):
    """Narrow application boundary implemented by ``retrieval.demo.service``.

    Idempotency belongs to the durable service rather than this process, so App restarts do not
    lose replay/conflict knowledge.
    """

    async def start(self) -> None: ...

    async def aclose(self) -> None: ...

    async def ready(self) -> object: ...

    async def create_run(self, *, idempotency_key: str) -> object: ...

    async def start_preflight(self, *, idempotency_key: str) -> object: ...

    async def get_preflight(self, workflow_id: str) -> object: ...

    async def get_proof(self, run_id: str) -> object: ...

    async def get_snapshot(self, run_id: str) -> object: ...

    async def start_sync(self, run_id: str, *, idempotency_key: str) -> object: ...

    async def end_workflow(
        self,
        run_id: str,
        workflow_id: str,
        *,
        idempotency_key: str,
    ) -> object: ...

    async def start_deactivation(self, run_id: str, *, idempotency_key: str) -> object: ...

    async def hold_late_document(self, run_id: str, *, idempotency_key: str) -> object: ...

    async def release_late_document(self, run_id: str, *, idempotency_key: str) -> object: ...

    async def ask(self, run_id: str, question: str, *, idempotency_key: str) -> object: ...

    async def get_operation(self, operation_id: str) -> object: ...

    async def list_events(
        self,
        run_id: str,
        *,
        after_event_id: int = 0,
        limit: int = 200,
    ) -> object: ...

    async def search(self, run_id: str, query: str, *, limit: int = 8) -> object: ...


class LazyRuntimeService:
    """Load the production service only when the FastAPI lifespan starts."""

    def __init__(self) -> None:
        self._delegate: DemoApplicationService | None = None

    def _service(self) -> DemoApplicationService:
        if self._delegate is None:
            raise RuntimeError("demo service has not started")
        return self._delegate

    async def start(self) -> None:
        # This import may parse process configuration and connect to Lakebase/Temporal. Keeping it
        # here guarantees that importing apps.retrieval_demo.app remains safe for tooling/tests.
        from retrieval.demo.service import create_service_from_env

        created = create_service_from_env()
        service = await created if inspect.isawaitable(created) else created
        self._delegate = service
        await service.start()

    async def aclose(self) -> None:
        if self._delegate is not None:
            await self._delegate.aclose()
            self._delegate = None

    async def ready(self) -> object:
        return await self._service().ready()

    async def create_run(self, *, idempotency_key: str) -> object:
        return await self._service().create_run(idempotency_key=idempotency_key)

    async def start_preflight(self, *, idempotency_key: str) -> object:
        return await self._service().start_preflight(idempotency_key=idempotency_key)

    async def get_preflight(self, workflow_id: str) -> object:
        return await self._service().get_preflight(workflow_id)

    async def get_proof(self, run_id: str) -> object:
        return await self._service().get_proof(run_id)

    async def get_snapshot(self, run_id: str) -> object:
        return await self._service().get_snapshot(run_id)

    async def start_sync(self, run_id: str, *, idempotency_key: str) -> object:
        return await self._service().start_sync(run_id, idempotency_key=idempotency_key)

    async def end_workflow(
        self,
        run_id: str,
        workflow_id: str,
        *,
        idempotency_key: str,
    ) -> object:
        return await self._service().end_workflow(
            run_id,
            workflow_id,
            idempotency_key=idempotency_key,
        )

    async def start_deactivation(self, run_id: str, *, idempotency_key: str) -> object:
        return await self._service().start_deactivation(
            run_id,
            idempotency_key=idempotency_key,
        )

    async def hold_late_document(self, run_id: str, *, idempotency_key: str) -> object:
        return await self._service().hold_late_document(
            run_id,
            idempotency_key=idempotency_key,
        )

    async def release_late_document(self, run_id: str, *, idempotency_key: str) -> object:
        return await self._service().release_late_document(
            run_id,
            idempotency_key=idempotency_key,
        )

    async def ask(self, run_id: str, question: str, *, idempotency_key: str) -> object:
        return await self._service().ask(
            run_id,
            question,
            idempotency_key=idempotency_key,
        )

    async def get_operation(self, operation_id: str) -> object:
        return await self._service().get_operation(operation_id)

    async def list_events(
        self,
        run_id: str,
        *,
        after_event_id: int = 0,
        limit: int = 200,
    ) -> object:
        return await self._service().list_events(
            run_id,
            after_event_id=after_event_id,
            limit=limit,
        )

    async def search(self, run_id: str, query: str, *, limit: int = 8) -> object:
        return await self._service().search(run_id, query, limit=limit)


def _to_json(value: object) -> Any:
    """Encode service dataclasses without exposing implementation-specific response classes."""

    if is_dataclass(value) and not isinstance(value, type):
        value = {field.name: _to_json(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _to_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_to_json(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return jsonable_encoder(value)


def _error_response(exc: Exception) -> JSONResponse:
    """Translate known domain failures to stable, non-sensitive HTTP errors."""

    class_name = type(exc).__name__
    declared_status = getattr(exc, "status_code", None)
    declared_code = getattr(exc, "error_code", None)
    if isinstance(declared_status, int) and 400 <= declared_status <= 599:
        http_status = declared_status
    elif class_name in {
        "DemoNotFoundError",
        "DemoRunNotFoundError",
        "DemoDisabledError",
    }:
        http_status = status.HTTP_404_NOT_FOUND
    elif class_name in {
        "DemoConflictError",
        "IdempotencyConflictError",
        "LifecycleStateRejectedError",
    }:
        http_status = status.HTTP_409_CONFLICT
    elif class_name in {"DemoUnavailableError", "ServiceUnavailableError"}:
        http_status = status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        http_status = status.HTTP_500_INTERNAL_SERVER_ERROR

    if isinstance(declared_code, str) and declared_code:
        error_code = declared_code
    elif http_status == status.HTTP_404_NOT_FOUND:
        error_code = "not_found"
    elif http_status == status.HTTP_409_CONFLICT:
        error_code = "conflict"
    elif http_status == status.HTTP_503_SERVICE_UNAVAILABLE:
        error_code = "service_unavailable"
    else:
        error_code = "internal_error"
    return JSONResponse(
        status_code=http_status,
        content={"error": {"code": error_code, "message": _safe_error_message(http_status)}},
    )


def _safe_error_message(http_status: int) -> str:
    return {
        status.HTTP_404_NOT_FOUND: "The requested demo resource was not found.",
        status.HTTP_409_CONFLICT: "The request conflicts with the current demo state.",
        status.HTTP_503_SERVICE_UNAVAILABLE: "A required service is temporarily unavailable.",
    }.get(http_status, "The request could not be completed.")


def _required_idempotency_key(request: Request) -> str:
    raw_key = request.headers.get("Idempotency-Key")
    if raw_key is None or not raw_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "missing_idempotency_key",
                "message": "Idempotency-Key is required for POST requests.",
            },
        )
    key = raw_key.strip()
    if len(key) > MAX_IDEMPOTENCY_KEY_LENGTH or any(ord(character) < 32 for character in key):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "invalid_idempotency_key",
                "message": "Idempotency-Key must be 1-200 printable characters.",
            },
        )
    return key


def _readiness_is_healthy(payload: object) -> bool:
    encoded = _to_json(payload)
    if not isinstance(encoded, Mapping):
        return False
    explicit = encoded.get("ready")
    if isinstance(explicit, bool):
        return explicit
    return str(encoded.get("status", "")).lower() in {"ok", "ready", "healthy"}


def _operation_id(operation_id: str) -> str:
    candidate = operation_id.strip()
    if (
        not candidate
        or len(candidate) > 512
        or any(ord(character) < 32 for character in candidate)
        or ".." in candidate.split("/")
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "invalid_operation_id", "message": "Invalid operation identifier."},
        )
    return candidate


def _temporal_workflow_url(workflow_id: str) -> str | None:
    base_url = os.environ.get("TEMPORAL_WEB_BASE_URL", "").strip()
    if not base_url:
        return None
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default").strip() or "default"
    encoded_workflow_id = quote(workflow_id, safe="")
    encoded_namespace = quote(namespace, safe="")
    if "{workflow_id}" in base_url or "{namespace}" in base_url:
        candidate = base_url.replace("{workflow_id}", encoded_workflow_id).replace(
            "{namespace}",
            encoded_namespace,
        )
    else:
        split_base = urlsplit(base_url)
        path = split_base.path.rstrip("/")
        if path.endswith("/workflows"):
            candidate = f"{base_url.rstrip('/')}/{encoded_workflow_id}"
        elif "/namespaces/" in path:
            candidate = f"{base_url.rstrip('/')}/workflows/{encoded_workflow_id}"
        else:
            candidate = (
                f"{base_url.rstrip('/')}/namespaces/{encoded_namespace}/"
                f"workflows/{encoded_workflow_id}"
            )
    parsed = urlsplit(candidate)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return None
    return candidate


def _safe_tool_url(name: str) -> str | None:
    candidate = os.environ.get(name, "").strip()
    if not candidate:
        return None
    parsed = urlsplit(candidate)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return None
    return candidate


def _add_temporal_links(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    workflow_ids: set[str] = set()
    controller = payload.get("controller")
    if isinstance(controller, Mapping):
        for key in ("controller_workflow_id", "active_deactivation_id"):
            candidate = controller.get(key)
            if isinstance(candidate, str) and candidate:
                workflow_ids.add(candidate)
        for key in ("active_sync_ids", "active_remediation_ids", "quota_workflow_ids"):
            candidates = controller.get(key)
            if isinstance(candidates, list):
                workflow_ids.update(item for item in candidates if isinstance(item, str) and item)
    events = payload.get("events")
    if isinstance(events, list):
        for event in events:
            if isinstance(event, Mapping):
                workflow_id = event.get("workflow_id")
                if isinstance(workflow_id, str) and workflow_id:
                    workflow_ids.add(workflow_id)
    links = {
        workflow_id: url
        for workflow_id in sorted(workflow_ids)
        if (url := _temporal_workflow_url(workflow_id)) is not None
    }
    if links:
        payload["workflow_links"] = links
    workflow_id = payload.get("workflow_id")
    if isinstance(workflow_id, str):
        url = _temporal_workflow_url(workflow_id)
        if url is not None:
            payload["temporal_url"] = url
    return payload


def create_app(service: DemoApplicationService | None = None) -> FastAPI:
    """Create the application with an injected service or a lazy runtime service."""

    application_service = service or LazyRuntimeService()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            await application_service.start()
            yield
        finally:
            await application_service.aclose()

    application = FastAPI(
        title="Lakebase + Temporal retrieval demo",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    application.state.demo_service = application_service

    @application.middleware("http")
    async def handle_domain_errors(request: Request, call_next: Any) -> Response:
        try:
            return await call_next(request)
        except Exception as exc:
            # Starlette's catch-all exception handler deliberately re-raises after writing the
            # response. Catch at middleware level so expected service-domain conflicts produce a
            # normal HTTP response in both production and TestClient.
            LOGGER.exception(
                "Unhandled demo request failure",
                extra={"request_method": request.method, "request_path": request.url.path},
            )
            return _error_response(exc)

    @application.get("/healthz", tags=["health"])
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/readyz", tags=["health"])
    async def readyz() -> Response:
        try:
            readiness = await application_service.ready()
        except Exception:  # the readiness endpoint reports, rather than hides, outages
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "ready": False,
                    "error": {
                        "code": "readiness_check_failed",
                        "message": "One or more readiness checks could not complete.",
                    },
                },
            )
        http_status = (
            status.HTTP_200_OK
            if _readiness_is_healthy(readiness)
            else status.HTTP_503_SERVICE_UNAVAILABLE
        )
        return JSONResponse(status_code=http_status, content=_to_json(readiness))

    @application.get("/api/demo/tooling", tags=["demo"])
    async def tooling() -> dict[str, str | None]:
        return {
            "google_drive": _safe_tool_url("GOOGLE_DRIVE_FOLDER_URL"),
            "temporal": _safe_tool_url("TEMPORAL_WEB_BASE_URL"),
            "lakebase": _safe_tool_url("LAKEBASE_TOOLING_URL"),
        }

    @application.post(
        "/api/preflight",
        status_code=status.HTTP_202_ACCEPTED,
        tags=["demo"],
    )
    @application.post(
        "/api/demo/preflight",
        status_code=status.HTTP_202_ACCEPTED,
        tags=["demo"],
    )
    async def start_preflight(
        idempotency_key: str = Depends(_required_idempotency_key),
    ) -> object:
        return _add_temporal_links(
            _to_json(await application_service.start_preflight(idempotency_key=idempotency_key))
        )

    @application.get("/api/preflight/{workflow_id}", tags=["demo"])
    @application.get("/api/demo/preflight/{workflow_id}", tags=["demo"])
    async def get_preflight(workflow_id: str) -> object:
        return _add_temporal_links(
            _to_json(await application_service.get_preflight(_operation_id(workflow_id)))
        )

    @application.get("/api/preflight/{workflow_id}/source-files", tags=["demo"])
    @application.get("/api/demo/preflight/{workflow_id}/source-files", tags=["demo"])
    async def get_source_files(workflow_id: str) -> object:
        payload = _to_json(await application_service.get_preflight(_operation_id(workflow_id)))
        result = payload.get("result", {}) if isinstance(payload, Mapping) else {}
        files = result.get("files", []) if isinstance(result, Mapping) else []
        return {"workflow_id": workflow_id, "files": files}

    @application.post("/api/demo/runs", status_code=status.HTTP_201_CREATED, tags=["demo"])
    async def create_run(
        idempotency_key: str = Depends(_required_idempotency_key),
    ) -> object:
        return _to_json(await application_service.create_run(idempotency_key=idempotency_key))

    @application.get("/api/demo/runs/{run_id}/snapshot", tags=["demo"])
    async def get_snapshot(run_id: UUID) -> object:
        # The service builds this from Lakebase first. A Temporal query failure is represented in
        # the returned controller-status fields and must not hide authoritative database state.
        return _add_temporal_links(_to_json(await application_service.get_snapshot(str(run_id))))

    @application.get("/api/demo/runs/{run_id}/events", tags=["demo"])
    async def list_events(
        run_id: UUID,
        after_event_id: int = Query(default=0, ge=0),
        limit: int = Query(default=200, ge=1, le=500),
    ) -> dict[str, object]:
        events = _to_json(
            await application_service.list_events(
                str(run_id),
                after_event_id=after_event_id,
                limit=limit,
            )
        )
        event_list = events if isinstance(events, list) else []
        next_after = after_event_id
        for event in event_list:
            if isinstance(event, Mapping):
                event_id = event.get("event_id")
                if isinstance(event_id, int):
                    next_after = max(next_after, event_id)
        return {"events": event_list, "next_after_event_id": next_after}

    @application.get("/api/demo/runs/{run_id}/proof", tags=["demo"])
    async def get_proof(run_id: UUID) -> object:
        return _to_json(await application_service.get_proof(str(run_id)))

    @application.get("/api/demo/runs/{run_id}/search", tags=["demo"])
    async def search(
        run_id: UUID,
        query: str = Query(min_length=2, max_length=500),
        limit: int = Query(default=8, ge=1, le=25),
    ) -> dict[str, object]:
        hits = _to_json(await application_service.search(str(run_id), query, limit=limit))
        return {"query": query, "hits": hits if isinstance(hits, list) else []}

    @application.post(
        "/api/demo/runs/{run_id}/sync",
        status_code=status.HTTP_202_ACCEPTED,
        tags=["demo"],
    )
    async def start_sync(
        run_id: UUID,
        idempotency_key: str = Depends(_required_idempotency_key),
    ) -> object:
        return _to_json(
            await application_service.start_sync(str(run_id), idempotency_key=idempotency_key)
        )

    @application.post(
        "/api/demo/runs/{run_id}/workflows/end",
        status_code=status.HTTP_202_ACCEPTED,
        tags=["demo"],
    )
    async def end_workflow(
        run_id: UUID,
        request: EndWorkflowRequest,
        idempotency_key: str = Depends(_required_idempotency_key),
    ) -> object:
        return _to_json(
            await application_service.end_workflow(
                str(run_id),
                _operation_id(request.workflow_id),
                idempotency_key=idempotency_key,
            )
        )

    @application.post(
        "/api/demo/runs/{run_id}/deactivate",
        status_code=status.HTTP_202_ACCEPTED,
        tags=["demo"],
    )
    async def start_deactivation(
        run_id: UUID,
        idempotency_key: str = Depends(_required_idempotency_key),
    ) -> object:
        return _to_json(
            await application_service.start_deactivation(
                str(run_id),
                idempotency_key=idempotency_key,
            )
        )

    @application.post("/api/demo/runs/{run_id}/controls/hold", tags=["demo"])
    async def hold_late_document(
        run_id: UUID,
        idempotency_key: str = Depends(_required_idempotency_key),
    ) -> object:
        return _to_json(
            await application_service.hold_late_document(
                str(run_id),
                idempotency_key=idempotency_key,
            )
        )

    @application.post("/api/demo/runs/{run_id}/controls/release", tags=["demo"])
    async def release_late_document(
        run_id: UUID,
        idempotency_key: str = Depends(_required_idempotency_key),
    ) -> object:
        return _to_json(
            await application_service.release_late_document(
                str(run_id),
                idempotency_key=idempotency_key,
            )
        )

    @application.post("/api/demo/runs/{run_id}/ask", tags=["demo"])
    async def ask(
        run_id: UUID,
        request: AskRequest,
        idempotency_key: str = Depends(_required_idempotency_key),
    ) -> object:
        return _to_json(
            await application_service.ask(
                str(run_id),
                request.question,
                idempotency_key=idempotency_key,
            )
        )

    @application.get("/api/operations/{operation_id:path}", tags=["operations"])
    async def get_operation(operation_id: str) -> object:
        return _add_temporal_links(
            _to_json(await application_service.get_operation(_operation_id(operation_id)))
        )

    # Mount last so /api and health routes always take precedence over the single-page app.
    application.mount("/", StaticFiles(directory=STATIC_DIRECTORY, html=True), name="static")
    return application


app = create_app()


def main() -> None:
    """Start Uvicorn using the Databricks Apps port contract."""

    import uvicorn

    inject_environment()
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
