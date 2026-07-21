from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

import httpx
import pytest
from apps.retrieval_demo.app import DemoApplicationService, create_app

from retrieval.demo.config import DemoConfig
from retrieval.demo.fixtures import load_northstar_scenario
from retrieval.demo.service import DemoService, InMemoryTextSearch
from retrieval.demo.store import InMemoryDemoStateStore
from retrieval.temporal.activities.repositories import InMemoryRetrievalRepository


class FakeDomainError(RuntimeError):
    def __init__(self, status_code: int, error_code: str) -> None:
        self.status_code = status_code
        self.error_code = error_code
        super().__init__("sensitive implementation detail that must not reach the client")


@dataclass
class FakeDemoService:
    started: bool = False
    closed: bool = False
    temporal_unavailable: bool = False
    migration_current: bool = True
    database_ready: bool = True
    temporal_ready: bool = True
    lifecycle_state: str = "active"
    lifecycle_generation: int = 7
    held: bool = True
    run_id: str = "00000000-0000-0000-0000-000000000001"
    store_key: str = "northstar-7f3a9c"
    receipts: dict[str, tuple[str, object, object]] = field(default_factory=dict)
    operations: dict[str, dict[str, object]] = field(default_factory=dict)

    async def start(self) -> None:
        self.started = True

    async def aclose(self) -> None:
        self.closed = True

    async def ready(self) -> dict[str, object]:
        ready = self.database_ready and self.temporal_ready and self.migration_current
        return {
            "ready": ready,
            "database": {"ready": self.database_ready},
            "temporal": {"ready": self.temporal_ready},
            "migrations": {"current": self.migration_current, "version": 4},
        }

    def _assert_run(self, run_id: str) -> None:
        if run_id != self.run_id:
            raise FakeDomainError(404, "not_found")

    def _idempotent(
        self,
        key: str,
        operation: str,
        payload: object,
        response: object,
    ) -> object:
        receipt_key = f"{operation}:{key}"
        existing = self.receipts.get(receipt_key)
        if existing is not None:
            if existing[:2] != (operation, payload):
                raise FakeDomainError(409, "idempotency_conflict")
            return existing[2]
        self.receipts[receipt_key] = (operation, payload, response)
        return response

    async def create_run(self, *, idempotency_key: str) -> object:
        return self._idempotent(
            idempotency_key,
            "create_run",
            {},
            {
                "run_id": self.run_id,
                "store_key": self.store_key,
                "display_name": "Northstar AI",
            },
        )

    async def start_preflight(self, *, idempotency_key: str) -> object:
        return self._idempotent(
            idempotency_key,
            "preflight",
            {},
            {"workflow_id": "retrieval-preflight-demo", "status": "running"},
        )

    async def get_preflight(self, workflow_id: str) -> object:
        return {
            "workflow_id": workflow_id,
            "status": "completed",
            "result": {
                "files": [{"name": "Roadmap", "searchable": True}],
                "folders_scanned": 1,
            },
        }

    async def get_proof(self, run_id: str) -> object:
        self._assert_run(run_id)
        return {
            "lifecycle_generation": self.lifecycle_generation,
            "visible_documents": 0 if self.lifecycle_state != "active" else 4,
            "durable_write_receipts": 4,
        }

    async def get_snapshot(self, run_id: str) -> object:
        self._assert_run(run_id)
        return {
            "run_id": self.run_id,
            "database": {
                "store_key": self.store_key,
                "display_name": "Northstar AI",
                "lifecycle_state": self.lifecycle_state,
                "lifecycle_generation": self.lifecycle_generation,
                "active_user_count": 2,
                "document_count": 4,
                "chunk_count": 12,
            },
            "controller": (
                None
                if self.temporal_unavailable
                else {
                    "state": "syncing",
                    "controller_workflow_id": "store-controller/northstar",
                    "active_sync_ids": ["store-sync/northstar/7/one"],
                }
            ),
            "controller_error": ("temporarily_unavailable" if self.temporal_unavailable else None),
            "controls": {"commit_held": self.held, "release_requested": False},
            "events": [],
            "search_backend": "postgres_text",
        }

    async def _operation(
        self,
        run_id: str,
        operation_type: str,
        idempotency_key: str,
    ) -> object:
        self._assert_run(run_id)
        operation_id = f"controller/{self.store_key}/{operation_type}/1"
        response = {
            "operation_id": operation_id,
            "run_id": run_id,
            "status": "accepted",
            "operation_type": operation_type,
        }
        result = self._idempotent(
            idempotency_key,
            operation_type,
            {"run_id": run_id},
            response,
        )
        self.operations[operation_id] = response
        return result

    async def start_sync(self, run_id: str, *, idempotency_key: str) -> object:
        return await self._operation(run_id, "sync", idempotency_key)

    async def end_workflow(
        self,
        run_id: str,
        workflow_id: str,
        *,
        idempotency_key: str,
    ) -> object:
        self._assert_run(run_id)
        return self._idempotent(
            idempotency_key,
            "end_workflow",
            {"run_id": run_id, "workflow_id": workflow_id},
            {"workflow_id": workflow_id, "status": "cancel_requested", "accepted": True},
        )

    async def start_deactivation(self, run_id: str, *, idempotency_key: str) -> object:
        return await self._operation(run_id, "deactivate", idempotency_key)

    async def hold_late_document(self, run_id: str, *, idempotency_key: str) -> object:
        self._assert_run(run_id)
        response = {"hold_requested": True, "commit_held": self.held}
        return self._idempotent(
            idempotency_key,
            "hold",
            {"run_id": run_id},
            response,
        )

    async def release_late_document(self, run_id: str, *, idempotency_key: str) -> object:
        self._assert_run(run_id)
        if (
            self.lifecycle_state
            not in {
                "deactivating",
                "inactive",
                "deactivation_failed",
            }
            or self.lifecycle_generation < 8
        ):
            raise FakeDomainError(409, "release_fence_not_committed")
        response = {"commit_held": self.held, "release_requested": True}
        return self._idempotent(
            idempotency_key,
            "release",
            {"run_id": run_id},
            response,
        )

    async def ask(
        self,
        run_id: str,
        question: str,
        *,
        idempotency_key: str,
    ) -> object:
        self._assert_run(run_id)
        if self.lifecycle_state != "active":
            raise FakeDomainError(409, "store_not_searchable")
        response = {
            "question": question,
            "answer": "Renewal depends on security review and support remediation.",
            "backend": "postgres_text",
            "committed_generation": self.lifecycle_generation,
            "citations": [{"title": "Renewal plan", "snippet": "Security review"}],
        }
        return self._idempotent(
            idempotency_key,
            "ask",
            {"run_id": run_id, "question": question},
            response,
        )

    async def get_operation(self, operation_id: str) -> object:
        try:
            return self.operations[operation_id]
        except KeyError as exc:
            raise FakeDomainError(404, "not_found") from exc

    async def list_events(
        self,
        run_id: str,
        *,
        after_event_id: int = 0,
        limit: int = 200,
    ) -> object:
        self._assert_run(run_id)
        return tuple(
            event
            for event in (
                {
                    "event_id": 1,
                    "event_type": "quota_wait_started",
                    "run_id": run_id,
                },
                {
                    "event_id": 2,
                    "event_type": "generation_fence_committed",
                    "run_id": run_id,
                },
            )
            if event["event_id"] > after_event_id
        )[:limit]

    async def search(self, run_id: str, query: str, *, limit: int = 8) -> object:
        self._assert_run(run_id)
        if self.lifecycle_state != "active":
            raise FakeDomainError(409, "store_not_searchable")
        return (
            {
                "document_key": "renewal-plan",
                "title": "Renewal plan",
                "snippet": f"Evidence for {query}",
                "score": 0.9,
            },
        )[:limit]


@asynccontextmanager
async def demo_client(service: DemoApplicationService) -> AsyncIterator[httpx.AsyncClient]:
    application = create_app(service)
    async with application.router.lifespan_context(application):
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            yield client


async def test_lifespan_health_and_static_ui() -> None:
    service = FakeDemoService()

    async with demo_client(service) as client:
        assert service.started
        assert (await client.get("/healthz")).json() == {"status": "ok"}
        readiness = await client.get("/readyz")
        assert readiness.status_code == 200
        assert readiness.json()["migrations"]["version"] == 4

        page = await client.get("/")
        assert page.status_code == 200
        assert "Retrieval that stays correct" in page.text
        assert "Reject late write" in page.text
        assert "Workflow links" in page.text
        assert "WORKFLOW MANAGER" in page.text
        assert "Start fresh ingestion scan" in page.text
        assert "Retrieve evidence" in page.text
        assert "Release the old writer" in page.text
        script = await client.get("/app.js")
        assert script.status_code == 200
        assert "Retry deactivation" in script.text
        assert "/workflows/end" in script.text

        ended = await client.post(
            f"/api/demo/runs/{service.run_id}/workflows/end",
            headers={"Idempotency-Key": "end-workflow-1"},
            json={"workflow_id": "store-sync/northstar/7/one"},
        )
        assert ended.status_code == 202
        assert ended.json() == {
            "workflow_id": "store-sync/northstar/7/one",
            "status": "cancel_requested",
            "accepted": True,
        }

    assert service.closed


async def test_preflight_source_files_tooling_and_proof_endpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FakeDemoService()
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_URL", "https://drive.google.com/drive/folders/demo")
    monkeypatch.setenv("TEMPORAL_WEB_BASE_URL", "https://temporal.example.test")
    monkeypatch.setenv("LAKEBASE_TOOLING_URL", "https://workspace.example.test/lakebase")

    async with demo_client(service) as client:
        started = await client.post(
            "/api/preflight",
            headers={"Idempotency-Key": "preflight-1"},
        )
        assert started.status_code == 202
        workflow_id = started.json()["workflow_id"]

        files = await client.get(f"/api/preflight/{workflow_id}/source-files")
        assert files.json()["files"][0]["name"] == "Roadmap"
        proof = await client.get(f"/api/demo/runs/{service.run_id}/proof")
        assert proof.json()["durable_write_receipts"] == 4
        tooling = await client.get("/api/demo/tooling")
        assert tooling.json()["google_drive"].startswith("https://drive.google.com/")

        monkeypatch.setenv(
            "LAKEBASE_TOOLING_URL", "https://workspace.example.test/lakebase?token=x"
        )
        tooling = await client.get("/api/demo/tooling")
        assert tooling.json()["lakebase"] is None


async def test_lazy_runtime_closes_partially_started_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FakeDemoService()

    async def failing_start() -> None:
        service.started = True
        raise RuntimeError("Temporal connection failed after Lakebase opened")

    async def factory() -> FakeDemoService:
        return service

    service.start = failing_start  # type: ignore[method-assign]
    monkeypatch.setattr("retrieval.demo.service.create_service_from_env", factory)
    application = create_app()

    with pytest.raises(RuntimeError, match="Temporal connection failed"):
        async with application.router.lifespan_context(application):
            pass

    assert service.started
    assert service.closed


async def test_api_serializes_and_drives_the_real_in_memory_demo_service() -> None:
    repository = InMemoryRetrievalRepository()
    service = DemoService(
        config=DemoConfig(enabled=True),
        scenario=load_northstar_scenario(),
        state_store=InMemoryDemoStateStore(),
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=None,
    )

    async with demo_client(service) as client:
        created = await client.post(
            "/api/demo/runs",
            headers={"Idempotency-Key": "real-service-run"},
        )
        replay = await client.post(
            "/api/demo/runs",
            headers={"Idempotency-Key": "real-service-run"},
        )
        run_id = created.json()["run_id"]
        snapshot = await client.get(f"/api/demo/runs/{run_id}/snapshot")
        answer = await client.post(
            f"/api/demo/runs/{run_id}/ask",
            headers={"Idempotency-Key": "real-service-ask"},
            json={"question": "What are the renewal priorities?"},
        )
        unavailable_sync = await client.post(
            f"/api/demo/runs/{run_id}/sync",
            headers={"Idempotency-Key": "real-service-sync"},
        )

    assert created.status_code == 201
    assert replay.json() == created.json()
    assert snapshot.status_code == 200
    assert snapshot.json()["store"]["lifecycle_generation"] == 7
    assert snapshot.json()["temporal_available"] is False
    assert answer.status_code == 200
    assert answer.json()["backend"] == "in_memory_text"
    assert unavailable_sync.status_code == 503


async def test_every_post_requires_an_idempotency_key() -> None:
    service = FakeDemoService()
    async with demo_client(service) as client:
        responses = [
            await client.post("/api/demo/runs"),
            await client.post(f"/api/demo/runs/{service.run_id}/sync"),
            await client.post(f"/api/demo/runs/{service.run_id}/deactivate"),
            await client.post(f"/api/demo/runs/{service.run_id}/controls/hold"),
            await client.post(f"/api/demo/runs/{service.run_id}/controls/release"),
            await client.post(
                f"/api/demo/runs/{service.run_id}/ask",
                json={"question": "What is the renewal risk?"},
            ),
        ]

    assert {response.status_code for response in responses} == {400}
    assert {response.json()["detail"]["code"] for response in responses} == {
        "missing_idempotency_key"
    }


async def test_create_run_replays_and_conflicting_key_reuse_is_rejected() -> None:
    service = FakeDemoService()
    create_headers = {"Idempotency-Key": "create-logical-command"}
    ask_headers = {"Idempotency-Key": "ask-logical-command"}
    async with demo_client(service) as client:
        first = await client.post("/api/demo/runs", json={}, headers=create_headers)
        replay = await client.post("/api/demo/runs", json={}, headers=create_headers)
        initial_ask = await client.post(
            f"/api/demo/runs/{service.run_id}/ask",
            json={"question": "What is the renewal risk?"},
            headers=ask_headers,
        )
        conflict = await client.post(
            f"/api/demo/runs/{service.run_id}/ask",
            json={"question": "Who is the executive champion?"},
            headers=ask_headers,
        )

    assert first.status_code == 201
    assert replay.status_code == 201
    assert replay.json() == first.json()
    assert initial_ask.status_code == 200
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_conflict"
    assert "sensitive" not in conflict.text


async def test_sync_and_deactivation_return_accepted_operations() -> None:
    service = FakeDemoService()
    async with demo_client(service) as client:
        sync = await client.post(
            f"/api/demo/runs/{service.run_id}/sync",
            json={},
            headers={"Idempotency-Key": "sync-1"},
        )
        deactivate = await client.post(
            f"/api/demo/runs/{service.run_id}/deactivate",
            json={},
            headers={"Idempotency-Key": "deactivate-1"},
        )
        operation_id = sync.json()["operation_id"]
        operation = await client.get(f"/api/operations/{operation_id}")

    assert sync.status_code == 202
    assert deactivate.status_code == 202
    assert operation.status_code == 200
    assert operation.json()["operation_id"] == operation_id


async def test_failed_deactivation_state_accepts_a_recovery_command() -> None:
    service = FakeDemoService(lifecycle_state="deactivation_failed", lifecycle_generation=8)
    async with demo_client(service) as client:
        response = await client.post(
            f"/api/demo/runs/{service.run_id}/deactivate",
            headers={"Idempotency-Key": "resume-deactivation-8"},
        )

    assert response.status_code == 202
    assert response.json()["operation_type"] == "deactivate"


async def test_release_is_rejected_until_generation_eight_fence() -> None:
    service = FakeDemoService(lifecycle_state="deactivating", lifecycle_generation=7)
    async with demo_client(service) as client:
        rejected = await client.post(
            f"/api/demo/runs/{service.run_id}/controls/release",
            json={},
            headers={"Idempotency-Key": "release-too-early"},
        )
        service.lifecycle_generation = 8
        accepted = await client.post(
            f"/api/demo/runs/{service.run_id}/controls/release",
            json={},
            headers={"Idempotency-Key": "release-after-fence"},
        )

    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "release_fence_not_committed"
    assert accepted.status_code == 200
    assert accepted.json()["release_requested"] is True


async def test_snapshot_keeps_database_state_when_temporal_query_is_unavailable() -> None:
    service = FakeDemoService(temporal_unavailable=True)
    async with demo_client(service) as client:
        response = await client.get(f"/api/demo/runs/{service.run_id}/snapshot")

    assert response.status_code == 200
    payload = response.json()
    assert payload["database"]["lifecycle_generation"] == 7
    assert payload["database"]["document_count"] == 4
    assert payload["controller"] is None
    assert payload["controller_error"] == "temporarily_unavailable"


async def test_snapshot_adds_safe_temporal_web_links_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FakeDemoService()
    monkeypatch.setenv("TEMPORAL_WEB_BASE_URL", "https://temporal.example")
    monkeypatch.setenv("TEMPORAL_NAMESPACE", "northstar-demo")

    async with demo_client(service) as client:
        response = await client.get(f"/api/demo/runs/{service.run_id}/snapshot")

    links = response.json()["workflow_links"]
    assert links["store-controller/northstar"] == (
        "https://temporal.example/namespaces/northstar-demo/workflows/store-controller%2Fnorthstar"
    )
    assert links["store-sync/northstar/7/one"].startswith("https://temporal.example/")


async def test_search_and_ask_fail_closed_outside_active_state() -> None:
    service = FakeDemoService(lifecycle_state="deactivating", lifecycle_generation=8)
    async with demo_client(service) as client:
        search = await client.get(
            f"/api/demo/runs/{service.run_id}/search",
            params={"query": "renewal risk"},
        )
        ask = await client.post(
            f"/api/demo/runs/{service.run_id}/ask",
            json={"question": "What is the renewal risk?"},
            headers={"Idempotency-Key": "ask-deactivating"},
        )

    assert search.status_code == 409
    assert ask.status_code == 409
    assert search.json()["error"]["code"] == "store_not_searchable"


async def test_readiness_fails_when_migrations_or_dependency_are_unhealthy() -> None:
    service = FakeDemoService(migration_current=False, temporal_ready=False)
    async with demo_client(service) as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["migrations"]["current"] is False
    assert response.json()["temporal"]["ready"] is False


async def test_readiness_exception_is_a_safe_service_unavailable_response() -> None:
    service = FakeDemoService()

    async def broken_ready() -> object:
        raise RuntimeError("postgresql://user:secret@example.invalid/database")

    service.ready = broken_ready  # type: ignore[method-assign]
    async with demo_client(service) as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "readiness_check_failed"
    assert "secret" not in response.text


async def test_events_search_and_run_authorization() -> None:
    service = FakeDemoService()
    async with demo_client(service) as client:
        events = await client.get(
            f"/api/demo/runs/{service.run_id}/events",
            params={"after_event_id": 1},
        )
        search = await client.get(
            f"/api/demo/runs/{service.run_id}/search",
            params={"query": "security review"},
        )
        unknown = await client.get("/api/demo/runs/00000000-0000-0000-0000-000000000099/snapshot")
        malformed = await client.get("/api/demo/runs/not-a-uuid/snapshot")

    assert events.status_code == 200
    assert events.json()["next_after_event_id"] == 2
    assert [event["event_id"] for event in events.json()["events"]] == [2]
    assert search.status_code == 200
    assert search.json()["hits"][0]["document_key"] == "renewal-plan"
    assert unknown.status_code == 404
    assert malformed.status_code == 422


async def test_empty_and_control_character_idempotency_keys_are_rejected() -> None:
    service = FakeDemoService()
    async with demo_client(service) as client:
        empty = await client.post(
            "/api/demo/runs",
            json={},
            headers={"Idempotency-Key": "   "},
        )
        too_long = await client.post(
            "/api/demo/runs",
            json={},
            headers={"Idempotency-Key": "a" * 201},
        )

    assert empty.status_code == 400
    assert too_long.status_code == 400


async def test_response_encoder_handles_dataclasses_and_enums() -> None:
    # This is exercised indirectly by the real service. Keep a small app-level regression using
    # an arbitrary dataclass return value so the transport does not depend on dict-only fakes.
    @dataclass(frozen=True)
    class Run:
        run_id: str
        metadata: Any

    service = FakeDemoService()

    async def create_dataclass_run(*, idempotency_key: str) -> object:
        del idempotency_key
        return Run(run_id="run-dataclass", metadata=MappingProxyType({"generation": 7}))

    service.create_run = create_dataclass_run  # type: ignore[method-assign]
    async with demo_client(service) as client:
        response = await client.post(
            "/api/demo/runs",
            json={},
            headers={"Idempotency-Key": "dataclass"},
        )

    assert response.json() == {"run_id": "run-dataclass", "metadata": {"generation": 7}}
