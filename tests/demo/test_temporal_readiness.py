from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from temporalio.api.enums.v1 import WorkerStatus
from temporalio.api.worker.v1 import (
    WorkerHeartbeat,
    WorkerInfo,
    WorkerListInfo,
    WorkerPollerInfo,
)
from temporalio.api.workflowservice.v1 import (
    DescribeWorkerResponse,
    ListWorkersResponse,
)
from temporalio.exceptions import WorkflowAlreadyStartedError

from retrieval.demo.service import LazyTemporalCommandGateway
from retrieval.temporal.activities.provider_api import (
    ProviderPreflightFile,
    ProviderPreflightRequest,
    ProviderPreflightResult,
)


class _HealthyServiceClient:
    def __init__(self) -> None:
        self.health_checks = 0

    async def check_health(self) -> None:
        self.health_checks += 1


class _WorkerVisibilityService:
    def __init__(
        self,
        pages: list[ListWorkersResponse],
        details: dict[str, DescribeWorkerResponse],
    ) -> None:
        self._pages = iter(pages)
        self._details = details
        self.list_requests: list[Any] = []
        self.describe_requests: list[Any] = []

    async def list_workers(self, request: Any) -> ListWorkersResponse:
        self.list_requests.append(request)
        return next(self._pages)

    async def describe_worker(self, request: Any) -> DescribeWorkerResponse:
        self.describe_requests.append(request)
        return self._details[request.worker_instance_key]


def _worker_summary(
    worker_key: str,
    task_queue: str,
    *,
    status: int = WorkerStatus.WORKER_STATUS_RUNNING,
) -> WorkerListInfo:
    return WorkerListInfo(
        worker_instance_key=worker_key,
        task_queue=task_queue,
        status=status,
    )


def _worker_detail(
    worker_key: str,
    task_queue: str,
    *,
    workflow_pollers: int = 0,
    activity_pollers: int = 0,
    status: int = WorkerStatus.WORKER_STATUS_RUNNING,
) -> DescribeWorkerResponse:
    return DescribeWorkerResponse(
        worker_info=WorkerInfo(
            worker_heartbeat=WorkerHeartbeat(
                worker_instance_key=worker_key,
                task_queue=task_queue,
                status=status,
                workflow_poller_info=WorkerPollerInfo(current_pollers=workflow_pollers),
                activity_poller_info=WorkerPollerInfo(current_pollers=activity_pollers),
            )
        )
    )


def _gateway(
    pages: list[ListWorkersResponse],
    details: dict[str, DescribeWorkerResponse],
) -> tuple[LazyTemporalCommandGateway, _HealthyServiceClient, _WorkerVisibilityService]:
    runtime = SimpleNamespace(
        namespace="tmprl-dem-cld-golden-retriever.a2dd6",
        retrieval_task_queue="retrieval-v2",
        provider_task_queue="retrieval-provider-v2",
    )
    gateway = LazyTemporalCommandGateway(runtime, SimpleNamespace())
    service_client = _HealthyServiceClient()
    workflow_service = _WorkerVisibilityService(pages, details)
    gateway._raw_client = SimpleNamespace(
        service_client=service_client,
        workflow_service=workflow_service,
    )
    return gateway, service_client, workflow_service


async def test_temporal_readiness_uses_running_worker_heartbeat_pollers_across_pages() -> None:
    gateway, service_client, workflow_service = _gateway(
        [
            ListWorkersResponse(
                workers=[_worker_summary("retrieval-worker", "retrieval-v2")],
                next_page_token=b"provider-page",
            ),
            ListWorkersResponse(
                workers=[_worker_summary("provider-worker", "retrieval-provider-v2")]
            ),
        ],
        {
            "retrieval-worker": _worker_detail(
                "retrieval-worker",
                "retrieval-v2",
                workflow_pollers=1,
                activity_pollers=5,
            ),
            "provider-worker": _worker_detail(
                "provider-worker",
                "retrieval-provider-v2",
                activity_pollers=5,
            ),
        },
    )

    assert await gateway.ready() is True
    assert service_client.health_checks == 1
    assert [request.next_page_token for request in workflow_service.list_requests] == [
        b"",
        b"provider-page",
    ]
    assert all(
        request.query == 'TaskQueue="retrieval-provider-v2" OR TaskQueue="retrieval-v2"'
        for request in workflow_service.list_requests
    )
    assert [request.worker_instance_key for request in workflow_service.describe_requests] == [
        "retrieval-worker",
        "provider-worker",
    ]


async def test_temporal_readiness_fails_when_a_required_poller_type_is_missing() -> None:
    gateway, _, _ = _gateway(
        [
            ListWorkersResponse(
                workers=[
                    _worker_summary("retrieval-worker", "retrieval-v2"),
                    _worker_summary("provider-worker", "retrieval-provider-v2"),
                ]
            )
        ],
        {
            "retrieval-worker": _worker_detail(
                "retrieval-worker",
                "retrieval-v2",
                workflow_pollers=1,
                activity_pollers=0,
            ),
            "provider-worker": _worker_detail(
                "provider-worker",
                "retrieval-provider-v2",
                activity_pollers=5,
            ),
        },
    )

    assert await gateway.ready() is False


async def test_temporal_readiness_ignores_workers_that_are_not_running() -> None:
    gateway, _, workflow_service = _gateway(
        [
            ListWorkersResponse(
                workers=[
                    _worker_summary(
                        "retrieval-worker",
                        "retrieval-v2",
                        status=WorkerStatus.WORKER_STATUS_SHUTTING_DOWN,
                    ),
                    _worker_summary("provider-worker", "retrieval-provider-v2"),
                ]
            )
        ],
        {
            "provider-worker": _worker_detail(
                "provider-worker",
                "retrieval-provider-v2",
                activity_pollers=5,
            )
        },
    )

    assert await gateway.ready() is False
    assert [request.worker_instance_key for request in workflow_service.describe_requests] == [
        "provider-worker"
    ]


async def test_preflight_replays_when_temporal_reports_workflow_already_started() -> None:
    runtime = SimpleNamespace(
        namespace="tmprl-dem-cld-golden-retriever.a2dd6",
        retrieval_task_queue="retrieval-v2",
        provider_task_queue="retrieval-provider-v2",
    )
    gateway = LazyTemporalCommandGateway(runtime, SimpleNamespace())

    class _AlreadyStartedClient:
        async def start_workflow(self, *_: Any, **kwargs: Any) -> None:
            raise WorkflowAlreadyStartedError(
                kwargs["id"],
                "ProviderPreflightWorkflow",
            )

    gateway._raw_client = _AlreadyStartedClient()

    workflow_id = await gateway.start_preflight(
        ProviderPreflightRequest(request_id="stable-request")
    )

    assert workflow_id == "retrieval-preflight-stable-request"


async def test_completed_preflight_is_decoded_with_its_result_type() -> None:
    runtime = SimpleNamespace(
        namespace="tmprl-dem-cld-golden-retriever.a2dd6",
        retrieval_task_queue="retrieval-v2",
        provider_task_queue="retrieval-provider-v2",
    )
    gateway = LazyTemporalCommandGateway(runtime, SimpleNamespace())
    expected = ProviderPreflightResult(
        request_id="stable-request",
        provider="google-drive",
        root_folder_id="drive-folder",
        files=(
            ProviderPreflightFile(
                document_key="document-1",
                name="Roadmap",
                mime_type="application/pdf",
                modified_time="2026-07-20T00:00:00Z",
                source_uri="https://drive.google.com/file/d/document-1/view",
                searchable=True,
            ),
        ),
        folders_scanned=1,
    )

    class _CompletedHandle:
        async def describe(self) -> Any:
            return SimpleNamespace(status=SimpleNamespace(name="COMPLETED"))

        async def result(self) -> ProviderPreflightResult:
            return expected

    class _CompletedClient:
        def __init__(self) -> None:
            self.result_type: type[Any] | None = None

        def get_workflow_handle(
            self,
            workflow_id: str,
            *,
            result_type: type[Any] | None = None,
        ) -> _CompletedHandle:
            assert workflow_id == "retrieval-preflight-stable-request"
            self.result_type = result_type
            return _CompletedHandle()

    client = _CompletedClient()
    gateway._raw_client = client

    preflight = await gateway.get_preflight("retrieval-preflight-stable-request")

    assert client.result_type is ProviderPreflightResult
    assert preflight["status"] == "completed"
    assert preflight["result"]["provider"] == "google-drive"
    assert preflight["result"]["files"][0]["name"] == "Roadmap"
