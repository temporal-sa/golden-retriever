"""One-command, no-cloud rehearsal of the Northstar fence and cleanup story."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass

from retrieval.temporal.activities.ingestion import IngestionActivities
from retrieval.temporal.activities.provider_api import (
    FetchResourcePageRequest,
    ListActiveUsersRequest,
    ProviderQuotaExhausted,
)
from retrieval.temporal.activities.repositories import InMemoryRetrievalRepository
from retrieval.temporal.models.documents import DocumentIngestionInput
from retrieval.temporal.models.lifecycle import StoreLifecycleState
from retrieval.temporal.models.operations import ResultStatus

from .config import DemoConfig
from .events import DemoIngestionEventSink
from .fixtures import FixtureStagingStore, load_northstar_scenario
from .ingestion_gate import DemoBeforeDocumentCommitHook
from .models import DemoEvent, DemoRunStatus
from .scripted_provider import ScriptedNorthstarProvider
from .service import DemoService, InMemoryTextSearch
from .store import InMemoryDemoStateStore


@dataclass(frozen=True)
class HeadlessStoryResult:
    run_id: str
    store_key: str
    quota_retry_after_seconds: float
    committed_before_fence: int
    stale_document_key: str
    citation_document_keys: tuple[str, ...]
    final_state: str
    final_generation: int
    final_document_count: int
    final_chunk_count: int
    event_types: tuple[str, ...]


async def run_headless_story() -> HeadlessStoryResult:
    """Run the deterministic data-plane story without Temporal, Lakebase, or network access."""

    scenario = load_northstar_scenario()
    config = DemoConfig(enabled=True, hold_timeout_seconds=30, control_poll_seconds=0.01)
    state_store = InMemoryDemoStateStore()
    repository = InMemoryRetrievalRepository()
    service = DemoService(
        config=config,
        scenario=scenario,
        state_store=state_store,
        repository=repository,
        search_adapter=InMemoryTextSearch(repository),
        command_gateway=None,
    )
    await service.start()
    try:
        run = await service.create_run(idempotency_key="northstar-headless-v1")
        provider = ScriptedNorthstarProvider(scenario, state_store)
        first_request = ListActiveUsersRequest(
            store_key=run.store_key,
            lifecycle_generation=scenario.baseline_generation,
            cursor=None,
            page_size=100,
            request_id="headless:list-users:0",
        )
        try:
            await provider.list_active_users(first_request)
        except ProviderQuotaExhausted as exc:
            quota_retry_after = float(exc.retry_after_seconds or 0)
        else:
            raise AssertionError("scripted provider did not inject its quota event")
        users = await provider.list_active_users(
            ListActiveUsersRequest(
                **{
                    **asdict(first_request),
                    "request_id": "headless:list-users:1",
                }
            )
        )
        if len(users.users) != 1:
            raise AssertionError("scripted provider did not resume after quota wait")
        manifest = await provider.fetch_resource_page(
            FetchResourcePageRequest(
                store_key=run.store_key,
                lifecycle_generation=scenario.baseline_generation,
                sync_sequence="headless-sync",
                user_key=scenario.user_key,
                resource_key=scenario.resource_key,
                cursor=None,
                page_size=100,
                request_id="headless:files:0",
            )
        )

        await repository.activate_user_if_current(
            run.store_key,
            scenario.baseline_generation,
            scenario.user_key,
        )
        await repository.mutate_retrieval_state_if_current(
            run.store_key,
            scenario.baseline_generation,
            "headless-sync",
            "started",
        )
        ingestion = IngestionActivities(
            repository,
            FixtureStagingStore(scenario),
            before_commit=DemoBeforeDocumentCommitHook(state_store, config=config),
            event_sink=DemoIngestionEventSink(state_store),
        )
        commands = {
            reference.document_key: DocumentIngestionInput(
                store_key=run.store_key,
                lifecycle_generation=scenario.baseline_generation,
                document=reference,
                idempotency_key=f"headless:{run.run_id}:{reference.document_key}",
                sync_sequence="headless-sync",
                user_key=scenario.user_key,
                resource_key=scenario.resource_key,
            )
            for reference in manifest.documents
        }
        late_task = asyncio.create_task(
            ingestion.ingest_staged_document(commands[scenario.held_document_key])
        )
        normal_results = await asyncio.gather(
            *(
                ingestion.ingest_staged_document(command)
                for key, command in commands.items()
                if key != scenario.held_document_key
            )
        )
        if any(result.status is not ResultStatus.SUCCEEDED for result in normal_results):
            raise AssertionError("a normal Northstar document failed to commit")
        await _wait_for_event(state_store, run.run_id, "document_commit_held")

        answer = await service.ask(
            run.run_id,
            "What should the account team prioritize before Northstar's renewal?",
            idempotency_key="headless-answer-v1",
        )
        fence = await repository.begin_deactivation(
            run.store_key,
            scenario.baseline_generation,
        )
        await state_store.append_event(
            DemoEvent(
                event_id=None,
                event_key="lifecycle:fence:8",
                run_id=run.run_id,
                store_key=run.store_key,
                event_type="deactivation_fenced",
                expected_generation=fence.previous_generation,
                actual_generation=fence.lifecycle_generation,
                details={"state": StoreLifecycleState.DEACTIVATING.value},
            )
        )
        await service.release_late_document(
            run.run_id,
            idempotency_key="headless-release-v1",
        )
        late_result = await late_task
        if late_result.status is not ResultStatus.STALE_GENERATION:
            raise AssertionError("released generation-7 writer was not rejected as stale")

        await repository.deactivate_users_if_current(
            run.store_key,
            fence.lifecycle_generation,
            (scenario.user_key,),
        )
        batch_index = 0
        while True:
            batch = await repository.remove_object_batch_if_current(
                run.store_key,
                fence.lifecycle_generation,
                2,
            )
            await state_store.append_event(
                DemoEvent(
                    event_id=None,
                    event_key=(
                        "cleanup:authoritative-zero:8"
                        if not batch.remaining
                        else f"headless:cleanup:{batch_index}"
                    ),
                    run_id=run.run_id,
                    store_key=run.store_key,
                    event_type="cleanup_batch_completed",
                    expected_generation=fence.lifecycle_generation,
                    actual_generation=fence.lifecycle_generation,
                    details={
                        "deleted_documents": batch.deleted_documents,
                        "deleted_chunks": batch.deleted_chunks,
                        "remaining": batch.remaining,
                    },
                )
            )
            batch_index += 1
            if not batch.remaining:
                break
        final = await repository.mark_inactive(run.store_key, fence.lifecycle_generation)
        await state_store.update_run_status(run.run_id, DemoRunStatus.COMPLETED, finished=True)
        await service.get_snapshot(run.run_id)
        events = await state_store.list_events(run.run_id)
        return HeadlessStoryResult(
            run_id=run.run_id,
            store_key=run.store_key,
            quota_retry_after_seconds=quota_retry_after,
            committed_before_fence=len(normal_results),
            stale_document_key=late_result.document_key,
            citation_document_keys=tuple(item.document_key for item in answer.citations),
            final_state=final.lifecycle_state.value,
            final_generation=final.lifecycle_generation,
            final_document_count=final.document_count,
            final_chunk_count=final.chunk_count,
            event_types=tuple(event.event_type for event in events),
        )
    finally:
        await service.aclose()


async def _wait_for_event(
    state_store: InMemoryDemoStateStore,
    run_id: str,
    event_type: str,
) -> None:
    for _ in range(200):
        if any(event.event_type == event_type for event in await state_store.list_events(run_id)):
            return
        await asyncio.sleep(0.005)
    raise TimeoutError(f"demo event {event_type!r} was not observed")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the no-cloud Northstar data-plane rehearsal")
    parser.add_argument("--json", action="store_true", help="emit one JSON object")
    args = parser.parse_args(argv)
    result = asyncio.run(run_headless_story())
    if args.json:
        print(json.dumps(asdict(result), sort_keys=True))
    else:
        print(
            "Northstar rehearsal passed: "
            f"{result.committed_before_fence} committed, "
            f"{result.stale_document_key} rejected stale, "
            f"final={result.final_state}/generation-{result.final_generation}/"
            f"documents-{result.final_document_count}/chunks-{result.final_chunk_count}"
        )


__all__ = ["HeadlessStoryResult", "main", "run_headless_story"]


if __name__ == "__main__":
    main()
