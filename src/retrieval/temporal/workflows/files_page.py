"""Bounded concurrent document ingestion for one provider page."""

from __future__ import annotations

import asyncio

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.common.ids import document_ingest_workflow_id
    from retrieval.temporal.models.documents import (
        DocumentIngestionInput,
        DocumentMutation,
        DocumentRef,
    )
    from retrieval.temporal.models.operations import ResultStatus
    from retrieval.temporal.models.sync import FilesPageInput, PageResult


@workflow.defn(name="FilesPageWorkflow")
class FilesPageWorkflow:
    def __init__(self) -> None:
        self._pending_children = 0
        self._completed = 0

    @workflow.query(name="get_status")
    def get_status(self) -> dict[str, int]:
        return {
            "pending_children": self._pending_children,
            "documents_completed": self._completed,
        }

    async def _ingest(
        self,
        command: FilesPageInput,
        document: DocumentRef,
        mutation: DocumentMutation,
        semaphore: asyncio.Semaphore,
    ) -> object:
        async with semaphore:
            self._pending_children += 1
            try:
                child_input = DocumentIngestionInput(
                    store_key=command.store_key,
                    lifecycle_generation=command.lifecycle_generation,
                    document=document,
                    idempotency_key=document_ingest_workflow_id(
                        command.store_key,
                        command.lifecycle_generation,
                        document.document_key,
                        document.source_version,
                    ),
                    sync_sequence=command.sync_sequence,
                    user_key=command.user_key,
                    resource_key=command.resource_key,
                    quota_scope=command.quota_scope,
                    work_class=command.work_class,
                    mutation=mutation,
                )
                result = await workflow.execute_child_workflow(
                    "DocumentIngestionWorkflow",
                    child_input,
                    id=child_input.idempotency_key,
                    cancellation_type=workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
                )
                self._completed += 1
                return result
            finally:
                self._pending_children -= 1

    @workflow.run
    async def run(self, command: FilesPageInput) -> PageResult:
        concurrency = max(1, command.document_ingestion_concurrency)
        semaphore = asyncio.Semaphore(concurrency)
        tasks: list[asyncio.Task[object]] = []
        for document in command.documents:
            tasks.append(
                asyncio.create_task(
                    self._ingest(command, document, DocumentMutation.UPSERT, semaphore)
                )
            )
        for document_key in command.deleted_document_keys:
            deletion_ref = DocumentRef(
                document_key=document_key,
                source_version=f"deleted:{command.page_key}",
                staging_uri="",
                content_hash="",
            )
            tasks.append(
                asyncio.create_task(
                    self._ingest(command, deletion_ref, DocumentMutation.DELETE, semaphore)
                )
            )

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

        errors: list[str] = []
        for result in results:
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                errors.append(type(result).__name__)
            elif getattr(result, "status", ResultStatus.SUCCEEDED) not in {
                ResultStatus.SUCCEEDED,
                ResultStatus.STALE_GENERATION,
            }:
                errors.append(getattr(result, "message", None) or "document rejected")

        return PageResult(
            page_key=command.page_key,
            status=ResultStatus.PARTIAL if errors else ResultStatus.SUCCEEDED,
            changed_documents=len(command.documents),
            deleted_documents=len(command.deleted_document_keys),
            errors=tuple(errors),
        )
