"""Joined document ingestion boundary carrying references instead of bodies."""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from retrieval.temporal.models.documents import (
        DocumentIngestionInput,
        DocumentIngestionResult,
    )
    from retrieval.temporal.workflows._policies import ingestion_activity_options


@workflow.defn(name="DocumentIngestionWorkflow")
class DocumentIngestionWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"

    @workflow.query(name="get_status")
    def get_status(self) -> str:
        return self._phase

    @workflow.run
    async def run(self, command: DocumentIngestionInput) -> DocumentIngestionResult:
        self._phase = "ingesting"
        result = await workflow.execute_activity(
            "ingest_staged_document",
            command,
            result_type=DocumentIngestionResult,
            **ingestion_activity_options(),
        )
        self._phase = result.status.value
        return result
